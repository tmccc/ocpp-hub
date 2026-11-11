"""Test setup: load the HA-free core as the package ``ocpp_hub_core`` without importing Home Assistant.

Importing ``custom_components.ocpp_hub.core`` would first run ``custom_components/ocpp_hub/__init__.py``, which imports
Home Assistant. Loading the core directory by path under its own package name keeps its relative imports working and
proves it needs nothing from Home Assistant.
"""

from __future__ import annotations

import importlib.util
import os
import ssl
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.worker_ports import listener_port_factory

CORE_DIR = Path(__file__).resolve().parents[1] / "custom_components" / "ocpp_hub" / "core"


# Intent: register the core package in sys.modules under the name ``ocpp_hub_core``.
# Why: tests import ``ocpp_hub_core.frames`` the same way the integration imports ``.core.frames``.
def _load_core() -> None:
    if "ocpp_hub_core" in sys.modules:
        return
    spec = importlib.util.spec_from_file_location(
        "ocpp_hub_core", CORE_DIR / "__init__.py", submodule_search_locations=[str(CORE_DIR)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["ocpp_hub_core"] = module
    spec.loader.exec_module(module)


_load_core()


# Intent: parallel workers reserve planned listeners in disjoint ranges, separate from automatic client ports.
# Why: bind-and-release ephemeral fixtures can be reused by another worker before the real server is created.
@pytest.fixture
def unused_tcp_port_factory(unused_tcp_port_factory: Callable[[], int]) -> Callable[[], int]:
    worker = os.environ.get("MODEL_WORKER_INDEX")
    return unused_tcp_port_factory if worker is None else listener_port_factory(int(worker))


# Intent: a single planned listener shares its fixture's collision-free allocation policy.
# Why: both pytest-asyncio port fixtures must follow the same per-worker range during parallel qualification.
@pytest.fixture
def unused_tcp_port(unused_tcp_port_factory: Callable[[], int]) -> int:
    return unused_tcp_port_factory()


class Hub:
    """A running central system with a recording listener and the fake chargers connected to it."""

    # Keep the central system, its listener and a list of chargers to close at the end.
    def __init__(self, central: Any, listener: Any) -> None:
        self.central = central
        self.listener = listener
        self.chargers: list[Any] = []

    # Connect a new fake charger to the hub and register it for cleanup.
    async def charger(self, cp_id: str = "TEST0001", subprotocol: str = "ocpp1.6") -> Any:
        from tests.fake_charger import FakeCharger

        charger = FakeCharger(self.central.port, cp_id, subprotocol)
        await charger.connect()
        self.chargers.append(charger)
        return charger


# Intent: factory fixture that starts a central system on a free port with test-friendly timings.
# Why: most tests do not want the automatic post-boot setup, so it is delayed for an hour unless a test asks otherwise.
@pytest.fixture
async def start_hub() -> AsyncIterator[Callable[..., Awaitable[Hub]]]:
    from ocpp_hub_core.central import CentralSystem
    from ocpp_hub_core.session import SessionSettings

    from tests.fake_charger import RecordingListener

    hubs: list[Hub] = []

    # Intent: start one hub with the given setting overrides and any extra listeners, such as the vendor mirror.
    # Why: tests differ only in a few timings, so overrides keep each test short.
    async def _start(*extra_listeners: Any, **overrides: Any) -> Hub:
        listener = RecordingListener()
        defaults = {"setup_delay_s": 3600.0, "boot_wait_s": 3600.0, "call_timeout_s": 1.0}
        settings = SessionSettings(**{**defaults, **overrides})
        central = CentralSystem([listener, *extra_listeners], settings)
        await central.start("127.0.0.1", 0)
        hub = Hub(central, listener)
        hubs.append(hub)
        return hub

    yield _start
    for hub in hubs:
        for charger in hub.chargers:
            await charger.close()
        await hub.central.stop()


@dataclass
class Rig:
    """A hub with a mirror in front of a fake vendor backend."""

    hub: Any
    mirror: Any
    vendor: Any


# Intent: factory fixture that starts a mirror with test-friendly timings.
# Why: reconnect and timeout tests would otherwise wait for the production delays of seconds.
@pytest.fixture
async def start_mirror() -> AsyncIterator[Callable[..., Awaitable[Any]]]:
    from ocpp_hub_core.mirror import VendorMirror
    from ocpp_hub_core.mirror_model import MirrorSettings

    mirrors: list[VendorMirror] = []

    # Intent: start one mirror for a vendor URL with the given overrides.
    # Why: most tests differ in one or two settings only.
    async def _start(url: str, ssl_context: ssl.SSLContext | None = None, **overrides: Any) -> Any:
        defaults = {"backoff_min_s": 0.05, "backoff_max_s": 0.2, "reply_timeout_s": 2.0, "connect_timeout_s": 2.0}
        mirror = VendorMirror(MirrorSettings(url=url, **{**defaults, **overrides}), ssl_context)
        await mirror.start()
        mirrors.append(mirror)
        return mirror

    yield _start
    for mirror in mirrors:
        await mirror.stop()


# Intent: factory fixture that wires a hub, a mirror and a fake vendor together.
# Why: every mirror test needs the same three parts; only the vendor behaviour and the mirror settings vary.
@pytest.fixture
async def start_rig(start_hub: Any, start_mirror: Any) -> AsyncIterator[Callable[..., Awaitable[Rig]]]:
    from tests.fake_vendor import FakeVendor

    vendors: list[FakeVendor] = []

    # Intent: build one rig, optionally around a prepared vendor.
    # Why: tests that need a stalled or restarting vendor create it first and hand it in.
    async def _start(vendor: Any = None, hub: dict[str, Any] | None = None, **mirror_overrides: Any) -> Rig:
        fake = vendor or FakeVendor()
        await fake.start()
        vendors.append(fake)
        mirror = await start_mirror(fake.url, **mirror_overrides)
        return Rig(await start_hub(mirror, **(hub or {})), mirror, fake)

    yield _start
    for fake in vendors:
        await fake.stop()
