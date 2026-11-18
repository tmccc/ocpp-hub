"""Managed current rigs for selected limits, commissioned native32 and verified holds."""

from __future__ import annotations

import asyncio
import copy
import ssl
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.managed_current_prepare import prepare_current_capability

from tests.charge_rig import ChargeRig, ready
from tests.handover_helpers import NativeVehiclePeer, crash
from tests.managed_rig import OwnerBench, healthy, introduced, managed_bench, managed_session


# Activate only the explicit synthetic USER32 capability while retaining the exact original spec and native binding.
@asynccontextmanager
async def adjustable(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
) -> AsyncIterator[ChargeRig]:
    async with ready(directory, contexts, port, monkeypatch, native_peer=native_peer, bench_type=bench_type) as test:
        gateway, journal = test.session.gateway, test.session.journal
        original = copy.deepcopy(journal.get("native_bounded_handover"))
        binding = copy.deepcopy(journal.get("trial_binding"))
        journal.put("managed_current_capability", capability_value(journal, test.rig.spec))
        gateway.policy = replace(gateway.policy, max_current_a=32)
        gateway.controls.policy = gateway.policy
        test.rig.peer.configuration["AmpLmt"] = "32"
        test.rig.peer.runtime_user = 320
        assert (await healthy(test.controller))["native_limit_a"] == 32
        assert test.controller.current.selected() == 6
        try:
            yield test
        finally:
            await test.controller.current.settle()
            if not test.session.closed:
                assert journal.get("native_bounded_handover") == original
                assert journal.get("trial_binding") == binding


# Offline readiness depends on fresh local authority even while the genuine cloud path is deliberately retired.
async def locally_ready(controller: ManagedController) -> dict[str, Any]:
    async with asyncio.timeout(5):
        while True:
            status = await controller.tick()
            if status.get("local_healthy"):
                return status
            assert status["state"] not in ("attention", "stopping"), status
            await asyncio.sleep(0.02)


# Use the real command/worker and inspect its actual physical completion rather than replacing its result.
async def select(test: ChargeRig, current: int) -> str:
    identifier = uuid.uuid4().hex
    revision = test.controller.requests.intent().revision
    test.controller.command(identifier, "set_current", revision, current_a=current)
    await test.controller.current.settle()
    return identifier


# A distinct command tag identifies even an owner lifecycle that closes entirely between native observations.


# Establish Pause through the actual command, TLS profile effect and independent native readback before yielding.
@asynccontextmanager
async def paused_ready(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
    cli_route: bool = False,
) -> AsyncIterator[ChargeRig]:
    async with ready(
        directory, contexts, port, monkeypatch, cli_route=cli_route, native_peer=native_peer, bench_type=bench_type
    ) as test:
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        status = await healthy(test.controller)
        assert status["local_hold_verified"] and status["station_ceiling_a"] == 0
        assert test.rig.bench.firmware.effective_limit() == 0
        yield test


# The only injected native writer changes the independently held USER scalar, never journal authority or OCPP state.
def native_writer(test: ChargeRig, writes: list[dict[str, str]]) -> Callable[[], Awaitable[None]]:
    # Model only the authorized native USER scalar and its runtime readback, leaving the independent zero intact.
    async def patch() -> None:
        writes.append({"AmpLmt": "32"})
        test.rig.peer.configuration.update(writes[-1])
        test.rig.peer.runtime_user = 320

    return patch


# Keep this synthetic peer or lifecycle helper independent of the engine under test.
@asynccontextmanager
async def commissioned(
    tmp_path,
    tls,
    port,
    monkeypatch,
    current,
    *,
    owner_tag=None,
    cli_route=False,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
):
    async with paused_ready(
        tmp_path, tls, port, monkeypatch, cli_route=cli_route, native_peer=native_peer, bench_type=bench_type
    ) as prior:
        writes = []
        await prepare_current_capability(
            prior.session.journal,
            prior.session.book,
            prior.rig.spec,
            prior.rig.prepared.observer.observe_handover,
            native_writer(prior, writes),
        )
        assert writes == [{"AmpLmt": "32"}]
        await prior.controller.charging.close()
        await crash(prior.session)
        session = managed_session(prior.rig)
        if owner_tag is not None:
            session.gateway.policy = replace(
                session.gateway.policy, allowed_tags=frozenset({owner_tag, *session.gateway.policy.allowed_tags})
            )
            session.gateway.controls.policy = session.gateway.policy
        await session.start()
        c = ManagedController(session)
        async with prior.rig.charger(session) as charger:
            await introduced(prior.rig, session, charger, "commissioned32")
            await healthy(c)
            assert session.gateway.policy.max_current_a == 32
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            await healthy(c)
            b = ChargeRig(prior.rig, session, c, charger)
            await select(b, current)
            assert (await healthy(c))["station_ceiling_a"] == current
            try:
                yield b
            finally:
                await c.charging.close()
                if c.pause_task is not None:
                    await asyncio.gather(c.pause_task, return_exceptions=True)
                await session.close()


# Use the existing explicit synthetic capability and normal current worker to establish a real selected ceiling.
@asynccontextmanager
async def selected_ready(
    tmp_path: Any,
    tls: Any,
    port: int,
    monkeypatch: Any,
    current: int,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
) -> AsyncIterator[ChargeRig]:
    factory = adjustable if current == 32 else ready
    async with factory(tmp_path, tls, port, monkeypatch, native_peer=native_peer, bench_type=bench_type) as test:
        if current == 32:
            await select(test, 32)
            await healthy(test.controller)
        assert test.rig.bench.firmware.effective_limit() == current
        assert test.session.journal.get("managed_temporary_schedule") is None
        assert test.session.journal.get("managed_current_change", {}).get("state") != "recovering"
        yield test


# Exercise the already established synthetic native32 commissioning fixture without changing engine decisions.
@asynccontextmanager
async def release_bench(
    tmp_path,
    tls,
    port,
    native_limit,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
):
    async with managed_bench(tmp_path, tls, port, native_peer=native_peer, bench_type=bench_type) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "release-recovery-boot")
            await healthy(c)
            if native_limit == 32:
                session.journal.put("managed_current_capability", capability_value(session.journal, rig.spec))
                session.gateway.policy = replace(session.gateway.policy, max_current_a=32)
                session.gateway.controls.policy = session.gateway.policy
                rig.peer.configuration["AmpLmt"] = "32"
                rig.peer.runtime_user = 320
                await healthy(c)
                c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=32)
                await c.current.settle()
                await healthy(c)
            yield SimpleNamespace(rig=rig, session=session, c=c, charger=charger)
