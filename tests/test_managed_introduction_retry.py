"""Transient introduction recovery retains real native HTTP and authenticated TLS boundaries."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_current import adjustable, select
from tests.test_managed_proxy import healthy, managed_bench, managed_session, starts
from tests.test_managed_test_charge import ready
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


class InterruptedNativePeer(NativeVehiclePeer):
    """The independent native endpoint can withhold only its own HTTP availability."""

    # Retain ordinary native physical state and expose the number of real failed HTTP requests.
    def __init__(self, bench: Any) -> None:
        super().__init__(bench)
        self.unavailable = False
        self.failed_reads: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    # Answer complete received requests with a temporary HTTP failure while ordinary peer behavior stays intact.
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if not self.unavailable:
            await super().handle(reader, writer)
            return
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in header.split(b"\r\n")[1:]
                if b":" in line
            }
            request = json.loads(await reader.readexactly(int(fields[b"content-length"])))
            self.failed_reads.put_nowait(request)
            writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


# Wait for repeated actual HTTP attempts without invoking or replacing the engine's introduction logic.
async def retries(peer: InterruptedNativePeer) -> None:
    async with asyncio.timeout(3):
        for _ in range(3):
            assert (await peer.failed_reads.get())["action"] == "getConfig"


# Use the existing explicit synthetic capability and normal current worker to establish a real selected ceiling.
@asynccontextmanager
async def selected_ready(tmp_path: Any, tls: Any, port: int, monkeypatch: Any, current: int) -> AsyncIterator[Any]:
    factory = adjustable if current == 32 else ready
    async with factory(tmp_path, tls, port, monkeypatch) as test:
        if current == 32:
            await select(test, 32)
            await healthy(test.controller)
        assert test.rig.bench.firmware.effective_limit() == current
        assert test.session.journal.get("managed_temporary_schedule") is None
        assert test.session.journal.get("managed_current_change", {}).get("state") != "recovering"
        yield test


# Intent: an interrupted first Boot audit retries on the same socket and establishes zero before any release.
# Why: recovering native availability must not require a second Boot or bypass the initial owned-profile receipt.
async def test_first_introduction_recovers_after_native_http503(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        rig.peer.unavailable = True
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "interrupted-first-boot", "BootNotification", BOOT))[0] == 3
                await retries(rig.peer)
                epoch = session.gateway._charger_epoch
                assert session.native_bounded.introduced_epoch != epoch
                assert not session.gateway.controls.ready and rig.bench.device_calls == []
                rig.peer.unavailable = False
                status = await healthy(controller)
                assert session.gateway._charger_epoch == session.native_bounded.introduced_epoch == epoch
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == 6
                assert rig.bench.received_limits[0] == 0 and rig.bench.received_limits[-1] == 6
                assert not starts(rig) and session.journal.active() is None
        finally:
            rig.peer.unavailable = False


# Intent: ordinary native6 and prepared native32 reconnects retry transient HTTP without another device Boot.
# Why: recovery must first install a verified zero and then restore exactly the owner's previous selected cap.
@pytest.mark.parametrize("current", [6, 32])
async def test_reconnect_introduction_recovers_after_native_http503(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as test:
        rig, session, controller = test.rig, test.session, test.controller
        await test.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        rig.peer.unavailable = True
        first_profile = len(rig.bench.received_limits)
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "interrupted-reconnect", "BootNotification", BOOT))[0] == 3
                await retries(rig.peer)
                epoch = session.gateway._charger_epoch
                assert session.native_bounded.introduced_epoch != epoch
                assert not session.gateway.controls.ready and rig.bench.firmware.effective_limit() == current
                assert rig.bench.received_limits[first_profile:] == []
                rig.peer.unavailable = False
                status = await healthy(controller)
                limits = rig.bench.received_limits[first_profile:]
                assert session.gateway._charger_epoch == session.native_bounded.introduced_epoch == epoch
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == current
                assert limits[0] == 0 and limits[-1] == current and set(limits) <= {0, current}
                assert not starts(rig) and session.journal.active() is None
        finally:
            rig.peer.unavailable = False


# Intent: Pause restricts an interrupted introduction immediately and its accepted Return finishes after HTTP recovers.
# Why: retrying the read must respect the newest owner intent and never leave completed physical zero unconfirmed.
@pytest.mark.parametrize("current", [6, 32])
async def test_pause_and_return_complete_after_interrupted_introduction(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as test:
        rig, session, controller = test.rig, test.session, test.controller
        await test.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        rig.peer.unavailable = True
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "pause-during-reconnect", "BootNotification", BOOT))[0] == 3
                await retries(rig.peer)
                epoch = session.gateway._charger_epoch
                pause = uuid.uuid4().hex
                controller.command(pause, "pause", controller.requests.intent().revision)
                await until(lambda: controller.pause_task is not None and controller.pause_task.done())
                assert rig.bench.firmware.effective_limit() == 0
                assert controller.requests.receipt(pause)["state"] == "accepted"
                return_id = uuid.uuid4().hex
                controller.command(return_id, "app_control", controller.requests.intent().revision)
                assert controller.requests.intent().mode == "paused"
                rig.peer.unavailable = False
                async with asyncio.timeout(5):
                    while controller.requests.receipt(return_id)["state"] != "completed":
                        status = await controller.tick()
                        assert status["state"] != "attention", status
                        await asyncio.sleep(0.02)
                status = await healthy(controller)
                assert session.gateway._charger_epoch == session.native_bounded.introduced_epoch == epoch
                assert controller.requests.receipt(pause)["state"] == "completed"
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == current
                assert rig.bench.firmware.effective_limit() == current and not starts(rig)
        finally:
            rig.peer.unavailable = False


# Intent: a complete conflicting native configuration stays blocked after reconnect and never becomes a retry.
# Why: transport recovery cannot legitimize an altered installation, even if its physical settings are later restored.
@pytest.mark.parametrize("current", [6, 32])
async def test_conflicting_binding_does_not_retry_introduction(tmp_path, tls, unused_tcp_port, monkeypatch, current):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as test:
        rig, session, controller = test.rig, test.session, test.controller
        await test.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        original = rig.peer.configuration["untouched"]
        rig.peer.configuration["untouched"] = "independent-external-change"
        first_profile = len(rig.bench.received_limits)
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "conflicting-reconnect", "BootNotification", BOOT))[0] == 3
                await until(lambda: session.journal.get("trial_phase") == "blocked")
                assert session.runtime.status.last_failure == "BootOrCeilingUnavailable"
                read_count = len(rig.peer.requests)
                rig.peer.configuration["untouched"] = original
                for _ in range(5):
                    assert (await controller.tick())["state"] == "starting"
                    await asyncio.sleep(0.03)
                assert len(rig.peer.requests) == read_count
                assert session.native_bounded.introduced_epoch != session.gateway._charger_epoch
                assert not session.gateway.controls.ready and rig.bench.received_limits[first_profile:] == []
                assert rig.bench.firmware.effective_limit() == current and not starts(rig)
        finally:
            rig.peer.configuration["untouched"] = original
