"""Managed owner control and persistent recovery through real encrypted loopback peers."""

from __future__ import annotations

import asyncio
import copy
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.trial_session import TrialSession
from websockets.asyncio.server import ServerConnection

import tests.test_native_bounded_session as native_bench
from tests.test_native_bounded_session import ConnectedBench, connected_bench
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


class OwnerBench(Bench):
    """The independent cloud may issue owner commands over its actual TLS connection."""

    # Retain only the synthetic cloud socket while the ordinary peer independently answers proxy requests.
    async def vendor(self, connection: ServerConnection) -> None:
        self.cloud = connection
        await super().vendor(connection)

    # Correlate an actual TLS command with the reply observed by the independently running cloud receiver.
    async def cloud_call(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        await self.cloud.send(build_call(token, action, payload))
        await until(lambda: any(frame[0] in (3, 4) and frame[1] == token for frame in self.calls))
        return next(frame for frame in self.calls if frame[0] in (3, 4) and frame[1] == token)


# Share the connected native bench while approving one synthetic timer identity for the owner-control tests.
@asynccontextmanager
async def managed_bench(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    initial_history: list[dict[str, Any]] | None = None,
    cli_route: bool = False,
) -> AsyncIterator[ConnectedBench]:
    monkeypatch.setattr(native_bench, "Bench", OwnerBench)
    async with connected_bench(directory, contexts, port, initial_history=initial_history) as rig:
        # Synthetic schedule identity is approved without changing private input files or production policy.
        rig.prepared.config = replace(rig.prepared.config, local_tags=frozenset({"NATIVE_TIMER"}))
        if cli_route:
            value = rig.spec.value()
            value["trial_route"]["sURL"] = rig.prepared.config.charger_path.rsplit("/", 1)[0] + "/"
            rig.spec = type(rig.spec).parse(value)
        yield rig


# Register a real bounded session for teardown with the assistant Start path disabled from construction.
def managed_session(rig: ConnectedBench) -> TrialSession:
    session = TrialSession(
        rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False, allow_unplugged_native=True
    )
    rig.sessions.append(session)
    return session


# Wait for genuine device Boot and cloud acceptance before inspecting introduction or release outcomes.
async def introduced(rig: ConnectedBench, session: TrialSession, charger: Any, token: str) -> None:
    assert (await rig.bench.call(charger, token, "BootNotification", BOOT))[0] == 3
    await until(lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted")


# Allow ordinary correlated work to settle without replacing fresh native evidence or manufacturing acknowledgements.
async def healthy(controller: ManagedController) -> dict[str, Any]:
    async with asyncio.timeout(5):
        while True:
            status = await controller.tick()
            if status["state"] == "healthy":
                return status
            assert status["state"] not in ("attention", "stopping"), status
            await asyncio.sleep(0.02)


# Count the physical peer's actual Start requests independently from the supervisor's saved state.
def starts(rig: ConnectedBench) -> list[list[Any]]:
    return [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStartTransaction"]


# Intent: each real introduction first holds zero, then returns bounded authority to the owner without Start.
# Why: raising a verified ceiling is distinct from the assistant initiating a charging transaction.
async def test_managed_boot_releases_six_without_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        assert (await controller.tick())["state"] == "starting"
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "managed-boot")
            assert rig.bench.firmware.effective_limit() == 0
            assert rig.bench.received_limits and set(rig.bench.received_limits) == {0}
            status = await healthy(controller)
            assert status["owner_control_enabled"] and status["local_start_disabled"]
            assert status["phase"] == "active" and status["station_ceiling_a"] == 6
            assert rig.bench.firmware.effective_limit() == 6
            assert session.journal.active() is None and not starts(rig)
            assert not any(
                frame[0] == 2 and frame[2] in ("StartTransaction", "StopTransaction") for frame in rig.bench.calls
            )
            released_profiles = len(rig.bench.received_limits)
            assert (await healthy(controller))["owner_control_enabled"]
            assert len(rig.bench.received_limits) == released_profiles and not starts(rig)
            await session.close()
            assert rig.bench.firmware.effective_limit() == 0


# Intent: assistant Start is refused while an actual owner app or native timer retains its normal permission.
# Why: disabling local testing must retain the user's charging controls and accept normal power as healthy.
@pytest.mark.parametrize("source", ["cloud", "native-schedule", "direct-schedule"])
async def test_managed_owner_charge_is_permitted_and_healthy(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "owner-boot")
            await healthy(controller)
            control = copy.deepcopy(session.journal.get("control"))
            device_count = len(rig.bench.device_calls)
            with pytest.raises(DeniedError, match="local Start is disabled"):
                await session.command({"command": "start"})
            assert session.journal.get("control") == control
            assert len(rig.bench.device_calls) == device_count and not starts(rig)
            tag = "CLOUD_OWNER" if source == "cloud" else "NATIVE_TIMER"
            if source == "cloud":
                assert isinstance(rig.bench, OwnerBench)
                reply = await rig.bench.cloud_call(
                    "owner-app-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": tag}
                )
                assert reply[0] == 3 and reply[2]["status"] == "Accepted"
                assert len(starts(rig)) == 1
            elif source == "direct-schedule":
                session.gateway.auth_info.put(tag, {"status": "Accepted", "expiryDate": "2000-01-01T00:00:00Z"})
            else:
                reply = await rig.bench.call(charger, "timer-authorize", "Authorize", {"idTag": tag})
                assert reply[2]["idTagInfo"]["status"] == "Accepted"
            reply = await rig.bench.call(charger, "owner-transaction", "StartTransaction", START | {"idTag": tag})
            local = reply[2]["transactionId"]
            assert reply[2]["idTagInfo"]["status"] == "Accepted"
            await until(
                lambda: session.journal.vendor_id(local) is not None and session.journal.next_delivery() is None
            )
            rig.peer.paused = False
            rig.peer.permission_enabled = True
            rig.bench.cp, rig.bench.connection = "C", "Charging"
            rig.bench.firmware.fire_plan(9)
            count = len(starts(rig))
            status = await healthy(controller)
            assert status["charging_session_open"] and status["current_a"] == 6
            assert status["power_w"] == 1440 and status["native_limit_a"] == 6
            assert status["phase"] == "active" and status["cloud_connected"] and status["charger_connected"]
            assert status["owner_control_enabled"] and not status["automatic_charge_restart"]
            assert len(starts(rig)) == count == (1 if source == "cloud" else 0)
            assert session.journal.active() == local and not session.journal.get("managed_control_fault")
            await rig.bench.call(
                charger,
                "owner-stopped",
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            rig.peer.permission_enabled = False
            await until(lambda: session.journal.active() is None and session.journal.next_delivery() is None)
            assert (await healthy(controller))["state"] == "healthy"


# Intent: sustained native or owned-resource drift latches attention instead of guessing a transaction or releasing.
# Why: an external session and a lost profile must never acquire ownership merely because the monitor is running.
@pytest.mark.parametrize("fault", ["missing-station", "configuration", "unknown-transaction"])
async def test_managed_authority_fault_latches_without_guessed_stop_or_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "fault-boot")
            await healthy(controller)
            if fault == "missing-station":
                rig.bench.firmware.profiles.pop(7001)
            elif fault == "configuration":
                rig.peer.configuration["untouched"] = "external-change"
            else:
                rig.bench.firmware.active = 9000011
                rig.bench.history.append({"transactionId": 9000011, "origin": "external"})
            for _ in range(3):
                assert (await controller.tick())["state"] == "degraded"
            assert (
                session.journal.get("managed_control_fault")["classification"]
                == "native_authority_or_preservation_failed"
            )
            assert session.journal.get("trial_phase") == "blocked"
            diagnostic = session.journal.get("managed_native_failure")
            assert diagnostic["type"] == "SecurityError" and diagnostic["detail"] and diagnostic["at_epoch"] > 0
            assert session.journal.get("control")["inhibit"] and session.gateway.controls.desired() == 0
            status = await controller.tick()
            assert status["state"] == "attention"
            assert session.journal.active() is None and not starts(rig)
            assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
            assert not any(
                frame[0] == 2 and frame[2] in ("StartTransaction", "StopTransaction") for frame in rig.bench.calls
            )
            if fault == "missing-station":
                assert 7001 not in rig.bench.firmware.profiles
            # Repairing external state cannot clear the durable managed fault or auto-release another six-ampere cap.
            if fault == "configuration":
                rig.peer.configuration["untouched"] = "sentinel"
            elif fault == "unknown-transaction":
                rig.bench.firmware.active = None
            released = sum(limit > 0 for limit in rig.bench.received_limits)
            assert (await controller.tick())["state"] == "attention"
            assert sum(limit > 0 for limit in rig.bench.received_limits) == released


# Intent: process replacement reconciles genuine Boot and retained zero before one fresh bounded release.
# Why: persistent operation must not replay Start or lose the original record boundary across supervisor restart.
async def test_managed_restart_releases_after_fresh_zero_without_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        first = managed_session(rig)
        await first.start()
        controller = ManagedController(first)
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await introduced(rig, first, charger, "first-managed-boot")
            await healthy(controller)
            allocation = copy.deepcopy(first.journal.get("trial_allocation"))
            mode = copy.deepcopy(first.journal.get("managed_mode"))
            history = copy.deepcopy(rig.bench.history)
            await first.close()
            assert rig.bench.firmware.effective_limit() == 0
        with pytest.raises(SecurityError, match="saved local Start restriction"):
            TrialSession(rig.prepared, FAST, native_bounded=rig.spec)
        replacement = managed_session(rig)
        await replacement.start()
        assert replacement.journal.get("trial_phase") == "recovering"
        recovered = ManagedController(replacement)
        before = len(rig.bench.received_limits)
        assert (await recovered.tick())["state"] == "starting"
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "replacement-managed-boot")
            assert rig.bench.firmware.effective_limit() == 0
            assert set(rig.bench.received_limits[before:]) == {0}
            status = await healthy(recovered)
            assert status["phase"] == "active" and status["owner_control_enabled"]
            assert rig.bench.firmware.effective_limit() == 6 and not starts(rig)
            assert replacement.journal.get("trial_allocation") == allocation
            assert replacement.journal.get("managed_mode") == mode
            assert rig.bench.history == history and replacement.journal.active() is None
            assert not any(
                frame[0] == 2 and frame[2] in ("StartTransaction", "StopTransaction") for frame in rig.bench.calls
            )


# Intent: a shutdown request revokes pending positive authority both during audit and at the actual send guard.
# Why: an already awaited native observation must not release six amperes after the service has begun stopping.
@pytest.mark.parametrize("read_number", [1, 3], ids=["initial-audit", "positive-send-guard"])
async def test_managed_shutdown_supersedes_awaited_release(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    read_number: int,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "shutdown-boot")
            assert session.native_bounded is not None
            observe = session.native_bounded.observe
            entered, proceed = asyncio.Event(), asyncio.Event()
            observed = 0

            # Delay a real independently sampled HTTP observation; its physical outcome remains peer-owned.
            async def delayed_native() -> Any:
                nonlocal observed
                native = await observe()
                observed += 1
                if observed == read_number:
                    entered.set()
                    await proceed.wait()
                return native

            session.native_bounded.observe = delayed_native
            task = asyncio.create_task(controller.tick())
            try:
                await asyncio.wait_for(entered.wait(), 3)
                controller.request_stop()
                controller.request_stop()  # Duplicate process signals retain one stopping ticket.
                ticket = session.journal.get("control")["ticket"]
                proceed.set()
                status = await asyncio.wait_for(task, 3)
                assert status["state"] == "stopping" and not status["owner_control_enabled"]
                assert controller.released_epoch is None and not session.journal.get("managed_control_fault")
                assert session.journal.get("control")["ticket"] == ticket
                assert session.gateway.controls.desired() == 0
                assert set(rig.bench.received_limits) == {0} and not starts(rig)
                assert rig.bench.firmware.effective_limit() == 0
                assert (await controller.tick())["state"] == "stopping"
                await session.close()
                assert rig.bench.firmware.effective_limit() == 0
            finally:
                proceed.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
