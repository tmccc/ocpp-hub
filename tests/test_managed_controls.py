"""Real native HTTP and OCPP TLS tests for deliberate holds, explicit return and restart intent."""

from __future__ import annotations

import asyncio
import copy
import ssl
import uuid
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype_tls import tls as tls


# Give each intentional user operation an independent bounded opaque identifier.
def identifier() -> str:
    return uuid.uuid4().hex


# Add only runtime envelope fields supplied by the service, rather than fabricating native proof.
def envelope(status: dict[str, Any]) -> dict[str, Any]:
    return {**status, "running": True, "certificate_remaining_seconds": 86400}


# Intent: deliberate Stop becomes a healthy actual0 hold, with explicit return6 and no Start or history change.
# Why: supervisor ownership must distinguish an operator pause from an unintended loss of shared control.
async def test_pause_and_explicit_return_keep_the_existing_native_safety_boundary(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ocpp_hub_core.prototype.managed_view import health_snapshot

    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "operator-boot")
            await healthy(controller)
            history = copy.deepcopy(rig.bench.history)
            pause_id = identifier()
            response = controller.command(pause_id, "pause", 0)
            assert response["state"] == "accepted"
            assert controller.requests.intent().mode == "paused"
            status = await healthy(controller)
            assert status["local_hold_verified"] and status["station_ceiling_a"] == 0
            assert not status["owner_control_enabled"] and health_snapshot(envelope(status))["healthy"]
            assert controller.requests.receipt(pause_id)["state"] == "completed"
            count = len(rig.bench.received_limits)
            assert controller.command(pause_id, "pause", 0)["state"] == "completed"
            await healthy(controller)
            assert len(rig.bench.received_limits) == count
            return_id = identifier()
            controller.command(return_id, "app_control", status["control_revision"])
            status = await healthy(controller)
            assert status["desired_control"] == "app" and status["owner_control_enabled"]
            assert rig.bench.firmware.effective_limit() == 6
            assert controller.requests.receipt(return_id)["state"] == "completed"
            assert not starts(rig) and rig.bench.history == history


# Intent: a paused choice and its actual zero persist through replacement until a new explicit return is made.
# Why: process recovery is not permission to resume a charging schedule which the user deliberately inhibited.
async def test_restart_remembers_requested_pause_without_automatic_release(
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
            await introduced(rig, first, charger, "pause-first-boot")
            await healthy(controller)
            controller.command(identifier(), "pause", 0)
            await healthy(controller)
            await first.close()
        replacement = managed_session(rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        before = len(rig.bench.received_limits)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "pause-replacement-boot")
            status = await healthy(recovered)
            assert status["desired_control"] == "paused" and status["local_hold_verified"]
            assert rig.bench.received_limits[before:] and set(rig.bench.received_limits[before:]) == {0}
            assert not starts(rig) and replacement.journal.get("managed_control_fault") is None


# Intent: stale return commands and reused identifiers cannot clear a newer Pause or become another operation.
# Why: an HA request based on an older dashboard revision must not undo a later user's Stop.
async def test_stale_return_and_conflicting_identifier_are_rejected_before_writes(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "stale-boot")
            await healthy(controller)
            request = identifier()
            controller.command(request, "pause", 0)
            await healthy(controller)
            with pytest.raises(SecurityError):
                controller.command(request, "app_control", 0)
            with pytest.raises(RecoveryRequiredError):
                controller.command(identifier(), "app_control", 0)
            assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)


# Intent: a pending return from a lost process is retained as uncertain and never reissued automatically.
# Why: a confirmed paused intent must remain authoritative when a positive command's outcome is unresolved.
async def test_accepted_return_is_not_retried_after_process_replacement(
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
            await introduced(rig, first, charger, "lost-return-boot")
            await healthy(controller)
            controller.command(identifier(), "pause", 0)
            status = await healthy(controller)
            request = identifier()
            controller.command(request, "app_control", status["control_revision"])
            await first.close()
        replacement = managed_session(rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        assert recovered.requests.receipt(request)["state"] == "uncertain"
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "lost-return-replacement")
            assert (await healthy(recovered))["desired_control"] == "paused"
            assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)


# Intent: Pause arriving while Return awaits a real native read invalidates the positive release before send.
# Why: a newer Stop must preempt prior work rather than wait for it to restore app authority first.
@pytest.mark.parametrize("boundary", ["native", "owned_task"])
async def test_newer_pause_preempts_return_during_native_validation(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "preempt-boot")
            await healthy(controller)
            controller.command(identifier(), "pause", 0)
            status = await healthy(controller)
            request = identifier()
            controller.command(request, "app_control", status["control_revision"])
            entered, proceed = asyncio.Event(), asyncio.Event()
            actual = controller.native.observe

            # Delay the independent peer observation, preserving every actual native evidence and RPC check.
            async def delayed() -> Any:
                entered.set()
                await proceed.wait()
                return await actual()

            if boundary == "native":
                monkeypatch.setattr(controller.native, "observe", delayed)
            else:
                original_release = session.gateway.controls._release

                # Delay only scheduling, then execute the real owned release including its pre-claim guard.
                async def delayed_release(acknowledge_uncertainty, guard=None):
                    entered.set()
                    await proceed.wait()
                    return await original_release(acknowledge_uncertainty, guard)

                monkeypatch.setattr(session.gateway.controls, "_release", delayed_release)
            actual_claim = session.gateway.controls._claim
            claims = []

            # Observe the actual claim ordering, retaining every real persistence and control effect.
            def claim(inhibit, stopping=False):
                claims.append(stopping)
                return actual_claim(inhibit, stopping)

            monkeypatch.setattr(session.gateway.controls, "_claim", claim)
            count = len(rig.bench.received_limits)
            task = asyncio.create_task(controller.tick())
            try:
                await entered.wait()
                claims.clear()
                controller.command(identifier(), "pause", status["control_revision"])
                proceed.set()
                await task
                await healthy(controller)
                assert all(limit == 0 for limit in rig.bench.received_limits[count:])
                assert controller.requests.intent().mode == "paused"
                assert controller.requests.receipt(request)["state"] == "failed"
                assert not starts(rig) and session.journal.get("managed_control_fault") is None
                assert claims and all(claims)
            finally:
                proceed.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)


# Intent: a crash after durable Pause but before its worker runs recovers to actual0 without any positive release.
# Why: intent durability must cover the interval in which the old physical ceiling can still be6.
async def test_crash_immediately_after_pause_acceptance_reconciles_to_zero(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.test_native_bounded_recovery import crash

    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        first = managed_session(rig)
        await first.start()
        controller = ManagedController(first)
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await introduced(rig, first, charger, "crash-pause-first")
            await healthy(controller)
            request = identifier()
            controller.command(request, "pause", 0)
            assert controller.pause_task is not None
            controller.pause_task.cancel()
            await asyncio.gather(controller.pause_task, return_exceptions=True)
            assert rig.bench.firmware.effective_limit() == 6
            await crash(first)
        replacement = managed_session(rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        before = len(rig.bench.received_limits)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "crash-pause-replacement")
            status = await healthy(recovered)
            assert status["local_hold_verified"] and status["desired_control"] == "paused"
            assert all(limit == 0 for limit in rig.bench.received_limits[before:])
            assert recovered.requests.receipt(request)["state"] == "completed" and not starts(rig)


# Intent: Stop Accepted alone cannot complete Pause; actual closure/zero still works when vendor delivery is offline.
# Why: a command ACK is not proof of power or lifecycle closure, and stopping must not depend on the cloud.
async def test_pause_requires_actual_closure_and_works_with_cloud_offline(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.test_prototype import START
    from tests.test_prototype_runtime import until

    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "offline-pause-boot")
            await healthy(controller)
            reply = await rig.bench.call(
                charger, "native-start-for-stop", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
            )
            local = reply[2]["transactionId"]
            await until(
                lambda: session.journal.vendor_id(local) is not None and session.journal.next_delivery() is None
            )
            rig.peer.paused, rig.peer.permission_enabled = False, True
            rig.bench.cp, rig.bench.connection = "C", "Charging"
            rig.bench.firmware.fire_plan(9)
            assert (await healthy(controller))["power_w"] == 1440
            await session.runtime.stop_vendor()
            request = identifier()
            controller.command(request, "pause", 0)
            await until(lambda: any(f[2] == "RemoteStopTransaction" for f in rig.bench.device_calls))
            assert controller.requests.receipt(request)["state"] == "accepted"
            assert session.journal.active() == local
            await rig.bench.call(
                charger,
                "actual-stop",
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            rig.peer.permission_enabled = False
            rig.bench.cp, rig.bench.connection = "B", "Plug"
            async with asyncio.timeout(5):
                while controller.requests.receipt(request)["state"] != "completed":
                    status = await controller.tick()
                    await asyncio.sleep(0.02)
            assert status["local_hold_verified"] and not status["cloud_connected"]
            assert session.journal.next_delivery() is not None
            assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)
