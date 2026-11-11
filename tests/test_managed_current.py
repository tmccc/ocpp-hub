"""Explicit adjustable current and offline local Start retain real native, TLS and durable authority checks."""

from __future__ import annotations

import asyncio
import copy
import ssl
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from websockets.asyncio.client import connect

from tests.test_managed_api import TOKEN, call
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_test_charge import ChargeRig, actual_stop, owner_start, positive_flow, ready
from tests.test_native_bounded_recovery import crash
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Activate only the explicit synthetic USER32 capability while retaining the exact original spec and native binding.
@asynccontextmanager
async def adjustable(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[ChargeRig]:
    async with ready(directory, contexts, port, monkeypatch) as test:
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


# Intent: approved current selections preserve real app authority and native HW32.
# Why: changing desired UI state alone is insufficient; the real owned profile and composite must confirm the setting.
@pytest.mark.parametrize("settings", [(7,), (16,), (32,), (16, 6)])
async def test_current_selection_confirms_real_profile_and_persists_revision(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    settings: tuple[int, ...],
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        for current in settings:
            revision = test.controller.requests.intent().revision
            identifier = await select(test, current)
            status = await healthy(test.controller)
            native = await test.controller.native.read()
            assert test.controller.requests.receipt(identifier)["state"] == "completed"
            assert test.controller.requests.intent().revision == revision + 1
            assert test.controller.requests.intent().mode == "app"
            assert test.controller.current.selected() == status["requested_current_a"] == current
            assert status["station_ceiling_a"] == test.rig.bench.firmware.effective_limit() == current
            assert native.runtime_user_a == native.runtime_hardware_a == 32
            assert test.rig.peer.configuration["maxOutCurrent0"] == "32"
            assert not starts(test.rig) and test.session.journal.active() is None
            if current > 6:
                assert not test.controller.tests.available(test.controller.latest_native)


# Intent: changing current during a verified pause changes the future setting without raising the actual zero hold.
# Why: selecting a larger current must not implicitly resume a deliberately paused charger.
async def test_paused_current_selection_preserves_zero_until_explicit_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        await healthy(test.controller)
        before = list(test.rig.bench.received_limits)
        identifier = await select(test, 16)
        status = await healthy(test.controller)
        assert test.controller.requests.receipt(identifier)["state"] == "completed"
        assert status["desired_control"] == "paused" and status["local_hold_verified"]
        assert status["requested_current_a"] == 16 and status["station_ceiling_a"] == 0
        assert test.rig.bench.received_limits == before and not starts(test.rig)
        test.controller.command(uuid.uuid4().hex, "start", test.controller.requests.intent().revision)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        assert (await healthy(test.controller))["current_a"] == 16
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: a selected16/32 profile produces its actual independent native load, with USER/HW32 unchanged.
# Why: a32A slider layered over an unchanged native6A setting would falsely claim useful adjustable operation.
@pytest.mark.parametrize("current", [16, 32])
async def test_explicit_start_delivers_the_selected_current_and_refuses_adjustment_while_open(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    current: int,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        await select(test, current)
        test.controller.command(uuid.uuid4().hex, "start", test.controller.requests.intent().revision)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        status = await healthy(test.controller)
        assert status["current_a"] == current and status["power_w"] == current * 240
        assert status["requested_current_a"] == status["station_ceiling_a"] == current
        assert status["native_limit_a"] == 32
        before = copy.deepcopy(test.rig.bench.device_calls)
        with pytest.raises(RecoveryRequiredError):
            test.controller.command(
                uuid.uuid4().hex, "set_current", test.controller.requests.intent().revision, current_a=7
            )
        assert test.rig.bench.device_calls == before and test.session.journal.active() == local
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: only integer6..32 input reaches selection; malformed or out-of-range requests have no durable/physical effect.
# Why: booleans, coercible strings and floats must not bypass the fixed single-phase electrical boundary.
@pytest.mark.parametrize("value", [True, False, 0, 5, 33, -1, 6.0, "16", None])
async def test_invalid_current_is_refused_before_any_effect(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    value: Any,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        before = copy.deepcopy(test.rig.bench.device_calls)
        control = copy.deepcopy(test.session.journal.get("control"))
        with pytest.raises((SecurityError, DeniedError, RecoveryRequiredError)):
            test.controller.command(uuid.uuid4().hex, "set_current", 0, current_a=value)
        assert test.rig.bench.device_calls == before and test.session.journal.get("control") == control
        assert test.session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0


# Intent: a duplicate identifier binds its exact selected value and revision without repeating a profile operation.
# Why: slider retries cannot silently change target or turn a stale client state into a new current request.
async def test_duplicate_current_and_stale_revision_do_not_reapply_or_change_target(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        identifier = await select(test, 16)
        receipt = test.controller.requests.receipt(identifier)
        before = copy.deepcopy(test.rig.bench.device_calls)
        assert test.controller.command(identifier, "set_current", 0, current_a=16) == receipt
        with pytest.raises(SecurityError):
            test.controller.command(identifier, "set_current", 0, current_a=17)
        with pytest.raises(RecoveryRequiredError):
            test.controller.command(uuid.uuid4().hex, "set_current", 0, current_a=32)
        assert test.rig.bench.device_calls == before and test.controller.current.selected() == 16


# Intent: the actual management socket accepts exactly one integer current and rejects omitted or arbitrary fields.
# Why: the HA boundary must not create alternate tag, duration or electrical-limit controls through extra JSON.
async def test_current_api_requires_exact_argument_shape_before_actual_profile_write(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as test:
        port = unused_tcp_port_factory()
        published = {**await healthy(test.controller), "running": True, "certificate_remaining_seconds": 86400}
        api = ManagementServer(
            ManagementSettings(port, tmp_path / "unused-secret", TOKEN), test.controller, lambda: published
        )
        await api.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as socket:
                before = copy.deepcopy(test.rig.bench.device_calls)
                for fields in [
                    {},
                    {"current_a": True},
                    {"current_a": 16.0},
                    {"current_a": 16, "tag": "UNAPPROVED"},
                    {"current_a": 16, "duration_seconds": 120},
                    {"current_a": 16, "hardware_limit": 99},
                ]:
                    result = await call(socket, "set_current", revision=0, **fields)
                    assert result == {"ok": False, "error": "invalid_request"}
                assert test.rig.bench.device_calls == before
                assert test.session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0

                identifier = uuid.uuid4().hex
                reply = await call(socket, "set_current", id=identifier, revision=0, current_a=16)
                assert reply["ok"] and reply["result"]["state"] == "accepted"
                await test.controller.current.settle()
                assert test.controller.requests.receipt(identifier)["state"] == "completed"
                assert test.controller.current.selected() == test.rig.bench.firmware.effective_limit() == 16
                assert not starts(test.rig)
        finally:
            await api.close()


# Intent: the original six-ampere service cannot acquire higher-current permission through the new command alone.
# Why: new software must preserve the existing commissioning boundary until explicit USER32 capability is installed.
async def test_unprepared_legacy_service_remains_six_amperes(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        before = copy.deepcopy(test.rig.bench.device_calls)
        with pytest.raises((SecurityError, DeniedError, RecoveryRequiredError)):
            test.controller.command(uuid.uuid4().hex, "set_current", 0, current_a=16)
        assert test.rig.bench.device_calls == before
        assert test.session.gateway.policy.max_current_a == test.rig.peer.runtime_user / 10 == 6
        assert not starts(test.rig)


# Intent: pending actual cloud Start, unresolved mutations or undelivered observations block a current change.
# Why: changing limits must not interfere with work that was accepted under a different authority or current proof.
@pytest.mark.parametrize("debt", ["outbox", "mutation", "cloud-start", "return"])
async def test_unresolved_work_blocks_current_admission_without_effects(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    debt: str,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        journal = test.session.journal
        if debt == "outbox":
            journal.enqueue("Heartbeat", {})
        elif debt == "mutation":
            journal.mutation_intent("vendor", "DataTransfer", {"unresolved": True})
        elif debt == "cloud-start":
            await owner_start(test)
        else:
            test.controller.command(uuid.uuid4().hex, "pause", 0)
            await healthy(test.controller)
            test.controller.command(uuid.uuid4().hex, "app_control", test.controller.requests.intent().revision)
        before = copy.deepcopy(test.rig.bench.device_calls)
        with pytest.raises(RecoveryRequiredError):
            test.controller.command(
                uuid.uuid4().hex, "set_current", test.controller.requests.intent().revision, current_a=16
            )
        assert test.rig.bench.device_calls == before and test.controller.current.selected() == 6


# Intent: a newer Pause arriving during actual native validation preempts a current increase before positive send.
# Why: a current worker awaiting I/O must not restore a positive profile after a user requested zero.
async def test_pause_wins_during_current_profile_native_validation(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        entered, proceed = asyncio.Event(), asyncio.Event()
        actual = test.session.gateway.charger.validate_send
        assert actual is not None

        # Delay only the known higher-current profile's actual native boundary; keep all real validation intact.
        async def delayed(action: str, payload: dict[str, Any]) -> None:
            if (
                action == "SetChargingProfile"
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 16
            ):
                entered.set()
                await proceed.wait()
            await actual(action, payload)

        monkeypatch.setattr(test.session.gateway.charger, "validate_send", delayed)
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "set_current", 0, current_a=16)
        try:
            await asyncio.wait_for(entered.wait(), 3)
            test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
            count = len(test.rig.bench.received_limits)
            proceed.set()
            await test.controller.current.settle()
            assert (await healthy(test.controller))["local_hold_verified"]
            assert test.controller.requests.intent().mode == "paused"
            assert test.controller.requests.receipt(identifier)["state"] != "completed"
            assert all(value == 0 for value in test.rig.bench.received_limits[count:])
            assert not starts(test.rig)
        finally:
            proceed.set()


# Intent: a selected paused current survives process replacement while actual output remains zero.
# Why: restoring software state must neither lose the requested setting nor start or release a deliberately paused car.
async def test_selected_current_survives_restart_without_losing_paused_zero(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        await healthy(test.controller)
        await select(test, 16)
        await test.controller.tests.close()
        await test.controller.current.settle()
        await crash(test.session)
        replacement = managed_session(test.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            assert recovered.current.selected() == 16
            assert replacement.gateway.policy.max_current_a == replacement.gateway.controls.policy.max_current_a == 32
            async with test.rig.charger(replacement) as charger:
                await introduced(test.rig, replacement, charger, "selected-current-replacement")
                status = await healthy(recovered)
                assert status["requested_current_a"] == 16 and status["native_limit_a"] == 32
                assert status["local_hold_verified"] and status["station_ceiling_a"] == 0
                assert not starts(test.rig)
        finally:
            await recovered.current.settle()
            await recovered.tests.close()
            await replacement.close()


# Intent: an actual rejected higher-current profile cannot be reported as confirmed and must fall back to zero.
# Why: accepting the requested value before a failed physical write must not silently leave charging enabled.
async def test_rejected_profile_request_keeps_uncertain_receipt_and_actual_zero(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        actual = test.rig.bench.firmware.answer

        # Only the independent physical peer refuses this profile; actual TLS delivery and reply handling remain.
        def reject_selected(action: str, payload: dict[str, Any]) -> dict[str, Any]:
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 16:
                    return {"status": "Rejected"}
            return actual(action, payload)

        monkeypatch.setattr(test.rig.bench.firmware, "answer", reject_selected)
        identifier = await select(test, 16)
        await until(lambda: test.rig.bench.firmware.effective_limit() == 0)
        assert test.controller.requests.receipt(identifier)["state"] == "uncertain"
        assert test.controller.requests.intent().mode == "paused" and not starts(test.rig)
        assert test.session.journal.active() is None


# Intent: actual current beyond a selected lower profile trips supervision even though it remains below native HW32.
# Why: the independent hardware ceiling cannot disguise a charger that ignores its selected software limit.
async def test_actual_current_above_selected_limit_is_not_healthy_under_user32(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        await select(test, 16)
        test.controller.command(uuid.uuid4().hex, "start", test.controller.requests.intent().revision)
        await test.controller.tests.settle()
        await positive_flow(test)
        assert (await healthy(test.controller))["current_a"] == 16
        actual = test.rig.peer.native

        # The independent native meter reports excessive draw while a positive output exists, then real zero.
        def excessive_meter(key: str) -> Any:
            value = actual(key)
            if key == "METER" and test.rig.bench.firmware.current > 0:
                value[0]["meterData"]["cur"] = [170, 0, 0]
                value[0]["meterData"]["power"] = [4080, 0, 0, 4080]
            return value

        monkeypatch.setattr(test.rig.peer, "native", excessive_meter)
        for _ in range(3):
            assert (await test.controller.tick())["state"] == "degraded"
        assert (await test.controller.tick())["state"] == "attention"
        await until(lambda: test.rig.bench.firmware.effective_limit() == 0)
        assert test.session.journal.get("managed_control_fault") is not None
        assert len(starts(test.rig)) == 1 and test.rig.peer.runtime_user == 320


# Intent: real offline Start/Stop facts are retained, then delivered once after actual reconnection.
# Why: cloud loss must not disable local charging or turn durable queued history into invented cloud acknowledgements.
async def test_offline_local_start_and_stop_then_real_cloud_backfill(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        await select(test, 16)
        await healthy(test.controller)
        await until(lambda: test.session.journal.next_delivery() is None and not test.session.gateway.vendor._pending)
        await test.session.runtime.stop_vendor()
        assert not test.session.runtime.status.vendor_connected
        cloud_calls = len(test.rig.bench.calls)
        status = await locally_ready(test.controller)
        assert not status["cloud_connected"] and status["normal_start_ready"]
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", test.controller.requests.intent().revision)
        await test.controller.tests.settle()
        reply = await test.rig.bench.call(
            test.charger, "real-offline-local-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
        )
        local = reply[2]["transactionId"]
        assert reply[2]["idTagInfo"]["status"] == "Accepted"
        test.rig.peer.plan_session = (local, 987654)
        test.rig.peer.paused, test.rig.peer.permission_enabled = False, True
        test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
        test.rig.bench.firmware.fire_plan(9)
        status = await locally_ready(test.controller)
        assert status["current_a"] == 16 and status["power_w"] == 3840
        assert test.controller.requests.receipt(identifier)["state"] == "completed"
        assert test.session.journal.vendor_id(local) is None
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        await asyncio.wait_for(test.rig.bench.remote_stop.wait(), 3)
        await test.rig.bench.call(
            test.charger,
            "real-offline-local-stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        test.rig.peer.permission_enabled = False
        status = await locally_ready(test.controller)
        assert status["local_hold_verified"] and status["power_w"] == 0 and not status["cloud_connected"]
        assert len(test.rig.bench.calls) == cloud_calls
        rows = test.session.journal.db.execute(
            "SELECT action FROM outbox WHERE action IN ('StartTransaction','StopTransaction') ORDER BY seq"
        ).fetchall()
        assert [row[0] for row in rows] == ["StartTransaction", "StopTransaction"]
        facts = tuple(
            test.session.journal.db.execute("SELECT start,stop FROM transactions WHERE local=?", (local,)).fetchone()
        )

        test.session.runtime._runner = asyncio.create_task(test.session.runtime._vendor_runner())
        await until(
            lambda: (
                test.session.gateway.vendor_registration == "Accepted" and test.session.journal.next_delivery() is None
            )
        )
        assert test.session.journal.vendor_id(local) is not None
        assert (
            tuple(
                test.session.journal.db.execute(
                    "SELECT start,stop FROM transactions WHERE local=?", (local,)
                ).fetchone()
            )
            == facts
        )
        lifecycle = [
            frame[2]
            for frame in test.rig.bench.calls[cloud_calls:]
            if frame[0] == 2 and frame[2] in ("StartTransaction", "StopTransaction")
        ]
        assert lifecycle == ["StartTransaction", "StopTransaction"] and len(starts(test.rig)) == 1
