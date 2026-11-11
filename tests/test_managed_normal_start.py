"""Regular six-ampere Start uses real TLS/native authority without inheriting a two-minute charge timer."""

from __future__ import annotations

import asyncio
import copy
import ssl
import uuid
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
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

# A distinct command tag identifies even an owner lifecycle that closes entirely between native observations.
owner_rig = partial(ready, owner_tag="APP_OWNER")


# A known real transaction may wait at zero watts while the vehicle decides whether to accept its six-ampere offer.
async def zero_power_session(test: ChargeRig) -> int:
    local = await positive_flow(test)
    test.rig.peer.paused = True
    test.rig.bench.cp, test.rig.bench.connection = "B", "Plug"
    return local


# Submit genuine device closure without inventing a remote Stop or changing the controller's requested mode.
async def natural_stop(test: ChargeRig, local: int) -> None:
    result = await test.rig.bench.call(
        test.charger,
        "natural-regular-stop",
        "StopTransaction",
        {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
    )
    assert result[0] == 3
    test.rig.peer.permission_enabled = False
    await until(lambda: test.session.journal.active() is None and test.session.journal.next_delivery() is None)


# Intent: explicit regular Start works from app authority or a verified pause and continues beyond both short budgets.
# Why: normal charging must not inherit the two-minute test cutoff or a retired startup permission after real Start.
@pytest.mark.parametrize("initial", ["app", "paused"])
async def test_regular_start_continues_until_explicit_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    initial: str,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, duration=0.04, startup=0.6) as test:
        controller = test.controller
        if initial == "paused":
            controller.command(uuid.uuid4().hex, "pause", 0)
            assert (await healthy(controller))["local_hold_verified"]
        before = controller.requests.intent()
        identifier = uuid.uuid4().hex
        controller.command(identifier, "start", before.revision)
        await controller.tests.settle()
        assert controller.requests.intent().mode == "app"
        assert controller.requests.intent().revision == before.revision + 1
        assert len(starts(test.rig)) == 1
        local = await positive_flow(test)
        assert (await healthy(controller))["power_w"] == 1440
        assert controller.requests.receipt(identifier)["state"] == "completed"

        await asyncio.sleep(0.75)
        assert controller.requests.intent().mode == "app"
        assert test.rig.bench.firmware.current == 6 and test.session.journal.active() == local
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert controller.tests.lease is None
        assert not test.session.gateway.policy.allow_local_start
        with pytest.raises(DeniedError, match="local Start is disabled"):
            await test.session.command({"command": "start"})
        controller.command(uuid.uuid4().hex, "pause", controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert test.rig.bench.firmware.effective_limit() == 0 and len(starts(test.rig)) == 1


# Intent: a known real session completes the Start receipt even when the car draws no current.
# Why: protocol acceptance and a real transaction must not be misreported as positive charging power.
async def test_regular_start_confirms_known_zero_power_session_without_invented_flow(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        assert test.controller.requests.receipt(identifier)["state"] == "accepted"
        local = await zero_power_session(test)
        status = await healthy(test.controller)
        receipt = test.controller.requests.receipt(identifier)
        assert status["charging_session_open"] and status["power_w"] == status["current_a"] == 0
        assert receipt["state"] == "completed" and not receipt["result"].get("positive_power_verified", False)
        assert test.controller.tests.lease is None
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: a Start acknowledged without an actual transaction expires to zero without a guessed transaction Stop.
# Why: normal charging loses its startup timer only after genuine known-session evidence, not an RPC reply alone.
async def test_no_session_startup_timeout_holds_zero_without_guessed_stop(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, duration=0.04, startup=0.4) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        assert len(starts(test.rig)) == 1 and test.session.journal.active() is None
        await until(lambda: test.controller.requests.intent().mode == "paused")
        await until(lambda: test.rig.bench.firmware.effective_limit() == 0)
        if test.controller.pause_task is not None:
            await test.controller.pause_task
        assert test.controller.requests.receipt(identifier)["state"] != "completed"
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert test.session.journal.active() is None and test.rig.bench.firmware.current == 0
        assert len(starts(test.rig)) == 1


# Intent: repeated request identifiers retain their existing receipt while an obsolete new request cannot start again.
# Why: lost replies and stale dashboards must not replay a regular Start or manufacture a second session.
async def test_regular_start_duplicate_and_stale_revision_do_not_repeat_rpc(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        identifier = uuid.uuid4().hex
        accepted = test.controller.command(identifier, "start", 0)
        assert test.controller.command(identifier, "start", 0) == accepted
        await test.controller.tests.settle()
        local = await zero_power_session(test)
        await healthy(test.controller)
        completed = test.controller.requests.receipt(identifier)
        assert test.controller.command(identifier, "start", 0) == completed
        before = copy.deepcopy(test.rig.bench.device_calls)
        with pytest.raises(RecoveryRequiredError):
            test.controller.command(uuid.uuid4().hex, "start", 0)
        with pytest.raises(SecurityError):
            test.controller.command(identifier, "pause", 0)
        assert test.rig.bench.device_calls == before and len(starts(test.rig)) == 1
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: newer Pause defeats a regular Start before its worker, while queued, and during real native send validation.
# Why: resuming a persistent pause must not override a Stop that arrived during its asynchronous handover.
@pytest.mark.parametrize("boundary", ["immediate", "owned_task", "native_send"])
async def test_newer_pause_preempts_regular_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        await healthy(test.controller)
        revision = test.controller.requests.intent().revision
        entered, proceed = asyncio.Event(), asyncio.Event()
        if boundary == "owned_task":
            actual = test.session.gateway.controls._start

            # Only scheduling is delayed; the actual owned operation still validates every real send.
            async def delayed_start(tag: str, **fields: Any) -> Any:
                entered.set()
                await proceed.wait()
                return await actual(tag, **fields)

            monkeypatch.setattr(test.session.gateway.controls, "_start", delayed_start)
        elif boundary == "native_send":
            actual_validate = test.session.gateway.charger.validate_send
            assert actual_validate is not None

            # Preserve independent native HTTP evidence while delaying the final Start send boundary.
            async def delayed_validate(action: str, payload: dict[str, Any]) -> None:
                if action == "RemoteStartTransaction":
                    entered.set()
                    await proceed.wait()
                await actual_validate(action, payload)

            monkeypatch.setattr(test.session.gateway.charger, "validate_send", delayed_validate)
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", revision)
        try:
            if boundary != "immediate":
                await asyncio.wait_for(entered.wait(), 3)
            test.controller.command(uuid.uuid4().hex, "pause", revision)
            limits = len(test.rig.bench.received_limits)
            proceed.set()
            await test.controller.tests.settle()
            await until(lambda: test.rig.bench.firmware.effective_limit() == 0)
            if test.controller.pause_task is not None:
                await test.controller.pause_task
            assert test.controller.requests.intent().mode == "paused"
            assert test.controller.requests.receipt(identifier)["state"] != "completed"
            assert not starts(test.rig)
            assert all(value == 0 for value in test.rig.bench.received_limits[limits:])
            if boundary == "native_send":
                assert not test.session.journal.get("control")["start_pending"]
                assert test.session.journal.get("local_start_receipt")["stage"] == "unsent"
                assert test.controller.requests.intent().mode == "paused"
                assert (await healthy(test.controller))["local_hold_verified"]
                returning = uuid.uuid4().hex
                test.controller.command(returning, "app_control", test.controller.requests.intent().revision)
                assert (await healthy(test.controller))["owner_control_enabled"]
                assert test.controller.requests.receipt(returning)["state"] == "completed" and not starts(test.rig)
        finally:
            proceed.set()


# Intent: spontaneous car/session closure completes normal charging while retaining app/schedule authority.
# Why: the normal Start path must not install a persistent pause merely because a car finishes charging.
async def test_natural_session_stop_keeps_app_control_without_forced_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        test.controller.command(uuid.uuid4().hex, "start", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
        intent = test.controller.requests.intent()
        calls = len(test.rig.bench.device_calls)
        await natural_stop(test, local)
        status = await healthy(test.controller)
        assert test.controller.requests.intent() == intent and intent.mode == "app"
        assert status["owner_control_enabled"] and status["station_ceiling_a"] == 6
        assert status["power_w"] == 0 and not status["charging_session_open"]
        assert test.controller.tests.state()["state"] == "completed"
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls[calls:])
        assert len(starts(test.rig)) == 1


# Intent: a local session can stop and a new app session can start between polls without inheriting local attribution.
# Why: a fresh nonzero meter alone cannot extend an old normal Start record over another genuine transaction.
async def test_between_poll_app_session_is_not_attributed_to_completed_local_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        test.controller.command(uuid.uuid4().hex, "start", 0)
        await test.controller.tests.settle()
        previous = await positive_flow(test)
        await healthy(test.controller)
        assert test.controller.tests.state()["local_transaction"] == previous
        assert test.controller.tests.state()["state"] == "running"
        intent = test.controller.requests.intent()

        # Both real lifecycles occur before the next service tick observes native state.
        await natural_stop(test, previous)
        await owner_start(test)
        answer = await test.rig.bench.call(
            test.charger,
            "new-app-session-between-polls",
            "StartTransaction",
            START | {"idTag": "CLOUD_OWNER", "meterStart": 1100},
        )
        current = answer[2]["transactionId"]
        assert answer[2]["idTagInfo"]["status"] == "Accepted" and current != previous
        await until(
            lambda: test.session.journal.vendor_id(current) is not None and test.session.journal.next_delivery() is None
        )
        test.rig.peer.paused, test.rig.peer.permission_enabled = False, True
        test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
        test.rig.bench.firmware.fire_plan(9)

        status = await healthy(test.controller)
        assert status["power_w"] == 1440 and status["charging_session_open"]
        assert status["local_charge"] == {"state": "completed", "charging": False}
        assert test.controller.tests.state()["local_transaction"] == previous
        assert test.controller.requests.intent() == intent and intent.mode == "app"
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert len(starts(test.rig)) == 2 and test.session.journal.active() == current

        test.controller.command(uuid.uuid4().hex, "pause", intent.revision)
        assert (await actual_stop(test, current))["local_hold_verified"]


# Intent: a genuine local Start and Stop before the first poll completes without installing a later timeout pause.
# Why: actual accepted lifecycle facts remain authoritative even when no poll observed the short session open.
async def test_complete_local_lifecycle_before_first_poll_retires_startup_without_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, duration=0.04, startup=0.6) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        assert test.controller.tests.state()["local_transaction"] == local
        await natural_stop(test, local)
        assert test.controller.requests.receipt(identifier)["state"] == "accepted"

        # This is the first supervisor poll after either actual StartTransaction or actual StopTransaction.
        status = await healthy(test.controller)
        assert status["local_charge"] == {"state": "completed", "charging": False}
        assert status["desired_control"] == "app" and status["power_w"] == 0
        receipt = test.controller.requests.receipt(identifier)
        assert receipt["state"] == "completed" and receipt["result"]["local_session_verified"]
        assert receipt["result"]["charging"] is False
        assert test.controller.tests.lease is None

        await asyncio.sleep(0.75)
        assert test.controller.requests.intent().mode == "app"
        assert test.rig.bench.firmware.effective_limit() == 6
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert len(starts(test.rig)) == 1 and test.session.journal.active() is None


# Intent: a complete local lifecycle followed by a legitimate app or same-tag timer session never rebinds the old Start.
# Why: source changes and tag reuse between polls cannot make an expired local request own or stop a later session.
@pytest.mark.parametrize("source", ["cloud", "native-timer"])
async def test_complete_lifecycle_then_external_start_before_first_poll_keeps_new_session(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, duration=0.04) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        previous = await positive_flow(test)
        assert test.controller.tests.state()["local_transaction"] == previous
        await natural_stop(test, previous)
        intent = test.controller.requests.intent()

        # No service tick has yet observed either lifecycle of the original local session.
        tag = "CLOUD_OWNER" if source == "cloud" else "NATIVE_TIMER"
        if source == "cloud":
            await owner_start(test)
        else:
            authorize = await test.rig.bench.call(
                test.charger, "subsequent-native-timer-authorize", "Authorize", {"idTag": tag}
            )
            assert authorize[2]["idTagInfo"]["status"] == "Accepted"
        reply = await test.rig.bench.call(
            test.charger,
            "subsequent-session-before-first-poll",
            "StartTransaction",
            START | {"idTag": tag, "meterStart": 1100},
        )
        current = reply[2]["transactionId"]
        assert reply[2]["idTagInfo"]["status"] == "Accepted" and current != previous
        await until(
            lambda: test.session.journal.vendor_id(current) is not None and test.session.journal.next_delivery() is None
        )
        assert test.controller.tests.state()["local_transaction"] == previous
        test.rig.peer.paused, test.rig.peer.permission_enabled = False, True
        test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
        test.rig.bench.firmware.fire_plan(9)

        status = await healthy(test.controller)
        assert status["power_w"] == 1440 and status["charging_session_open"]
        assert status["local_charge"] == {"state": "completed", "charging": False}
        receipt = test.controller.requests.receipt(identifier)
        assert receipt["state"] == "completed" and receipt["result"]["local_session_verified"]
        assert receipt["result"]["charging"] is False
        assert test.controller.requests.intent() == intent and intent.mode == "app"
        assert test.controller.tests.lease is None
        await asyncio.sleep(0.1)
        assert test.rig.bench.firmware.current == 6 and test.session.journal.active() == current
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert len(starts(test.rig)) == (2 if source == "cloud" else 1)

        test.controller.command(uuid.uuid4().hex, "pause", intent.revision)
        assert (await actual_stop(test, current))["local_hold_verified"]


# Intent: the timer itself notices an actual closed local lifecycle when no supervisor poll ever observed it.
# Why: expiry must retire a completed request instead of installing a persistent pause after the car already stopped.
async def test_closed_lifecycle_retired_by_timer_without_any_poll_or_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, duration=0.04, startup=0.8) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        await natural_stop(test, local)
        intent = test.controller.requests.intent()
        assert test.controller.requests.receipt(identifier)["state"] == "accepted"

        # The independent startup timer must complete this request without any call to controller.tick().
        await until(lambda: test.controller.tests.lease is None)
        receipt = test.controller.requests.receipt(identifier)
        assert receipt["state"] == "completed" and receipt["result"]["local_session_verified"]
        assert test.controller.tests.normal_summary() == {"state": "completed", "charging": False}
        assert test.controller.requests.intent() == intent and intent.mode == "app"
        assert test.rig.bench.firmware.effective_limit() == 6
        assert test.controller.pause_task is None and not test.session.journal.get("control")["inhibit"]
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert len(starts(test.rig)) == 1


# Intent: crashing an established normal charge creates a persisted pause and stops only the known real session.
# Why: a running normal record must not become automatic Start authority after service replacement.
async def test_crash_during_regular_charge_recovers_to_pause_without_replay(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
        await test.controller.tests.close()
        await crash(test.session)
        replacement = managed_session(test.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            assert recovered.requests.intent().mode == "paused"
            assert recovered.tests.normal_summary()["charging"] is False
            async with test.rig.charger(replacement) as charger:
                await introduced(test.rig, replacement, charger, "normal-start-replacement")
                recovered_test = ChargeRig(test.rig, replacement, recovered, charger)
                stopped = await actual_stop(recovered_test, local)
                assert stopped["local_hold_verified"] and stopped["power_w"] == 0
                assert stopped["local_charge"]["charging"] is False
                assert len(starts(test.rig)) == 1 and test.rig.bench.firmware.effective_limit() == 0
                assert recovered.requests.intent().mode == "paused" and recovered.tests.lease is None
        finally:
            await recovered.tests.close()
            if recovered.pause_task is not None:
                await asyncio.gather(recovered.pause_task, return_exceptions=True)
            await replacement.close()


# Intent: regular admission preserves one management slot for a later explicit Pause.
# Why: a continuously running normal charge must not fill the receipt table so completely that Stop is refused.
@pytest.mark.parametrize("count", [4094, 4095])
async def test_normal_start_reserves_future_explicit_pause_capacity(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    count: int,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        journal = test.session.journal
        with journal.atomic():
            journal.db.executemany(
                "INSERT INTO managed_requests VALUES(?,'pause',0,'completed','{}')",
                [(format(index, "032x"),) for index in range(1, count + 1)],
            )
        identifier = uuid.uuid4().hex
        if count == 4095:
            before = copy.deepcopy(test.rig.bench.device_calls)
            with pytest.raises(RecoveryRequiredError):
                test.controller.command(identifier, "start", 0)
            assert test.rig.bench.device_calls == before and not starts(test.rig)
            return
        test.controller.command(identifier, "start", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4095
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4096


# Intent: the authenticated normal Start protocol rejects caller-selected current, tag or duration before acceptance.
# Why: exposing a regular button must not expose a general charger RPC or override the fixed six-ampere policy.
@pytest.mark.parametrize("extra", [{"current_a": 32}, {"id_tag": "UNAPPROVED"}, {"duration_seconds": 99999}])
async def test_normal_start_api_rejects_arbitrary_parameters_without_effects(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
    extra: dict[str, Any],
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as test:
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
                result = await call(socket, "start", revision=0, **extra)
                assert result == {"ok": False, "error": "invalid_request"}
                assert test.rig.bench.device_calls == before and not starts(test.rig)
                assert test.session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0
        finally:
            await api.close()


# Intent: Return is rejected while already in app mode and remains an explicit valid transition from verified pause.
# Why: normal Start is a separate action; redundant Return must not become another release or Start route.
async def test_return_requires_verified_paused_mode(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        before = copy.deepcopy(test.rig.bench.device_calls)
        with pytest.raises(RecoveryRequiredError):
            test.controller.command(uuid.uuid4().hex, "app_control", 0)
        assert test.rig.bench.device_calls == before
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        assert (await healthy(test.controller))["local_hold_verified"]
        revision = test.controller.requests.intent().revision
        request = uuid.uuid4().hex
        test.controller.command(request, "app_control", revision)
        status = await healthy(test.controller)
        assert status["desired_control"] == "app" and status["station_ceiling_a"] == 6
        assert test.controller.requests.receipt(request)["state"] == "completed"
        assert not starts(test.rig)
