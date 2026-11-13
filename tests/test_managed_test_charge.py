"""One-shot managed charging permissions exercise real TLS peers and independent native HTTP evidence."""

from __future__ import annotations

import asyncio
import copy
import sqlite3
import ssl
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp_hub_core.prototype import managed_test_charge as charge_tests
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.test_managed_proxy import OwnerBench, healthy, introduced, managed_bench, managed_session, starts
from tests.test_native_bounded_recovery import crash
from tests.test_native_bounded_session import ConnectedBench
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


@dataclass
class ChargeRig:
    """One real managed service and independent charger/cloud/native peers retain their own state."""

    rig: ConnectedBench
    session: TrialSession
    controller: ManagedController
    charger: Any


# Keep test durations short while using the real protocol, native observer, ledger and physical profile checks.
@asynccontextmanager
async def ready(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    duration: float = 0.15,
    startup: float = 3,
    owner_tag: str | None = None,
) -> AsyncIterator[ChargeRig]:
    monkeypatch.setattr(charge_tests, "DURATION", duration)
    monkeypatch.setattr(charge_tests, "STARTUP_BUDGET", startup)
    async with managed_bench(directory, contexts, port, monkeypatch) as rig:
        session = managed_session(rig)
        if owner_tag is not None:
            gateway = session.gateway
            gateway.policy = replace(gateway.policy, allowed_tags=frozenset({owner_tag, *gateway.policy.allowed_tags}))
            gateway.controls.policy = gateway.policy
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "test-charge-boot")
            await healthy(controller)
            try:
                yield ChargeRig(rig, session, controller, charger)
            finally:
                await controller.tests.close()
                if controller.pause_task is not None:
                    await asyncio.gather(controller.pause_task, return_exceptions=True)
                await session.close()


# Create an actual owner-cloud Start when requested; arming alone never issues that command.
async def owner_start(test: ChargeRig) -> None:
    assert isinstance(test.rig.bench, OwnerBench)
    reply = await test.rig.bench.cloud_call(
        "cloud-two-minute-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
    )
    assert reply[0] == 3 and reply[2]["status"] == "Accepted"


# Send a real charger lifecycle and let the independent native peer report genuine bounded power.
async def positive_flow(
    test: ChargeRig,
    source: str = "local",
    *,
    call_id: str = "actual-test-start",
    meter_start: int | None = None,
    timestamp: str | None = None,
) -> int:
    if source == "daily":
        tag = "NATIVE_TIMER"
    elif source == "local":
        tag = starts(test.rig)[-1][3]["idTag"]
    else:
        tag = "CLOUD_OWNER"
    payload = START | {"idTag": tag} | ({} if meter_start is None else {"meterStart": meter_start})
    if timestamp is not None:
        payload["timestamp"] = timestamp
    answer = await test.rig.bench.call(test.charger, call_id, "StartTransaction", payload)
    assert answer[0] == 3, answer
    assert answer[2]["idTagInfo"]["status"] == "Accepted"
    local = answer[2]["transactionId"]
    test.rig.peer.plan_session = (local, 9 if source == "daily" else 987654)
    await until(
        lambda: test.session.journal.vendor_id(local) is not None and test.session.journal.next_delivery() is None
    )
    test.rig.peer.paused, test.rig.peer.permission_enabled = False, True
    test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
    test.rig.bench.firmware.fire_plan(9)
    return local


# A real StopTransaction closes the independent peer only after the physical Stop command has arrived.
async def actual_stop(test: ChargeRig, local: int) -> dict[str, Any]:
    await asyncio.wait_for(test.rig.bench.remote_stop.wait(), 3)
    assert test.rig.bench.firmware.current == 0
    reply = await test.rig.bench.call(
        test.charger,
        "actual-test-stop",
        "StopTransaction",
        {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
    )
    assert reply[0] == 3
    test.rig.peer.permission_enabled = False
    await until(lambda: test.session.journal.active() is None and test.session.journal.next_delivery() is None)
    return await healthy(test.controller)


# Intent: one explicit local request starts one genuine bounded charge, then an independent timer closes at zero.
# Why: acceptance, actual Start, positive power and final Stop are separate physical and durable facts.
async def test_local_charge_confirms_power_then_stops_without_further_tick(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        controller, session = test.controller, test.session
        identifier = uuid.uuid4().hex
        receipt = controller.command(identifier, "start_test", 0)
        assert receipt["state"] == "accepted"
        assert controller.command(identifier, "start_test", 0) == receipt
        await controller.tests.settle()
        assert len(starts(test.rig)) == 1 and session.gateway.controls.desired() == 6
        assert controller.requests.receipt(identifier)["state"] == "accepted"
        count = len(test.rig.bench.device_calls)
        with pytest.raises(DeniedError, match="local Start is disabled"):
            await session.command({"command": "start"})
        assert len(test.rig.bench.device_calls) == count
        local = await positive_flow(test)
        status = await healthy(controller)
        assert status["power_w"] == 1440 and status["current_a"] == status["native_limit_a"] == 6
        assert controller.requests.receipt(identifier)["state"] == "completed"
        assert controller.requests.receipt(identifier)["result"]["positive_power_verified"]
        assert controller.tests.state()["state"] == "charging"

        # No service tick runs between positive-flow verification and the actual independently timed Stop.
        await asyncio.wait_for(test.rig.bench.remote_stop.wait(), 3)
        assert controller.requests.intent().mode == "paused"
        assert controller.tests.state()["stop_reason"] == "deadline"
        stopped = await actual_stop(test, local)
        assert stopped["local_hold_verified"] and stopped["power_w"] == 0
        assert not stopped["charging_session_open"] and stopped["station_ceiling_a"] == 0
        assert controller.tests.state()["state"] == "completed"
        assert len(starts(test.rig)) == 1 and not session.gateway.policy.allow_local_start


# Intent: arming the cloud path sends no Start; only a real subsequent cloud mutation proves that source.
# Why: a native timetable or a local Start must not be reported as successful app/cloud compatibility.
async def test_cloud_arm_waits_for_real_cloud_start_and_positive_native_power(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=10) as test:
        identifier = uuid.uuid4().hex
        reply = test.controller.command(identifier, "arm_cloud_test", 0)
        assert reply["result"] == {"armed": True, "charging_started": False}
        assert test.controller.tests.state()["state"] == "armed" and not starts(test.rig)
        await owner_start(test)
        assert len(starts(test.rig)) == 1
        local = await positive_flow(test, "cloud")
        status = await healthy(test.controller)
        assert status["power_w"] == 1440 and test.controller.tests.state()["state"] == "charging"
        assert test.controller.tests.state()["start_source_verified"]
        assert test.controller.tests.summary()["source"] == "cloud"
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert len(starts(test.rig)) == 1


# Intent: invalid current revisions and existing unresolved authority fail synchronously with no physical mutation.
# Why: accepting a test is not a way to skip pending work or borrow an older app-control permission.
@pytest.mark.parametrize("unsafe", ["stale", "paused", "pending-outbox", "uncertain"])
async def test_unsafe_admission_has_no_charger_effects(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        journal = test.session.journal
        revision = 1 if unsafe == "stale" else 0
        if unsafe == "paused":
            test.controller.command(uuid.uuid4().hex, "pause", 0)
            await healthy(test.controller)
        elif unsafe == "pending-outbox":
            journal.enqueue("Heartbeat", {})
        elif unsafe == "uncertain":
            journal.put("control", journal.get("control") | {"uncertain": True})
        calls = copy.deepcopy(test.rig.bench.device_calls)
        state = copy.deepcopy(journal.get("managed_test_charge"))
        with pytest.raises(RecoveryRequiredError):
            test.controller.command(uuid.uuid4().hex, "start_test", revision)
        assert test.rig.bench.device_calls == calls and not starts(test.rig)
        assert journal.get("managed_test_charge") == state


# Intent: a newer Pause wins both before the worker starts and while a real native send check is awaiting evidence.
# Why: queued or asynchronously validated test Start must not escape a Stop received during the handover.
@pytest.mark.parametrize("boundary", ["immediate", "owned_task", "native_send"])
async def test_pause_preempts_queued_start_before_actual_send(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        entered, proceed = asyncio.Event(), asyncio.Event()
        if boundary == "owned_task":
            actual = test.session.gateway.controls._start

            # Delay scheduling only; the real owned operation still performs its own guard before any effect.
            async def delayed_start(tag: str, **fields: Any) -> Any:
                entered.set()
                await proceed.wait()
                return await actual(tag, **fields)

            monkeypatch.setattr(test.session.gateway.controls, "_start", delayed_start)
        elif boundary == "native_send":
            actual_validate = test.session.gateway.charger.validate_send
            assert actual_validate is not None

            # Preserve independent native HTTP validation while pausing just before the real Start send.
            async def delayed_validate(action: str, payload: dict[str, Any]) -> None:
                if action == "RemoteStartTransaction":
                    entered.set()
                    await proceed.wait()
                await actual_validate(action, payload)

            monkeypatch.setattr(test.session.gateway.charger, "validate_send", delayed_validate)
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", 0)
        try:
            if boundary != "immediate":
                await asyncio.wait_for(entered.wait(), 3)
            test.controller.command(uuid.uuid4().hex, "pause", 0)
            count = len(test.rig.bench.received_limits)
            proceed.set()
            await test.controller.tests.settle()
            if boundary == "native_send":
                await until(lambda: test.rig.bench.firmware.effective_limit() == 0)
                if test.controller.pause_task is not None:
                    await test.controller.pause_task
                control = test.session.journal.get("control")
                assert (
                    not control["start_pending"]
                    and test.session.journal.get("local_start_receipt")["stage"] == "unsent"
                )
                assert test.controller.requests.intent().mode == "paused"
                assert not starts(test.rig)
                native = await test.controller.native.read()
                assert native.physical.power_w == 0 and not native.physical.transaction_open
                assert all(limit == 0 for limit in test.rig.bench.received_limits[count:])
                # The newer Pause remains in force until the owner Returns after fresh physical verification.
                request = uuid.uuid4().hex
                test.controller.command(request, "app_control", test.controller.requests.intent().revision)
                await healthy(test.controller)
                await healthy(test.controller)
                assert test.controller.requests.intent().mode == "app"
                assert test.controller.requests.receipt(request)["state"] == "completed"
                assert test.rig.bench.firmware.effective_limit() == 6 and not starts(test.rig)
                return
            status = await healthy(test.controller)
            assert status["desired_control"] == "paused" and status["local_hold_verified"]
            assert not starts(test.rig) and test.rig.bench.firmware.effective_limit() == 0
            assert all(limit == 0 for limit in test.rig.bench.received_limits[count:])
            assert test.controller.requests.receipt(identifier)["state"] != "completed"
        finally:
            proceed.set()


# Intent: stale, unplugged or unknown-session native evidence refuses admission before any Stop or positive effect.
# Why: a new test must not interrupt a charge which began externally after the last service poll.
@pytest.mark.parametrize("unsafe", ["runtime-limit", "unplugged", "unknown-session"])
async def test_fresh_native_admission_failure_has_no_external_control_effects(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        if unsafe == "runtime-limit":
            test.rig.peer.runtime_user = 70
        elif unsafe == "unplugged":
            test.rig.bench.cp, test.rig.bench.connection = "A", "Unplug"
        else:
            test.rig.bench.firmware.active = 9999999
            test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
            test.rig.bench.history.append({"transactionId": 9999999, "origin": "independent-external-session"})
        calls = copy.deepcopy(test.rig.bench.device_calls)
        before = copy.deepcopy(test.session.journal.get("control"))
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", 0)
        await test.controller.tests.settle()
        assert test.controller.requests.receipt(identifier)["state"] == "failed"
        assert test.rig.bench.device_calls == calls and not starts(test.rig)
        assert test.session.journal.get("control") == before
        assert test.controller.requests.intent().mode == "app"


# Intent: the final free receipt slot may accept a test without depriving its deadline of Stop capacity.
# Why: bounded management storage must never turn a successfully started charge into one without its safety timer.
async def test_deadline_stop_has_reserved_capacity_after_last_request_slot_is_used(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        journal = test.session.journal
        with journal.atomic():
            journal.db.executemany(
                "INSERT INTO managed_requests VALUES(?,'pause',0,'completed','{}')",
                [(format(index, "032x"),) for index in range(1, 4096)],
            )
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4096
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert test.controller.tests.state()["stop_reason"] == "deadline"
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4096
        assert test.rig.bench.firmware.effective_limit() == 0 and len(starts(test.rig)) == 1


# Intent: a real transient SQLite write failure rolls back the first Stop intent and the independent timer retries.
# Why: a storage error at the deadline cannot renew Start authority or leave an already flowing charge without Stop.
async def test_timer_retries_atomic_stop_after_one_sqlite_failure_without_tick(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=0.3) as test:
        journal = test.session.journal
        test.controller.command(uuid.uuid4().hex, "start_test", 0)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
        before = copy.deepcopy(journal.get("control"))
        failure = asyncio.Event()
        attempts = []

        # The trigger fails an actual database statement once; subsequent writes use ordinary SQLite unchanged.
        def fail_once() -> int:
            attempts.append(True)
            if len(attempts) == 1:
                failure.set()
                raise sqlite3.OperationalError("synthetic first timed Stop write failure")
            return 0

        journal.db.create_function("test_timed_stop_failure", 0, fail_once)
        journal.db.execute(
            "CREATE TRIGGER fail_first_timed_stop BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_test_charge' AND json_extract(NEW.value,'$.state')='stopping' "
            "BEGIN SELECT test_timed_stop_failure(); END"
        )
        await asyncio.wait_for(failure.wait(), 3)
        assert journal.get("control") == before
        assert test.controller.requests.intent().mode == "app"
        assert test.controller.tests.state()["state"] == "charging"
        lease = test.controller.tests.lease
        assert lease is not None and lease.deadline <= time.monotonic()
        with pytest.raises(DeniedError):
            test.controller.tests.guard(lease)

        # No tick performs recovery: the timer itself repeats the atomic hold and launches the ordinary Stop.
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert len(attempts) >= 2 and len(starts(test.rig)) == 1
        assert test.rig.bench.firmware.effective_limit() == 0


# Intent: a native timetable cannot satisfy a cloud test, even after the charger rejected an actual cloud Start.
# Why: a confirmed RPC receipt can contain Rejected, and unrelated positive power is not evidence of cloud control.
@pytest.mark.parametrize("cloud_attempt", ["none", "rejected"])
async def test_unrelated_timer_flow_cannot_prove_cloud_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    cloud_attempt: str,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=10) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "arm_cloud_test", 0)
        if cloud_attempt == "rejected":
            actual = test.rig.bench.firmware.answer

            # Only the independent physical peer rejects this Start; actual encrypted RPC delivery is retained.
            def rejected_start(action: str, payload: dict[str, Any]) -> dict[str, Any]:
                return {"status": "Rejected"} if action == "RemoteStartTransaction" else actual(action, payload)

            monkeypatch.setattr(test.rig.bench.firmware, "answer", rejected_start)
            assert isinstance(test.rig.bench, OwnerBench)
            reply = await test.rig.bench.cloud_call(
                "rejected-cloud-test", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
            )
            assert reply[2]["status"] == "Rejected"
            assert not test.session.journal.get("vendor_start_grant")
        authorize = await test.rig.bench.call(
            test.charger, "independent-timer-authorize", "Authorize", {"idTag": "NATIVE_TIMER"}
        )
        assert authorize[2]["idTagInfo"]["status"] == "Accepted"
        local = await positive_flow(test, "daily")
        assert (await healthy(test.controller))["power_w"] == 1440
        assert test.controller.tests.state()["state"] == "armed"
        assert not test.controller.tests.state().get("start_source_verified")
        assert not test.controller.requests.receipt(identifier)["result"].get("positive_power_verified")
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: a lease bound to another identity, capability, introduction or control generation cannot send a Start.
# Why: persisted-looking request data cannot replace the current physical/service authorization at send time.
@pytest.mark.parametrize("binding", ["identity", "spec", "epoch", "control_revision", "tag_revoked"])
async def test_start_guard_rejects_wrong_bindings_and_revoked_tag(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    binding: str,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", 0)
        lease = test.controller.tests.lease
        assert lease is not None
        before = len(test.rig.bench.device_calls)
        if binding in ("identity", "spec"):
            setattr(lease, binding, fingerprint("another synthetic binding"))
        elif binding in ("epoch", "control_revision"):
            setattr(lease, binding, getattr(lease, binding) + 1)
        else:
            test.session.gateway.auth_info.put("NATIVE_TIMER", {"status": "Blocked"})
        await test.controller.tests.settle()
        assert not starts(test.rig)
        assert test.controller.requests.receipt(identifier)["state"] != "completed"
        if binding != "tag_revoked":
            assert len(test.rig.bench.device_calls) == before
            assert test.controller.requests.receipt(identifier)["state"] == "failed"
        else:
            assert (await healthy(test.controller))["local_hold_verified"]
            assert test.rig.bench.firmware.effective_limit() == 0


# Intent: cancellation of the initiating client task cannot cancel the accepted physical Start or its Stop timer.
# Why: management-client disappearance must not strand a partially applied charging operation.
async def test_client_disappearance_leaves_one_service_owned_start_and_deadline(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        accepted = asyncio.Event()

        # Model the caller task disappearing immediately after the real service has durably accepted its command.
        async def client() -> None:
            test.controller.command(uuid.uuid4().hex, "start_test", 0)
            accepted.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(client())
        await accepted.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await test.controller.tests.settle()
        assert len(starts(test.rig)) == 1
        local = await positive_flow(test)
        await healthy(test.controller)
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert len(starts(test.rig)) == 1


# Intent: late first power is admitted only within startup time, and its Stop deadline cannot exceed the hard budget.
# Why: power appearing near a timeout must not renew an expired permission or create a longer charge than authorized.
@pytest.mark.parametrize("source", ["local", "cloud"])
@pytest.mark.parametrize("elapsed", [59, 61, 179])
async def test_late_first_flow_respects_startup_and_immutable_hard_deadline(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    elapsed: int,
) -> None:
    clock = [1000.0]
    monkeypatch.setattr(charge_tests, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=120, startup=60) as test:
        identifier = uuid.uuid4().hex
        action = "start_test" if source == "local" else "arm_cloud_test"
        test.controller.command(identifier, action, 0)
        await test.controller.tests.settle()
        if source == "cloud":
            await owner_start(test)
        local = await positive_flow(test, source)
        lease = test.controller.tests.lease
        assert lease is not None and lease.deadline == 1060 and lease.hard_deadline == 1180
        clock[0] += elapsed
        native = await test.controller.native.read()
        test.controller.tests.observed(native)
        if elapsed == 59:
            assert test.controller.tests.state()["state"] == "charging"
            assert lease.deadline == (1179 if source == "local" else 1180)
            assert lease.deadline <= lease.hard_deadline == 1180
            test.controller.command(uuid.uuid4().hex, "pause", 0)
        else:
            assert test.controller.tests.state()["state"] == "stopping"
            assert test.controller.requests.intent().mode == "paused"
            assert lease.deadline == 1060 and lease.hard_deadline == 1180
            assert not test.controller.requests.receipt(identifier)["result"].get("positive_power_verified")
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: an interrupted accepted test recovers as a durable pause and never repeats its already acknowledged Start.
# Why: a persisted test record is evidence of prior work, not permission to create a replacement charging session.
async def test_replacement_retires_accepted_test_without_replaying_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(charge_tests, "DURATION", 10)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        first = managed_session(rig)
        await first.start()
        controller = ManagedController(first)
        rig.route_to_trial()
        try:
            async with rig.charger(first) as charger:
                await introduced(rig, first, charger, "interrupted-test-boot")
                await healthy(controller)
                identifier = uuid.uuid4().hex
                controller.command(identifier, "start_test", 0)
                await controller.tests.settle()
                assert len(starts(rig)) == 1
                await controller.tests.close()
                await crash(first)
            replacement = managed_session(rig)
            await replacement.start()
            recovered = ManagedController(replacement)
            try:
                assert recovered.requests.intent().mode == "paused"
                assert recovered.tests.state()["state"] == "interrupted"
                assert recovered.requests.receipt(identifier)["state"] == "uncertain"
                async with rig.charger(replacement) as charger:
                    await introduced(rig, replacement, charger, "interrupted-test-replacement")
                    assert rig.bench.firmware.effective_limit() == 0
                    assert len(starts(rig)) == 1 and recovered.tests.lease is None
                    assert recovered.requests.intent().mode == "paused"
            finally:
                await recovered.tests.close()
                await replacement.close()
        finally:
            await controller.tests.close()


# Intent: a crash between cloud-arm acceptance and its reply retires the unacknowledged receipt as uncertain.
# Why: a persisted accepted arm is neither a completed acknowledgement nor permission to initiate a new Start.
async def test_cloud_arm_acceptance_gap_recovers_without_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, startup=10) as test:
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "arm_cloud_test", 0)
        test.session.journal.db.execute("UPDATE managed_requests SET state='accepted' WHERE id=?", (identifier,))
        await test.controller.tests.close()
        await crash(test.session)
        replacement = managed_session(test.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            assert recovered.requests.receipt(identifier)["state"] == "uncertain"
            assert recovered.requests.intent().mode == "paused"
            assert recovered.tests.state()["state"] == "interrupted"
            assert not starts(test.rig) and recovered.tests.lease is None
        finally:
            await recovered.tests.close()
            await replacement.close()


# Intent: an expired earlier-sorted approved tag is skipped while another currently authorized local tag may charge.
# Why: a valid alternative must not require deleting expired or revoked cloud authorization evidence.
async def test_local_test_selects_currently_allowed_tag_without_erasing_expired_cache(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        gateway, journal = test.session.gateway, test.session.journal
        gateway.policy = replace(gateway.policy, allowed_tags=frozenset({"A_EXPIRED", "NATIVE_TIMER"}))
        gateway.controls.policy = gateway.policy
        gateway.auth_info.put("A_EXPIRED", {"status": "Accepted", "expiryDate": "2001-01-01T00:00:00Z"})
        expired_key = fingerprint("a_expired")
        before = tuple(
            journal.db.execute("SELECT digest,info,observed FROM auth_info WHERE digest=?", (expired_key,)).fetchone()
        )
        assert gateway.auth_info.get("A_EXPIRED")["status"] == "Expired"
        assert gateway._local_start_allowed("NATIVE_TIMER") and not gateway._local_start_allowed("A_EXPIRED")

        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", 0)
        await test.controller.tests.settle()
        assert len(starts(test.rig)) == 1 and starts(test.rig)[0][3]["idTag"] == "NATIVE_TIMER"
        local = await positive_flow(test)
        assert (await healthy(test.controller))["power_w"] == 1440
        assert test.controller.requests.receipt(identifier)["result"]["positive_power_verified"]
        stopped = await actual_stop(test, local)
        assert stopped["local_hold_verified"] and stopped["power_w"] == 0
        assert len(starts(test.rig)) == 1 and test.rig.bench.firmware.effective_limit() == 0
        assert (
            tuple(
                journal.db.execute(
                    "SELECT digest,info,observed FROM auth_info WHERE digest=?", (expired_key,)
                ).fetchone()
            )
            == before
        )
        assert gateway.auth_info.get("A_EXPIRED")["status"] == "Expired"
