"""One-shot managed charging permissions exercise real TLS peers and independent native HTTP evidence."""

from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import ssl
import time
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp_hub_core.prototype import managed_charging as charge_tests
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.recovery_contracts import GateDecision
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint

from tests.charge_rig import actual_stop, owner_start, positive_flow, ready
from tests.handover_helpers import crash
from tests.managed_rig import OwnerBench, healthy, introduced, managed_bench, managed_session, starts
from tests.management_peer import TOKEN
from tests.native_peer_failures import InterruptedNativePeer
from tests.runtime_rig import until
from tests.tls_support import tls as tls


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
        await controller.charging.settle()
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
        assert controller.charging.state()["state"] == "charging"

        # No service tick runs between positive-flow verification and the actual independently timed Stop.
        await asyncio.wait_for(test.rig.bench.remote_stop.wait(), 3)
        assert controller.requests.intent().mode == "paused"
        assert controller.charging.state()["stop_reason"] == "deadline"
        stopped = await actual_stop(test, local)
        assert stopped["local_hold_verified"] and stopped["power_w"] == 0
        assert not stopped["charging_session_open"] and stopped["station_ceiling_a"] == 0
        assert controller.charging.state()["state"] == "completed"
        assert len(starts(test.rig)) == 1 and not session.gateway.policy.allow_local_start


# Intent: a refused first-flow receipt leaves the timed charge metadata and deadline unchanged until the next poll.
# Why: a partial outcome must neither extend permission nor abort the service that owns the original Start and Stop.
@pytest.mark.parametrize("failure", ["statement", "commit"])
async def test_first_timed_flow_retries_atomic_publication(tmp_path, tls, unused_tcp_port, monkeypatch, failure):
    clock = [time.monotonic()]
    monkeypatch.setattr(charge_tests, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60, startup=60) as test:
        c, j = test.controller, test.session.journal
        identifier = uuid.uuid4().hex
        c.command(identifier, "start_test", c.requests.intent().revision)
        await c.charging.settle()
        local = await positive_flow(test)
        lease = c.charging.lease
        assert lease is not None
        deadline, receipt, state = lease.deadline, c.requests.receipt(identifier), c.charging.state()
        effect = "SELECT RAISE(ABORT,'first test-flow receipt refusal')"
        if failure == "commit":
            j.db.execute("CREATE TABLE flow_parent(id INTEGER PRIMARY KEY)")
            j.db.execute("CREATE TABLE flow_child(id REFERENCES flow_parent(id) DEFERRABLE INITIALLY DEFERRED)")
            effect = "INSERT INTO flow_child VALUES(1)"
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_test_flow BEFORE UPDATE OF state ON managed_requests "
            "WHEN NEW.action='start_test' AND NEW.state='completed' BEGIN " + effect + "; END"
        )
        try:
            clock[0] += 1
            assert (await c.tick())["state"] == "reconciling"
            assert not j.db.in_transaction and c.charging.state() == state
            assert c.requests.receipt(identifier) == receipt
            assert lease.deadline == deadline and lease.first_flow is not None
            first_flow = lease.first_flow
        finally:
            j.db.execute("DROP TRIGGER refuse_test_flow")
        clock[0] += 5
        assert (await healthy(c))["power_w"] > 10
        state, receipt = c.charging.state(), c.requests.receipt(identifier)
        assert state["state"] == "charging" and state["local_transaction"] == local
        assert state["flow_started_at"] == first_flow.observed_at
        assert state["stop_deadline_epoch"] == first_flow.observed_at + 60
        assert receipt["state"] == "completed"
        assert receipt["result"] == {"positive_power_verified": True, "duration_seconds": 60}
        assert lease.deadline == first_flow.deadline == deadline + 1
        assert len(starts(test.rig)) == 1
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]


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
        assert test.controller.charging.state()["state"] == "armed" and not starts(test.rig)
        await owner_start(test)
        assert len(starts(test.rig)) == 1
        local = await positive_flow(test, "cloud")
        status = await healthy(test.controller)
        assert status["power_w"] == 1440 and test.controller.charging.state()["state"] == "charging"
        assert test.controller.charging.state()["start_source_verified"]
        assert test.controller.charging.summary()["source"] == "cloud"
        assert test.controller.requests.receipt(identifier) == reply
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
            async def delayed_validate(action: str, payload: dict[str, Any]) -> GateDecision | None:
                decision = await actual_validate(action, payload)
                if action == "RemoteStartTransaction":
                    entered.set()
                    await proceed.wait()
                return decision

            monkeypatch.setattr(test.session.gateway.charger, "validate_send", delayed_validate)
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", 0)
        try:
            if boundary != "immediate":
                await asyncio.wait_for(entered.wait(), 3)
            count = len(test.rig.bench.received_limits)
            if boundary == "native_send":
                api = ManagementServer(
                    ManagementSettings(19001, tmp_path / "unused", TOKEN), test.controller, lambda: {}
                )
                proceed.set()  # The real native check is ready to let the Start continue.
                pending_pause = asyncio.create_task(
                    api.dispatch(json.dumps({"id": uuid.uuid4().hex, "command": "pause", "revision": 0})),
                    eager_start=True,
                )
                assert pending_pause.done()  # Revoke Start before yielding to its ready native check.
                pause = await pending_pause
                assert pause["result"]["state"] == "accepted"
                assert test.controller.requests.intent().mode == "paused" and not starts(test.rig)
            else:
                test.controller.command(uuid.uuid4().hex, "pause", 0)
                proceed.set()
            await test.controller.charging.settle()
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
        await test.controller.charging.settle()
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
        await test.controller.charging.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4096
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert test.controller.charging.state()["stop_reason"] == "deadline"
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4096
        assert test.rig.bench.firmware.effective_limit() == 0 and len(starts(test.rig)) == 1


# Intent: the independent timer retains zero and retries a refused timed Stop record.
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
        await test.controller.charging.settle()
        local = await positive_flow(test)
        await healthy(test.controller)
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
        await until(lambda: test.controller.requests.intent().mode == "paused")
        assert journal.get("control")["inhibit"] is True
        lease = test.controller.charging.lease
        assert lease is not None and lease.deadline <= time.monotonic()
        with pytest.raises(DeniedError):
            test.controller.charging.guard(lease)

        # No tick performs recovery: the timer itself repeats the atomic hold and launches the ordinary Stop.
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert len(attempts) >= 2 and len(starts(test.rig)) == 1
        assert test.rig.bench.firmware.effective_limit() == 0


# Intent: a timed charge sends a new zero at expiry during both failures and retains it through either recovery order.
# Why: the exact timed session can draw power before the proxy observes it, and recovery still needs the real Stop.
@pytest.mark.parametrize(
    ("source", "state", "first_repair"),
    [
        ("local", "charging", "together"),
        ("local", "awaiting_power", "storage"),
        ("cloud", "armed", "native"),
    ],
    ids=["flow-recorded", "local-storage-first", "cloud-native-first"],
)
async def test_timer_holds_zero_when_native_and_cutoff_storage_are_unavailable(
    tmp_path, tls, unused_tcp_port, monkeypatch, source, state, first_repair
):
    clock = [time.monotonic()]
    monkeypatch.setattr(charge_tests, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    async with ready(
        tmp_path,
        tls,
        unused_tcp_port,
        monkeypatch,
        duration=60,
        startup=60,
        native_peer=InterruptedNativePeer,
        owner_tag="APP_OWNER",
    ) as test:
        c, journal, bench, peer = test.controller, test.session.journal, test.rig.bench, test.rig.peer
        if source == "cloud":
            c.command(uuid.uuid4().hex, "arm_cloud_test", 0)
            await owner_start(test)
        else:
            c.command(uuid.uuid4().hex, "start_test", 0)
            await c.charging.settle()
        local = await positive_flow(test, source)
        if state == "charging":
            assert (await healthy(c))["power_w"] > 0
        assert c.charging.state()["state"] == state and c.charging.timed_stop_target(c.charging.state()) == local
        assert journal.active() == local and bench.firmware.current == 6
        before = len(bench.received_limits)
        wire_start = len(bench.device_calls)
        refusals = 0

        # Witness a rejected write without substituting a fake Stop result for the real SQLite transaction.
        def refuse_cutoff() -> None:
            nonlocal refusals
            refusals += 1
            raise sqlite3.OperationalError("synthetic timed cutoff record failure")

        journal.db.create_function("test_cutoff_refusal", 0, refuse_cutoff)
        journal.db.execute(
            "CREATE TEMP TRIGGER refuse_timed_cutoff BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_test_charge' AND json_extract(NEW.value,'$.state')='stopping' "
            "BEGIN SELECT test_cutoff_refusal(); END"
        )
        peer.unavailable = True
        try:
            assert (await c.tick())["fault"] == "native_observation_unavailable"
            assert (await asyncio.wait_for(peer.failed_reads.get(), 2))["action"] == "getConfig"
            assert c.charging.state()["state"] == state and bench.firmware.current == 6
            lease = c.charging.lease
            assert lease is not None
            clock[0] = lease.deadline
            c.charging.changed.set()

            await until(lambda: refusals > 0, timeout=2)
            await until(lambda: bench.firmware.effective_limit() == 0, timeout=2)
            assert bench.received_limits[before:] and all(limit == 0 for limit in bench.received_limits[before:])
            assert c.requests.intent().mode == "paused" and c.charging.state()["state"] == state
            assert journal.active() == local and not bench.remote_stop.is_set()

            if first_repair == "storage":
                assert peer.failed_reads.empty()
                journal.db.execute("DROP TRIGGER refuse_timed_cutoff")
                await until(lambda: c.charging.state().get("stop_local") == local)
                assert (await asyncio.wait_for(peer.failed_reads.get(), 2))["action"] == "getConfig"
                assert c.charging.state()["state"] == "stopping"
            elif first_repair == "native":
                peer.unavailable = False
                observed = (await c.native.observe()).physical
                assert observed.transaction_open and observed.native_transaction_id == local
                earlier = refusals
                await until(lambda: refusals > earlier, timeout=2)
                assert c.charging.state()["state"] == state
            assert journal.active() == local and not bench.remote_stop.is_set()
            assert bench.firmware.current == 0
        finally:
            peer.unavailable = False
            journal.db.execute("DROP TRIGGER IF EXISTS refuse_timed_cutoff")

        # Once both recover, supervision may retry Stop. It still needs that session's actual lifecycle report.
        await until(lambda: c.charging.state().get("stop_local") == local)
        await c.tick()
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert c.charging.state()["stop_reason"] == "deadline"
        assert all(limit == 0 for limit in bench.received_limits[before:]) and len(starts(test.rig)) == 1
        assert [frame[3] for frame in bench.device_calls[wire_start:] if frame[2] == "RemoteStopTransaction"] == [
            {"transactionId": local}
        ]


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
        assert test.controller.charging.state()["state"] == "armed"
        assert not test.controller.charging.state().get("start_source_verified")
        assert not test.controller.requests.receipt(identifier)["result"].get("positive_power_verified")
        charging = test.controller.charging
        assert charging.lease is not None and charging.timer is not None
        charging.lease.deadline = time.monotonic() - 1
        charging.changed.set()
        await asyncio.wait_for(charging.timer, 3)
        assert charging.state()["state"] == "completed" and charging.lease is None
        assert test.session.journal.active() == local and test.rig.bench.firmware.current == 6
        assert not test.rig.bench.remote_stop.is_set()
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
        lease = test.controller.charging.lease
        assert lease is not None
        before = len(test.rig.bench.device_calls)
        if binding in ("identity", "spec"):
            setattr(lease, binding, fingerprint("another synthetic binding"))
        elif binding in ("epoch", "control_revision"):
            setattr(lease, binding, getattr(lease, binding) + 1)
        else:
            test.session.gateway.auth_info.put("NATIVE_TIMER", {"status": "Blocked"})
        await test.controller.charging.settle()
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
        await test.controller.charging.settle()
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
        await test.controller.charging.settle()
        if source == "cloud":
            await owner_start(test)
        local = await positive_flow(test, source)
        lease = test.controller.charging.lease
        assert lease is not None and lease.deadline == 1060 and lease.hard_deadline == 1180
        clock[0] += elapsed
        native = await test.controller.native.read()
        test.controller.charging.observed(native)
        if elapsed == 59:
            assert test.controller.charging.state()["state"] == "charging"
            assert lease.deadline == (1179 if source == "local" else 1180)
            assert lease.deadline <= lease.hard_deadline == 1180
            summary = test.controller.charging.summary()
            assert summary is not None and summary["seconds_remaining"] == 120
            test.controller.command(uuid.uuid4().hex, "pause", 0)
        else:
            assert test.controller.charging.state()["state"] == "stopping"
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
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
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
                await controller.charging.settle()
                assert len(starts(rig)) == 1
                await controller.charging.close()
                await crash(first)
            replacement = managed_session(rig)
            await replacement.start()
            recovered = ManagedController(replacement)
            try:
                assert recovered.requests.intent().mode == "paused"
                assert recovered.charging.state()["state"] == "interrupted"
                assert recovered.requests.receipt(identifier)["state"] == "uncertain"
                async with rig.charger(replacement) as charger:
                    await introduced(rig, replacement, charger, "interrupted-test-replacement")
                    assert rig.bench.firmware.effective_limit() == 0
                    assert len(starts(rig)) == 1 and recovered.charging.lease is None
                    assert recovered.requests.intent().mode == "paused"
            finally:
                await recovered.charging.close()
                await replacement.close()
        finally:
            await controller.charging.close()


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
        await test.controller.charging.close()
        await crash(test.session)
        replacement = managed_session(test.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            assert recovered.requests.receipt(identifier)["state"] == "uncertain"
            assert recovered.requests.intent().mode == "paused"
            assert recovered.charging.state()["state"] == "interrupted"
            assert not starts(test.rig) and recovered.charging.lease is None
        finally:
            await recovered.charging.close()
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
        await test.controller.charging.settle()
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
