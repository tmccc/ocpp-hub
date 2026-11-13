"""Failed idle selections retain their verified ceiling across independent daily-plan lifecycles."""

from __future__ import annotations

import asyncio
import copy
import sqlite3
import uuid
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Complete the independent one-off before selecting current, leaving only the original daily plan installed.
async def complete_timer(b):
    await b.arm()
    b.clock.now = b.start + 601
    await healthy(b.c)
    assert b.c.schedule.summary()["state"] == "complete" and set(b.device.plans) == {9}


# Hold one actual native observation before or after the station has physically applied the requested increase.
@asynccontextmanager
async def interrupted_increase(b, applied):
    observe = b.c.native.observe
    entered, release = asyncio.Event(), asyncio.Event()

    # Keep native bytes genuine while controlling the ordering of the worker and a separate daily occurrence.
    async def delayed():
        snapshot = await observe()
        if not entered.is_set() and (
            not applied or b.device.effective_limit() == 32 and not b.session.gateway.controls._operations
        ):
            entered.set()
            await release.wait()
        return snapshot

    b.c.native.observe = delayed
    request = uuid.uuid4().hex
    b.c.command(request, "set_current", b.c.requests.intent().revision, current_a=32)
    try:
        await asyncio.wait_for(entered.wait(), 3)
        yield request, release
    finally:
        release.set()
        await b.c.current.settle()
        b.c.native.observe = observe


# Confirm real daily continuation and the next midnight through unchanged firmware plan fields and protocol events.
async def assert_daily_recovery(b, local, original, demand, prior_stops=()):
    identifier = b.session.journal.get("managed_current_change")["id"]
    receipt = b.c.requests.receipt(identifier)
    status = await healthy(b.c)
    after = b.c.requests.receipt(identifier)
    if receipt["state"] == "failed":
        assert after == receipt
    elif receipt["state"] == "uncertain":
        assert after["state"] == "failed"
        assert after["result"]["classification"] == "superseded_by_verified_overlap_recovery"
    assert b.c.requests.intent().mode == "app" and b.c.current.selected() == 6
    assert b.device.effective_limit() == status["station_ceiling_a"] == 6
    assert b.device.current == (6 if demand else 0)
    assert b.session.journal.active() == local and b.stops() == list(prior_stops)
    assert b.device.plans == {9: original} and not starts(b.rig)
    await b.close_transaction(local)
    await healthy(b.c)
    day = datetime.fromtimestamp(b.clock.now, ZONE)
    following = int(day.replace(hour=0, minute=0, second=0).timestamp()) + 86400
    next_local = await b.advance_idle_to(following)
    assert next_local not in (None, local) and (await healthy(b.c))["current_a"] == 6
    assert b.stops() == list(prior_stops) and b.device.plans == {9: original} and not starts(b.rig)


# Intent: all eight idle-increase overlaps preserve app intent, the old six-ampere selection and independent daily work.
# Why: a failed current request cannot acquire owner Pause authority after a one-off has completed or without one.
@pytest.mark.parametrize("completed_timer", [False, True])
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("demand", [False, True])
async def test_daily_interrupts_increase(tmp_path, tls, unused_tcp_port, monkeypatch, completed_timer, applied, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        if completed_timer:
            await complete_timer(b)
        async with interrupted_increase(b, applied) as (request, release):
            local = await b.start_plan(9, demand=demand)
            await b.c.native.read()
            release.set()
        assert b.c.current.selected() == 6 and b.device.effective_limit() == 0 and not b.stops()
        assert b.c.requests.receipt(request)["state"] == "failed"
        await assert_daily_recovery(b, local, original, demand)


# Intent: native-first original activity remains at zero until its genuine OCPP Start and positive plan identification.
# Why: native activity alone is neither a local transaction nor permission to Stop or restore a positive ceiling.
@pytest.mark.parametrize("completed_timer", [False, True])
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_native_first_awaits_real_daily_report(
    tmp_path, tls, unused_tcp_port, monkeypatch, completed_timer, applied, demand, restart
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        if completed_timer:
            await complete_timer(b)
        async with interrupted_increase(b, applied) as (request, release):
            provisional = b.session.journal.get("next_transaction") + 1
            b.device.active = provisional
            b.rig.peer.plan_session = (provisional, 9)
            b.device.fire_plan(9)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = demand
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            if not demand:
                b.device.requested_current = 0
            await b.c.native.read()
            release.set()
        assert b.c.requests.receipt(request)["state"] == "failed"
        assert b.c.current.selected() == 6 and b.device.effective_limit() == 0
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "current-native-first-restart")
            for _ in range(4):
                await b.c.tick()
                assert b.device.current == b.device.effective_limit() == 0 and not b.stops()
                assert b.session.journal.active() is None and b.c.requests.intent().mode == "app"
            reply = await b.rig.bench.call(
                b.sock,
                "genuine-daily-report",
                "StartTransaction",
                START | {"idTag": original["idTag"], "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
            )
            assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
            local = reply[2]["transactionId"]
            b.rig.peer.plan_session = (local, 9)
            await assert_daily_recovery(b, local, original, demand)


# Intent: process loss before a pending increase completes cannot turn it into Pause or Stop an independent daily.
# Why: both unapplied and physically applied writes must recover the previous verified ceiling from durable state.
@pytest.mark.parametrize("completed_timer", [False, True])
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("demand", [False, True])
async def test_restart_during_pending_increase(
    tmp_path, tls, unused_tcp_port, monkeypatch, completed_timer, applied, demand
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        if completed_timer:
            await complete_timer(b)
        async with interrupted_increase(b, applied) as (_, release):
            local = await b.start_plan(9, demand=demand)
            await b.c.native.read()
            async with asyncio.timeout(3):
                while b.session.journal.next_delivery() is not None or b.session.gateway.vendor._pending:
                    await asyncio.sleep(0.01)
            b.c.current.worker.cancel()
            await asyncio.gather(b.c.current.worker, return_exceptions=True)
            release.set()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        assert b.c.current.selected() == 6 and b.c.requests.intent().mode == "app"
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "pending-current-replacement")
            assert not b.stops() and b.device.effective_limit() == 0
            await assert_daily_recovery(b, local, original, demand)


# Intent: actual automatic recovery owns the transaction that settles metadata and its process-replaced receipt.
# Why: a helper-only transaction test would miss removal of the production caller's atomic boundary.
@pytest.mark.parametrize("failure", ["metadata", "receipt", "commit", "unknown-source", "new-pause"])
async def test_automatic_overlap_caller_atomic_rollback(tmp_path, tls, unused_tcp_port, monkeypatch, failure):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        async with interrupted_increase(b, True) as (identifier, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            async with asyncio.timeout(3):
                while b.session.journal.next_delivery() is not None or b.session.gateway.vendor._pending:
                    await asyncio.sleep(0.01)
            b.c.current.worker.cancel()
            await asyncio.gather(b.c.current.worker, return_exceptions=True)
            release.set()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        j = b.session.journal
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "atomic-overlap-replacement")
            before = b.c.requests.receipt(identifier)
            assert before["state"] == "uncertain"
            if failure == "commit":
                j.db.execute("CREATE TABLE overlap_parent(id INTEGER PRIMARY KEY)")
                j.db.execute(
                    "CREATE TABLE overlap_child(id REFERENCES overlap_parent(id) DEFERRABLE INITIALLY DEFERRED)"
                )
                trigger = "AFTER UPDATE ON managed_requests WHEN NEW.state='failed'"
                effect = "INSERT INTO overlap_child VALUES(1)"
            else:
                trigger = (
                    "BEFORE INSERT ON meta WHEN NEW.key='managed_current_change' AND "
                    "json_extract(NEW.value,'$.state')='cancelled'"
                    if failure == "metadata"
                    else "BEFORE UPDATE ON managed_requests WHEN NEW.state='failed'"
                )
                effect = "SELECT RAISE(ABORT,'synthetic overlap settlement refusal')"
            j.db.execute(f"CREATE TRIGGER refuse_overlap {trigger} BEGIN {effect}; END")
            with pytest.raises(sqlite3.IntegrityError):
                await b.c.current.recover_overlap()
            assert not j.db.in_transaction
            assert j.get("managed_current_change")["state"] == "recovering"
            assert b.c.requests.receipt(identifier) == before
            assert b.device.effective_limit() == 6 and not b.stops()
            j.db.execute("DROP TRIGGER refuse_overlap")
            if failure == "new-pause":
                b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
                await b.c.pause_task
                pause_control = copy.deepcopy(j.get("control"))
                pause_intent = b.c.requests.intent()
                await b.c.current.recover_overlap()
                assert j.get("control") == pause_control and b.c.requests.intent() == pause_intent
                assert b.c.requests.receipt(identifier) == before and b.device.effective_limit() == 0
                await b.close_transaction(local)
                assert (await healthy(b.c))["local_hold_verified"]
                b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
                assert (await healthy(b.c))["owner_control_enabled"]
                assert b.c.requests.receipt(identifier)["state"] == "failed"
                assert b.c.requests.receipt(identifier)["result"]["classification"] == "superseded_by_verified_return"
                return
            if failure == "unknown-source":
                b.rig.peer.plan_session = (local, 987654)
                with pytest.raises((SecurityError, RecoveryRequiredError)):
                    await b.c.current.recover_overlap()
                assert b.device.effective_limit() == 0 and not b.stops()
                assert j.get("managed_current_change")["state"] == "recovering"
                assert b.c.requests.receipt(identifier) == before
                b.rig.peer.plan_session = (local, 9)
            trace = len(b.rig.bench.device_calls)
            await b.c.current.recover_overlap()
            limits = [
                frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                for frame in b.rig.bench.device_calls[trace:]
                if frame[2] == "SetChargingProfile"
            ]
            assert limits and limits[-1] == 6
            if failure != "unknown-source":
                assert limits[0] == 0
            assert j.get("managed_current_change")["state"] == "cancelled"
            assert b.c.requests.receipt(identifier)["state"] == "failed"
            assert j.active() == local and b.device.effective_limit() == 6 and not b.stops()


# Intent: an accepted daily lifecycle with missing or conflicting native plan evidence stays at zero until identified.
# Why: an open zero-draw session and a daily tag alone cannot establish which operation owns the physical activity.
@pytest.mark.parametrize("completed_timer", [False, True])
@pytest.mark.parametrize("plan", [None, 987654])
@pytest.mark.parametrize("demand", [False, True])
async def test_unknown_source_holds_zero(tmp_path, tls, unused_tcp_port, monkeypatch, completed_timer, plan, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        if completed_timer:
            await complete_timer(b)
        async with interrupted_increase(b, True) as (_, release):
            local = await b.start_plan(9, demand=demand)
            b.rig.peer.plan_session = (local, plan)
            await b.c.native.read()
            release.set()
        for _ in range(4):
            await b.c.tick()
            assert b.device.effective_limit() == b.device.current == 0
            assert b.c.current.selected() == 6 and b.c.requests.intent().mode == "app" and not b.stops()
        b.rig.peer.plan_session = (local, 9)
        await assert_daily_recovery(b, local, original, demand)


# Intent: owner Pause retains unrestricted Stop authority during an overlapping selection and its zero recovery.
# Why: automatic cancellation must preserve the daily, but an explicit owner Pause must still stop and hold it.
@pytest.mark.parametrize("completed_timer", [False, True])
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("during_worker", [False, True])
async def test_explicit_pause_preempts_current_recovery(
    tmp_path, tls, unused_tcp_port, monkeypatch, completed_timer, applied, during_worker
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        if completed_timer:
            await complete_timer(b)
        pause = uuid.uuid4().hex
        async with interrupted_increase(b, applied) as (_, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            if during_worker:
                b.c.command(pause, "pause", b.c.requests.intent().revision)
                await b.c.pause_task
            release.set()
        if not during_worker:
            b.c.command(pause, "pause", b.c.requests.intent().revision)
            await b.c.pause_task
        assert set(b.stops()) == {local} and b.device.effective_limit() == 0
        assert b.c.requests.intent().mode == "paused" and b.c.current.selected() == 6
        await b.close_transaction(local)
        assert (await healthy(b.c))["local_hold_verified"]
        assert b.c.requests.receipt(pause)["state"] == "completed"
        returning = uuid.uuid4().hex
        b.c.command(returning, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert b.c.requests.receipt(returning)["state"] == "completed"
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (None, local) and (await healthy(b.c))["current_a"] == 6
        assert set(b.stops()) == {local} and b.device.plans == {9: original} and not starts(b.rig)


# Intent: after a real one-off cutoff and physical cleanup, a later failed increase preserves both daily windows.
# Why: historical timer completion must not create or remove authority over unrelated future daily transactions.
@pytest.mark.parametrize("demand", [False, True])
async def test_next_window_after_real_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        await b.arm()
        b.clock.now = b.start
        temporary = await b.fire(b.timer, demand=False)
        await healthy(b.c)
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        await b.c.tick()
        assert b.stops() == [temporary]
        await b.close_transaction(temporary)
        await healthy(b.c)
        assert b.c.schedule.summary()["state"] == "complete" and b.device.plans == {9: original}
        b.clock.now = int(datetime(2026, 11, 11, 23, 59, tzinfo=ZONE).timestamp())
        async with interrupted_increase(b, True) as (_, release):
            b.clock.now += 60
            daily = await b.fire(9, demand=demand)
            await b.c.native.read()
            release.set()
        assert b.c.current.selected() == 6 and b.device.effective_limit() == 0
        await assert_daily_recovery(b, daily, original, demand, (temporary,))


# Intent: source changes during final send validation revoke positive recovery and retain zero until reidentified.
# Why: an earlier matching native plan cannot authorize a release after fresher evidence contradicts its source.
@pytest.mark.parametrize("completed_timer", [False, True])
async def test_source_changes_before_positive_send(tmp_path, tls, unused_tcp_port, monkeypatch, completed_timer):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        if completed_timer:
            await complete_timer(b)
        async with interrupted_increase(b, True) as (_, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            release.set()
        native_send = b.c.native.validate_send
        changed = False

        # Change only independent peer evidence immediately before the actual outgoing positive-profile gate.
        async def changed_source(action, payload):
            nonlocal changed
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 6 and not changed:
                    b.rig.peer.plan_session = (local, 987654)
                    changed = True
            await native_send(action, payload)

        b.session.gateway.charger.validate_send = changed_source
        await b.c.tick()
        assert changed and b.device.effective_limit() == b.device.current == 0 and not b.stops()
        for _ in range(4):
            await b.c.tick()
            assert b.device.effective_limit() == 0 and b.c.requests.intent().mode == "app"
        b.rig.peer.plan_session = (local, 9)
        await assert_daily_recovery(b, local, original, True)


# Intent: repeated unavailable native reads retain zero recovery instead of consuming permission or inventing Pause.
# Why: loss of source evidence must remain recoverable once the same daily transaction is positively identified.
async def test_native_outage_during_recovery(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        async with interrupted_increase(b, True) as (_, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            release.set()
        observer = b.session.prepared.observer
        original_port = observer.port

        # Drop only the independent local HTTP read; the engine and OCPP station-zero receipt stay real.
        async def unavailable(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.close()
            await writer.wait_closed()

        async with await asyncio.start_server(unavailable, "127.0.0.1", 0) as server:
            observer.port = server.sockets[0].getsockname()[1]
            try:
                for _ in range(5):
                    await b.c.tick()
                    assert b.device.effective_limit() == 0 and not b.stops() and b.c.requests.intent().mode == "app"
                    assert not b.session.journal.get("managed_control_fault")
            finally:
                observer.port = original_port
        await assert_daily_recovery(b, local, original, True)


# Intent: an explicit Pause accepted before process loss retains its own authority and the prior selected six amperes.
# Why: recovery of the older pending increase must neither replace the Pause ticket nor promote its unverified target.
@pytest.mark.parametrize("applied", [False, True])
async def test_restart_after_explicit_pause_of_pending_increase(tmp_path, tls, unused_tcp_port, monkeypatch, applied):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        async with interrupted_increase(b, applied) as (_, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            pause = uuid.uuid4().hex
            b.c.command(pause, "pause", b.c.requests.intent().revision)
            await b.c.pause_task
            b.c.current.worker.cancel()
            await asyncio.gather(b.c.current.worker, return_exceptions=True)
            release.set()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        assert b.c.current.selected() == 6 and b.c.requests.intent().mode == "paused"
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "paused-pending-selection")
            assert set(b.stops()) == {local} and b.device.effective_limit() == 0
            await b.close_transaction(local)
            assert (await healthy(b.c))["local_hold_verified"] and b.c.current.selected() == 6


# Intent: idle replacement retains Pause and the previous verified current after an interrupted increase.
# Why: process loss cannot promote an unverified target or implicitly return control to the app.
async def test_idle_restart_keeps_existing_fail_closed_policy(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        async with interrupted_increase(b, True) as (_, release):
            b.c.current.worker.cancel()
            await asyncio.gather(b.c.current.worker, return_exceptions=True)
            release.set()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "idle-pending-selection")
            assert (await healthy(b.c))["local_hold_verified"]
            assert b.c.requests.intent().mode == "paused" and b.c.current.selected() == 6
            assert b.device.effective_limit() == 0 and not b.stops() and not starts(b.rig)


# Intent: either order of genuine daily closure remains at zero until both native and OCPP confirm closure.
# Why: a StopTransaction alone or an idle native observation cannot release an unresolved operation hold.
@pytest.mark.parametrize("first", ["native", "ocpp"])
async def test_closure_disagreement_stays_zero(tmp_path, tls, unused_tcp_port, monkeypatch, first):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        async with interrupted_increase(b, True) as (_, release):
            local = await b.start_plan(9)
            b.rig.peer.plan_session = (local, 987654)
            await b.c.native.read()
            release.set()
        if first == "native":
            b.device.active = None
            b.device.requested_current = 0
            b.rig.peer.permission_enabled = False
            b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        else:
            await b.close_transaction(local)
            b.device.active = local
        for _ in range(4):
            await b.c.tick()
            assert b.device.effective_limit() == 0 and not b.stops() and b.c.requests.intent().mode == "app"
        if first == "native":
            await b.close_transaction(local)
        else:
            b.device.active = None
        assert (await healthy(b.c))["station_ceiling_a"] == 6 and b.c.current.selected() == 6
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (None, local) and (await healthy(b.c))["current_a"] == 6
        assert not b.stops() and b.device.plans == {9: original} and not starts(b.rig)


# Intent: a lost zero acknowledgement leaves retryable operation recovery without Stop or a positive restoration.
# Why: accepting the target earlier must not turn failed restriction into Pause authority over the independent daily.
async def test_zero_rejection_retries_without_stop(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        answer = b.device.answer

        # Refuse only the restrictive station-profile write at the independent firmware endpoint.
        def reject_zero(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    return {"status": "Rejected"}
            return answer(action, payload)

        async with interrupted_increase(b, True) as (_, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            monkeypatch.setattr(b.device, "answer", reject_zero)
            release.set()
        for _ in range(3):
            status = await b.c.tick()
            assert not status["owner_control_enabled"] and not status["local_hold_verified"]
            assert b.session.gateway.controls.desired() == 0
            assert b.c.current.selected() == 6 and b.c.requests.intent().mode == "app" and not b.stops()
        monkeypatch.setattr(b.device, "answer", answer)
        await assert_daily_recovery(b, local, original, True)


# Intent: a new owner Pause at the actual positive send gate revokes recovery and still stops the genuine daily.
# Why: successful earlier source checks cannot let an old operation bypass newer owner intent.
async def test_pause_at_positive_send_gate(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        async with interrupted_increase(b, True) as (_, release):
            local = await b.start_plan(9)
            await b.c.native.read()
            release.set()
        native_send = b.c.native.validate_send
        pause = uuid.uuid4().hex
        requested = False

        # Submit the real command while source validation awaits the physical positive-profile send gate.
        async def pause_before_send(action, payload):
            nonlocal requested
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 6 and not requested:
                    requested = True
                    b.c.command(pause, "pause", b.c.requests.intent().revision)
            await native_send(action, payload)

        b.session.gateway.charger.validate_send = pause_before_send
        await b.c.tick()
        await b.c.pause_task
        assert requested and b.device.effective_limit() == 0 and b.c.requests.intent().mode == "paused"
        assert b.c.current.selected() == 6 and set(b.stops()) == {local}
        await b.close_transaction(local)
        assert (await healthy(b.c))["local_hold_verified"]
        assert b.c.requests.receipt(pause)["state"] == "completed" and not starts(b.rig)
