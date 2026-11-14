"""Real storage refusal, older owner receipts and shielded child work bound cancelled-Start retirement."""

import asyncio
import copy
import json
import uuid
from datetime import timedelta

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests import test_managed_proxy as mp
from tests.cloud_start_helpers import two_later_sessions
from tests.recovery_barriers import fresh_daily_values, witnessed_native_loss
from tests.test_managed_normal_start import natural_stop
from tests.test_managed_start_cancelled_outer import cancel_outer_before_execution, finish_recovery, selected_source
from tests.test_managed_test_charge import actual_stop, positive_flow
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests_model.test_start_recovery_sequences import close_daily


# Admit a new genuine original-plan daily whose timestamp and meters follow every earlier actual cloud lifecycle.
async def daily_after_history(b):
    beginning, meter = fresh_daily_values(b.rig, meter_floor=1200)
    local = await positive_flow(
        b,
        "daily",
        call_id=uuid.uuid4().hex,
        timestamp=beginning.isoformat().replace("+00:00", "Z"),
        meter_start=meter,
    )
    assert b.rig.peer.plan_session == (local, 9)
    return local, (beginning + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"), meter + 100


# Refuse the exact current failed-request UPDATE at its actual statement or deferred transaction COMMIT.
def refuse_cancelled_retirement(j, identifier, failure):
    attempts = []

    # Observe real SQLite trigger executions even when their transaction subsequently rolls back.
    def observed_attempt():
        attempts.append(asyncio.get_running_loop().time())
        return 0

    j.db.create_function("cancel_retirement_attempt", 0, observed_attempt)
    if failure == "commit":
        j.db.execute("CREATE TABLE cancelled_retirement_parent(id INTEGER PRIMARY KEY)")
        j.db.execute(
            "CREATE TABLE cancelled_retirement_child("
            "id REFERENCES cancelled_retirement_parent(id) DEFERRABLE INITIALLY DEFERRED)"
        )
        effect = "INSERT INTO cancelled_retirement_child VALUES(42);"
    else:
        effect = "SELECT RAISE(ABORT,'actual cancelled retirement statement refusal');"
    j.db.execute(
        "CREATE TEMP TRIGGER cancelled_retirement_refusal BEFORE UPDATE ON managed_requests "
        "WHEN NEW.id='" + identifier + "' AND NEW.state='failed' BEGIN "
        "SELECT cancel_retirement_attempt(); " + effect + " END"
    )
    return attempts


# Retain exact row bytes, rather than accepting an uncertain/partly-completed substitute after a refused COMMIT.
def request_row(j, identifier):
    return dict(j.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone())


# Intent: failed-request statement/COMMIT refusal through the real deadline cannot Pause a different daily.
# Why: failed atomic retirement remains pending; neither polling nor the timer may fall back to a broad Stop.
@pytest.mark.parametrize("current", [6, 16, 32])
@pytest.mark.parametrize("failure", ["statement", "commit"])
async def test_cancelled_unprepared_retirement_sql_wait_preserves_daily(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, failure
):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        identifier, intent = await cancel_outer_before_execution(b)
        lease = c.tests.lease
        assert lease is not None
        original_state, original_row = copy.deepcopy(c.tests.state()), request_row(j, identifier)
        local, stop_time, stop_meter = await daily_after_history(b)
        attempts = refuse_cancelled_retirement(j, identifier, failure)
        try:
            while asyncio.get_running_loop().time() <= lease.deadline + 0.05:
                await asyncio.wait_for(c.tick(), 1)
                assert c.requests.intent() == intent and c.tests.lease is lease
                assert c.tests.state() == original_state and request_row(j, identifier) == original_row
                assert j.get("local_start_receipt") is None and not j.db.in_transaction
                assert j.active() == local and rig.bench.firmware.effective_limit() == current
                assert rig.bench.firmware.current == current
                assert not mp.starts(rig) and not any(
                    frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls
                )
                await asyncio.sleep(0.04)
            before_passive = len(attempts)
            await asyncio.sleep(0.25)
            assert len(attempts) > before_passive, "the actual expired timer must retry while no poll assists it"
            assert c.requests.intent() == intent and c.tests.lease is lease
            assert c.tests.state() == original_state and request_row(j, identifier) == original_row
            assert rig.bench.firmware.effective_limit() == rig.bench.firmware.current == current
            assert not c.tests.timer.done() and attempts and not j.db.in_transaction
        finally:
            j.db.execute("DROP TRIGGER cancelled_retirement_refusal")
        if failure == "commit":
            assert j.db.execute("SELECT COUNT(*) FROM cancelled_retirement_child").fetchone()[0] == 0
        assert (await mp.healthy(c))["station_ceiling_a"] == current
        assert c.requests.receipt(identifier)["state"] == "failed" and c.tests.state()["state"] == "completed"
        assert c.tests.lease is None and c.requests.intent() == intent
        await until(lambda: c.tests.timer.done())
        assert await witnessed_native_loss(b, current, local) >= 3
        await close_daily(b, local, "ledger", timestamp=stop_time, meter_stop=stop_meter)
        await finish_recovery(b, identifier, current)
        assert len(rig.bench.backend_starts) == 3 and all(row["closed"] for row in rig.bench.backend_starts)


# Cancel the next outer coroutine without assuming that a genuine earlier closed owner receipt is absent.
async def cancel_outer_with_old_receipt(b, prior_receipt):
    c, j = b.controller, b.session.journal
    loop, factory = asyncio.get_running_loop(), asyncio.get_running_loop().get_task_factory()
    identifier, victims = uuid.uuid4().hex, []
    original_intent, original_calls = c.requests.intent(), copy.deepcopy(b.rig.bench.device_calls)

    # Select only this exact outer managed task and preserve normal scheduling of all real control children.
    def cancel_outer(loop, coroutine, **kwargs):
        from ocpp_hub_core.prototype.managed_test_charge import ManagedChargeTests

        task = asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)
        if (
            getattr(coroutine, "cr_code", None) is ManagedChargeTests._start.__code__
            and coroutine.cr_frame.f_locals.get("self") is c.tests
        ):
            task.cancel()
            victims.append(task)
        return task

    try:
        loop.set_task_factory(cancel_outer)
        c.command(identifier, "start", original_intent.revision)
        await asyncio.gather(c.tests.worker, return_exceptions=True)
    finally:
        loop.set_task_factory(factory)
    assert victims == [c.tests.worker] and c.tests.worker.cancelled()
    assert c.tests.state()["start_attempted"] is False and not c.tests.state()["start_rpc_accepted"]
    assert j.get("local_start_receipt") == prior_receipt and b.rig.bench.device_calls == original_calls
    return identifier, original_intent


# Intent: exact retirement of a later cancelled Start preserves the earlier genuinely accepted and closed receipt.
# Why: a blanket old-receipt veto strands this request, while deleting the receipt destroys real transaction provenance.
@pytest.mark.parametrize("current", [6, 32])
async def test_cancelled_unprepared_preserves_genuine_older_owner_receipt(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        previous = uuid.uuid4().hex
        c.command(previous, "start", c.requests.intent().revision)
        await c.tests.settle()
        owner = await positive_flow(b)
        await mp.healthy(c)
        await natural_stop(b, owner)
        await mp.healthy(c)
        old_receipt, old_result = copy.deepcopy(j.get("local_start_receipt")), c.requests.receipt(previous)
        old_transaction = dict(j.db.execute("SELECT * FROM transactions WHERE local=?", (owner,)).fetchone())
        assert old_receipt["request_id"] == previous and old_receipt["local"] == owner
        assert old_receipt["stage"] == "accepted" and old_transaction["stop"] is not None
        assert old_result["state"] == "completed" and len(mp.starts(rig)) == 1
        identifier, intent = await cancel_outer_with_old_receipt(b, old_receipt)
        local, stop_time, stop_meter = await daily_after_history(b)
        assert (await mp.healthy(c))["station_ceiling_a"] == current
        await until(lambda: c.tests.timer.done())
        assert c.requests.intent() == intent and c.requests.receipt(identifier)["state"] == "failed"
        assert c.tests.state()["state"] == "completed" and j.get("local_start_receipt") == old_receipt
        assert c.requests.receipt(previous) == old_result
        assert dict(j.db.execute("SELECT * FROM transactions WHERE local=?", (owner,)).fetchone()) == old_transaction
        assert await witnessed_native_loss(b, current, local) >= 3
        await close_daily(b, local, "ledger", timestamp=stop_time, meter_stop=stop_meter)
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await mp.healthy(c))["local_hold_verified"]
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await mp.healthy(c))["station_ceiling_a"] == current
        await two_later_sessions(b, selected=current, original_starts=1)
        assert c.requests.receipt(previous) == old_result
        assert j.get("local_start_receipt")["request_id"] == previous
        assert j.get("local_start_receipt")["local"] == owner and not rig.bench.backend_errors


# Intent: a shielded created-but-unstarted child prevents retirement, then only its real dispatched outcome settles.
# Why: a cancelled outer waiter says nothing about an independently owned LocalControl child which can still send.
@pytest.mark.parametrize("current", [6, 32])
async def test_cancelled_outer_waits_for_real_shielded_unstarted_child(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, s, rig = b.controller, b.session, b.rig
        loop, old_factory = asyncio.get_running_loop(), asyncio.get_running_loop().get_task_factory()
        child_created, allow_child, issued, allow_reply = (asyncio.Event() for _ in range(4))
        children = []
        identifier, intent = uuid.uuid4().hex, c.requests.intent()

        # Delay only scheduling of the original child body; its result and every engine instruction stay untouched.
        async def delayed_child(original):
            started = False
            try:
                await allow_child.wait()
                started = True
                return await original
            finally:
                if not started:
                    original.close()

        # The real outer waiter is cancelled after its genuine shielded child exists in LocalControl's owned pool.
        def hold_child_and_cancel_parent(loop, coroutine, **kwargs):
            if (
                getattr(coroutine, "cr_code", None) is LocalControl._start.__code__
                and coroutine.cr_frame.f_locals.get("self") is s.gateway.controls
            ):
                task = asyncio.Task(delayed_child(coroutine), loop=loop, eager_start=False, **kwargs)
                children.append(task)
                c.tests.worker.cancel()
                child_created.set()
                return task
            return asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)

        # Independent firmware applies the actual Start before its real correlated TLS reply is released.
        async def hold_actual_start_reply(action, payload):
            if action == "RemoteStartTransaction":
                issued.set()
                await allow_reply.wait()

        rig.bench.before_reply = hold_actual_start_reply
        try:
            loop.set_task_factory(hold_child_and_cancel_parent)
            c.command(identifier, "start", intent.revision)
            await asyncio.wait_for(child_created.wait(), 3)
            await asyncio.gather(c.tests.worker, return_exceptions=True)
            loop.set_task_factory(old_factory)
            assert c.tests.worker.cancelled() and len(children) == 1 and not children[0].done()
            assert children[0] in s.gateway.controls._operations
            assert c.tests.state()["start_attempted"] is False and s.journal.get("local_start_receipt") is None
            lease, state, row = c.tests.lease, copy.deepcopy(c.tests.state()), request_row(s.journal, identifier)
            for _ in range(2):
                await asyncio.wait_for(c.tick(), 1)
                assert c.tests.lease is lease and c.tests.state() == state
                assert request_row(s.journal, identifier) == row and c.requests.intent() == intent
                assert s.journal.get("local_start_receipt") is None and not mp.starts(rig)
            allow_child.set()
            await asyncio.wait_for(issued.wait(), 3)
            receipt = s.journal.get("local_start_receipt")
            assert receipt["request_id"] == identifier and receipt["stage"] == "issued"
            assert c.tests.state()["start_attempted"] is True and len(mp.starts(rig)) == 1
            assert c.requests.receipt(identifier)["state"] in ("accepted", "running", "uncertain")
            status = await asyncio.wait_for(c.tick(), 1)
            witness = {
                "boundary": "genuine issued child with actual correlated reply still held",
                "outer_cancelled": c.tests.worker.cancelled(),
                "child_done": children[0].done(),
                "child_owned": children[0] in s.gateway.controls._operations,
                "actual_pending_rpc_count": len(s.gateway.charger._pending),
                "local_start_receipt": s.journal.get("local_start_receipt"),
                "state": c.tests.state(),
                "request": c.requests.receipt(identifier),
                "actual_start_wire": mp.starts(rig),
                "fault_after_poll": s.journal.get("managed_control_fault"),
                "physical_limit": rig.bench.firmware.effective_limit(),
                "status": status,
            }
            (tmp_path / "shielded-issued-poll-witness.json").write_text(json.dumps(witness, indent=2) + "\n")
            assert not s.journal.get("managed_control_fault"), (
                "a still-owned issued child must settle before supervision"
            )
            assert children[0] in s.gateway.controls._operations and not children[0].done()
            assert c.requests.receipt(identifier)["state"] in ("accepted", "running", "uncertain")
        finally:
            loop.set_task_factory(old_factory)
            allow_child.set()
            allow_reply.set()
            rig.bench.before_reply = None
            await asyncio.gather(*children, return_exceptions=True)
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        await c.tick()
        await until(lambda: c.tests.timer.done(), timeout=5)
        assert c.requests.receipt(identifier)["state"] == "uncertain"
        local = await positive_flow(b)
        assert rig.bench.firmware.current == 0
        for _ in range(3):
            await c.tick()
        await actual_stop(b, local)
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await mp.healthy(c))["local_hold_verified"]
        terminal = c.requests.receipt(identifier)
        assert terminal["state"] == "completed"
        assert terminal["result"] == {"local_session_verified": True, "charging": False, "timed": False}
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await mp.healthy(c))["station_ceiling_a"] == current
        await two_later_sessions(b, selected=current, original_starts=1)
        assert c.requests.receipt(identifier) == terminal and not s.journal.get("managed_control_fault")
