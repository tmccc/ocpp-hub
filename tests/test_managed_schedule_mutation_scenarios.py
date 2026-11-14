"""N08 retries preserve physical authority after refusal and post-application reply loss."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from tests.managed_scenario_helpers import (
    END,
    close_in_order,
    next_daily,
    originals,
    replace_controller,
    require_originals,
    select_current,
    settle,
    unknown_report,
)
from tests.test_managed_proxy import healthy, introduced
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_scenarios import early_cleanup_selected
from tests.test_prototype_tls import tls as tls


# Match one real outgoing operation without replacing its controller or outcome handling.
def matches_operation(operation, action, payload):
    if operation == "stop":
        return action == "RemoteStopTransaction"
    if operation == "clear":
        return action == "DataTransfer" and payload.get("messageId") == "clearPlans"
    if action != "SetChargingProfile":
        return False
    limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
    return limit == 0 if operation in ("zero", "zero_before_cutoff") else limit > 0


# Refuse before physical application or withhold its actual response after the independent device applied it.
@asynccontextmanager
async def fail_one_operation(b, operation, lost_ack):
    answer, before_reply = b.device.answer, b.rig.bench.before_reply
    entered, release = asyncio.Event(), asyncio.Event()

    # The peer itself refuses one request and leaves its independently stored physical state intact.
    def refuse_once(action, payload):
        if not lost_ack and not entered.is_set() and matches_operation(operation, action, payload):
            entered.set()
            return {"status": "Rejected", "data": ""} if operation == "clear" else {"status": "Rejected"}
        return answer(action, payload)

    # A missing reply is injected only after the firmware's actual application of the matching request.
    async def withhold_once(action, payload):
        if lost_ack and not entered.is_set() and matches_operation(operation, action, payload):
            entered.set()
            try:
                await asyncio.wait_for(release.wait(), b.session.gateway.charger.timeout + 0.05)
            except TimeoutError:
                pass  # The lost acknowledgement expires; the peer then remains available for subsequent requests.
        if before_reply is not None:
            await before_reply(action, payload)

    b.device.answer, b.rig.bench.before_reply = refuse_once, withhold_once
    try:
        yield entered
    finally:
        release.set()
        b.device.answer, b.rig.bench.before_reply = answer, before_reply
        await asyncio.sleep(0)


# Intent: each real physical mutation can refuse or lose its reply, then recover in the same or a replaced process.
# Why: uncertainty cannot expand Stop scope, report a failed zero as success, or remove an unrelated timer.
@pytest.mark.parametrize("operation", ["zero", "zero_before_cutoff", "stop", "clear", "restore"])
@pytest.mark.parametrize("lost_ack", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_n08_physical_mutation_recovery(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, operation, lost_ack, restart
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        if operation == "clear":
            # A clear requires a physically retained plan; inject before its removal, after real transaction closure.
            b.clock.now = b.start
            local = await b.start_plan(b.timer)
            await healthy(b.c)
            b.clock.now = END
            await settle(b)
            assert b.stops() == [local]
            await b.close_transaction(local)
        else:
            await early_cleanup_selected(b, 32)
            b.clock.now = END - 60
            local = await unknown_report(b, b.start, None, True)
            zero = operation in ("zero", "zero_before_cutoff")
            if not zero:
                await settle(b)
            # Fault the first required zero, rather than waiting until the unknown-source hold has already succeeded.
            if operation != "zero_before_cutoff":
                b.clock.now = END
            if not zero:
                await settle(b)
            if operation == "stop":
                b.rig.peer.plan_session = (local, b.timer)
            elif operation == "restore":
                b.rig.peer.plan_session = (local, 9)
        async with fail_one_operation(b, operation, lost_ack) as entered:
            work = asyncio.create_task(b.c.tick())
            try:
                await asyncio.wait_for(entered.wait(), 5)
                if operation == "restore" and lost_ack:
                    assert b.device.effective_limit() == 32
                if operation == "clear":
                    assert b.device.effective_limit() == 0
                status = await asyncio.wait_for(work, 12)
                if operation in ("zero", "zero_before_cutoff") and not lost_ack:
                    assert b.device.effective_limit() == 32 and status["state"] != "healthy"
                elif operation == "clear" and lost_ack and b.device.effective_limit() == 32:
                    native = await b.c.native.read(closed=True)
                    assert native.saved_schedule_digest == b.rig.spec.saved_schedule_digest
                    assert not native.physical.transaction_open and b.timer not in b.device.plans
                elif operation != "restore" or not lost_ack:
                    assert b.device.effective_limit() == 0
                assert set(b.stops()).issubset({local}) if operation in ("stop", "clear") else not b.stops()
            finally:
                if not work.done():
                    work.cancel()
                    await asyncio.gather(work, return_exceptions=True)
        b.clock.now += 301
        if restart:
            await replace_controller(b)
        from contextlib import nullcontext

        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "mutation-recovery")
            if operation == "clear":
                await settle(b, 6)
                await healthy(b.c)
                stops = tuple(b.stops())
                assert set(stops) == {local}
            else:
                if operation in ("zero", "zero_before_cutoff"):
                    await settle(b)
                    assert b.device.effective_limit() == 0 and not b.stops()
                    b.rig.peer.plan_session = (local, 9)
                await settle(b, 6)
                stops = tuple(b.stops())
                assert set(stops) == ({local} if operation == "stop" else set())
                if operation != "stop":
                    assert b.device.effective_limit() == 32
                await close_in_order(b, local, "native", stops)
                await healthy(b.c)
            require_originals(b, saved)
            await next_daily(b, 32, saved, stops)
