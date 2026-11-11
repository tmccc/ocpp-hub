"""Independent event compositions against the immutable candidate and real loopback peers."""

import uuid
from datetime import UTC, datetime

import pytest

from tests.managed_scenario_helpers import (
    END,
    close_in_order,
    deadline_task,
    demand_from_vehicle,
    next_daily,
    originals,
    select_current,
    settle,
    unknown_report,
)
from tests.test_managed_current_overlap import interrupted_increase
from tests.test_managed_proxy import healthy
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_idle_cutoff import early_cleanup
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


# Intent: an actual one-off start overlaps an idle current increase in both transport orders.
# Why: cancelling the new current must preserve the original selected current and original 06:00 end.
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("native_first", [False, True])
async def test_temporary_start_during_current_selection(
    tmp_path, tls, unused_tcp_port, monkeypatch, applied, demand, native_first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        saved = originals(b)
        b.clock.now = b.start - 1
        async with interrupted_increase(b, applied) as (request, release):
            b.clock.now = b.start
            if native_first:
                b.device.active = 987654
                b.rig.peer.plan_session = (987654, b.timer)
                b.device.fire_plan(b.timer)
                demand_from_vehicle(b, demand)
            else:
                local = await b.start_plan(b.timer, demand=demand)
            await b.c.native.read()
            release.set()
        if native_first:
            reply = await b.rig.bench.call(
                b.sock,
                uuid.uuid4().hex,
                "StartTransaction",
                START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
            )
            assert reply[0] == 3, reply
            local = reply[2]["transactionId"]
            b.rig.peer.plan_session = (local, b.timer)
        assert b.c.current.selected() == 6
        assert b.c.requests.receipt(request)["state"] == "failed"
        await settle(b, 5)
        assert not b.stops(), "timer sent Stop before 06:00 after current-selection overlap"
        assert b.device.effective_limit() == 6
        assert b.device.current == (6 if demand else 0)
        b.clock.now = END
        await settle(b)
        assert b.stops() == [local]
        await close_in_order(b, local, "ledger", (local,))
        await healthy(b.c)
        await next_daily(b, 6, saved, (local,))


# Intent: reject the timer Stop and preserve both closure indicators across their independent ordering.
# Why: a command ACK, or native zero with an open transaction, cannot prove cleanup or release.
@pytest.mark.parametrize("first", ["native", "ledger"])
@pytest.mark.parametrize("demand", [False, True])
async def test_rejected_stop_then_independent_closure(tmp_path, tls, unused_tcp_port, monkeypatch, first, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        saved = originals(b)
        b.clock.now = b.start
        local = await b.start_plan(b.timer, demand=demand)
        await healthy(b.c)
        answer = b.device.answer

        # Refuse only the remote Stop so actual native and OCPP closure remain independent facts.
        def reject_stop(action, payload):
            if action == "RemoteStopTransaction":
                return {"status": "Rejected"}
            return answer(action, payload)

        b.device.answer = reject_stop
        b.clock.now = END
        await settle(b)
        assert b.device.effective_limit() == 0 and b.session.journal.active() == local
        assert b.timer in b.device.plans and b.stops() == [local]
        b.device.answer = answer
        await close_in_order(b, local, first, (local,))
        await healthy(b.c)
        await next_daily(b, 6, saved, (local,))


# Intent: releasing one identified independent session cannot exempt a later unidentified session at cutoff.
# Why: origin belongs to an exact live transaction, while the cutoff belongs to the original timer occurrence.
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("closure_first", ["native", "ledger"])
async def test_unknown_after_prior_independent_release(
    tmp_path, tls, unused_tcp_port, monkeypatch, demand, closure_first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        saved = originals(b)
        await early_cleanup(b)
        b.clock.now = END - 120
        # A genuine daily source appears native-first while its OCPP allocation is delayed.
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, 9)
        demand_from_vehicle(b, demand)
        await settle(b)
        assert b.device.effective_limit() == 0
        reply = await b.rig.bench.call(
            b.sock,
            uuid.uuid4().hex,
            "StartTransaction",
            START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert reply[0] == 3, reply
        first = reply[2]["transactionId"]
        b.rig.peer.plan_session = (first, 9)
        await settle(b, 5)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        await close_in_order(b, first, closure_first, ())
        await healthy(b.c)
        b.clock.now = END - 30
        second = await unknown_report(b, b.clock.now, None, demand)
        await settle(b)
        b.clock.now = END
        async with deadline_task(b):
            await settle(b, 5)
        observed = b.device.effective_limit()
        assert not b.stops()
        # Resolve its source and establish cleanup and next daily regardless of the cutoff assertion.
        b.rig.peer.plan_session = (second, 9)
        await settle(b)
        await close_in_order(b, second, "native", ())
        await healthy(b.c)
        await next_daily(b, 6, saved)
        assert observed == 0, "prior independent restoration exempted a different unknown session at 06:00"


# Intent: source evidence loss after an earlier completed-timer continuation must revoke only that positive exception.
# Why: the same OCPP ID and live socket cannot identify a physical session whose native source is now unknown.
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("source", [None, 987655])
async def test_completed_timer_loses_source_after_release(tmp_path, tls, unused_tcp_port, monkeypatch, demand, source):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, 32)
        await b.arm()
        saved = originals(b)
        b.clock.now = b.start + 601
        await settle(b, 5)
        assert b.c.schedule.summary()["state"] == "complete"
        b.clock.now = END - 120
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, 9)
        demand_from_vehicle(b, demand)
        await settle(b)
        assert b.device.effective_limit() == 0
        reply = await b.rig.bench.call(
            b.sock,
            uuid.uuid4().hex,
            "StartTransaction",
            START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert reply[0] == 3, reply
        local = reply[2]["transactionId"]
        b.rig.peer.plan_session = (local, 9)
        await settle(b, 5)
        assert (await healthy(b.c))["station_ceiling_a"] == 32
        assert b.session.journal.get("managed_temporary_schedule")["idle_cutoff_released"] == "verified_session"
        b.clock.now = END - 30
        b.rig.peer.plan_session = (local, source)
        async with deadline_task(b):
            await settle(b)
            b.clock.now = END
            await settle(b, 5)
        observed = b.device.effective_limit()
        assert not b.stops()
        b.rig.peer.plan_session = (local, 9)
        await settle(b)
        await close_in_order(b, local, "ledger", ())
        await healthy(b.c)
        await next_daily(b, 32, saved)
        assert observed == 0, "completed timer inherited an exemption after current source evidence was lost"
