"""Shared real-peer lifecycle steps for the scenario inventory."""

from __future__ import annotations

import asyncio
import copy
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import ZONE
from tests.test_prototype import START
from tests.test_prototype_runtime import until

END = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())


# Set the ordinary selected current through the same service entry point as the owner control.
async def select_current(b, current):
    if b.c.current.selected() != current:
        b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=current)
        await b.c.current.settle()
        assert (await healthy(b.c))["requested_current_a"] == current


# Run the independently owned deadline task while a trace also drives ordinary service ticks.
@asynccontextmanager
async def deadline_task(b):
    task = asyncio.create_task(b.c.schedule.watch_deadline())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if not task.cancelled():
            task.result()


# Yield after clock changes so both the foreground and independent supervisor can act.
async def settle(b, count=4):
    b.c.schedule.changed.set()
    for _ in range(count):
        await b.c.tick()
        await asyncio.sleep(0)


# Keep device demand separate from station permission and protocol allocation.
def demand_from_vehicle(b, demand):
    b.rig.peer.paused = False
    b.rig.peer.permission_enabled = demand
    b.device.requested_current = 32 if demand else 0
    b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")


# Report one actual physical lifecycle before disclosing its source plan.
async def unknown_report(b, reported, source, demand):
    result = await b.rig.bench.call(
        b.sock,
        uuid.uuid4().hex,
        "StartTransaction",
        START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(reported, UTC).isoformat()},
    )
    assert result[0] == 3 and result[2]["idTagInfo"]["status"] == "Accepted", result
    local = result[2]["transactionId"]
    b.rig.peer.plan_session = (local, source)
    demand_from_vehicle(b, demand)
    return local


# Preserve both saved settings and the entire original plan across the trace.
def originals(b):
    return copy.deepcopy(b.device.plans[9]), copy.deepcopy(b.rig.peer.configuration)


# Check complete independent originals and the absence of synthetic starts.
def require_originals(b, saved, starts_count=0):
    plan, configuration = saved
    assert b.device.plans == {9: plan}
    assert b.rig.peer.configuration == configuration
    assert len(starts(b.rig)) == starts_count


# Separate the two real closure indicators and exercise repeated polls during the disagreement.
async def close_in_order(b, local, first, stops):
    if first == "native":
        b.device.active = None
    else:
        await b.close_transaction(local)
        b.device.active = local
    b.device.requested_current = 0
    b.rig.peer.permission_enabled = False
    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
    await settle(b)
    assert b.device.effective_limit() == 0 and b.stops() == list(stops)
    assert not b.session.journal.get("managed_control_fault")
    if first == "native":
        await b.close_transaction(local)
    else:
        b.device.active = None
    await settle(b)


# Start, draw and autonomously end the next complete original daily occurrence.
async def next_daily(b, current, saved, stops=(), starts_count=0):
    today = datetime.fromtimestamp(b.clock.now, ZONE)
    midnight = today.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    local = await b.advance_idle_to(int(midnight.timestamp()))
    assert local is not None
    status = await healthy(b.c)
    assert b.device.effective_limit() == status["station_ceiling_a"] == current
    assert b.device.current == status["current_a"] == current
    assert local not in b.stops() and b.stops() == list(stops)
    require_originals(b, saved, starts_count)
    await b.advance_running_plan(9, local, int(midnight.replace(hour=6).timestamp()))
    await settle(b)
    assert b.session.journal.active() is None and b.device.current == 0
    assert b.stops() == list(stops)
    require_originals(b, saved, starts_count)
    return local


# Replace the process only after actual known lifecycle delivery has drained.
async def replace_controller(b):
    await until(lambda: b.session.journal.next_delivery() is None and not b.session.gateway.vendor._pending)
    policy = b.session.gateway.policy
    await b.c.tests.close()
    await crash(b.session)
    from tests.test_managed_proxy import managed_session

    b.session = managed_session(b.rig)
    b.session.gateway.policy = policy
    b.session.gateway.controls.policy = policy
    await b.session.start()
    b.c = ManagedController(b.session)
