"""N09 observes complete scheduling traces through the production management client and server."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import aiohttp
import pytest

from custom_components.ics_proxy.api import ProxyClient
from tests import conftest as _engine  # noqa: F401 - load the standalone engine package.
from tests.managed_scenario_helpers import (
    END,
    deadline_task,
    demand_from_vehicle,
    originals,
    replace_controller,
    unknown_report,
)
from tests.test_managed_api import TOKEN, ManagementServer, ManagementSettings
from tests.test_managed_proxy import introduced, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_start_overlap import delayed_owner, owner_tag
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


@dataclass
class PublishedState:
    """Observe the single real service poller without invoking a second control loop."""

    status: dict[str, Any]
    generation: int = 0

    # Wait for actual completed publications after the caller changes an external event.
    async def settle(self, count=4):
        target = self.generation + count
        await until(lambda: self.generation >= target)

    # Require a fresh healthy publication after real lifecycle or device events.
    async def healthy(self):
        await self.settle(1)
        await until(lambda: self.status.get("state") == "healthy")
        return self.status


# Deliver both real closure indicators while only the service owns control reconciliation.
async def close_in_order(b, local, first, stops, published):
    if first == "native":
        b.device.active = None
    else:
        await b.close_transaction(local)
        b.device.active = local
    b.device.requested_current = 0
    b.rig.peer.permission_enabled = False
    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
    await published.settle()
    assert b.device.effective_limit() == 0 and tuple(b.stops()) == tuple(stops)
    assert not b.session.journal.get("managed_control_fault")
    if first == "native":
        await b.close_transaction(local)
    else:
        b.device.active = None
    await published.settle()


# Exercise the full next scheduled day through the sole service publisher and actual device lifecycle.
async def next_daily(b, saved, stops, starts_count, published):
    midnight = datetime.fromtimestamp(b.clock.now, ZONE).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        days=1
    )
    b.clock.now = int(midnight.timestamp())
    await published.settle()
    local = await b.start_plan(9)
    state = await published.healthy()
    assert state["station_ceiling_a"] == b.device.effective_limit() == b.device.current == 32
    assert local not in b.stops() and tuple(b.stops()) == tuple(stops)
    await b.advance_running_plan(9, local, int(midnight.replace(hour=6).timestamp()))
    await published.settle()
    await published.healthy()
    assert b.session.journal.active() is None and b.device.current == 0
    assert tuple(b.stops()) == tuple(stops) and len(starts(b.rig)) == starts_count
    assert b.device.plans == {9: saved[0]} and b.rig.peer.configuration == saved[1]


# Verify actual early timer cleanup through service observations rather than an extra controller caller.
async def early_cleanup_selected(b, selected, published):
    b.clock.now = b.start + 601
    await published.settle(5)
    await published.healthy()
    assert b.c.schedule.summary()["state"] == "complete"
    assert set(b.device.plans) == {9} and b.device.effective_limit() == selected


# Publish actual supervised observations independently of the client's polling and connection lifetime.
@asynccontextmanager
async def management_client(b, directory, port):
    published = PublishedState(await b.c.tick())
    server = ManagementServer(
        ManagementSettings(port, directory / "unused", TOKEN),
        b.c,
        lambda: dict(published.status, running=True, certificate_remaining_seconds=86400),
    )

    # The running service owns both observation and deadline progress.
    async def supervise():
        async with deadline_task(b):
            while True:
                published.status = await b.c.tick()
                published.generation += 1
                await asyncio.sleep(0.02)

    await server.start()
    worker = asyncio.create_task(supervise())
    try:
        async with aiohttp.ClientSession() as session:
            yield ProxyClient(session, port, TOKEN), published
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await server.close()
        if not worker.cancelled():
            worker.result()


# Wait for the independently published service observation instead of assuming a client request advances it.
async def confirmed_status(client, ceiling):
    async with asyncio.timeout(5):
        while True:
            state = await client.request("status")
            if state.get("station_ceiling_a") == ceiling:
                return state
            await asyncio.sleep(0.25)


# Admit a one-off using its real client command and verify independent native inventory.
async def arm_from_client(b, client):
    state = await client.request("status")
    receipt = await client.request("temporary_schedule", revision=state["control_revision"], start_epoch=b.start)
    assert receipt["state"] == "accepted"
    await until(lambda: b.c.schedule.summary().get("state") == "armed")
    b.timer = b.session.journal.get("managed_temporary_schedule")["body"]["planId"]
    assert b.timer in b.device.plans
    return receipt["id"]


# Drive an eligible actual temporary session through its cutoff and both real closures.
async def finish_temporary(b, local, demand, published):
    await published.healthy()
    assert b.device.effective_limit() == 32 and b.device.current == (32 if demand else 0)
    b.clock.now = int(datetime.fromtimestamp(b.clock.now, ZONE).replace(hour=6).timestamp())
    await published.settle()
    assert b.device.effective_limit() == 0 and set(b.stops()) == {local}
    stops = tuple(b.stops())
    await close_in_order(b, local, "native", stops, published)
    await published.healthy()
    return stops


# Make the original midnight trigger explicit when a full car has no new actual charging lifecycle.
async def next_date_idle(b, published):
    b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
    b.device.fire_plan(9)
    demand_from_vehicle(b, False)
    await published.settle()
    assert b.session.journal.active() is None and b.device.effective_limit() == 0
    b.clock.now += 3 * 3600
    await published.settle()
    b.start = b.clock.now + 180


# Intent: the production client witnesses six complete recovery compositions with a full or demanding car.
# Why: real service publication, receipts and process replacement must agree with physical peer effects and next daily.
@pytest.mark.parametrize(
    "trace",
    ["unknown", "unplug", "daily-reconnect", "owner-restart", "ledger-first", "replacement"],
)
@pytest.mark.parametrize("demand", [False, True])
async def test_n09_management_scheduling_trace(tmp_path, tls, unused_tcp_port_factory, monkeypatch, trace, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        port = unused_tcp_port_factory()
        stops = ()
        starts_count = 0
        async with management_client(b, tmp_path, port) as (client, published):
            initial = await client.request("status")
            await client.command("set_current", initial["control_revision"], current_a=32)
            await published.healthy()
            saved = originals(b)
            if trace == "owner-restart":
                owner_tag(b)
                current = await client.request("status")
                accepted = await client.request("start", revision=current["control_revision"])
                assert accepted["state"] == "accepted"
                await b.c.tests.settle()
                local = await b.start_plan(9, demand=demand)
                await published.healthy()
                assert len(starts(b.rig)) == 1 and b.session.journal.get("control")["start_pending"]
                starts_count = 1
            else:
                receipt = await arm_from_client(b, client)
                if trace == "daily-reconnect":
                    b.clock.now = b.start
                    temporary = await b.start_plan(b.timer, demand=demand)
                    stops = await finish_temporary(b, temporary, demand, published)
                    state = await client.request("receipt", request_id=receipt)
                    assert state["state"] == "completed"
                    b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
                    local = await b.start_plan(9, demand=demand)
                    await published.healthy()
                elif trace == "unplug":
                    b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
                    await b.rig.bench.call(
                        b.sock,
                        "actual-cable-departure",
                        "StatusNotification",
                        {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
                    )
                    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
                    await published.settle(5)
                    assert b.c.schedule.summary()["result"] == "vehicle_disconnected"
                else:
                    await early_cleanup_selected(b, 32, published)

            if trace in ("unknown", "ledger-first"):
                b.clock.now = END - 60
                if trace == "unknown":
                    local = await unknown_report(b, b.start, None, demand)
                    b.clock.now = END
                    await published.settle()
                    state = await client.request("status")
                    assert b.device.effective_limit() == 0
                    assert state.get("station_ceiling_a") in (None, 0)
                    assert not state["healthy"] and not b.stops()
                    b.rig.peer.plan_session = (local, b.timer)
                    await published.settle()
                    assert set(b.stops()) == {local}
                    stops = tuple(b.stops())
                    await close_in_order(b, local, "ledger", stops, published)
                else:
                    local = await b.start_plan(9, demand=demand)
                    await published.healthy()
                    await b.close_transaction(local)
                    b.device.active = local
                    b.device.requested_current = 0
                    await published.settle()
                    state = await client.request("status")
                    assert b.device.effective_limit() == 0
                    assert state.get("station_ceiling_a") in (None, 0) and not state["healthy"]
                    assert not b.session.journal.get("managed_control_fault") and not b.stops()
                    b.device.active = None
                    await published.settle()
                await published.healthy()
            elif trace in ("unplug", "replacement"):
                b.clock.now = END
                await published.settle()
                await confirmed_status(client, 0)
                await next_date_idle(b, published)
                await b.wait_for_admission()
                receipt = await arm_from_client(b, client)
                assert b.device.effective_limit() == 0
                b.clock.now = b.start
                local = await b.start_plan(b.timer, demand=demand)
                stops = await finish_temporary(b, local, demand, published)
                assert (await client.request("receipt", request_id=receipt))["state"] == "completed"

            if trace not in ("daily-reconnect", "owner-restart"):
                await next_daily(b, saved, stops, 0, published)
                state = await confirmed_status(client, 32)
                assert not state["charging_session_open"]
                return

        # Service replacement retires the old management endpoint and its publication task too.
        await replace_controller(b)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "management-restarted")
            async with management_client(b, tmp_path, port) as (client, published):
                await published.healthy()
                state = await client.request("status")
                assert state["desired_control"] == "app" and state["station_ceiling_a"] == 32
                assert b.device.current == (32 if demand else 0)
                assert tuple(b.stops()) == stops
                await close_in_order(b, local, "native", stops, published)
                if trace == "owner-restart":
                    owner = await delayed_owner(b)
                    await published.settle()
                    assert set(b.stops()) == {owner}
                    stops = tuple(b.stops())
                    await close_in_order(b, owner, "ledger", stops, published)
                await published.healthy()
                await next_daily(b, saved, stops, starts_count, published)
                state = await confirmed_status(client, 32)
                assert not state["charging_session_open"]
