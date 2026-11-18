"""N09 observes complete scheduling traces through the production management client and server."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import aiohttp
import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings

from custom_components.ics_proxy.api import ProxyClient
from tests import conftest as _engine  # noqa: F401 - load the standalone engine package.
from tests.managed_rig import introduced, starts
from tests.managed_scenario_helpers import originals, replace_controller
from tests.management_peer import TOKEN
from tests.native_schedule_support import ZONE, timer_bench
from tests.runtime_rig import until
from tests.test_managed_start_overlap import delayed_owner, owner_tag
from tests.tls_support import tls as tls


@dataclass
class PublishedState:
    """Observe the single real service poller without invoking a second control loop."""

    status: dict[str, Any]
    generation: int = 0

    async def settle(self, count=4):
        target = self.generation + count
        await until(lambda: self.generation >= target)

    async def healthy(self):
        await self.settle(1)
        await until(lambda: self.status.get("state") == "healthy")
        return self.status


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


@asynccontextmanager
async def management_client(b, directory, port):
    published = PublishedState(await b.c.tick())
    server = ManagementServer(
        ManagementSettings(port, directory / "unused", TOKEN),
        b.c,
        lambda: dict(published.status, running=True, certificate_remaining_seconds=86400),
    )

    async def supervise():
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


async def confirmed_status(client, ceiling):
    async with asyncio.timeout(5):
        while True:
            state = await client.request("status")
            if state.get("station_ceiling_a") == ceiling:
                return state
            await asyncio.sleep(0.25)


@pytest.mark.parametrize("trace", ["daily-reconnect", "owner-restart", "ledger-first"])
@pytest.mark.parametrize("demand", [False, True])
async def test_n09_management_scheduling_trace(tmp_path, tls, unused_tcp_port_factory, monkeypatch, trace, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory()) as b:
        port = unused_tcp_port_factory()
        stops = ()
        starts_count = int(trace == "owner-restart")
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
                await b.c.charging.settle()
            local = await b.start_plan(9, demand=demand)
            await published.healthy()
            if trace == "owner-restart":
                assert len(starts(b.rig)) == 1 and b.session.journal.get("control")["start_pending"]
            if trace == "ledger-first":
                await close_in_order(b, local, "ledger", stops, published)
                await published.healthy()
                await next_daily(b, saved, stops, 0, published)
                state = await confirmed_status(client, 32)
                assert not state["charging_session_open"]
                return

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
