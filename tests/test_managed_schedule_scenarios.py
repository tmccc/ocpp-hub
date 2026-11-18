"""Independent daily and owner lifecycle scenarios across restart and delayed closure."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import pytest

from tests.managed_rig import healthy, introduced, starts
from tests.managed_scenario_helpers import (
    END,
    close_in_order,
    demand_from_vehicle,
    next_daily,
    originals,
    replace_controller,
    require_originals,
    select_current,
    settle,
)
from tests.native_schedule_support import timer_bench
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.test_managed_start_overlap import delayed_owner, owner_tag
from tests.tls_support import tls as tls


# Intent: a restart retires an unexpired unbound owner Start without stopping its independent daily session.
# Why: command acknowledgement order and source identity remain distinct from explicit owner Pause authority.
@pytest.mark.parametrize("before_ack", [False, True])
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_n06_unexpired_start_daily_restart(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, before_ack, selected, demand, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory()) as b:
        await select_current(b, selected)
        saved = originals(b)
        owner_tag(b)
        daily_job = None

        # A real independent Start can arrive while the device withholds the earlier command acknowledgement.
        async def before_reply(action, payload):
            nonlocal daily_job
            if before_ack and action == "RemoteStartTransaction":
                daily_job = asyncio.create_task(b.start_plan(9, demand=demand))
                await until(lambda: b.session.journal.active() is not None)

        b.rig.bench.before_reply = before_reply
        try:
            b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
            await b.c.charging.settle()
            daily = await daily_job if daily_job is not None else await b.start_plan(9, demand=demand)
        finally:
            b.rig.bench.before_reply = None
        await healthy(b.c)
        assert len(starts(b.rig)) == 1 and b.session.journal.get("control")["start_pending"]
        await replace_controller(b)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "pending-owner-restart")
            await healthy(b.c)
            assert b.c.requests.intent().mode == "app"
            assert b.session.journal.get("local_start_receipt")["local"] is None
            assert b.device.effective_limit() == selected and not b.stops()
            await close_in_order(b, daily, "native", ())
            assert b.session.journal.get("control")["start_pending"] and not b.stops()
            owner = await delayed_owner(b)
            await settle(b)
            assert set(b.stops()) == {owner} and b.device.effective_limit() == 0
            stops = tuple(b.stops())
            await close_in_order(b, owner, first, stops)
            await healthy(b.c)
            assert not b.session.journal.get("control").get("start_pending")
            require_originals(b, saved, 1)
            await next_daily(b, selected, saved, stops, 1)


# Submit a distinct, acknowledged owner command and its actual lifecycle through both production protocol peers.
async def confirmed_cloud_owner(b, demand):
    accepted = await b.rig.bench.cloud_call(
        "independent-owner", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
    )
    assert accepted[0] == 3 and accepted[2]["status"] == "Accepted"
    reported = await b.rig.bench.call(
        b.sock,
        "owner-lifecycle",
        "StartTransaction",
        START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
    )
    assert reported[0] == 3 and reported[2]["idTagInfo"]["status"] == "Accepted"
    local = reported[2]["transactionId"]
    b.rig.peer.plan_session = (local, 987655)
    demand_from_vehicle(b, demand)
    return local


# Intent: daily and owner closure lag remains recoverable across independent reports and reconnection.
# Why: a daily or independently acknowledged owner session needs both closures without an unrelated permanent fault.
@pytest.mark.parametrize("cloud_owner", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_n07_post_completion_closure(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, cloud_owner, demand, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory()) as b:
        await select_current(b, 32)
        saved = originals(b)
        stops = ()
        b.clock.now = END - 60
        local = await confirmed_cloud_owner(b, demand) if cloud_owner else await b.start_plan(9, demand=demand)
        assert (await healthy(b.c))["station_ceiling_a"] == 32
        assert b.device.current == (32 if demand else 0)
        await close_in_order(b, local, first, stops)
        await healthy(b.c)
        starts_count = int(cloud_owner)
        require_originals(b, saved, starts_count)
        await next_daily(b, 32, saved, stops, starts_count)
