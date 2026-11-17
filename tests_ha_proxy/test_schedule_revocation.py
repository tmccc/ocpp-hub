"""Unknown native sources remain held after real reconnect through the production management client."""

import uuid
from contextlib import AsyncExitStack
from datetime import UTC, datetime

import pytest

from tests import conftest as _engine  # noqa: F401 - load the standalone engine package.
from tests.managed_scenario_helpers import END, demand_from_vehicle, originals, unknown_report
from tests.native_schedule_support import timer_bench
from tests.test_managed_proxy import introduced
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests_ha_proxy.test_schedule_scenarios import (
    close_in_order,
    management_client,
    next_daily,
)


# Intent: continuation authority for one source cannot exempt a lost source or a second unknown transaction.
# Why: UI health and the physical ceiling must retain genuine reconnect zero without fresh source authority.
@pytest.mark.parametrize("loss", ["same-id", "new-id"])
@pytest.mark.parametrize("demand", [False, True])
async def test_management_reconnect_revokes_unknown_source(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, loss, demand
):
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b,
        AsyncExitStack() as connections,
    ):
        async with management_client(b, tmp_path, unused_tcp_port_factory()) as (client, published):
            state = await client.request("status")
            await client.command("set_current", state["control_revision"], current_a=32)
            await published.healthy()
            saved = originals(b)
            b.clock.now = END - 120
            b.device.active = 987654
            b.rig.peer.plan_session = (987654, 9)
            demand_from_vehicle(b, demand)
            await published.settle()
            reply = await b.rig.bench.call(
                b.sock,
                uuid.uuid4().hex,
                "StartTransaction",
                START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
            )
            assert reply[0] == 3, reply
            local = reply[2]["transactionId"]
            b.rig.peer.plan_session = (local, 9)
            await published.settle(5)
            await published.healthy()
            assert b.device.effective_limit() == 32
            if loss == "new-id":
                await close_in_order(b, local, "ledger", (), published)
                await published.healthy()
            b.clock.now = END - 30
            if loss == "new-id":
                local = await unknown_report(b, b.clock.now, None, demand)
            else:
                b.rig.peer.plan_session = (local, None)
            await published.settle()
            await b.sock.close()
            await until(lambda: not b.session.runtime.status.charger_connected)
            b.sock = await connections.enter_async_context(b.rig.charger(b.session))
            await introduced(b.rig, b.session, b.sock, "unknown-source-real-reconnect")
            await published.settle(5)
            state = await client.request("status")
            ceiling, current = b.device.effective_limit(), b.device.current
            assert ceiling == current == 0
            assert state.get("station_ceiling_a") in (None, 0) and not state["healthy"]
            assert state.get("charging_session_open") is not False
            assert b.session.journal.active() == local and b.device.active == local
            assert not b.stops()
            b.rig.peer.plan_session = (local, 9)
            await published.settle()
            await close_in_order(b, local, "native", (), published)
            await published.healthy()
            await next_daily(b, saved, (), 0, published)
            assert ceiling == 0, (
                "management client reports an unknown open session at a positive ceiling after reconnect"
            )
