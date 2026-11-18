"""Unknown native sources remain held after real reconnect through the production management client."""

from contextlib import AsyncExitStack

import pytest

from tests import conftest as _engine  # noqa: F401 - load the standalone engine package.
from tests.managed_rig import introduced
from tests.managed_scenario_helpers import END, originals
from tests.native_schedule_support import timer_bench
from tests.runtime_rig import until
from tests.tls_support import tls as tls
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
        timer_bench(tmp_path, tls, unused_tcp_port_factory()) as b,
        AsyncExitStack() as connections,
    ):
        async with management_client(b, tmp_path, unused_tcp_port_factory()) as (client, published):
            state = await client.request("status")
            await client.command("set_current", state["control_revision"], current_a=32)
            await published.healthy()
            saved = originals(b)
            b.clock.now = END - 120
            local = await b.start_plan(9, demand=demand)
            await published.settle(5)
            await published.healthy()
            assert b.device.effective_limit() == 32
            if loss == "new-id":
                await close_in_order(b, local, "ledger", (), published)
                await published.healthy()
            b.clock.now = END - 30
            if loss == "new-id":
                previous = local
                local = await b.start_plan(9, demand=demand)
                await published.healthy()
                assert local != previous
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
