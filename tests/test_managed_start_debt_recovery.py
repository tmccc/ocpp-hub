"""A verified daily can retain its current without consuming an expired same-tag owner Start."""

import uuid
from contextlib import AsyncExitStack
from datetime import datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from ocpp_hub_core.prototype.trial_rollback import require_lifecycle_idle

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_start_overlap import close_in_order, delayed_owner, expire, owner_tag
from tests.test_prototype_tls import tls as tls


# Intent: same-tag owner debt survives daily recovery, source loss, explicit Pause and actual owner closure.
# Why: exact native daily proof permits continuation without granting the unrelated manual command a lifecycle.
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("boundary", ["outage", "restart"])
@pytest.mark.parametrize("distinct", [False, True])
async def test_expired_debt_preserves_verified_daily(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, selected, demand, boundary, distinct
):
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b,
        AsyncExitStack() as replacements,
    ):
        if distinct:
            owner_tag(b)
        if selected != 6:
            b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=selected)
            await b.c.current.settle()
            await healthy(b.c)
        request = uuid.uuid4().hex
        b.c.command(request, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        receipt = b.session.journal.get("local_start_receipt")
        assert receipt["stage"] == "accepted" and receipt["local"] is None
        if boundary == "outage":
            observer = b.session.prepared.observer
            port = observer.port
            observer.port = unused_tcp_port_factory()
            try:
                with pytest.raises((SecurityError, TimeoutError, OSError)):
                    await b.c.native.read()
                await expire(b)
                assert b.device.effective_limit() == 0 and not b.stops()
            finally:
                observer.port = port
        else:
            await expire(b)
            await b.c.tests.close()
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
            if distinct:
                owner_tag(b)
            b.sock = await replacements.enter_async_context(b.rig.charger(b.session))
            await introduced(b.rig, b.session, b.sock, "expired-debt-recovery")
        for _ in range(3):
            await b.c.tick()
        assert b.c.tests.current_daily(daily)
        assert b.device.effective_limit() == selected
        assert b.session.journal.get("local_start_receipt") == receipt
        assert b.session.journal.get("control")["start_pending"] and not b.stops()
        assert b.c.requests.receipt(request)["state"] == "uncertain"
        if not distinct:
            with pytest.raises(RecoveryRequiredError, match="unbound start"):
                require_lifecycle_idle(b.session.gateway, continuing_local=daily)

        # A fresh observation of lost origin revokes this exact daily's continuation immediately.
        b.rig.peer.plan_session = (daily, None)
        for _ in range(2):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        b.rig.peer.plan_session = (daily, 9)
        assert (await healthy(b.c))["station_ceiling_a"] == selected

        # Explicit Pause still owns Stop, but cannot declare the separate unbound command closed.
        pause = uuid.uuid4().hex
        b.c.command(pause, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        assert b.stops() == [daily] and b.device.effective_limit() == 0
        await b.close_transaction(daily)
        for _ in range(2):
            await b.c.tick()
        assert b.c.requests.receipt(pause)["state"] != "completed"
        assert b.session.journal.get("control")["start_pending"]
        owner = await delayed_owner(b)
        for _ in range(3):
            await b.c.tick()
        assert set(b.stops()) == {daily, owner} and b.device.effective_limit() == 0
        await close_in_order(b, owner, "native" if boundary == "outage" else "ledger")
        assert (await healthy(b.c))["local_hold_verified"]
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == selected
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (daily, owner) and (await healthy(b.c))["current_a"] == selected
        await b.advance_running_plan(9, following, int(datetime(2026, 11, 12, 6, tzinfo=ZONE).timestamp()))
        assert (await healthy(b.c))["power_w"] == 0
        assert set(b.stops()) == {daily, owner} and len(starts(b.rig)) == 1
