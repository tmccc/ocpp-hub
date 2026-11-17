"""Exercise cutoff obligations after native inventory cleanup through actual loopback peers."""

from __future__ import annotations

import copy
import uuid
from datetime import datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.native_schedule_support import ZONE, timer_bench
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_start_overlap import close_in_order, delayed_owner, owner_tag
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

END = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
NEXT = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())


# Intent: replacement before Start expiry preserves the independent daily and the separate late owner debt.
# Why: startup's zero hold cannot turn the missing owner lifecycle into authority to Stop a different charge.
@pytest.mark.parametrize("demand", [False, True])
async def test_restart_before_owner_start_expiry(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        assert b.session.journal.get("managed_test_charge")["independent_seen"]
        await b.c.charging.close()
        await until(lambda: b.session.journal.next_delivery() is None)
        vendor = b.session.journal.vendor_id(daily)
        assert type(vendor) is int and vendor > 0
        assert b.session.journal.get("local_start_receipt")["local"] is None
        assert b.session.journal.get("control")["start_pending"] is True
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        owner_tag(b)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "restart-before-start-expiry")
            assert b.device.effective_limit() == 0
            for _ in range(4):
                await b.c.tick()
            assert daily not in b.stops()
            status = await healthy(b.c)
            assert status["station_ceiling_a"] == b.device.effective_limit() == 6
            assert status["current_a"] == (6 if demand else 0)
            assert b.c.requests.intent().mode == "app"
            assert b.session.journal.get("control")["start_pending"] is True
            assert b.session.journal.get("local_start_receipt")["local"] is None
            await b.close_transaction(daily)
            await healthy(b.c)
            owner = await delayed_owner(b)
            for _ in range(3):
                await b.c.tick()
            assert set(b.stops()) == {owner} and b.device.effective_limit() == 0
            await close_in_order(b, owner, "native" if demand else "ledger")
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            following = await b.advance_idle_to(NEXT)
            assert following not in (None, daily, owner) and (await healthy(b.c))["current_a"] == 6
            assert set(b.stops()) == {owner} and len(starts(b.rig)) == 1


# Intent: reconnect preserves an independently identified daily transaction.
# Why: startup recovery owns zero while it establishes source, without borrowing an owner Stop.
@pytest.mark.parametrize("demand", [False, True])
async def test_reconnect_following_daily(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        original = copy.deepcopy(b.device.plans[9])
        b.clock.now = NEXT
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        await until(lambda: not b.session.journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone())
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "reconnect-following-daily")
            states = []
            for _ in range(6):
                states.append(await b.c.tick())
            assert all(status["state"] not in ("attention", "stopping") for status in states)
            assert states[-1]["state"] == "healthy" and states[-1]["station_ceiling_a"] == 6
            assert daily not in b.stops()
            assert b.device.effective_limit() == 6 and b.device.plans == {9: original}
