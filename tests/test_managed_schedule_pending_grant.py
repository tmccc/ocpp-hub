"""A later genuine daily occurrence and an older owner's outstanding Start retain separate lifecycles."""

from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


# Intent: let successive genuine daily occurrences run while retaining an unrelated acknowledged cloud Start.
# Why: the older grant may never report, or its report may immediately follow a daily Stop before the next tick.
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("keep_old_native", [False, True])
async def test_daily_and_unbound_owner(tmp_path, tls, unused_tcp_port, monkeypatch, restart, demand, keep_old_native):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await b.acknowledge_older_owner_start()
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "pending-owner-restart")
            j = b.session.journal
            grant = {**j.get("vendor_start_grant"), "expires": 0}
            j.put("vendor_start_grant", grant)
            b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            for _ in range(5):
                await b.c.tick()
            assert b.device.effective_limit() == 0 and set(b.device.plans) == {9}
            tag = b.device.plans[9]["idTag"]
            for day in (12, 13):
                b.clock.now = int(datetime(2026, 11, day, tzinfo=ZONE).timestamp())
                provisional = 1000 + day
                b.device.active = provisional
                b.rig.peer.plan_session = (provisional, 9)
                b.device.fire_plan(9)
                b.rig.peer.paused = False
                b.rig.peer.permission_enabled = demand
                b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
                if not demand:
                    b.device.requested_current = 0
                await b.c.tick()
                result = await b.rig.bench.call(
                    b.sock,
                    f"daily-{day}",
                    "StartTransaction",
                    START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
                )
                assert result[0] == 3 and result[2]["idTagInfo"]["status"] == "Accepted"
                local = result[2]["transactionId"]
                assert b.device.effective_limit() == 0 and j.get("vendor_start_grant") == grant
                b.rig.peer.plan_session = (local, 9)
                assert (await healthy(b.c))["station_ceiling_a"] == 6
                assert b.device.current == (6 if demand else 0) and not b.stops()
                b.clock.now += 60
                await b.close_transaction(local)
                assert j.get("vendor_start_grant") == grant
            if keep_old_native:
                b.device.active = local
                b.rig.peer.plan_session = (local, 9)
            result = await b.rig.bench.call(
                b.sock,
                "delayed-owner",
                "StartTransaction",
                START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
            )
            assert result[0] == 3 and b.device.effective_limit() == 0
            owner = result[2]["transactionId"]
            assert owner != local and j.get("vendor_start_grant") == {**grant, "local": owner}
            if keep_old_native:
                # This peer normally updates SESSION on the OCPP reply. Keep its separate native view behind.
                b.device.active = local
                assert (await b.c.native.read()).physical.native_transaction_id == local
                for _ in range(3):
                    await b.c.tick()
                assert b.device.effective_limit() == 0 and not b.stops()
            b.device.active = owner
            b.rig.peer.plan_session = (owner, 987655)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = demand
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            b.device.requested_current = 32 if demand else 0
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            assert b.device.current == (6 if demand else 0) and not b.stops()
            b.clock.now += 60
            await b.close_transaction(owner)
            await healthy(b.c)
            assert not j.get("vendor_start_grant") and set(b.device.plans) == {9}
            assert len(starts(b.rig)) == 1 and not b.stops()
            next_daily = await b.advance_idle_to(int(datetime(2026, 11, 14, tzinfo=ZONE).timestamp()))
            assert next_daily is not None and (await healthy(b.c))["power_w"] > 0


# Intent: a held lifecycle on the original plan must retain a distinct previously acknowledged local owner's Start.
# Why: both a successful Start report and the positive profile restoration used to clear the one shared pending flag.
async def test_independent_daily_preserves_local_owner(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        g, j = b.session.gateway, b.session.journal
        g.policy = replace(g.policy, allowed_tags=frozenset({"APP_OWNER", *g.policy.allowed_tags}))
        g.controls.policy = g.policy
        guard = g.remote_start_guard
        g.remote_start_guard = None
        try:
            b.c.command("e1dec715d70a414d82a57fec3bd160bb", "start", b.c.requests.intent().revision)
            await b.c.tests.settle()
        finally:
            g.remote_start_guard = guard
        owner_tag = starts(b.rig)[0][3]["idTag"]
        assert owner_tag.casefold() != b.device.plans[9]["idTag"].casefold()
        assert j.get("control")["start_pending"] and b.c.tests.state()["start_rpc_accepted"]
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        for _ in range(4):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and set(b.device.plans) == {9}
        b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        b.device.active = 123456
        b.rig.peer.plan_session = (123456, 9)
        local = await b.start_plan(9)
        assert j.get("control")["start_pending"] and b.device.effective_limit() == 0
        await healthy(b.c)
        assert b.device.current == 6 and j.get("control")["start_pending"]
        b.clock.now += 60
        await b.close_transaction(local)
        result = await b.rig.bench.call(
            b.sock,
            "delayed-local",
            "StartTransaction",
            START | {"idTag": owner_tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert result[0] == 3 and b.device.effective_limit() == 0 and not j.get("control")["start_pending"]
        owner_local = result[2]["transactionId"]
        b.rig.peer.plan_session = (owner_local, 132456)
        await healthy(b.c)
        assert b.device.effective_limit() == 6 and not b.stops()
        await b.close_transaction(owner_local)
        await healthy(b.c)
        assert not b.c.schedule.awaiting_start()
        await b.c.tests.close()


# Intent: an unverified or matching pending command cannot become evidence of a distinct earlier owner.
# Why: a legacy grant may be incomplete even when the independent native plan and plug are both visible.
@pytest.mark.parametrize("changed", ["mutation", "digest"])
async def test_unproven_pending_grant_blocks_separate_daily(tmp_path, tls, unused_tcp_port, monkeypatch, changed):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await b.acknowledge_older_owner_start()
        j = b.session.journal
        original = j.get("vendor_start_grant")
        grant = {**original, changed: ""}
        j.put("vendor_start_grant", grant)
        tag = b.device.plans[9]["idTag"]
        b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        for _ in range(3):
            await b.c.tick()
        b.device.active = 123455
        b.rig.peer.plan_session = (123455, 9)
        await b.c.tick()
        body = START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()}
        reply = await b.rig.bench.call(b.sock, "unconfirmed-daily", "StartTransaction", body)
        assert reply[0] == 4 and j.active() is None and b.device.effective_limit() == 0 and not b.stops()
        assert j.get("vendor_start_grant") == grant
        j.put("vendor_start_grant", original)
        retry = await b.rig.bench.call(b.sock, "confirmed-daily", "StartTransaction", body)
        assert retry[0] == 3 and j.get("vendor_start_grant") == original and b.device.effective_limit() == 0
