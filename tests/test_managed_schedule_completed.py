"""Exercise cutoff obligations after native inventory cleanup through actual loopback peers."""

from __future__ import annotations

import copy
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_schedule_idle_cutoff import early_cleanup
from tests.test_managed_start_overlap import close_in_order, delayed_owner, owner_tag
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

END = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
NEXT = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())


# Deliver a genuine delayed lifecycle separately from its independent native source evidence.
async def report_unknown(b, reported):
    reply = await b.rig.bench.call(
        b.sock,
        uuid.uuid4().hex,
        "StartTransaction",
        START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(reported, UTC).isoformat()},
    )
    assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted", reply
    local = reply[2]["transactionId"]
    b.rig.peer.plan_session = (local, None)
    b.rig.peer.paused = False
    return local


# Intent: cleanup, reporting time and demand cannot release a session whose source remains unidentified at cutoff.
# Why: completing inventory reconciliation does not discharge the independent wall-clock obligation.
@pytest.mark.parametrize("arrives", [-60, 0, 60])
@pytest.mark.parametrize("source", [None, 987655, 9])
@pytest.mark.parametrize("demand", [False, True])
async def test_removed_timer_crosses_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch, arrives, source, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        original = copy.deepcopy(b.device.plans[9])
        await early_cleanup(b)
        b.clock.now = END + arrives
        for _ in range(2):
            await b.c.tick()
        local = await report_unknown(b, b.start)
        b.rig.peer.plan_session = (local, source)
        b.rig.peer.permission_enabled = demand
        b.device.requested_current = 32 if demand else 0
        b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
        b.clock.now = max(END, b.clock.now)
        for _ in range(4):
            await b.c.tick()
        assert b.device.effective_limit() == (6 if source == 9 else 0)
        assert b.device.current == (6 if demand and source == 9 else 0)
        assert not b.stops()
        b.rig.peer.plan_session = (local, 9)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        await b.close_transaction(local)
        await healthy(b.c)
        daily = await b.advance_idle_to(NEXT)
        assert daily not in (None, local) and (await healthy(b.c))["current_a"] == 6
        assert b.device.plans == {9: original} and not b.stops() and not starts(b.rig)


# Intent: reintroduction enforces zero without a speculative Stop even when native telemetry is absent.
# Why: a removed timer's durable station obligation must remain usable across every supported Boot boundary.
@pytest.mark.parametrize("boundary", ["reconnect", "restart", "boot"])
@pytest.mark.parametrize("outage", [False, True])
async def test_removed_timer_reintroduction(tmp_path, tls, unused_tcp_port_factory, monkeypatch, boundary, outage):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await b.arm()
        await early_cleanup(b)
        b.clock.now = END - 60
        local = await report_unknown(b, b.start)
        for _ in range(2):
            await b.c.tick()
        b.clock.now = END
        await b.c.tick()
        await until(lambda: b.session.journal.next_delivery() is None and not b.session.gateway.vendor._pending)
        if boundary == "restart":
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if boundary != "boot" else nullcontext(b.sock) as b.sock:
            observer = b.session.prepared.observer
            port = observer.port
            if outage:
                observer.port = unused_tcp_port_factory()
            try:
                assert (await b.rig.bench.call(b.sock, "cutoff-boot", "BootNotification", BOOT))[0] == 3
                await until(
                    lambda: (
                        b.session.gateway.controls.ready
                        or b.session.runtime.status.last_failure == "BootOrCeilingUnavailable"
                    )
                )
                for _ in range(3):
                    await b.c.tick()
                assert b.device.effective_limit() == 0 and not b.stops()
            finally:
                observer.port = port
            await introduced(b.rig, b.session, b.sock, "source-returns")
            b.rig.peer.plan_session = (local, 9)
            await healthy(b.c)
            assert not b.stops()
            await b.close_transaction(local)
            await healthy(b.c)


# Intent: native- and protocol-detected departures preserve an unused installed timer's original cutoff.
# Why: a cable departure proves no charge start or closure and cannot release the selected idle policy.
@pytest.mark.parametrize("wire_departure", [False, True])
async def test_unused_departure_keeps_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch, wire_departure):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
        if wire_departure:
            await b.rig.bench.call(
                b.sock,
                "departed",
                "StatusNotification",
                {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
            )
        else:
            await b.c.native.read()
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        for _ in range(5):
            await b.c.tick()
        assert b.c.schedule.summary()["result"] == "vehicle_disconnected" and set(b.device.plans) == {9}
        b.clock.now = END
        assert (await healthy(b.c))["station_ceiling_a"] == 0
        assert not b.stops() and not starts(b.rig)


# Intent: replacement before Start expiry preserves the independent daily and the separate late owner debt.
# Why: startup's zero hold cannot turn the missing owner lifecycle into authority to Stop a different charge.
@pytest.mark.parametrize("demand", [False, True])
async def test_restart_before_owner_start_expiry(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        assert b.session.journal.get("managed_test_charge")["independent_seen"]
        assert b.session.journal.get("local_start_receipt")["local"] is None
        await b.c.tests.close()
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
        await b.arm()
        original = copy.deepcopy(b.device.plans[9])
        b.clock.now = b.start
        temporary = await b.start_plan(b.timer, demand=demand)
        await healthy(b.c)
        b.clock.now = END
        await b.c.tick()
        await b.close_transaction(temporary)
        await healthy(b.c)
        assert set(b.device.plans) == {9}
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


# Intent: a closing session after timer cleanup can report its two closure indicators in either order.
# Why: a temporary disagreement must hold zero, clear on actual closure and allow the following daily charge.
@pytest.mark.parametrize("first", ["native", "ledger"])
@pytest.mark.parametrize("source", [None, 9])
async def test_completed_timer_closure_disagreement(tmp_path, tls, unused_tcp_port, monkeypatch, first, source):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await early_cleanup(b)
        b.clock.now = END - 60
        local = await report_unknown(b, b.start)
        b.rig.peer.plan_session = (local, source)
        for _ in range(3):
            await b.c.tick()
        if first == "native":
            b.device.active = None
        else:
            await b.close_transaction(local)
            b.device.active = local
        b.device.requested_current = 0
        b.rig.peer.permission_enabled = False
        for _ in range(4):
            await b.c.tick()
        assert not b.session.journal.get("managed_control_fault")
        assert not b.stops() and b.device.effective_limit() == 0
        if first == "native":
            await b.close_transaction(local)
        else:
            b.device.active = None
        for _ in range(4):
            await b.c.tick()
        await healthy(b.c)
        daily = await b.advance_idle_to(NEXT)
        assert daily not in (None, local) and (await healthy(b.c))["current_a"] == 6
        assert not b.stops()
