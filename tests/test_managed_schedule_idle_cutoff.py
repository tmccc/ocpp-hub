"""Idle cutoff and genuine following daily occurrences on independent loopback charger/native peers."""

from __future__ import annotations

import asyncio
import copy
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_schedule import KEY
from ocpp_hub_core.prototype.managed_view import health_snapshot

from tests.handover_helpers import crash
from tests.test_managed_controls import envelope
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls

END = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
NEXT = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())


# Remove a missed one-off using real native inventory before its local deadline.
async def early_cleanup(b):
    b.clock.now = b.start + 601
    for _ in range(5):
        await b.c.tick()
    assert b.c.schedule.summary()["state"] == "complete"
    assert set(b.device.plans) == {9} and b.device.effective_limit() == 6


# A physical daily occurrence can precede its genuine OCPP report while the station still enforces zero.
def native_daily(b, demand):
    b.device.active = 987654
    b.device.fire_plan(9)
    b.rig.peer.plan_session = (987654, 9)
    b.rig.peer.paused = False
    b.rig.peer.permission_enabled = demand
    b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
    if not demand:
        b.device.requested_current = 0


# Deliver a real delayed Start only after native inventory identifies the original daily plan.
async def daily_report(b):
    reply = await b.rig.bench.call(
        b.sock,
        uuid.uuid4().hex,
        "StartTransaction",
        START
        | {"idTag": b.device.plans[9]["idTag"], "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
    )
    assert reply[0] == 3, reply
    local = reply[2]["transactionId"]
    b.rig.peer.plan_session = (local, 9)
    return local


# Intent: healthy idle evidence at 06:00 holds zero even when native cleanup finished hours earlier.
# Why: deleting an unused one-off is inventory reconciliation, not proof of an actual temporary closure.
@pytest.mark.parametrize("cleanup_early", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_idle_cutoff_survives_inventory_completion_and_restart(
    tmp_path, tls, unused_tcp_port, monkeypatch, cleanup_early, restart
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        original = copy.deepcopy(b.device.plans[9])
        if cleanup_early:
            await early_cleanup(b)
        b.clock.now = END
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "idle-cutoff-restart")
            status = await healthy(b.c)
            assert b.session.journal.get(KEY)["idle_cutoff_hold"] is True
            assert status["local_hold_verified"] and health_snapshot(envelope(status))["healthy"]
            assert status["station_ceiling_a"] == b.device.effective_limit() == 0
            assert b.c.requests.intent().mode == "app" and not b.stops() and not starts(b.rig)
            assert b.device.plans[9] == original and set(b.device.plans) == {9}
            b.clock.now = NEXT + 2 * 3600
            for _ in range(4):
                assert (await healthy(b.c))["station_ceiling_a"] == 0
            assert b.session.journal.active() is None and not b.stops() and not starts(b.rig)


# Intent: an independent deadline task applies zero to a completed idle lease without any foreground tick.
# Why: a blocked or absent health poll cannot leave positive physical permission past charger-local 06:00.
async def test_idle_cutoff_watchdog_owns_completed_timer(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await early_cleanup(b)
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            await asyncio.sleep(0)
            b.clock.now = END
            b.c.schedule.changed.set()
            async with asyncio.timeout(3):
                while b.device.effective_limit() != 0:
                    await asyncio.sleep(0.01)
            assert b.session.journal.get(KEY)["idle_cutoff_hold"] is True
            assert not b.stops() and not starts(b.rig)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)


# Intent: only the actual next daily lifecycle plus native identity restores ordinary demand or full-car operation.
# Why: native-first evidence alone cannot invent a Start, and zero demand cannot prevent a valid restoration.
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_next_verified_daily_clears_idle_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch, demand, restart):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        original = copy.deepcopy(b.device.plans[9])
        await early_cleanup(b)
        b.clock.now = END
        await healthy(b.c)
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "daily-after-idle-restart")
                await healthy(b.c)
            b.clock.now = NEXT
            native_daily(b, demand)
            for _ in range(3):
                await b.c.tick()
            assert b.device.effective_limit() == 0 and b.session.journal.active() is None
            assert not b.stops() and not starts(b.rig)
            local = await daily_report(b)
            status = await healthy(b.c)
            lease = b.session.journal.get(KEY)
            assert lease["idle_cutoff_hold"] is False and lease["idle_cutoff_released"] == "verified_session"
            assert status["owner_control_enabled"] and status["station_ceiling_a"] == 6
            assert b.device.current == (6 if demand else 0) and not b.stops() and not starts(b.rig)
            await b.close_transaction(local)
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            b.clock.now = NEXT + 6 * 3600
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            assert b.device.plans[9] == original


# Intent: explicit Pause and Return can override an idle cutoff without synthetic charging or perpetual rearming.
# Why: the automatic safety hold must preserve the owner's existing control path.
async def test_owner_pause_return_clears_idle_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await early_cleanup(b)
        b.clock.now = END
        await healthy(b.c)
        pause = uuid.uuid4().hex
        b.c.command(pause, "pause", b.c.requests.intent().revision)
        assert (await healthy(b.c))["local_hold_verified"]
        returning = uuid.uuid4().hex
        b.c.command(returning, "app_control", b.c.requests.intent().revision)
        status = await healthy(b.c)
        assert b.c.requests.receipt(returning)["state"] == "completed"
        assert status["owner_control_enabled"] and b.device.effective_limit() == 6
        assert b.session.journal.get(KEY)["idle_cutoff_released"] == "owner_return"
        for _ in range(3):
            assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert not starts(b.rig) and not b.stops()


# Intent: a replacement installs at zero and only a real eligible session restores the selected current.
# Why: booking a future timer cannot discharge an expired idle hold or extend that timer's own cutoff.
@pytest.mark.parametrize(
    "source", ["temporary", "native_first", "daily", "owner", "closed", "missed", "early", "expired"]
)
@pytest.mark.parametrize("demand", [False, True])
async def test_new_one_off_can_replace_idle_hold(tmp_path, tls, unused_tcp_port, monkeypatch, source, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=16)
        await b.c.current.settle()
        await healthy(b.c)
        await b.arm()
        b.clock.now = b.start + 601
        for _ in range(5):
            await b.c.tick()
        assert b.c.schedule.summary()["state"] == "complete" and set(b.device.plans) == {9}
        original = copy.deepcopy(b.device.plans[9])
        old_timer = b.timer
        b.clock.now = END
        assert (await healthy(b.c))["station_ceiling_a"] == b.device.effective_limit() == 0
        assert b.session.journal.active() is b.c.latest_native.physical.native_transaction_id is None
        assert not b.c.latest_native.physical.transaction_open
        writes = len(b.rig.bench.received_limits)
        b.clock.now = NEXT + 3 * 3600
        await healthy(b.c)
        b.start = b.clock.now + 180
        await b.wait_for_admission()
        await b.arm()
        assert b.timer != old_timer and b.session.journal.get(KEY)["idle_cutoff_hold"] is True
        assert b.device.effective_limit() == 0 and b.session.journal.active() is None
        assert set(b.device.plans) == {9, b.timer} and not starts(b.rig) and not b.stops()
        for now in (b.start - 60, b.start):
            b.clock.now = now
            assert (await healthy(b.c))["station_ceiling_a"] == b.device.effective_limit() == 0
        assert all(value == 0 for value in b.rig.bench.received_limits[writes:])
        if source == "missed":
            b.clock.now = NEXT + 6 * 3600
            for _ in range(5):
                await b.c.tick()
            assert (await healthy(b.c))["station_ceiling_a"] == b.device.effective_limit() == 0
            assert b.session.journal.active() is None and not starts(b.rig) and not b.stops()
            assert b.device.plans[9] == original and set(b.device.plans) == {9}
            return
        if source == "owner":
            b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
            assert (await healthy(b.c))["local_hold_verified"]
            returning = uuid.uuid4().hex
            b.c.command(returning, "app_control", b.c.requests.intent().revision)
            await healthy(b.c)
            assert b.c.requests.receipt(returning)["state"] == "completed"
            accepted = await b.rig.bench.cloud_call(
                "owner-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
            )
            assert accepted[0] == 3 and accepted[2]["status"] == "Accepted"
            reply = await b.rig.bench.call(
                b.sock,
                "owner-session",
                "StartTransaction",
                START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
            )
            assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
            local = reply[2]["transactionId"]
            b.rig.peer.plan_session = (local, 0)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = demand
            b.device.requested_current = 32 if demand else 0
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
        else:
            plan_id = 9 if source == "daily" else b.timer
            if source == "native_first":
                b.device.active = 987654
                b.device.fire_plan(plan_id)
                b.rig.peer.plan_session = (987654, plan_id)
                b.rig.peer.paused = False
                b.rig.peer.permission_enabled = demand
                b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
                if not demand:
                    b.device.requested_current = 0
                for _ in range(3):
                    await b.c.tick()
                    assert b.device.effective_limit() == b.device.current == 0
                    assert b.session.journal.active() is None and not b.stops()
            # A genuine OCPP report without a matching current physical source cannot raise the ceiling.
            occurred = b.start - 60 if source == "early" else b.start
            if source == "expired":
                b.clock.now = NEXT + 6 * 3600
            reply = await b.rig.bench.call(
                b.sock,
                uuid.uuid4().hex,
                "StartTransaction",
                START
                | {
                    "idTag": b.device.plans[plan_id]["idTag"],
                    "timestamp": datetime.fromtimestamp(occurred, UTC).isoformat(),
                },
            )
            assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
            local = reply[2]["transactionId"]
            b.rig.peer.plan_session = (local, 987655)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = demand
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            b.device.fire_plan(plan_id)
            if not demand:
                b.device.requested_current = 0
            for _ in range(3):
                await b.c.tick()
                assert b.device.effective_limit() == b.device.current == 0 and not b.stops()
            b.rig.peer.plan_session = (local, plan_id)
        if source in ("early", "expired"):
            for _ in range(4):
                await b.c.tick()
            assert b.device.effective_limit() == b.device.current == 0
            assert b.stops() == ([local] if source == "expired" else [])
            if source == "expired":
                await b.close_transaction(local)
                for _ in range(5):
                    await b.c.tick()
                assert (await healthy(b.c))["station_ceiling_a"] == 16
                assert b.c.schedule.summary()["result"] == "session_closed"
            else:
                b.clock.now = NEXT + 6 * 3600
                for _ in range(4):
                    await b.c.tick()
                assert b.device.effective_limit() == b.device.current == 0 and not b.stops()
            assert b.device.plans[9] == original and not starts(b.rig)
            return
        status = await healthy(b.c)
        assert status["station_ceiling_a"] == b.device.effective_limit() == 16
        assert b.device.current == (16 if demand else 0) and not b.stops()
        assert b.session.journal.get(KEY)["idle_cutoff_released"] == (
            "owner_return" if source == "owner" else "verified_session"
        )
        if source == "closed":
            b.clock.now += 60
            await b.close_transaction(local)
            for _ in range(5):
                await b.c.tick()
            assert b.c.schedule.summary()["result"] == "session_closed"
        b.clock.now = NEXT + 6 * 3600
        for _ in range(4):
            await b.c.tick()
        if source in ("temporary", "native_first"):
            assert b.stops() == [local] and b.device.effective_limit() == b.device.current == 0
            await b.close_transaction(local)
            for _ in range(5):
                await b.c.tick()
        else:
            assert not b.stops()
            assert b.device.effective_limit() == 16
            assert b.device.current == (16 if demand and source != "closed" else 0)
            if source in ("daily", "owner"):
                await b.close_transaction(local)
        assert (await healthy(b.c))["station_ceiling_a"] == 16
        assert b.device.plans[9] == original and set(b.device.plans) == {9}
        assert len(starts(b.rig)) == int(source == "owner")


# Intent: a confirmed real temporary closure keeps ordinary app/schedule operation after 06:00.
# Why: the new idle obligation applies only when no temporary start and closure have actually been confirmed.
@pytest.mark.parametrize("demand", [False, True])
async def test_confirmed_temporary_closure_does_not_arm_idle_hold(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start
        local = await b.fire(b.timer, demand=demand)
        await healthy(b.c)
        b.clock.now += 60
        await b.close_transaction(local)
        for _ in range(5):
            await b.c.tick()
        assert b.c.schedule.summary()["result"] == "session_closed"
        b.clock.now = END
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert not b.session.journal.get(KEY)["idle_cutoff_hold"] and not b.stops() and not starts(b.rig)


# Intent: an actual temporary closure reported after the idle cutoff also discharges the durable hold.
# Why: a genuine late lifecycle must restore ordinary operation once both real closure indicators agree.
async def test_late_confirmed_temporary_closure_clears_idle_hold(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start
        b.device.active = 987654
        b.device.fire_plan(b.timer)
        b.rig.peer.plan_session = (987654, b.timer)
        b.rig.peer.paused = False
        b.rig.peer.permission_enabled = True
        b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
        await b.c.tick()
        b.clock.now = END
        await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        reply = await b.rig.bench.call(
            b.sock,
            "late-temporary-start",
            "StartTransaction",
            START
            | {
                "idTag": b.device.plans[b.timer]["idTag"],
                "timestamp": datetime.fromtimestamp(b.start, UTC).isoformat(),
            },
        )
        assert reply[0] == 3
        local = reply[2]["transactionId"]
        b.rig.peer.plan_session = (local, b.timer)
        await b.c.tick()
        assert b.stops() == [local] and b.device.effective_limit() == 0
        await b.close_transaction(local)
        for _ in range(5):
            await b.c.tick()
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert b.session.journal.get(KEY)["idle_cutoff_hold"] is False and not starts(b.rig)
