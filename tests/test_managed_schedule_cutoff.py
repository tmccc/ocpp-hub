"""Native timing, deadline supervision and abrupt recovery on an independent charger and backend."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_managed_schedule import SchedulePeer
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls

ZONE = ZoneInfo("Europe/London")
MORNING = int(datetime(2026, 11, 11, 3, 42, tzinfo=ZONE).timestamp())


# Evaluate the installed firmware fields without looking at the controller's start/end calculation or journal.
def window(plan: dict[str, Any], day: datetime) -> tuple[int, int] | None:
    if plan["kindType"] == "Relative":
        if datetime.fromisoformat(plan["validTime"]).astimezone(ZONE).date() != day.date():
            return None
    elif not plan["recurring"] & (1 << day.weekday()):
        return None
    start = day.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=plan["startTime"])
    assert plan["stopModel"] == "Duration"
    return int(start.timestamp()), int(start.timestamp() + plan["stopValue"] * 60)


class TimerBench:
    """Send genuine lifecycle messages from the independent charger as its simulated clock advances."""

    # Keep the physical ledger and native readings independent from controller state.
    def __init__(self, rig: Any, session: Any, sock: Any, clock: Any) -> None:
        self.rig, self.session, self.sock, self.clock = rig, session, sock, clock
        self.c = ManagedController(session)
        self.start = int(clock.now) + 180
        self.device = rig.bench.firmware
        self.timer = 0

    # Register the owner's future timer through the same durable admission path used by management.
    def request(self) -> None:
        self.c.schedule.accept(uuid.uuid4().hex, self.c.requests.intent().revision, self.start)
        self.timer = self.session.journal.get(managed_schedule.KEY)["body"]["planId"]

    # Wait for independent physical visibility after sending the installation once.
    async def arm(self) -> None:
        self.request()
        for _ in range(2):
            await healthy(self.c)
        assert self.c.schedule.summary()["state"] == "armed"

    # Drive a new transaction only when the wire plan is scheduled on this exact local minute.
    async def fire(self, plan_id: int, *, demand: bool = True) -> int:
        plan = self.device.plans[plan_id]
        current = datetime.fromtimestamp(self.clock.now, ZONE)
        bounds = window(plan, current)
        assert plan_id in self.device.plans and bounds and self.clock.now == bounds[0]
        return await self.start_plan(plan_id, demand=demand)

    # The firmware also can deliver a late Start. Its actual plan identifier follows that transaction only.
    async def start_plan(self, plan_id: int, *, demand: bool = True) -> int:
        plan = self.device.plans[plan_id]
        current = datetime.fromtimestamp(self.clock.now, ZONE)
        payload = START | {"idTag": plan["idTag"], "timestamp": current.astimezone(UTC).isoformat()}
        response = await self.rig.bench.call(self.sock, uuid.uuid4().hex, "StartTransaction", payload)
        assert response[2]["idTagInfo"]["status"] == "Accepted"
        self.rig.peer.plan_session = (response[2]["transactionId"], plan_id)
        self.rig.peer.paused = False
        self.rig.peer.permission_enabled = demand
        self.rig.bench.cp, self.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
        self.device.fire_plan(plan_id)
        if not demand:
            self.device.requested_current = 0
        return response[2]["transactionId"]

    # Advance an idle peer through the next trigger, selecting the plan from its own complete saved timetable.
    async def advance_idle_to(self, instant: int) -> int | None:
        assert self.device.active is None
        day = datetime.fromtimestamp(self.clock.now, ZONE)
        final = datetime.fromtimestamp(instant, ZONE)
        due = []
        while day.date() <= final.date():
            for plan_id, plan in self.device.plans.items():
                bounds = window(plan, day)
                if bounds and self.clock.now < bounds[0] <= instant:
                    due.append((bounds[0], plan_id))
            day += timedelta(days=1)
        if not due:
            self.clock.now = instant
            return None
        assert len(due) == 1
        self.clock.now, plan_id = due[0]
        local = await self.fire(plan_id)
        await self.advance_running_plan(plan_id, local, instant)
        return local

    # Let the native clock deliver its autonomous end when the installed duration runs out.
    async def advance_running_plan(self, plan_id: int, local: int, instant: int) -> None:
        plan = self.device.plans[plan_id]
        bounds = window(plan, datetime.fromtimestamp(self.clock.now, ZONE))
        self.clock.now = instant
        if bounds and instant >= bounds[1]:
            await self.close_transaction(local)

    # Deliver actual Stop separately from a Stop acknowledgement or a reduction to zero current.
    async def close_transaction(self, local: int) -> None:
        self.rig.peer.permission_enabled = False
        self.rig.bench.cp, self.rig.bench.connection = "B", "Plug"
        body = {
            "transactionId": local,
            "meterStop": START["meterStart"],
            "timestamp": datetime.fromtimestamp(self.clock.now, UTC).isoformat(),
        }
        await self.rig.bench.call(self.sock, uuid.uuid4().hex, "StopTransaction", body)
        self.device.requested_current = 0

    # Count only actual remote Stops reaching the device.
    def stops(self) -> list[int]:
        return [f[3]["transactionId"] for f in self.rig.bench.device_calls if f[2] == "RemoteStopTransaction"]


# Start independent loopback peers, an introduced controller, and a station-local synthetic clock.
@asynccontextmanager
async def timer_bench(
    tmp_path: Path, tls: Any, port: int, monkeypatch: pytest.MonkeyPatch, now: int = MORNING
) -> AsyncIterator[TimerBench]:
    clock = SimpleNamespace(now=now)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", SchedulePeer)
    monkeypatch.setitem(native_bench.ORIGINAL, "timeZone", ZONE.key)
    monkeypatch.setattr(managed_schedule, "time", SimpleNamespace(time=lambda: clock.now))
    async with managed_bench(tmp_path, tls, port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        rig.route_to_trial()
        async with rig.charger(session) as sock:
            await introduced(rig, session, sock, "timer-boot")
            b = TimerBench(rig, session, sock, clock)
            await healthy(b.c)
            session.journal.put("managed_current_capability", capability_value(session.journal, rig.spec))
            rig.peer.configuration["AmpLmt"] = "32"
            rig.peer.runtime_user = 320
            session.gateway.policy = replace(session.gateway.policy, max_current_a=32)
            session.gateway.controls.policy = session.gateway.policy
            await healthy(b.c)
            yield b


# Intent: a delayed or zero-draw scheduled transaction is supervised until actual closure at the local deadline.
# Why: an Accepted Stop or zero measured draw alone cannot release the station hold or prove timer restoration.
@pytest.mark.parametrize("outcome", ["native-end", "still-drawing", "full-open", "other-plan", "replacement"])
async def test_deadline_and_next_daily(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        original = dict(b.device.plans[9])
        assert await b.advance_idle_to(b.start - 1) is None
        local = await b.advance_idle_to(b.start)
        assert local is not None
        if outcome == "other-plan":
            b.rig.peer.plan_session = (local, 9)
        assert (await healthy(b.c))["power_w"] > 0
        if outcome == "full-open":
            b.rig.peer.permission_enabled = False
            b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
            b.device.requested_current = 0
            assert (await healthy(b.c))["power_w"] == 0 and b.session.journal.active() == local
        end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        assert window(b.device.plans[b.timer], datetime.fromtimestamp(b.start, ZONE)) == (b.start, end)
        b.clock.now = end - 1
        assert (await healthy(b.c))["temporary_schedule"]["state"] == "armed" and not b.stops()
        b.clock.now += 1
        if outcome == "replacement":
            await crash(b.session)
            replacement = managed_session(b.rig)
            await replacement.start()
            b.c, b.session = ManagedController(replacement), replacement
            async with b.rig.charger(replacement) as b.sock:
                await introduced(b.rig, replacement, b.sock, "deadline-restart")
                for _ in range(4):
                    await b.c.tick()
                b.clock.now += 61
                assert (await b.c.tick()).get("fault") == "temporary_schedule_stop_unconfirmed"
                assert len(b.stops()) >= 2 and set(b.stops()) == {local}
                assert b.device.effective_limit() == 0 and b.session.journal.active() == local
                assert not b.session.journal.get("managed_control_fault")
                await b.close_transaction(local)
                for _ in range(3):
                    await healthy(b.c)
                assert set(b.device.plans) == {9} and b.device.effective_limit() == 6 and not starts(b.rig)
            return
        if outcome in ("still-drawing", "full-open"):
            await b.c.tick()
            assert b.stops() == [local] and b.session.journal.active() == local
            assert b.device.effective_limit() == 0 and b.timer in b.device.plans
            b.clock.now += 61
            assert (await b.c.tick())["fault"] == "temporary_schedule_stop_unconfirmed"
        else:
            if outcome == "other-plan":
                await healthy(b.c)
            assert not b.stops()
        if outcome == "native-end":
            await b.advance_running_plan(b.timer, local, end)
            assert b.session.journal.active() is None
        else:
            await b.close_transaction(local)
        for _ in range(3):
            await healthy(b.c)
        assert set(b.device.plans) == {9} and original == b.device.plans[9]
        assert b.c.schedule.summary()["result"] == (
            "session_source_unverified" if outcome == "other-plan" else "session_closed"
        )
        assert b.device.effective_limit() == 6 and not starts(b.rig)
        next_day = datetime(2026, 11, 12, 0, tzinfo=ZONE)
        assert window(b.device.plans[9], next_day) == (int(next_day.timestamp()), int(next_day.timestamp()) + 21600)
        daily = await b.advance_idle_to(int(next_day.timestamp()))
        assert daily is not None
        assert (await healthy(b.c))["power_w"] > 0 and daily != local
        await b.advance_running_plan(9, daily, b.clock.now + 21600)
        assert (await healthy(b.c))["power_w"] == 0 and daily not in b.stops()


# Intent: a local 06:00 cutoff survives both clock changes and rejects a zero or negative charging window.
# Why: firmware durations measure elapsed minutes while the owner specifies a wall-clock end.
@pytest.mark.parametrize(("day", "minutes"), [(datetime(2026, 3, 29), 265), (datetime(2026, 10, 25), 385)])
async def test_clock_change(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, day: datetime, minutes: int
) -> None:
    now = day.replace(hour=0, minute=32, tzinfo=ZONE)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch, int(now.timestamp())) as b:
        for hour, minute in ((6, 0), (6, 1)):
            with pytest.raises(SecurityError):
                b.c.schedule.accept(uuid.uuid4().hex, 0, int(now.replace(hour=hour, minute=minute).timestamp()))
        if day.month == 10:
            for fold in (0, 1):
                with pytest.raises(SecurityError):
                    b.c.schedule.accept(uuid.uuid4().hex, 0, int(now.replace(hour=1, fold=fold).timestamp()))
        await b.arm()
        firmware = b.device.plans[b.timer]
        assert firmware["stopValue"] == minutes
        assert window(firmware, now) == (b.start, int(now.replace(hour=6, minute=0).timestamp()))


# Intent: a disconnected vehicle cannot leave a reusable timer, even if another cable arrives before the next tick.
# Why: the native read which observes removal may be a release/current check outside normal timer supervision.
@pytest.mark.parametrize(
    "phase", ["requested", "installing", "armed", "wire-events", "selecting", "replacement", "held-replacement"]
)
async def test_disconnect_retires_timer(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.request()
        if phase != "requested":
            await b.c.tick()
            if phase != "installing":
                await healthy(b.c)
        if phase == "selecting":
            b.c.current.accept(uuid.uuid4().hex, 0, 16)
        notification = {"connectorId": 1, "status": "Available", "errorCode": "NoError"}
        b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
        if phase == "wire-events":
            for payload in (
                notification | {"connectorId": 0},
                notification | {"timestamp": datetime.fromtimestamp(MORNING - 3600, UTC).isoformat()},
            ):
                await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StatusNotification", payload)
            assert not b.session.journal.get(managed_schedule.KEY).get("retire_reason")
            await b.rig.bench.call(b.sock, "departed", "StatusNotification", notification)
        elif phase == "selecting":
            await b.c.current.settle()
            assert b.device.effective_limit() <= 6 and b.c.current.selected() == 6
        elif phase == "held-replacement":
            answer = b.device.answer

            # Retain the timer despite a zero hold and a clear attempt immediately before the crash.
            def reject_clear(action, payload):
                if action == "DataTransfer" and payload.get("messageId") == "clearPlans":
                    return {"status": "Rejected", "data": ""}
                return answer(action, payload)

            b.device.answer = reject_clear
            await b.c.tick()
            b.device.answer = answer
            b.clock.now += 31
            assert b.device.effective_limit() == 0 and b.timer in b.device.plans
        else:
            await b.c.native.read()
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        if phase == "wire-events":
            await b.rig.bench.call(b.sock, "new-cable", "StatusNotification", notification | {"status": "Preparing"})
        if phase in ("armed", "wire-events", "selecting"):
            b.clock.now = b.start
            start = START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(b.start, UTC).isoformat()}
            reply = await b.rig.bench.call(b.sock, "retired-timer", "StartTransaction", start)
            assert reply[0] == 4 and b.session.journal.active() is None
        if phase in ("replacement", "held-replacement"):
            await crash(b.session)
            replacement = managed_session(b.rig)
            await replacement.start()
            async with b.rig.charger(replacement) as sock:
                await introduced(b.rig, replacement, sock, "reconnect-boot")
                b.c = ManagedController(replacement)
                await healthy(b.c)
        else:
            for _ in range(2):
                await healthy(b.c)
        expected = "cancelled_before_install" if phase == "requested" else "vehicle_disconnected"
        assert b.c.schedule.summary()["result"] == expected
        assert set(b.device.plans) == {9} and not starts(b.rig)


# Intent: owner current selection preserves an armed timer, its absolute deadline and the selected station cap.
# Why: duplicate or successive adjustments must neither replace the plan nor supersede its original authorization.
async def test_armed_current_changes(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.c.current.accept(uuid.uuid4().hex, 0, 32)
        await b.c.current.settle()
        await healthy(b.c)
        await b.arm()
        installed = dict(b.device.plans[b.timer])
        for value in (8, 16):
            identifier = uuid.uuid4().hex
            rev = b.c.requests.intent().revision
            b.c.current.accept(identifier, rev, value)
            await b.c.current.settle()
            assert b.c.current.accept(identifier, rev, value)["state"] == "completed"
            assert (await healthy(b.c))["station_ceiling_a"] == value
            assert b.c.schedule.summary()["state"] == "armed" and installed == b.device.plans[b.timer]
        b.clock.now = b.start
        local = await b.fire(b.timer)
        assert (await healthy(b.c))["temporary_schedule"]["native_plan_confirmed"] and b.device.current == 16
        b.clock.now += 10
        await b.close_transaction(local)
        for _ in range(2):
            await healthy(b.c)
        assert set(b.device.plans) == {9} and b.device.effective_limit() == 16 and not starts(b.rig)


# Intent: persistent refusal, lost replies without application and false ACKs surface a recoverable cleanup alarm.
# Why: only exact native restoration clears the alarm; repeated ticks must not flood the charger with clear commands.
@pytest.mark.parametrize("failure", ["rejected", "lost", "accepted-without-removal"])
async def test_unconfirmed_cleanup(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        sender = b.session.gateway.charger.sender
        clear_count = 0

        # Emulate three real transport outcomes while retaining the installed physical plan.
        async def refuse(raw: str) -> None:
            nonlocal clear_count
            f = json.loads(raw)
            if f[0] == 2 and f[2] == "DataTransfer" and f[3]["messageId"] == "clearPlans":
                clear_count += 1
                if failure == "lost":
                    raise OSError("device did not receive this message")
                result = {"status": "Rejected" if failure == "rejected" else "Accepted", "data": ""}
                b.session.gateway.charger.receive([3, f[1], result])
                return
            await sender(raw)

        b.session.gateway.charger.sender = refuse
        b.clock.now = b.start + 601
        first = await b.c.tick()
        if failure == "rejected":
            assert first["state"] == "degraded"
        for _ in range(2):
            await b.c.tick()
        assert clear_count == 1 and b.timer in b.device.plans
        b.clock.now += 61
        status = await b.c.tick()
        assert status["state"] == "degraded" and status["fault"] == "temporary_schedule_cleanup_unconfirmed"
        assert clear_count == 2 and status["temporary_schedule"]["cleanup_attempts"] == 2
        assert not b.session.journal.get("managed_control_fault")
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        async with b.rig.charger(replacement) as sock:
            await introduced(b.rig, replacement, sock, "retry-boot")
            c = ManagedController(replacement)
            assert (await c.tick())["state"] == "degraded" and c.schedule.summary()["cleanup_attempts"] == 2
            b.clock.now += 61
            await healthy(c)
            assert set(b.device.plans) == {9} and c.schedule.problem() is None and not starts(b.rig)


# Intent: interrupted native installation/removal recovers from independently retained physical state and SQLite.
# Why: a durable sending record cannot distinguish a lost write, a lost reply and a crash just before readback.
@pytest.mark.parametrize("operation", ["setPlan", "clearPlans"])
@pytest.mark.parametrize("boundary", ["before-delivery", "after-delivery", "before-readback"])
async def test_abrupt_timer_mutation(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, operation: str, boundary: str
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        if operation == "setPlan":
            b.request()
        else:
            await b.arm()
            b.clock.now = b.start + 601
        if boundary == "before-readback":
            await b.c.tick()
            assert b.c.schedule.summary()["last_response"] == "accepted"
        else:
            barrier = asyncio.Event()
            sender = b.session.gateway.charger.sender

            # Pause precisely after the local write-ahead record, on either side of actual physical application.
            async def interrupt(raw: str) -> None:
                f = json.loads(raw)
                if f[0] == 2 and f[2] == "DataTransfer" and f[3]["messageId"] == operation:
                    if boundary == "after-delivery":
                        await sender(raw)
                        async with asyncio.timeout(2):
                            while (b.timer in b.device.plans) != (operation == "setPlan"):
                                await asyncio.sleep(0)
                    barrier.set()
                    await asyncio.Future()
                await sender(raw)

            b.session.gateway.charger.sender = interrupt
            task = asyncio.create_task(b.c.tick())
            async with asyncio.timeout(2):
                await barrier.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        state = "installing" if operation == "setPlan" else "clearing"
        assert b.c.schedule.summary()["state"] == state
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        b.clock.now = b.start + 1000
        async with b.rig.charger(replacement) as sock:
            await introduced(b.rig, replacement, sock, "abrupt-restart")
            c = ManagedController(replacement)
            for _ in range(8):
                status = await c.tick()
                if status["state"] == "healthy" and c.schedule.summary()["state"] == "complete":
                    break
                b.clock.now += 3
            assert status["state"] == "healthy" and c.schedule.summary()["state"] == "complete"
            assert set(b.device.plans) == {9} and b.device.effective_limit() == 6 and not starts(b.rig)
            calls = [f[3]["messageId"] for f in b.rig.bench.device_calls if f[2] == "DataTransfer"]
            installed = int(operation != "setPlan" or boundary != "before-delivery")
            assert calls.count("setPlan") == installed and calls.count("clearPlans") == installed


# Intent: local cutoff and selective cleanup continue if the cloud disconnects while a scheduled car is charging.
# Why: the station must stay held until genuine queued lifecycle messages can be reconciled on reconnection.
async def test_cloud_outage_at_cutoff(
    tmp_path: Path, tls: Any, unused_tcp_port_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start
        local = await b.fire(b.timer)
        assert (await healthy(b.c))["temporary_schedule"]["native_plan_confirmed"]
        async with asyncio.timeout(3):
            while b.session.journal.next_delivery() is not None:
                await asyncio.sleep(0.01)
        transport = b.session.runtime.transport
        original = transport.config
        transport.config = replace(
            original,
            vendor_url=urlunsplit(
                urlsplit(original.vendor_url)._replace(netloc=f"127.0.0.1:{unused_tcp_port_factory()}")
            ),
        )
        await b.rig.bench.cloud.close()
        async with asyncio.timeout(5):
            while b.session.runtime.status.vendor_connected:
                await asyncio.sleep(0.01)
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        await b.c.tick()
        assert b.stops() == [local] and b.device.effective_limit() == 0
        await b.close_transaction(local)
        for _ in range(4):
            await b.c.tick()
        assert set(b.device.plans) == {9} and b.device.effective_limit() == 0
        assert b.session.journal.next_delivery() is not None
        transport.config = original
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert b.session.journal.next_delivery() is None and not starts(b.rig)


# Intent: an observed vehicle change keeps a rejected physical timer inhibited through its start and cleanup retry.
# Why: notification alone leaves a retained native plan able to supply power to the newly connected car.
async def test_replug_during_cleanup_refusal(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        answer = b.device.answer

        # A physically retained plan keeps its normal autonomous capability after a rejected selective clear.
        def reject(action, payload):
            if action == "DataTransfer" and payload["messageId"] == "clearPlans":
                return {"status": "Rejected", "data": ""}
            return answer(action, payload)

        b.device.answer = reject
        b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
        await b.c.tick()
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        for _ in range(2):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and b.timer in b.device.plans and not b.stops()
        b.clock.now = b.start
        b.device.fire_plan(b.timer)
        assert b.device.current == 0
        b.device.requested_current = 0
        b.device.answer = answer
        await healthy(b.c)
        assert set(b.device.plans) == {9} and b.device.effective_limit() == 6 and not starts(b.rig)


# Intent: a Start already committed before a disconnect report can be held only when its native plan ID matches.
# Why: recording a retirement reason closes new admissions but cannot undo an already acknowledged Start.
@pytest.mark.parametrize("other_plan", [True, False])
async def test_departure_after_an_acknowledged_start(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, other_plan: bool
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start
        local = await b.fire(b.timer)
        if other_plan:
            b.rig.peer.plan_session = (local, 9)
        await b.rig.bench.call(
            b.sock, "departure", "StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"}
        )
        await b.c.tick()
        assert b.stops() == ([] if other_plan else [local])
        assert b.device.effective_limit() == (6 if other_plan else 0)
        await b.close_transaction(local)
        for _ in range(3):
            await healthy(b.c)
        assert set(b.device.plans) == {9} and not starts(b.rig)


# Intent: exact native identity and the post-admission ledger identify late triggers across the missed-start boundary.
# Why: a timer firing one second outside the grace interval must still stop at 06:00 without adopting another plan.
@pytest.mark.parametrize("delay", [600, 601])
async def test_late_identified_start(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, delay: int
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start + delay
        local = await b.start_plan(b.timer)
        assert (await healthy(b.c))["temporary_schedule"]["native_plan_confirmed"]
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        await b.c.tick()
        assert b.stops() == [local] and b.device.effective_limit() == 0
        await b.close_transaction(local)
        await healthy(b.c)
        assert set(b.device.plans) == {9} and b.c.schedule.summary()["result"] == "session_closed"


# Intent: an altered original daily plan changes both its real readback and its next simulated occurrence.
# Why: a canonical constant substituted by the fake for plan 9 would silently mask a damaged timetable.
async def test_next_daily_reads_device_storage(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.device.plans[9].update(startTime=720, stopValue=1, recurring=0)
        with pytest.raises(SecurityError):
            await b.c.native.read()
        assert await b.advance_idle_to(int(datetime(2026, 11, 12, 6, tzinfo=ZONE).timestamp())) is None
        assert b.session.journal.active() is None and not starts(b.rig)


# Intent: an armed lease written by the previous release keeps its physical digest but gains the 06:00 deadline.
# Why: restarting with an old six-hour payload must never extend a late session to 09:45 or replay installation.
async def test_previous_release_open_timer(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        j = b.session.journal
        old = j.get(managed_schedule.KEY)
        del old["end_epoch"], old["accepted_epoch"]
        old["body"]["stopValue"] = 360
        native_plan = b.device.plans[b.timer]
        native_plan["stopValue"] = 360.0
        old["install"]["data"] = json.dumps(native_plan)
        native = await b.rig.prepared.observer.observe_handover()
        old["augmented_digest"] = native.saved_schedule_digest
        j.put(managed_schedule.KEY, old)
        b.clock.now = b.start
        local = await b.fire(b.timer)
        deadline = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        assert (await healthy(b.c))["temporary_schedule"]["end_epoch"] == deadline
        b.clock.now = deadline
        await b.c.tick()
        assert b.stops() == [local] and b.device.effective_limit() == 0
        await b.close_transaction(local)
        await healthy(b.c)
        assert set(b.device.plans) == {9} and not starts(b.rig)


# Intent: a second session between polls cannot inherit the first one's authority to keep charging or to receive Stop.
# Why: only independent confirmation that the same one-off fired again permits stopping the new transaction.
@pytest.mark.parametrize("source", ["temporary", "daily"])
async def test_session_turnover_between_polls(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start
        first = await b.fire(b.timer)
        await healthy(b.c)
        await b.close_transaction(first)
        b.clock.now += 1200
        second = await b.start_plan(b.timer if source == "temporary" else 9)
        assert second != first
        await b.c.tick()
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        await b.c.tick()
        assert set(b.stops()) == ({second} if source == "temporary" else set())
        await b.close_transaction(second)
        for _ in range(2):
            await healthy(b.c)
        assert set(b.device.plans) == {9} and b.device.effective_limit() == 6 and not starts(b.rig)


# Intent: a same-second Available report retires the request even when the station drops fractional seconds.
# Why: comparing whole-second device time to precise receipt time can admit a replacement vehicle.
async def test_same_second_departure(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.clock.now += 0.3
        b.request()
        b.clock.now += 0.3
        event = {
            "connectorId": 1,
            "status": "Available",
            "errorCode": "NoError",
            "timestamp": datetime.fromtimestamp(int(b.clock.now), UTC).isoformat(),
        }
        await b.rig.bench.call(b.sock, "left", "StatusNotification", event)
        await b.rig.bench.call(b.sock, "another", "StatusNotification", event | {"status": "Preparing"})
        await healthy(b.c)
        assert b.c.schedule.summary()["result"] == "cancelled_before_install" and set(b.device.plans) == {9}


# Intent: retirement holds the physical station throughout failed removal, including a one-minute last window.
# Why: the missed-start grace period and a prior retirement reason cannot extend native permission past the cutoff.
@pytest.mark.parametrize("why", ["closed-before-end", "missed-start", "05:59"])
async def test_retired_timer_cannot_supply_power(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, why: str
) -> None:
    now = int(datetime(2026, 11, 11, 5, 56, tzinfo=ZONE).timestamp()) if why == "05:59" else MORNING
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch, now) as b:
        await b.arm()
        answer = b.device.answer

        # Retain the one-off physically until an explicitly retried clear is allowed through.
        def reject(action, payload):
            if action == "DataTransfer" and payload["messageId"] == "clearPlans":
                return {"status": "Rejected", "data": ""}
            return answer(action, payload)

        b.device.answer = reject
        b.clock.now = b.start
        if why == "closed-before-end":
            local = await b.fire(b.timer)
            await healthy(b.c)
            b.clock.now += 60
            await b.close_transaction(local)
        else:
            b.clock.now += 60 if why == "05:59" else 601
        await b.c.tick()
        assert b.device.effective_limit() == 0 and b.timer in b.device.plans
        await b.rig.bench.call(
            b.sock, "left", "StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"}
        )
        b.device.fire_plan(b.timer)
        assert b.device.current == 0
        b.device.requested_current = 0
        b.clock.now += 61
        b.device.answer = answer
        await healthy(b.c)
        assert b.device.effective_limit() == 6 and set(b.device.plans) == {9}


# Intent: full at zero is still open; Pause waits for the native sensor and OCPP ledger to agree in either order.
# Why: an existing full session cannot be treated as idle for a temporary schedule or a return to positive current.
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_full_session_needs_both_closures(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, first: str
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        old = await b.start_plan(9, demand=False)
        status = await healthy(b.c)
        assert status["charging_session_open"] and status["power_w"] == 0
        with pytest.raises(RecoveryRequiredError):
            b.request()
        request = uuid.uuid4().hex
        b.c.command(request, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        if first == "native":
            b.device.active = None
        else:
            await b.close_transaction(old)
            b.device.active = old
        await b.c.tick()
        assert b.c.requests.receipt(request)["state"] != "completed" and b.device.effective_limit() == 0
        if first == "native":
            await b.close_transaction(old)
        else:
            b.device.active = None
        assert (await healthy(b.c))["local_hold_verified"]
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        await healthy(b.c)
        await b.arm()
        b.clock.now = b.start
        new = await b.fire(b.timer, demand=False)
        assert new != old and (await healthy(b.c))["power_w"] == 0
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        await b.c.tick()
        assert b.stops() == [old, new]
        await b.close_transaction(new)
        await healthy(b.c)
        assert set(b.device.plans) == {9} and not starts(b.rig)
