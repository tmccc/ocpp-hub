"""Real native timer peers and lifecycle fixtures, independent from test collection and recovery policy."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current import capability_value

from tests.handover_helpers import NativeVehiclePeer
from tests.managed_rig import OwnerBench, healthy, introduced, managed_bench, managed_session
from tests.prototype_rig import START

ORIGINAL_PLAN = {
    "planId": 9,
    "type": "Plan",
    "idTag": "NATIVE_TIMER",
    "eco": "Fast",
    "stopModel": "Duration",
    "stopValue": 360,
    "limitCurrent": 320,
    "ConnectorId": 1,
    "validTime": "2020-01-01T00:00:00Z",
    "startTime": 0,
    "kindType": "Recurring",
    "recurring": 127,
    "maxEnergy": 0,
}


class SchedulePeer(NativeVehiclePeer):
    """The physical getter uses tenths of amps and different connector casing from its OCPP request."""

    # Seed the device with actual mutable wire-format daily settings before the baseline is read.
    def __init__(self, bench: Any) -> None:
        super().__init__(bench)
        wire = dict(ORIGINAL_PLAN)
        wire.update(connectorId=wire.pop("ConnectorId"), limitCurrent=wire["limitCurrent"] / 10)
        del wire["maxEnergy"]
        bench.firmware.plans[9] = wire

    # Simulate the complete native envelope and its independent representation, including fields never sent on OCPP.
    def native(self, key: str) -> Any:
        value = super().native(key)
        if key == "SETPLAN":
            plans = []
            for raw in value["planList"]:
                plan = dict(raw)
                plan.update(
                    ConnectorId=plan.pop("connectorId"),
                    limitCurrent=int(plan["limitCurrent"] * 10),
                    stopValue=int(plan["stopValue"]),
                    maxEnergy=0,
                )
                plans.append(plan)
            return {"planList": plans, "defaultEco": {"mode": "Fast"}}
        return value


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


class SchedulerClock:
    """Calendar advances model elapsed time; clock corrections never rewind the independent retry clock."""

    # Keep explicit elapsed time separate from the calendar used by native plans.
    def __init__(self, now: float) -> None:
        self._now = now
        self.elapsed = 0.0

    # Expose the existing scenario driver's calendar input.
    @property
    def now(self) -> float:
        return self._now

    # Ordinary forward scenario advances also allow cooldowns to elapse.
    @now.setter
    def now(self, value: float) -> None:
        self.elapsed += max(0, value - self._now)
        self._now = value

    # A host clock correction changes calendar time without asserting any elapsed time.
    def step_wall(self, value: float) -> None:
        self._now = value


class TimerBench:
    """Send genuine lifecycle messages from the independent charger as its simulated clock advances."""

    # Keep the physical ledger and native readings independent from controller state.
    def __init__(self, rig: Any, session: Any, sock: Any, clock: Any) -> None:
        self.rig, self.session, self.sock, self.clock = rig, session, sock, clock
        self.c = ManagedController(session)
        self.start = int(clock.now) + 180
        self.device = rig.bench.firmware

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

    # Acknowledge a distinct owner command before its delayed lifecycle arrives.
    async def acknowledge_older_owner_start(self) -> None:
        accepted = await self.rig.bench.cloud_call(
            "older-owner-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
        )
        assert accepted[0] == 3 and accepted[2]["status"] == "Accepted"

    # Deliver the real lifecycle separately from the older Start acknowledgement.
    async def older_owner_session(self, plan_id: int) -> int:
        await self.acknowledge_older_owner_start()
        response = await self.rig.bench.call(
            self.sock,
            "owner-lifecycle",
            "StartTransaction",
            START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(self.clock.now, UTC).isoformat()},
        )
        local = response[2]["transactionId"]
        self.rig.peer.plan_session = (local, plan_id)
        self.rig.peer.paused = False
        self.rig.peer.permission_enabled = True
        self.rig.bench.cp, self.rig.bench.connection = "C", "Charging"
        self.device.requested_current = 32
        return local

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
    tmp_path: Path,
    tls: Any,
    port: int,
    now: int = MORNING,
    *,
    peer_type: type[SchedulePeer] | None = None,
    bench_type: type[OwnerBench] = OwnerBench,
) -> AsyncIterator[TimerBench]:
    clock = SchedulerClock(now)
    selected_peer = SchedulePeer if peer_type is None else peer_type
    async with managed_bench(
        tmp_path, tls, port, native_peer=selected_peer, bench_type=bench_type, time_zone=ZONE.key
    ) as rig:
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


class SelectiveConfigPeer(SchedulePeer):
    """The independent HTTP station can fail one chosen configuration request before sending its body."""

    # Count real received configuration requests without asking the engine to select its own failure boundary.
    def __init__(self, bench):
        super().__init__(bench)
        self.config_requests = 0
        self.fail_at = None
        self.failed_at = None
        self.gate_at = None
        self.entered, self.release = asyncio.Event(), asyncio.Event()

    # Replay ordinary complete HTTP requests after deciding this independent peer's one-shot availability.
    async def handle(self, reader, writer):
        header = await reader.readuntil(b"\r\n\r\n")
        fields = {
            line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
            for line in header.split(b"\r\n")[1:]
            if b":" in line
        }
        body = await reader.readexactly(int(fields[b"content-length"]))
        request = json.loads(body)
        if request["action"] == "getConfig":
            self.config_requests += 1
            if self.config_requests == self.gate_at:
                self.entered.set()
                await self.release.wait()
            if self.config_requests == self.fail_at:
                self.failed_at = self.config_requests
                writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return
        replay = asyncio.StreamReader()
        replay.feed_data(header + body)
        replay.feed_eof()
        await super().handle(replay, writer)
