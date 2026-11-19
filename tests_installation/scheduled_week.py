"""Independent calendar, charger, and saved-history checks for a single installation."""

import asyncio
import copy
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ocpp_hub_core.control import HUB_PROFILE_ID

from tests.managed_rig import starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.native_schedule_support import ZONE, SchedulePeer, window
from tests.prototype_rig import START


class WeekPeer(InterruptedNativePeer, SchedulePeer):
    """Use the complete recurring timetable and retain observed native read interruptions."""


@dataclass(frozen=True)
class Charge:
    """Keep a day's identity, calendar slot, and meter reading separate from implementation state."""

    local: int
    vendor: int
    meter: int
    started: datetime
    ends: datetime
    amperes: int


class ScheduledWeek:
    """Each simulated midnight advances the same charger's original plan and its real records."""

    # Record both independent history baselines before the first daily session.
    def __init__(self, proxy):
        self.proxy = proxy
        self.bench, self.peer = proxy.rig.bench, proxy.rig.peer
        self.firmware = self.bench.firmware
        self.plan = copy.deepcopy(self.firmware.plans[9])
        self.starting_history = copy.deepcopy(self.bench.history)
        self.starting_rows = proxy.durable()["transactions"]
        self.records = []
        self.cloud_pairs = []

    # Open a scheduled session only at the start time read from the device's retained recurring plan.
    async def start(self, day, amperes):
        assert self.firmware.plans[9] == self.plan
        date = datetime(2026, 1, 5, tzinfo=ZONE) + timedelta(days=day)
        start, end = window(self.plan, date)
        started, ends = datetime.fromtimestamp(start, UTC), datetime.fromtimestamp(end, UTC)
        meter = 10_000 + day * 30_000
        message = START | {"idTag": self.plan["idTag"], "meterStart": meter, "timestamp": started.isoformat()}
        reply = await self.bench.call(self.proxy.socket, uuid.uuid4().hex, "StartTransaction", message)
        assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
        local = reply[2]["transactionId"]
        self.peer.plan_session = (local, self.plan["planId"])
        self.peer.paused, self.peer.permission_enabled = False, True
        self.bench.cp, self.bench.connection = "C", "Charging"
        self.firmware.fire_plan(self.plan["planId"])
        assert self.firmware.requested_current == 32
        await self.proxy.status(
            lambda state: (
                state.get("requested_current_a") == state.get("current_a") == amperes
                and state.get("power_w") == amperes * 240
            )
        )
        assert self.firmware.current == self.firmware.effective_limit() == amperes
        recorded = await self.proxy.delivered(local)
        (sent,) = (
            frame
            for frame in self.bench.calls
            if frame[0] == 2
            and frame[2] == "StartTransaction"
            and frame[3]["timestamp"] == started.isoformat()
            and frame[3]["meterStart"] == meter
        )
        (allocated,) = (
            frame[2]["transactionId"] for frame in self.bench.sent_vendor_replies if frame[0:2] == [3, sent[1]]
        )
        assert recorded["vendor"] == allocated
        return Charge(local, allocated, meter, started, ends, amperes)

    # Close the exact scheduled session and compare the device, saved history and the vendor's own transcript.
    async def finish(self, charge, *, early=False):
        stopped = charge.started + timedelta(minutes=1) if early else charge.ends
        watt_hours = round(charge.amperes * 240 * (stopped - charge.started).total_seconds() / 3600)
        message = {
            "transactionId": charge.local,
            "meterStop": charge.meter + watt_hours,
            "timestamp": stopped.isoformat(),
        }
        reply = await self.bench.call(self.proxy.socket, uuid.uuid4().hex, "StopTransaction", message)
        assert reply[0] == 3
        self.peer.permission_enabled = False
        row = await self.proxy.delivered(charge.local, closed=True)
        physical = self.bench.history[len(self.starting_history) + len(self.records)]
        assert physical["transactionId"] == charge.local and physical["stop"] == message
        assert json.loads(row["start"]) == physical["start"] and json.loads(row["stop"]) == message
        assert row["vendor"] == charge.vendor
        self.records.append(row)
        self.cloud_pairs.extend(
            [
                ("StartTransaction", charge.started.isoformat(), charge.meter),
                ("StopTransaction", stopped.isoformat(), charge.vendor),
            ]
        )
        self.verify_history()
        assert self.firmware.current == 0

    # Every restart and daily closure must keep exactly the previously observed records.
    def verify_history(self):
        snapshot = self.proxy.durable()
        assert snapshot["transactions"] == self.starting_rows + self.records
        assert not snapshot["outbox"]
        assert self.bench.history[: len(self.starting_history)] == self.starting_history
        actual = [
            (
                frame[2],
                frame[3]["timestamp"],
                frame[3]["meterStart"] if frame[2] == "StartTransaction" else frame[3]["transactionId"],
            )
            for frame in self.bench.calls
            if frame[0] == 2 and frame[2] in ("StartTransaction", "StopTransaction")
        ]
        assert actual == self.cloud_pairs
        assert not starts(self.proxy.rig)

    # Send an ordinary departure and reconnection, keeping the saved limit and charging history.
    async def reconnect_vehicle(self, amperes):
        for connected, cp, native, status in ((False, "A", "Unplug", "Available"), (True, "B", "Plug", "Preparing")):
            self.bench.cp, self.bench.connection = cp, native
            assert (
                await self.bench.call(
                    self.proxy.socket,
                    uuid.uuid4().hex,
                    "StatusNotification",
                    {"connectorId": 1, "status": status, "errorCode": "NoError"},
                )
            )[0] == 3
            await self.proxy.status(
                lambda state: (
                    state.get("vehicle_connected") is connected
                    and state.get("requested_current_a") == amperes
                    and state.get("owner_control_enabled") is True
                    and state.get("power_w") == 0
                )
            )
        self.verify_history()

    # An app Start while Pause owns control must be refused without a new command to the charger.
    async def refuse_app_start(self, token):
        declined = await asyncio.wait_for(
            self.bench.cloud_call(token, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}), 4
        )
        assert declined[0:2] == [4, token] and not starts(self.proxy.rig)
        assert self.firmware.current == 0

    # Observe the actual zero profile after application, before the simulated charger returns its answer.
    async def pause_at_start(self, hass, entry, charge):
        entered, release = asyncio.Event(), asyncio.Event()

        # Delay just the first zero on the station-wide profile. Every actual command remains in the transcript.
        async def hold_zero(action, payload):
            if action != "SetChargingProfile" or entered.is_set():
                return
            profile = payload["csChargingProfiles"]
            periods = profile["chargingSchedule"]["chargingSchedulePeriod"]
            if profile["chargingProfileId"] == HUB_PROFILE_ID and all(period["limit"] == 0 for period in periods):
                entered.set()
                await release.wait()

        before = {row["id"] for row in self.proxy.durable()["requests"]}
        await entry.runtime_data.async_refresh()
        self.bench.before_reply = hold_zero
        press = asyncio.create_task(press_button(hass, "pause"))
        try:
            await asyncio.wait_for(entered.wait(), 8)
            (pause,) = [row for row in self.proxy.durable()["requests"] if row["id"] not in before]
            assert pause["action"] == "pause" and pause["state"] == "accepted"
            assert self.firmware.current == self.firmware.effective_limit() == 0 and not press.done()
            await self.proxy.status(
                lambda state: state.get("desired_control") == "paused" and state.get("local_hold_verified") is False
            )
            await entry.runtime_data.async_refresh()
            assert hass.states.get("sensor.ics_proxy_control_mode").state == "Pause requested"
            await self.refuse_app_start("owner-at-midnight")
            assert self.firmware.active == charge.local and self.firmware.current == 0
            release.set()
            await asyncio.wait_for(self.bench.remote_stop.wait(), 8)
            stops = [
                frame[3]["transactionId"] for frame in self.bench.device_calls if frame[2] == "RemoteStopTransaction"
            ]
            assert stops and set(stops) == {charge.local}
            await self.finish(charge, early=True)
            await press
            return await self.proxy.completed_control(pause["id"])
        finally:
            release.set()
            self.bench.before_reply = None
            if not press.done():
                press.cancel()
            await asyncio.gather(press, return_exceptions=True)


# Use the same registered Home Assistant action as the owner.
async def press_button(hass, action):
    await hass.services.async_call("button", "press", {"entity_id": f"button.ics_proxy_{action}"}, blocking=True)
