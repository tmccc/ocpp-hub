"""The limits that apply to a charger: the hub's, the vendor's, what the charger says, and what survives a restart."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from ocpp_hub_core import readings
from ocpp_hub_core.model import ChargerSnapshot, LimitMemory

from tests.fake_charger import FakeCharger
from tests.fake_vendor import eventually

PROFILE = {
    "connectorId": 1,
    "csChargingProfiles": {
        "chargingProfileId": 5,
        "stackLevel": 0,
        "chargingProfilePurpose": "ChargePointMaxProfile",
        "chargingProfileKind": "Absolute",
        "chargingSchedule": {"chargingRateUnit": "A", "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 10}]},
    },
}


# A booted fake charger behind a hub, with the control of its session.
async def _booted(start_hub: Any) -> tuple[Any, Any, Any]:
    hub = await start_hub()
    charger = await hub.charger()
    await charger.boot()
    return hub, charger, hub.central.control("TEST0001")


# Set a limit through the control while the fake charger accepts the profile it receives.
async def _set(control: Any, charger: Any, amps: float) -> None:
    task = asyncio.create_task(control.set_current_limit(amps))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await task


# Intent: after the hub sets a limit it asks the charger what is in effect; the answer wins over what the hub computes,
# and a charger that cannot answer leaves the lowest limit the hub knows.
# Why: the hub's own limit and the vendor's can both apply and the charger takes the lower; only the charger knows what
# it really does, and the hub must not show more certainty than it has.
async def test_the_limit_in_effect_comes_from_the_charger_when_it_can_say(start_hub) -> None:
    hub, charger, control = await _booted(start_hub)

    await _set(control, charger, 8)
    await eventually(lambda: hub.listener.latest.charge_limit_a == 8.0)
    await asyncio.sleep(0.1)
    fallback = readings.effective_limit_a(hub.listener.latest)

    charger.composite_limit = 6.0
    await _set(control, charger, 8)
    await eventually(lambda: hub.listener.latest.composite_limit_a == 6.0, message="composite not read")

    assert fallback == 8.0 and hub.listener.latest.composite_limit_a == 6.0
    assert readings.effective_limit_a(hub.listener.latest) == 6.0


# Intent: with forwarding on, a profile the vendor sets and the charger accepts shows up as the vendor's limit, the
# lower of the two is the one in effect, and clearing the profile removes it.
# Why: a limit set in the vendor's app must show in Home Assistant too, next to the one Home Assistant set.
async def test_a_limit_set_in_the_vendor_app_shows_in_home_assistant(start_rig) -> None:
    rig = await start_rig(commands="forward")
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")
    control = rig.hub.central.control("TEST0001")
    await _set(control, charger, 16)
    await eventually(lambda: rig.hub.listener.latest.charge_limit_a == 16.0)

    set_task = asyncio.create_task(rig.vendor.command("SetChargingProfile", PROFILE))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await set_task
    await eventually(lambda: rig.hub.listener.latest.vendor_limit_a == 10.0, message="vendor limit not shown")
    lowest = readings.effective_limit_a(rig.hub.listener.latest)

    clear_task = asyncio.create_task(rig.vendor.command("ClearChargingProfile", {"id": 5}))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await clear_task
    await eventually(lambda: rig.hub.listener.latest.vendor_limit_a is None, message="vendor limit not cleared")

    assert lowest == 10.0 and rig.hub.listener.latest.charge_limit_a == 16.0


# Intent: the vendor asking for the composite schedule is answered with the limit in effect, and told there is none when
# nothing is known.
# Why: this is how a limit set in Home Assistant reaches the vendor, besides the offered current in the meter values.
async def test_the_vendor_is_told_the_limit_in_effect(start_rig) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")
    unknown = await rig.vendor.command("GetCompositeSchedule", {"connectorId": 1, "duration": 600})
    control = rig.hub.central.control("TEST0001")
    await _set(control, charger, 6)
    await eventually(lambda: rig.hub.listener.latest.charge_limit_a == 6.0)

    known = await rig.vendor.command("GetCompositeSchedule", {"connectorId": 1, "duration": 600})

    assert unknown[2] == {"status": "Rejected"}
    period = known[2]["chargingSchedule"]["chargingSchedulePeriod"][0]
    assert known[2]["status"] == "Accepted" and period == {"startPeriod": 0, "limit": 6.0}


# Intent: limits restored from storage show on a charger that connects, a vendor charge profile is not restored, and a
# limit survives a reconnect.
# Why: after a restart of Home Assistant the number must show what was last set instead of "unknown", while a charge
# profile died with its charge.
async def test_remembered_limits_show_when_the_charger_connects(start_hub) -> None:
    hub = await start_hub()
    hub.central.restore_limits(
        {
            "TEST0001": LimitMemory(
                hub_a=6.0,
                hub_purpose="ChargePointMaxProfile",
                vendor_a=10.0,
                vendor_profile_id=5,
                vendor_purpose="TxProfile",
            )
        }
    )
    charger = await hub.charger()
    await charger.boot()

    shown = hub.listener.latest
    assert shown.charge_limit_a == 6.0 and shown.limit_purpose == "ChargePointMaxProfile"
    await charger.close()
    await charger.wait_closed()
    again = await hub.charger()
    await again.boot()

    assert hub.central.limit_memory("TEST0001").hub_a == 6.0
    assert hub.central.limit_memory("TEST0001").vendor_a is None
    assert hub.listener.latest.charge_limit_a == 6.0


# Intent: a limit the vendor set with a charge profile is forgotten when that charge stops.
# Why: a charge profile ends with its charge; Home Assistant must not keep showing a limit that no longer applies.
async def test_a_vendor_charge_profile_limit_ends_with_the_charge(start_hub) -> None:
    hub, charger, _ = await _booted(start_hub)
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    hub.central.session("TEST0001").record_vendor_limit(10.0, 5, "TxProfile")
    assert hub.listener.latest.vendor_limit_a == 10.0

    await charger.call(
        "StopTransaction",
        {"transactionId": started[2]["transactionId"], "meterStop": 1, "timestamp": "2026-10-21T09:00:00Z"},
    )

    assert hub.listener.latest.vendor_limit_a is None


# Intent: the limit the charger reported is forgotten when a charge ends, and read again.
# Why: a charge profile that paused the car ends with its charge; the old answer of the charger must not go on showing a
# pause (or tell the vendor about one) until the next write.
async def test_the_charger_reported_limit_does_not_outlive_the_charge(start_hub) -> None:
    hub, charger, control = await _booted(start_hub)
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    charger.composite_limit = 0.0
    task = asyncio.create_task(control.set_current_limit(0))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Rejected"})
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await task
    await eventually(lambda: hub.listener.latest.composite_limit_a == 0.0, message="composite not read")
    charger.composite_limit = None

    await charger.call(
        "StopTransaction",
        {"transactionId": started[2]["transactionId"], "meterStop": 1, "timestamp": "2026-10-21T09:00:00Z"},
    )
    await eventually(lambda: hub.listener.latest.connectors[1].transaction_id is None)

    assert hub.listener.latest.composite_limit_a is None
    assert readings.effective_limit_a(hub.listener.latest) is None


# Intent: a limit restored from storage is shown as unconfirmed, is dropped when the charger says something else is in
# effect, is kept when the charger confirms it, and is not told to the vendor while it is unconfirmed.
# Why: after a restart the charger may have lost the limit (a reboot) or the vendor may have changed it; showing the
# stored pause as the truth would hide that the charger charges at full current, and telling the vendor so would
# repeat the mistake.
async def test_a_restored_limit_is_checked_against_the_charger(start_hub) -> None:
    memory = {"TEST0001": LimitMemory(hub_a=0.0, hub_purpose="ChargePointMaxProfile")}

    # A hub with the stored limit restored and a booted charger that answers the composite schedule with this limit.
    async def connect(composite: float | None) -> Any:
        hub = await start_hub()
        hub.central.restore_limits(dict(memory))
        charger = await hub.charger()
        charger.composite_limit = composite
        await charger.boot()
        return hub

    unconfirmed = await connect(None)
    shown = unconfirmed.listener.latest
    answer = await unconfirmed.central.session("TEST0001").refresh_effective_limit()
    still = unconfirmed.listener.latest
    contradicted = await connect(32.0)
    await contradicted.central.session("TEST0001").refresh_effective_limit()
    confirmed = await connect(0.0)
    await confirmed.central.session("TEST0001").refresh_effective_limit()

    assert answer is None and shown.limits_restored and still.limits_restored and still.charge_limit_a == 0.0
    assert readings.confirmed_limit_a(still) is None
    assert contradicted.listener.latest.charge_limit_a is None and not contradicted.listener.latest.limits_restored
    assert contradicted.listener.latest.composite_limit_a == 32.0
    assert confirmed.listener.latest.charge_limit_a == 0.0 and not confirmed.listener.latest.limits_restored


# Intent: a matching aggregate preserves uncertainty about each separately restored profile.
# Why: the lower profile can explain the effect even if the higher profile disappeared during a restart.
async def test_composite_does_not_confirm_two_restored_profiles(start_hub) -> None:
    hub = await start_hub()
    hub.central.restore_limits(
        {
            "TEST0001": LimitMemory(
                hub_a=16, hub_purpose="ChargePointMaxProfile", vendor_a=6, vendor_purpose="ChargePointMaxProfile"
            ),
        }
    )
    charger = await hub.charger()
    charger.composite_limit = 6
    await charger.boot()
    await hub.central.session("TEST0001").refresh_effective_limit()
    shown = hub.listener.latest
    assert shown.composite_limit_a == 6 and shown.charge_limit_a == 16 and shown.vendor_limit_a == 6
    assert shown.hub_limit_restored and shown.vendor_limit_restored


# Intent: stored limits that are damaged, not finite, negative or beyond any charger are refused when read back.
# Why: the storage file can be edited or damaged, and a number from it must never reach the screen or the vendor
# unchecked.
def test_damaged_stored_limits_are_refused() -> None:
    for record in (
        {"hub_a": float("inf")},
        {"hub_a": -3},
        {"hub_a": 500},
        {"vendor_a": "ten"},
        {"hub_a": 6.0, "vendor_profile_id": "x"},
        "not a record",
    ):
        try:
            LimitMemory.from_dict(record)  # type: ignore[arg-type]
        except KeyError, TypeError, ValueError, AttributeError:
            continue
        raise AssertionError(f"{record!r} must be refused")


# Intent: an accepted command of the vendor does not drop a hub limit that was restored from storage; the charger's
# answer then confirms both together.
# Why: the vendor's write says nothing about the hub's own profile; dropping the stored pause would show "no limit"
# while the charger is paused, and the drop would be saved, so the pause could never be checked again.
async def test_a_vendor_write_does_not_drop_a_restored_hub_pause(start_rig) -> None:
    rig = await start_rig(commands="forward")
    rig.hub.central.restore_limits({"TEST0001": LimitMemory(hub_a=0.0, hub_purpose="ChargePointMaxProfile")})
    charger = await rig.hub.charger()
    charger.composite_limit = 0.0
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    set_task = asyncio.create_task(rig.vendor.command("SetChargingProfile", PROFILE))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await set_task
    await eventually(lambda: rig.hub.listener.latest.composite_limit_a == 0.0, message="composite not read")

    shown = rig.hub.listener.latest
    assert shown.charge_limit_a == 0.0 and shown.vendor_limit_a == 10.0 and not shown.limits_restored
    assert rig.hub.central.limit_memory("TEST0001").hub_a in (None, 0.0)


# Intent: when the vendor asks for more than the present moment, it is answered with the charger's own schedule as seen
# from now (the limit in effect now, then each change); when the hub has no schedule it is answered with the one limit.
# Why: this is how a schedule set in Home Assistant can show in the vendor's app, if the app asks for it; a one-limit
# answer to a day-long question would hide every change.
async def test_the_vendor_is_told_the_schedule_when_it_asks_for_a_day(start_rig) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    charger.composite_limit = 6.0
    charger.composite_periods = [(0, 6.0), (25200, 16.0)]
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")
    control = rig.hub.central.control("TEST0001")
    await _set(control, charger, 6)
    await eventually(lambda: rig.hub.listener.latest.schedule_periods, message="schedule not read")

    day = await rig.vendor.command("GetCompositeSchedule", {"connectorId": 1, "duration": 86400})
    now_only = await rig.vendor.command("GetCompositeSchedule", {"connectorId": 1, "duration": 600})

    periods = day[2]["chargingSchedule"]["chargingSchedulePeriod"]
    assert periods[0] == {"startPeriod": 0, "limit": 6.0} and any(p["limit"] == 16.0 for p in periods)
    assert len(now_only[2]["chargingSchedule"]["chargingSchedulePeriod"]) == 1


# Intent: short and long schedule reads respect the charger's start time before supplying a limit in effect now.
# Why: a future limit cannot replace a confirmed present limit in Home Assistant or the vendor app.
async def test_composite_schedules_respect_the_start_and_active_period(start_rig, monkeypatch) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    now = datetime.now(UTC)

    # Answer through the real charger connection with different periods for short and long reads.
    def schedule_reply(charger: FakeCharger, frame: list[Any]) -> list[Any]:
        duration = frame[3]["duration"]
        start = now + timedelta(seconds=1800 if duration > 3600 else -120)
        periods = [(0, 32), (3600, 16)] if duration > 3600 else [(0, 32), (60, 6)]
        return [
            3,
            frame[1],
            {
                "status": "Accepted",
                "connectorId": 1,
                "scheduleStart": start.isoformat(),
                "chargingSchedule": {
                    "duration": 7200 if duration > 3600 else duration,
                    "chargingRateUnit": "A",
                    "chargingSchedulePeriod": [{"startPeriod": offset, "limit": limit} for offset, limit in periods],
                },
            },
        ]

    monkeypatch.setattr(FakeCharger, "_composite_reply", schedule_reply)
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")
    await _set(rig.hub.central.control("TEST0001"), charger, 8)
    await eventually(lambda: rig.hub.listener.latest.schedule_periods, message="schedule not read")

    assert rig.hub.listener.latest.composite_limit_a == 6
    short = await rig.vendor.command("GetCompositeSchedule", {"connectorId": 1, "duration": 600})
    assert short[2]["chargingSchedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 6}]
    day = await rig.vendor.command("GetCompositeSchedule", {"connectorId": 1, "duration": 86400})
    schedule = day[2]["chargingSchedule"]
    present, future, later = schedule["chargingSchedulePeriod"]
    assert present == {"startPeriod": 0, "limit": 6}
    assert 0 < future["startPeriod"] < later["startPeriod"] < schedule["duration"] <= 9000
    assert future["limit"] == 32 and later["limit"] == 16


# Intent: a future period provides no current limit and a fraction of a second still counts as future.
# Why: rounding or guessing would turn an unconfirmed offer into the limit shown for the current moment.
def test_future_schedule_requires_a_known_present_limit() -> None:
    now = datetime(2026, 10, 21, 12, tzinfo=UTC)
    start = now + timedelta(milliseconds=500)
    snapshot = ChargerSnapshot("TEST0001", schedule_start=start, schedule_periods=((0, 32.0),), schedule_duration_s=60)
    assert readings.schedule_from_now(snapshot, now, 3600) is None
    confirmed = replace(snapshot, composite_limit_a=6)
    assert readings.schedule_from_now(confirmed, now, 3600) == readings.ScheduleWindow(60, ((0, 6), (1, 32)))
    assert readings.schedule_from_now(snapshot, start, 3600) == readings.ScheduleWindow(60, ((0, 32),))
    assert readings.schedule_from_now(snapshot, start + timedelta(seconds=60), 3600) is None
