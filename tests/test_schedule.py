"""Daily schedules of charge limits: the rules, what is sent to the charger, and reading the charger's own schedule."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from ocpp_hub_core.control import ControlError, ControlPolicy, checked_schedule
from ocpp_hub_core.session import CONFIG_KEYS

from tests.fake_vendor import eventually

POLICY = ControlPolicy(max_current_a=16.0)
ANCHOR = datetime(2026, 10, 21, 22, 0, tzinfo=UTC)


# A booted fake charger behind a hub, with the control of its session.
async def _booted(start_hub: Any) -> tuple[Any, Any, Any]:
    hub = await start_hub()
    charger = await hub.charger()
    await charger.boot()
    return hub, charger, hub.central.control("TEST0001")


# Intent: a schedule must start at 00:00, have increasing starts within the day, at most 24 periods, and every limit
# obeys the single-limit rules: the ceiling holds and a value under 6 A becomes a pause.
# Why: the schedule is a write to the charger with many limits in it; one period above the ceiling would put the whole
# thing past the owner's safety limit, and nothing is repaired silently.
@pytest.mark.parametrize(
    ("periods", "problem"),
    [
        ([(0, 6), (25200, 16)], None),
        ([(0, 4), (3600, 16)], None),
        ([], "invalid_schedule"),
        ([(3600, 6)], "invalid_schedule"),
        ([(0, 6), (0, 10)], "invalid_schedule"),
        ([(0, 6), (7200, 10), (3600, 12)], "invalid_schedule"),
        ([(0, 6), (86400, 10)], "invalid_schedule"),
        ([(0, 6), (True, 10)], "invalid_schedule"),
        ([(0, 6), (3600, 17)], "above_ceiling"),
        ([(0, float("nan"))], "invalid_value"),
        ([(n * 3600, 6) for n in range(25)], "invalid_schedule"),
    ],
)
def test_the_rules_of_a_schedule(periods: list[tuple[Any, Any]], problem: str | None) -> None:
    if problem is None:
        checked = checked_schedule(periods, POLICY)
        assert [start for start, _ in checked] == [start for start, _ in periods]
        return
    with pytest.raises(ControlError) as refused:
        checked_schedule(periods, POLICY)
    assert refused.value.code == problem


# Intent: a schedule is sent as a recurring daily profile on the station, with the hub's own profile id, the anchor in
# UTC, the periods as sent, and a pause as 0 A; the charger's acceptance is recorded as the purpose.
# Why: this is the exact shape a charger needs to run a schedule; the id is shared with the single limit so each
# replaces the other and the hub never leaves two of its own profiles behind.
async def test_a_schedule_is_sent_as_a_recurring_daily_profile(start_hub) -> None:
    hub, charger, control = await _booted(start_hub)

    task = asyncio.create_task(control.set_daily_schedule(ANCHOR, [(0, 4), (25200, 16)]))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    sent = await task

    profile = call[3]["csChargingProfiles"]
    assert call[2] == "SetChargingProfile" and call[3]["connectorId"] == 0 and sent == [(0, 0.0), (25200, 16.0)]
    assert profile["chargingProfileId"] == 7001 and profile["chargingProfilePurpose"] == "ChargePointMaxProfile"
    assert profile["chargingProfileKind"] == "Recurring" and profile["recurrencyKind"] == "Daily"
    schedule = profile["chargingSchedule"]
    assert schedule["startSchedule"] == "2026-10-21T22:00:00.000Z" and schedule["chargingRateUnit"] == "A"
    assert schedule["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 0.0},
        {"startPeriod": 25200, "limit": 16.0},
    ]
    assert hub.listener.latest.limit_purpose == "ChargePointMaxProfile" and hub.listener.latest.charge_limit_a is None


# Intent: when the station profile is refused the default profile is tried, a charge profile is never used for a
# schedule, and when both are refused the write fails with the answers listed.
# Why: a charge profile cannot recur, so offering it would install something other than what was asked.
async def test_a_schedule_falls_back_to_the_default_profile_and_never_to_a_charge_profile(start_hub) -> None:
    _, charger, control = await _booted(start_hub)

    task = asyncio.create_task(control.set_daily_schedule(ANCHOR, [(0, 6)]))
    first = await charger.next_hub_call()
    await charger.answer(first, {"status": "Rejected"})
    second = await charger.next_hub_call()
    await charger.answer(second, {"status": "Rejected"})
    with pytest.raises(ControlError) as refused:
        await task

    purposes = [c[3]["csChargingProfiles"]["chargingProfilePurpose"] for c in (first, second)]
    assert purposes == ["ChargePointMaxProfile", "TxDefaultProfile"] and refused.value.code == "rejected"
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(charger.next_hub_call(), 0.2)


# Intent: the hub reads the charger's own schedule for the next day and keeps it with its start; a charger that cannot
# answer the long question leaves the schedule empty without costing the answer about the limit in effect.
# Why: the schedule shown must be what the charger reports, not what the hub believes it sent; and the two questions are
# separate so that a charger that refuses the long one still tells its present limit.
async def test_the_schedule_the_charger_reports_is_read_and_kept(start_hub) -> None:
    hub, charger, control = await _booted(start_hub)
    charger.composite_limit = 6.0
    charger.composite_periods = [(0, 6.0), (25200, 16.0)]

    task = asyncio.create_task(control.set_current_limit(8))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await task
    await eventually(lambda: hub.listener.latest.schedule_periods, message="schedule not read")

    shown = hub.listener.latest
    assert shown.schedule_periods == ((0, 6.0), (25200, 16.0))
    assert abs((shown.schedule_start - datetime.now(UTC)).total_seconds()) < 60 and shown.composite_limit_a == 6.0

    charger.refuse_long_composite = True
    task = asyncio.create_task(control.set_current_limit(8))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await task
    await eventually(lambda: not hub.listener.latest.schedule_periods, message="schedule not cleared")

    assert hub.listener.latest.composite_limit_a == 6.0


# Intent: a composite schedule answer with an offset beyond a week, offsets that do not increase, or a start far from
# now is refused as a whole.
# Why: the charger is not trusted; an offset of 10**12 seconds made the limit sensor raise on every update, which would
# stop the one sensor that tells what limit is in effect.
@pytest.mark.parametrize(
    "periods",
    [
        [{"startPeriod": 10**12, "limit": 6}],
        [{"startPeriod": 0, "limit": 6}, {"startPeriod": 3600, "limit": 8}, {"startPeriod": 3600, "limit": 9}],
        [{"startPeriod": 7200, "limit": 6}, {"startPeriod": 3600, "limit": 8}],
        [{"startPeriod": 0, "limit": 6}, {"startPeriod": 700000, "limit": 8}],
    ],
)
def test_hostile_schedule_answers_are_refused(periods: list[dict[str, Any]]) -> None:
    from ocpp_hub_core.session import _composite_schedule

    now = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
    answer = {"status": "Accepted", "chargingSchedule": {"chargingSchedulePeriod": periods}}
    far = {
        "status": "Accepted",
        "scheduleStart": "9999-12-31T00:00:00Z",
        "chargingSchedule": {"chargingSchedulePeriod": [{"startPeriod": 0, "limit": 6}]},
    }

    assert _composite_schedule(answer, now, 86400) is None
    assert _composite_schedule(far, now, 86400) is None


# Intent: many requests to read the limit in effect, made while a read runs, end in a small number of reads.
# Why: each accepted vendor command asks for a read; a flood of commands must not become a flood of reads that holds up
# the owner's own writes on the same connection.
async def test_a_burst_of_read_requests_is_folded_into_few_reads(start_hub) -> None:
    hub, charger, _ = await _booted(start_hub)
    session = hub.central.session("TEST0001")
    before = charger.composite_calls

    for _ in range(20):
        session.request_effective_limit()
    await asyncio.sleep(0.5)

    assert 0 < charger.composite_calls - before <= 4


# Intent: a weekly schedule is sent with the weekly recurrence and a duration of a week and accepts offsets beyond a
# day, while the same offsets are refused in a daily schedule.
# Why: the helper in Home Assistant plans a whole week; a weekly offset must not be mistaken for a day offset, and a
# daily schedule must not accept one.
async def test_a_weekly_schedule_is_sent_with_week_offsets(start_hub) -> None:
    hub, charger, control = await _booted(start_hub)
    periods = [(0, 0), (90000, 16), (180000, 6)]

    task = asyncio.create_task(control.set_schedule(ANCHOR, periods, "Weekly"))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    await task

    profile = call[3]["csChargingProfiles"]
    assert profile["recurrencyKind"] == "Weekly" and profile["chargingSchedule"]["duration"] == 604800
    assert [p["startPeriod"] for p in profile["chargingSchedule"]["chargingSchedulePeriod"]] == [0, 90000, 180000]
    with pytest.raises(ControlError):
        checked_schedule([(0, 6), (90000, 6)], POLICY, span_s=86400)


# Intent: the schedule is read once the charger has been set up after a boot, without waiting for a write.
# Why: after a restart of Home Assistant the schedule attribute stayed empty until something was written, which hid a
# schedule the charger was already running.
async def test_the_schedule_is_read_after_the_boot_setup(start_hub) -> None:
    hub = await start_hub(setup_delay_s=0.0)
    charger = await hub.charger()
    charger.composite_limit = 6.0
    charger.composite_periods = [(0, 6.0), (25200, 16.0)]
    await charger.boot()
    # Answer the setup: every configuration key as unknown, and the metering write as accepted.
    for _ in range(len(CONFIG_KEYS)):
        request = await charger.next_hub_call()
        await charger.answer(request, {"configurationKey": [], "unknownKey": request[3]["key"]})
    while True:
        try:
            request = await asyncio.wait_for(charger.next_hub_call(), 0.5)
        except TimeoutError:
            break
        await charger.answer(request, {"status": "Accepted"})
    await eventually(lambda: hub.listener.latest.schedule_periods, message="schedule not read after boot")

    assert hub.listener.latest.schedule_periods == ((0, 6.0), (25200, 16.0))
