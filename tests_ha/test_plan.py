"""The plan of the hub: set from Home Assistant or by the app, written to the charger as a profile, and kept."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ocpp_hub.const import CONF_VENDOR_COMMANDS, CONF_VENDOR_URL, DOMAIN
from custom_components.ocpp_hub.core import raedian
from custom_components.ocpp_hub.core.raedian import PlanWindow
from tests.fake_vendor import FakeVendor
from tests_ha.conftest import state_of, wait_for

PLAN = "sensor.ocpp_hub_test0001_app_plan"
MONDAY_NOON = {"day": "monday", "from": "12:00", "to": "14:00"}
TUESDAY_AFTERNOON = {"day": "tuesday", "from": "13:00", "to": "17:00"}


# A booted charger with its entities.
async def _booted(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any, **options: Any
) -> tuple[Any, MockConfigEntry]:
    entry = await setup_hub(options)
    charger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, PLAN) is not None, "no entities")
    return charger, entry


# Call a service while the fake charger answers the one write it causes, and return that write.
async def _call(hass: HomeAssistant, charger: Any, data: dict[str, Any], status: str = "Accepted") -> list[Any]:
    call = asyncio.create_task(hass.services.async_call(DOMAIN, "set_plan", data, blocking=True))
    request = await charger.next_hub_call()
    await charger.answer(request, {"status": status})
    await call
    return request


# Intent: an accepted native planning switch completes its secondary profile change after a vendor reconnect.
# Why: the charger's acknowledgement survives the vendor socket and HA must reflect the actual installed profile.
async def test_accepted_planning_switch_survives_vendor_disconnect(hass, setup_hub, connect_charger):
    vendor = FakeVendor()
    await vendor.start()
    try:
        entry = await setup_hub({CONF_VENDOR_URL: vendor.url, CONF_VENDOR_COMMANDS: "forward"})
        charger = await connect_charger(entry)
        await charger.boot()
        await wait_for(hass, lambda: vendor.calls_of("BootNotification"), "the boot did not reach the vendor")
        await _call(hass, charger, {"windows": [MONDAY_NOON]})
        session = entry.runtime_data.central.session("TEST0001")
        assert session.snapshot.limit_purpose == "ChargePointMaxProfile"
        data = raedian.build_frame(
            raedian.HEAD_CLOUD,
            raedian.COMMAND_PLANNING_SWITCH,
            bytes.fromhex("0123456789abcdbe"),
            bytes([raedian.PLANNING_OFF]),
        )
        payload = {
            "vendorId": "READIAN",
            "messageId": "RemoteConfiguration",
            "data": json.dumps({"instructType": "b3", "originalData": data}),
        }
        switch = asyncio.create_task(vendor.command("DataTransfer", payload))
        native_switch = await charger.next_hub_call()
        prior_connections = vendor.connection_count
        await vendor.stop()
        switch.cancel()
        await asyncio.gather(switch, return_exceptions=True)
        await vendor.start()
        await wait_for(hass, lambda: vendor.connection_count > prior_connections, "vendor did not reconnect")
        assert session.snapshot.limit_purpose == "ChargePointMaxProfile"
        await charger.answer(native_switch, {"status": "Accepted"})
        clear = await charger.next_hub_call()
        assert clear[2] == "ClearChargingProfile"
        await charger.answer(clear, {"status": "Accepted"})
        await wait_for(hass, lambda: session.snapshot.limit_purpose is None, "accepted profile clear was lost")
        assert entry.runtime_data.plans.plan("TEST0001") == [PlanWindow(1, 43200, 50400)]
        assert not entry.runtime_data.plans.planning_on("TEST0001")
        assert entry.runtime_data.plans.profile_sync_error("TEST0001") is None
    finally:
        await vendor.stop()


# Intent: setting a plan writes one weekly profile to the charger with the limit inside the windows from the ceiling and
# a pause outside them, shows the windows, and keeps them as the plan.
# Why: the plan is what the app and Home Assistant both show; the charger must be given what the plan says.
async def test_a_plan_is_written_to_the_charger_and_kept(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)

    request = await _call(hass, charger, {"windows": [MONDAY_NOON, TUESDAY_AFTERNOON]})
    await wait_for(hass, lambda: state_of(hass, PLAN) == "2", "plan not shown")

    schedule = request[3]["csChargingProfiles"]["chargingSchedule"]
    assert request[3]["csChargingProfiles"]["recurrencyKind"] == "Weekly"
    assert [(p["startPeriod"], p["limit"]) for p in schedule["chargingSchedulePeriod"]] == [
        (0, 0.0),
        (12 * 3600, 16.0),
        (14 * 3600, 0.0),
        (86400 + 13 * 3600, 16.0),
        (86400 + 17 * 3600, 0.0),
    ]
    assert hass.states.get(PLAN).attributes["windows"] == [
        {"day": "monday", "from": "12:00", "to": "14:00"},
        {"day": "tuesday", "from": "13:00", "to": "17:00"},
    ]
    assert entry.runtime_data.plans.plan("TEST0001") == [PlanWindow(1, 43200, 50400), PlanWindow(2, 46800, 61200)]


# Intent: when the charger refuses the profile the plan stays as it was and the service reports an error; an empty plan
# removes the hub's profile instead of holding the charger at the outside limit.
# Why: a plan the charger does not obey must not be shown as the plan, and an empty one means no restriction.
async def test_a_refused_plan_is_not_kept_and_an_empty_plan_clears_the_profile(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    await wait_for(hass, lambda: state_of(hass, PLAN) == "1", "plan not shown")

    refusal = asyncio.create_task(
        hass.services.async_call(DOMAIN, "set_plan", {"windows": [MONDAY_NOON, TUESDAY_AFTERNOON]}, blocking=True)
    )
    for _ in range(2):
        request = await charger.next_hub_call()
        await charger.answer(request, {"status": "Rejected"})
    with pytest.raises(HomeAssistantError):
        await refusal
    kept = state_of(hass, PLAN)
    cleared = await _call(hass, charger, {"windows": []})
    await wait_for(hass, lambda: state_of(hass, PLAN) == "0", "plan not cleared")

    assert kept == "1" and cleared[2] == "ClearChargingProfile"


# Intent: when something else replaces the hub's profile after a plan was set (a single limit, here), the plan is
# emptied instead of going on to claim what the charger no longer follows, and the app is answered with the empty plan.
# Why: the plan and the single limit share one profile on the charger; showing the old windows after a limit replaced
# them would tell the owner and the app something untrue.
async def test_a_limit_written_after_the_plan_empties_the_plan(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    await wait_for(hass, lambda: state_of(hass, PLAN) == "1", "plan not shown")

    call = asyncio.create_task(
        hass.services.async_call(
            "number", "set_value", {"entity_id": "number.ocpp_hub_test0001_charge_limit", "value": 10}, blocking=True
        )
    )
    request = await charger.next_hub_call()
    await charger.answer(request, {"status": "Accepted"})
    await call
    await wait_for(hass, lambda: state_of(hass, PLAN) == "0", "plan not emptied")

    assert entry.runtime_data.plans.plan("TEST0001") == []


# Intent: an accepted manual limit during a rejected plan replacement retires the old plan.
# Why: a plan's pending fallback does not own the separate scalar ACK.
async def test_rejected_replacement_cannot_hide_external_limit(hass, setup_hub, connect_charger):
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    plans = entry.runtime_data.plans
    replacement = asyncio.create_task(plans.app_wrote("TEST0001", [PlanWindow(2, 46800, 61200)]))
    first = await charger.next_hub_call()
    control = entry.runtime_data.central.control("TEST0001")
    scalar = asyncio.create_task(control.set_current_limit(10))
    await asyncio.sleep(0)
    await charger.answer(first, {"status": "Rejected"})
    manual = await charger.next_hub_call()
    assert manual[3]["csChargingProfiles"]["chargingProfileKind"] != "Recurring"
    await charger.answer(manual, {"status": "Accepted"})
    await scalar
    fallback = await charger.next_hub_call()
    assert fallback[3]["csChargingProfiles"]["chargingProfileKind"] == "Recurring"
    await charger.answer(fallback, {"status": "Rejected"})
    assert not await replacement
    assert plans.plan("TEST0001") == []


# Intent: a native enable waiting on a plan edit uses the replacement windows after the edit's ACK.
# Why: keeping the new windows in HA while reinstalling the earlier ones would misreport what the charger follows.
async def test_native_enable_uses_concurrent_plan_replacement(hass, setup_hub, connect_charger):
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    plans = entry.runtime_data.plans
    off = asyncio.create_task(plans.planning_switched("TEST0001", False))
    await charger.answer(await charger.next_hub_call(), {"status": "Accepted"})
    await off
    replacement = asyncio.create_task(plans.app_wrote("TEST0001", [PlanWindow(2, 46800, 61200)]))
    clearing = await charger.next_hub_call()
    assert clearing[2] == "ClearChargingProfile"
    on = asyncio.create_task(plans.planning_switched("TEST0001", True))
    await wait_for(hass, lambda: plans.planning_on("TEST0001"), "native on not saved")
    await charger.answer(clearing, {"status": "Accepted"})
    assert await replacement
    request = await charger.next_hub_call()
    periods = request[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
    assert [(p["startPeriod"], p["limit"]) for p in periods] == [(0, 0.0), (133200, 16.0), (147600, 0.0)]
    await charger.answer(request, {"status": "Accepted"})
    await on
    assert plans.plan("TEST0001") == [PlanWindow(2, 46800, 61200)]


# Intent: when the app switches its planning off the hub's profile is cleared but the plan stays; switching it on writes
# the profile of the plan again, and the sensor says which of the two it is.
# Why: with the planning off the charger must charge freely, yet the windows are what the owner set up for later.
async def test_the_planning_switch_clears_and_restores_the_profile(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    await wait_for(hass, lambda: state_of(hass, PLAN) == "1", "plan not shown")
    plans = entry.runtime_data.plans

    off = asyncio.create_task(plans.planning_switched("TEST0001", False))
    cleared = await charger.next_hub_call()
    await charger.answer(cleared, {"status": "Accepted"})
    await off
    on = asyncio.create_task(plans.planning_switched("TEST0001", True))
    written = await charger.next_hub_call()
    await charger.answer(written, {"status": "Accepted"})
    await on

    assert cleared[2] == "ClearChargingProfile" and written[2] == "SetChargingProfile"
    assert plans.plan("TEST0001") == [PlanWindow(1, 43200, 50400)] and plans.planning_on("TEST0001")
    assert hass.states.get(PLAN).attributes["planning"] == "on"


# Intent: rejected secondary profile writes retain the accepted native switch and expose persistent pending sync.
# Why: an app's genuine native acknowledgement must not hide a failed hub restriction or be reverted as a fake fix.
@pytest.mark.parametrize("on", [False, True])
async def test_failed_planning_profile_sync_is_visible_until_success(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any, on: bool
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    plans = entry.runtime_data.plans
    if on:
        off = asyncio.create_task(plans.planning_switched("TEST0001", False))
        await charger.answer(await charger.next_hub_call(), {"status": "Accepted"})
        await off
    switch = asyncio.create_task(plans.planning_switched("TEST0001", on))
    for _ in range(2 if on else 1):
        request = await charger.next_hub_call()
        assert request[2] == ("SetChargingProfile" if on else "ClearChargingProfile")
        await charger.answer(request, {"status": "Rejected"})
    await switch
    await wait_for(hass, lambda: hass.states.get(PLAN).attributes.get("profile_sync_pending") is True, "sync not shown")
    assert plans.planning_on("TEST0001") is on and plans.plan("TEST0001") == [PlanWindow(1, 43200, 50400)]
    assert hass.states.get(PLAN).attributes["profile_sync_error"] == "rejected"

    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    plans = entry.runtime_data.plans
    assert plans.planning_on("TEST0001") is on and plans.profile_sync_error("TEST0001") == "rejected"
    charger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: hass.states.get(PLAN).attributes.get("profile_sync_pending") is True, "pending lost")
    retry = asyncio.create_task(plans.planning_switched("TEST0001", on))
    await charger.answer(await charger.next_hub_call(), {"status": "Accepted"})
    await retry
    await wait_for(
        hass, lambda: hass.states.get(PLAN).attributes.get("profile_sync_pending") is False, "sync not cleared"
    )
    assert plans.planning_on("TEST0001") is on and plans.profile_sync_error("TEST0001") is None


# Intent: an older failed profile clear cannot mark a newer accepted native switch as failed.
# Why: the app may switch again while a secondary profile reply is still pending.
async def test_old_profile_failure_does_not_overwrite_newer_switch(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    plans = entry.runtime_data.plans
    off = asyncio.create_task(plans.planning_switched("TEST0001", False))
    clearing = await charger.next_hub_call()
    on = asyncio.create_task(plans.planning_switched("TEST0001", True))
    await wait_for(hass, lambda: plans.planning_on("TEST0001"), "new switch not accepted")
    await charger.answer(clearing, {"status": "Rejected"})
    await off
    assert plans.profile_sync_error("TEST0001") is None and plans.planning_on("TEST0001")
    restoring = await charger.next_hub_call()
    assert restoring[2] == "SetChargingProfile"
    await charger.answer(restoring, {"status": "Accepted"})
    await on
    assert plans.profile_sync_error("TEST0001") is None
    assert plans.plan("TEST0001") == [PlanWindow(1, 43200, 50400)]


# Intent: a failed native-off profile clear remains visible when an earlier HA write replaced the plan meanwhile.
# Why: changed windows do not remove the need to clear a still-installed hub profile while native planning is off.
async def test_failed_switch_sync_remains_visible_after_plan_replacement(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(hass, charger, {"windows": [MONDAY_NOON]})
    plans = entry.runtime_data.plans
    replacement = asyncio.create_task(
        hass.services.async_call(DOMAIN, "set_plan", {"windows": [TUESDAY_AFTERNOON]}, blocking=True)
    )
    writing = await charger.next_hub_call()
    off = asyncio.create_task(plans.planning_switched("TEST0001", False))
    await wait_for(hass, lambda: not plans.planning_on("TEST0001"), "native off not retained")
    await charger.answer(writing, {"status": "Accepted"})
    await replacement
    clearing = await charger.next_hub_call()
    await charger.answer(clearing, {"status": "Rejected"})
    await off
    assert plans.plan("TEST0001") == [PlanWindow(2, 46800, 61200)] and not plans.planning_on("TEST0001")
    assert plans.profile_sync_error("TEST0001") == "rejected"
    assert hass.states.get(PLAN).attributes["profile_sync_pending"] is True
    await _call(hass, charger, {"windows": [TUESDAY_AFTERNOON]})
    assert plans.profile_sync_error("TEST0001") is None
    assert hass.states.get(PLAN).attributes["profile_sync_pending"] is False


# Intent: the plan shows as a calendar: a Monday window gives an event on that Monday, an overnight window runs into the
# next day, and a switched-off window gives none.
# Why: the calendar is the way to see the plan in Home Assistant, and it is built from the same plan as the sensor.
async def test_the_plan_is_shown_as_a_calendar(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger, entry = await _booted(hass, setup_hub, connect_charger)
    await _call(
        hass,
        charger,
        {"windows": [MONDAY_NOON, {"day": "friday", "from": "22:00", "to": "06:00"}]},
    )
    await wait_for(hass, lambda: state_of(hass, PLAN) == "2", "plan not shown")

    answer = await hass.services.async_call(
        "calendar",
        "get_events",
        {
            "entity_id": "calendar.ocpp_hub_test0001_charging_plan",
            "start_date_time": "2026-10-26T00:00:00",
            "end_date_time": "2026-11-02T00:00:00",
        },
        blocking=True,
        return_response=True,
    )

    events = answer["calendar.ocpp_hub_test0001_charging_plan"]["events"]
    assert [(e["start"][:16], e["end"][:16]) for e in events] == [
        ("2026-10-26T12:00", "2026-10-26T14:00"),
        ("2026-10-30T22:00", "2026-10-31T06:00"),
    ]
