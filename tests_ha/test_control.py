"""The charge limit and the charging switch: what Home Assistant sends to the charger, and what it refuses to."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ocpp_hub.const import CONF_MAX_CURRENT, DOMAIN
from tests_ha.conftest import state_of, wait_for

LIMIT = "number.ocpp_hub_test0001_charge_limit"
CHARGING = "switch.ocpp_hub_test0001_charging"
STATUS = "sensor.ocpp_hub_test0001_status"


# Call a Home Assistant service while a fake charger answers the one request it causes, and return that request.
async def _call_and_answer(
    hass: HomeAssistant, charger: Any, domain: str, service: str, data: dict[str, Any], status: str = "Accepted"
) -> list[Any]:
    call = asyncio.create_task(hass.services.async_call(domain, service, data, blocking=True))
    request = await charger.next_hub_call()
    await charger.answer(request, {"status": status})
    await call
    return request


# A booted charger with its entities.
async def _booted(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any, **options: Any
) -> Any:
    entry = await setup_hub(options)
    charger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, LIMIT) is not None, "no entities")
    return charger


# Intent: setting the charge limit sends the profile to the charger and the number then shows what it accepted; a value
# under 6 A pauses it.
# Why: this is the control a person uses; it must reach the charger in the right shape and not claim a limit nobody set.
async def test_the_charge_limit_reaches_the_charger_and_shows_what_it_accepted(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger = await _booted(hass, setup_hub, connect_charger)
    assert state_of(hass, LIMIT) == "unknown"

    request = await _call_and_answer(hass, charger, "number", "set_value", {"entity_id": LIMIT, "value": 12})
    await wait_for(hass, lambda: state_of(hass, LIMIT) == "12.0", "limit not shown")
    paused = await _call_and_answer(hass, charger, "number", "set_value", {"entity_id": LIMIT, "value": 3})

    assert request[2] == "SetChargingProfile" and request[3]["csChargingProfiles"]["chargingSchedule"][
        "chargingSchedulePeriod"
    ] == [{"startPeriod": 0, "limit": 12.0}]
    assert paused[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0.0


# Intent: the ceiling comes from the options: the number's range ends there, and a value above it is refused with a
# readable error and nothing is sent.
# Why: the owner's ceiling must hold through the service call too, not only in the form.
async def test_the_ceiling_from_the_options_limits_the_number(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger = await _booted(hass, setup_hub, connect_charger, **{CONF_MAX_CURRENT: 10})

    assert hass.states.get(LIMIT).attributes["max"] == 10.0
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call("number", "set_value", {"entity_id": LIMIT, "value": 12}, blocking=True)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(charger.next_hub_call(), 0.2)


# Intent: the charging switch starts a charge with the hub's tag on the socket and stops the running one by its
# transaction id; a refusal from the charger becomes an error the owner sees.
# Why: starting without a car must say so instead of a switch that silently flips back.
async def test_the_charging_switch_starts_and_stops_a_charge(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger = await _booted(hass, setup_hub, connect_charger)

    refused = asyncio.create_task(hass.services.async_call("switch", "turn_on", {"entity_id": CHARGING}, blocking=True))
    request = await charger.next_hub_call()
    await charger.answer(request, {"status": "Rejected"})
    with pytest.raises(HomeAssistantError):
        await refused
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "OCPPHUB", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    await wait_for(hass, lambda: state_of(hass, CHARGING) == "on", "switch not on during a charge")
    stop = await _call_and_answer(hass, charger, "switch", "turn_off", {"entity_id": CHARGING})

    assert request[2] == "RemoteStartTransaction" and request[3] == {"idTag": "OCPPHUB", "connectorId": 1}
    assert stop[2] == "RemoteStopTransaction" and stop[3] == {"transactionId": started[2]["transactionId"]}


# Intent: the clear button removes the hub's profile from the charger and the number goes back to showing nothing.
# Why: it is the owner's way out of a pause the hub set; without it only setting 6 A or more would lift the pause.
async def test_the_clear_button_removes_the_hub_profile(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger = await _booted(hass, setup_hub, connect_charger)
    await _call_and_answer(hass, charger, "number", "set_value", {"entity_id": LIMIT, "value": 0})
    await wait_for(hass, lambda: state_of(hass, LIMIT) == "0.0", "pause not shown")

    request = await _call_and_answer(
        hass, charger, "button", "press", {"entity_id": "button.ocpp_hub_test0001_clear_charge_limit"}
    )
    await wait_for(hass, lambda: state_of(hass, LIMIT) == "unknown", "limit not cleared")

    assert request[2] == "ClearChargingProfile" and request[3] == {"id": 7001}


# Intent: the limit Home Assistant set is saved, shows again after the entry is reloaded and the charger reconnects, and
# the limit-in-effect sensor shows it with its source.
# Why: a restart of Home Assistant used to leave the number on "unknown" while the charger still held the limit, which
# hid a pause; the saved value is also what the vendor is told when it asks.
async def test_the_limit_is_saved_and_shown_again_after_a_reload(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    entry = await setup_hub()
    charger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, LIMIT) is not None, "no entities")
    in_effect = "sensor.ocpp_hub_test0001_limit_in_effect"
    await _call_and_answer(hass, charger, "number", "set_value", {"entity_id": LIMIT, "value": 6})
    await wait_for(hass, lambda: state_of(hass, in_effect) == "6.0", "limit in effect not shown")
    source = hass.states.get(in_effect).attributes["source"]

    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    again = await connect_charger(entry)
    await again.boot()
    await wait_for(hass, lambda: state_of(hass, LIMIT) == "6.0", "limit not shown after the reload")

    assert source == "calculated" and state_of(hass, in_effect) == "6.0"


# Intent: a damaged limits file (not an object, or records of the wrong shape) does not stop the integration from
# loading; the damaged part is skipped and the rest is kept.
# Why: the file lives in Home Assistant's storage and can be edited or damaged; a failure there used to make the entry
# fail to load on every start until the file was removed by hand.
@pytest.mark.parametrize(
    "stored",
    [
        ["not", "an", "object"],
        {"limits": ["x"]},
        {"limits": {"TEST0001": "x", "OTHER": {"hub_a": 6.0}}},
        {"limits": {"TEST0001": {"hub_a": 10**400}, "OTHER": {"hub_a": 6.0}}},
    ],
)
async def test_damaged_limit_storage_does_not_stop_the_setup(
    hass: HomeAssistant,
    hass_storage: dict[str, Any],
    setup_hub: Callable[..., Awaitable[MockConfigEntry]],
    stored: object,
) -> None:
    hass_storage["ocpp_hub.limits"] = {"version": 1, "minor_version": 1, "key": "ocpp_hub.limits", "data": stored}

    entry = await setup_hub()

    assert entry.state.name == "LOADED"


# Intent: the set_schedule service sends a daily recurring profile anchored at midnight local time with the times as
# offsets, refuses a period above the ceiling before anything is sent, and the limit-in-effect sensor lists the
# schedule the charger reports with absolute times.
# Why: this is how a schedule reaches the charger from an automation or a script; a bad period must stop at the door,
# and what the dashboard shows must be what the charger says it will do.
async def test_the_schedule_service_and_the_reported_schedule(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger = await _booted(hass, setup_hub, connect_charger)
    in_effect = "sensor.ocpp_hub_test0001_limit_in_effect"
    charger.composite_limit = 6.0
    charger.composite_periods = [(0, 6.0), (25200, 16.0)]
    periods = [{"time": "00:00", "limit": 6}, {"time": "07:00", "limit": 16}]

    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN, "set_schedule", {"periods": [{"time": "00:00", "limit": 17}]}, blocking=True
        )
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(charger.next_hub_call(), 0.2)
    request = await _call_and_answer(hass, charger, DOMAIN, "set_schedule", {"periods": periods})
    await wait_for(hass, lambda: hass.states.get(in_effect).attributes["schedule"], "schedule not shown")

    profile = request[3]["csChargingProfiles"]
    assert profile["chargingProfileKind"] == "Recurring" and profile["recurrencyKind"] == "Daily"
    assert [p["startPeriod"] for p in profile["chargingSchedule"]["chargingSchedulePeriod"]] == [0, 25200]
    shown = hass.states.get(in_effect).attributes["schedule"]
    assert [entry["limit_a"] for entry in shown] == [6.0, 16.0] and shown[0]["from"][:4].isdigit()


# Intent: the plan sensor shows nothing until a plan is known, then the number of windows with the day and the times of
# each, and keeps showing it while the charger is away.
# Why: the planning is the hub's, so it is still there when the charger drops its connection for a moment.
async def test_the_app_plan_sensor_lists_the_windows(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    charger = await _booted(hass, setup_hub, connect_charger)
    plan = "sensor.ocpp_hub_test0001_app_plan"
    assert state_of(hass, plan) == "unknown"
    runtime = hass.config_entries.async_loaded_entries(DOMAIN)[0].runtime_data

    runtime.central.session("TEST0001").record_app_plan([(1, 43200, 50400), (2, 46800, 61200)])
    await wait_for(hass, lambda: state_of(hass, plan) == "2", "plan not shown")

    windows = hass.states.get(plan).attributes["windows"]
    assert windows == [
        {"day": "monday", "from": "12:00", "to": "14:00"},
        {"day": "tuesday", "from": "13:00", "to": "17:00"},
    ]
    await charger.close()
    await wait_for(hass, lambda: state_of(hass, "binary_sensor.ocpp_hub_test0001_connected") == "off", "not gone")
    assert state_of(hass, plan) == "2"
