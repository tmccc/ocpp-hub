"""Writes to a charger: the ceiling, the pause rule, the purposes tried in turn and start and stop."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from ocpp_hub_core.control import ChargerControl, ControlError, ControlPolicy, effective_current_a


# Answer the hub's next requests with the given statuses, in order, and return the requests that were seen.
# A status of None answers with a CALLERROR instead, the way a charger that does not know a request does.
async def _answer_with(charger: Any, *statuses: str | None) -> list[list[Any]]:
    seen: list[list[Any]] = []
    for status in statuses:
        call = await charger.next_hub_call()
        seen.append(call)
        if status is None:
            await charger.answer_error(call)
        else:
            await charger.answer(call, {"status": status})
    return seen


# A booted fake charger and the control of its live session, under the default ceiling of 16 A.
async def _control(start_hub: Any) -> tuple[Any, Any, ChargerControl]:
    hub = await start_hub()
    charger = await hub.charger()
    await charger.boot()
    control = hub.central.control("TEST0001")
    assert control is not None
    return hub, charger, control


# Intent: accept a limit only when the charger sends an exact successful result.
# Why: an error code or a malformed status must never publish a setting the charger refused.
@pytest.mark.parametrize("error_reply", [False, True])
async def test_accepted_must_be_an_exact_result(start_hub, error_reply):
    hub, charger, control = await _control(start_hub)
    task = asyncio.create_task(control.set_current_limit(10))
    call = await charger.next_hub_call()
    if error_reply:
        await charger.answer_error(call, code="Accepted")
    else:
        await charger.answer(call, {"status": " Accepted "})
    await _answer_with(charger, "Rejected")
    with pytest.raises(ControlError, match="rejected"):
        await task
    assert hub.listener.latest.charge_limit_a is None


# Intent: a limit inside the ceiling is sent as a station-wide ChargePointMaxProfile on connector 0 with the hub's own
# profile id, and the snapshot shows what the charger accepted.
# Why: this is the one write that matters; its exact shape decides whether a charger obeys it, and Home Assistant shows
# the snapshot as the current limit.
async def test_a_limit_is_sent_as_a_station_profile_and_remembered(start_hub) -> None:
    hub, charger, control = await _control(start_hub)

    task = asyncio.create_task(control.set_current_limit(12))
    (call,) = await _answer_with(charger, "Accepted")

    assert await task == 12.0
    assert call[2] == "SetChargingProfile" and call[3]["connectorId"] == 0
    profile = call[3]["csChargingProfiles"]
    assert profile["chargingProfilePurpose"] == "ChargePointMaxProfile" and profile["chargingProfileId"] == 7001
    assert profile["chargingSchedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 12.0}]
    assert hub.listener.latest.charge_limit_a == 12.0
    assert hub.listener.latest.limit_purpose == "ChargePointMaxProfile"


# Intent: a wish below what a car needs to charge is sent as zero, which pauses the charger.
# Why: a charger below 6 A does not charge; an in-between value like 4 A would leave the charger in an undefined state.
async def test_a_wish_below_the_minimum_pauses_the_charger(start_hub) -> None:
    _, charger, control = await _control(start_hub)

    task = asyncio.create_task(control.set_current_limit(4))
    (call,) = await _answer_with(charger, "Accepted")

    assert await task == 0.0
    assert call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0.0


# Intent: a wish above the ceiling, a negative one, a bool and a non-number are refused before anything is sent.
# Why: the ceiling is the owner's safety limit; it must hold however the wish arrives, and nothing may be sent.
@pytest.mark.parametrize(
    ("wish", "code"),
    [
        (17, "above_ceiling"),
        (-1, "invalid_value"),
        (True, "invalid_value"),
        (float("nan"), "invalid_value"),
        ("12", "invalid_value"),
    ],
)
async def test_wishes_outside_the_rules_are_refused_and_nothing_is_sent(start_hub, wish: object, code: str) -> None:
    _, charger, control = await _control(start_hub)

    with pytest.raises(ControlError) as refused:
        await control.set_current_limit(wish)

    assert refused.value.code == code
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(charger.next_hub_call(), 0.2)


# Intent: when the charger refuses a purpose or does not know the request, the next purpose is tried, and the one it
# accepts is tried first the next time.
# Why: which purpose a charger honours is learned, not assumed; asking in the wrong order every time wastes writes.
async def test_purposes_are_tried_in_turn_and_the_accepted_one_is_remembered(start_hub) -> None:
    hub, charger, control = await _control(start_hub)

    task = asyncio.create_task(control.set_current_limit(10))
    first = await _answer_with(charger, None, "Accepted")
    await task
    again = asyncio.create_task(control.set_current_limit(8))
    second = await _answer_with(charger, "Accepted")
    await again

    assert [c[3]["csChargingProfiles"]["chargingProfilePurpose"] for c in first] == [
        "ChargePointMaxProfile",
        "TxDefaultProfile",
    ]
    assert second[0][3]["csChargingProfiles"]["chargingProfilePurpose"] == "TxDefaultProfile"
    assert hub.listener.latest.limit_purpose == "TxDefaultProfile"


# Intent: when every purpose is refused the write fails with the answers listed, and the snapshot keeps no limit.
# Why: the owner must see that the charger did not obey, and Home Assistant must not show a limit nobody applied.
async def test_a_charger_that_refuses_everything_is_reported(start_hub) -> None:
    hub, charger, control = await _control(start_hub)

    task = asyncio.create_task(control.set_current_limit(10))
    await _answer_with(charger, "Rejected", "Rejected")
    with pytest.raises(ControlError) as refused:
        await task

    assert refused.value.code == "rejected" and "ChargePointMaxProfile=Rejected" in refused.value.detail
    assert hub.listener.latest.charge_limit_a is None


# Intent: while a charge runs, the per-charge TxProfile with the hub's transaction id is part of the ladder; a stop
# without a running charge is refused, and a stop with one names the hub's transaction id.
# Why: a default profile only applies to charges that start later, and a stop that names no or a wrong transaction
# would stop nothing or the wrong charge.
async def test_a_running_charge_gets_a_tx_profile_and_can_be_stopped(start_hub) -> None:
    hub, charger, control = await _control(start_hub)
    with pytest.raises(ControlError) as idle:
        await control.remote_stop()
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    transaction_id = started[2]["transactionId"]

    limit = asyncio.create_task(control.set_current_limit(10))
    seen = await _answer_with(charger, "Rejected", "Accepted")
    await limit
    stop = asyncio.create_task(control.remote_stop())
    (stop_call,) = await _answer_with(charger, "Accepted")
    await stop

    assert idle.value.code == "no_charge"
    assert seen[1][3]["csChargingProfiles"]["chargingProfilePurpose"] == "TxProfile"
    assert seen[1][3]["csChargingProfiles"]["transactionId"] == transaction_id
    assert stop_call[2] == "RemoteStopTransaction" and stop_call[3] == {"transactionId": transaction_id}


# Intent: a remote start names the socket and the hub's tag, and a refusal is reported.
# Why: a start the charger refused (no car connected) must not look like it worked.
async def test_remote_start_names_the_socket_and_reports_a_refusal(start_hub) -> None:
    _, charger, control = await _control(start_hub)

    task = asyncio.create_task(control.remote_start())
    (call,) = await _answer_with(charger, "Rejected")
    with pytest.raises(ControlError) as refused:
        await task

    assert call[2] == "RemoteStartTransaction" and call[3] == {"idTag": "OCPPHUB", "connectorId": 1}
    assert refused.value.code == "rejected"


# Intent: a ceiling outside 6 to 80 A is refused when the policy is made, and the policy's own rule matches the helper.
# Why: a typo in the option must not widen what the hub may ask a charger to do.
def test_a_policy_with_an_impossible_ceiling_is_refused() -> None:
    for bad in (0.0, 5.0, 81.0, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            ControlPolicy(max_current_a=bad)
    assert effective_current_a(16, ControlPolicy(max_current_a=16.0)) == 16.0


# Intent: while a charge runs, a charger that refuses the station profile and the charge profile but accepts a default
# profile is reported as refused, and a default profile learned while idle does not take the place of a charge profile.
# Why: a default profile only applies to charges that start later; calling it a success would show a limit or a pause
# that the running charge never got.
async def test_a_default_profile_never_counts_for_a_running_charge(start_hub) -> None:
    hub, charger, control = await _control(start_hub)
    idle = asyncio.create_task(control.set_current_limit(10))
    await _answer_with(charger, "Rejected", "Accepted")
    await idle
    assert hub.listener.latest.limit_purpose == "TxDefaultProfile"
    pending_idle = asyncio.create_task(control.set_current_limit(12))
    pending_call = await charger.next_hub_call()
    assert pending_call[3]["csChargingProfiles"]["chargingProfilePurpose"] == "TxDefaultProfile"
    await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    await charger.answer(pending_call, {"status": "Accepted"})
    with pytest.raises(ControlError, match="a charge started"):
        await pending_idle

    running = asyncio.create_task(control.set_current_limit(8))
    seen = await _answer_with(charger, "Rejected", "Rejected")
    with pytest.raises(ControlError):
        await running

    assert [c[3]["csChargingProfiles"]["chargingProfilePurpose"] for c in seen] == [
        "ChargePointMaxProfile",
        "TxProfile",
    ]
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(charger.next_hub_call(), 0.2)
    assert hub.listener.latest.charge_limit_a == 10.0


# Intent: a limit the charger accepted as a charge profile is forgotten when that charge stops.
# Why: a charge profile ends with its charge; the next charge would run unlimited while the number still showed a limit.
async def test_a_charge_profile_limit_ends_with_the_charge(start_hub) -> None:
    hub, charger, control = await _control(start_hub)
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    limit = asyncio.create_task(control.set_current_limit(10))
    await _answer_with(charger, "Rejected", "Accepted")
    await limit
    assert hub.listener.latest.limit_purpose == "TxProfile"

    await charger.call(
        "StopTransaction",
        {"transactionId": started[2]["transactionId"], "meterStop": 100, "timestamp": "2026-10-21T09:00:00Z"},
    )

    assert hub.listener.latest.charge_limit_a is None and hub.listener.latest.limit_purpose is None


# Intent: a station-wide limit the hub set is still shown after the charger reconnects.
# Why: the charger keeps a station profile across a dropped link, so a pause must stay visible instead of showing
# "unknown" while no car charges; a charge profile died with its charge.
async def test_a_station_limit_is_still_shown_after_a_reconnect(start_hub) -> None:
    hub, charger, control = await _control(start_hub)
    limit = asyncio.create_task(control.set_current_limit(0))
    await _answer_with(charger, "Accepted")
    await limit

    await charger.close()
    await charger.wait_closed()
    again = await hub.charger()
    await again.boot()

    assert hub.listener.latest.charge_limit_a == 0.0 and hub.listener.latest.limit_purpose == "ChargePointMaxProfile"
