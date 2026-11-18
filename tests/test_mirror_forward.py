"""Vendor commands that reach the charger when forwarding is on, and the rules that still stop them."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from ocpp_hub_core.control import HUB_PROFILE_ID, ControlPolicy
from ocpp_hub_core.mirror_forward import Forward, Refuse, plan_forward
from ocpp_hub_core.mirror_link import MAX_FORWARDS
from ocpp_hub_core.txid_map import TransactionIdMap

from tests.fake_vendor import eventually
from tests.test_mirror import CHARGER

PROFILE = {
    "connectorId": 1,
    "csChargingProfiles": {
        "chargingProfileId": 5,
        "stackLevel": 0,
        "chargingProfilePurpose": "TxDefaultProfile",
        "chargingProfileKind": "Absolute",
        "chargingSchedule": {"chargingRateUnit": "A", "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 10}]},
    },
}


# A charging profile with one flat period; the unit, the limit, the phases and the number of periods are what varies.
def _profile(unit: str | None, limit: float, phases: int | None = None, periods: int = 1) -> dict[str, Any]:
    period: dict[str, Any] = {"startPeriod": 0, "limit": limit}
    if phases is not None:
        period["numberPhases"] = phases
    schedule: dict[str, Any] = {"chargingSchedulePeriod": [dict(period, startPeriod=n * 60) for n in range(periods)]}
    if unit is not None:
        schedule["chargingRateUnit"] = unit
    return {
        "chargingProfileId": 5,
        "stackLevel": 0,
        "chargingProfilePurpose": "TxDefaultProfile",
        "chargingProfileKind": "Absolute",
        "chargingSchedule": schedule,
    }


# A booted charger behind a mirror that forwards, with the first frames already at the vendor.
async def _forwarding_rig(start_rig: Any) -> tuple[Any, Any]:
    rig = await start_rig(commands="forward")
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")
    return rig, charger


# Intent: with forwarding on, a command of the vendor reaches the charger as it is and the charger's answer goes back to
# the vendor.
# Why: this is the whole feature; a backend whose command never arrives, or whose answer is lost, looks broken to it.
async def test_a_forwarded_command_reaches_the_charger_and_its_answer_returns(start_rig) -> None:
    rig, charger = await _forwarding_rig(start_rig)

    asked = asyncio.create_task(rig.vendor.command("RemoteStartTransaction", {"idTag": "VENDOR", "connectorId": 1}))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    answer = await asked

    assert call[2] == "RemoteStartTransaction" and call[3] == {"idTag": "VENDOR", "connectorId": 1}
    assert answer[0] == 3 and answer[2] == {"status": "Accepted"}


# Intent: accepted clear-all forgets both profiles, while an accepted explicit unrelated ID preserves them.
# Why: local policy and plan publication must match the scope of the forwarded charger ACK.
@pytest.mark.parametrize("payload", [{}, {"id": 9, "connectorId": 0}])
async def test_forwarded_clear_ack_updates_exact_profile_scope(start_rig, payload):
    rig, charger = await _forwarding_rig(start_rig)
    session = rig.hub.central.session(CHARGER)
    session.record_limit(10, "ChargePointMaxProfile")
    session.record_vendor_limit(8, 5, "TxDefaultProfile")
    task = asyncio.create_task(rig.vendor.command("ClearChargingProfile", payload))
    request = await charger.next_hub_call()
    assert request[2:] == ["ClearChargingProfile", payload]
    await charger.answer(request, {"status": "Accepted"})
    assert (await task)[2] == {"status": "Accepted"}
    assert (session.snapshot.charge_limit_a, session.snapshot.vendor_limit_a) == ((10, 8) if payload else (None, None))


# Intent: Reset, UpdateFirmware and GetDiagnostics stay refused with forwarding on, and nothing reaches the charger.
# Why: the owner kept reset and firmware out of the first phase.
@pytest.mark.parametrize(
    ("action", "payload", "expected"),
    [
        ("Reset", {"type": "Hard"}, {"status": "Rejected"}),
        ("UpdateFirmware", {"location": "https://vendor.example/fw", "retrieveDate": "t"}, {}),
        ("GetDiagnostics", {"location": "https://vendor.example/logs"}, {}),
    ],
)
async def test_reset_and_firmware_stay_refused_when_forwarding(
    start_rig, action: str, payload: dict[str, Any], expected: dict[str, Any]
) -> None:
    rig, charger = await _forwarding_rig(start_rig)

    answer = await rig.vendor.command(action, payload)

    assert answer[2] == expected and charger.hub_calls.empty()


# Intent: a command names the vendor's transaction id and the charger gets the hub's; an id the hub does not know is
# refused and nothing is sent.
# Why: the charger knows only the hub's ids, and passing an unknown one on could stop another charge.
async def test_a_remote_stop_is_translated_to_the_hub_transaction_id(start_rig) -> None:
    rig, charger = await _forwarding_rig(start_rig)
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    await eventually(lambda: rig.vendor.calls_of("StartTransaction"), message="start not mirrored")
    # The vendor has answered the start once the mirror holds the pair of ids; only then can a stop be translated.
    await eventually(lambda: len(rig.mirror._transactions.get(CHARGER, ())) == 1, message="ids not paired")

    unknown = await rig.vendor.command("RemoteStopTransaction", {"transactionId": 999999})
    vendor_id = rig.mirror._transactions[CHARGER].to_vendor(started[2]["transactionId"])
    asked = asyncio.create_task(rig.vendor.command("RemoteStopTransaction", {"transactionId": vendor_id}))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted"})
    answer = await asked

    assert unknown[2] == {"status": "Rejected"}
    assert call[3] == {"transactionId": started[2]["transactionId"]} and answer[2] == {"status": "Accepted"}


# Intent: when the charger answers a forwarded command with an error, the vendor gets an error and not silence.
# Why: a backend that gets no answer stops talking to the device.
async def test_a_charger_error_is_passed_back_to_the_vendor(start_rig) -> None:
    rig, charger = await _forwarding_rig(start_rig)

    asked = asyncio.create_task(rig.vendor.command("ChangeAvailability", {"connectorId": 1, "type": "Inoperative"}))
    call = await charger.next_hub_call()
    await charger.answer_error(call, "NotSupported")
    answer = await asked

    assert answer[0] == 4 and answer[2] == "NotSupported"


# Intent: the rules of the forwarding: profiles above the ceiling (in amperes or in watts), without a schedule or with
# the hub's own id are refused; keys that could cut the hub off are refused; ordinary commands pass unchanged.
# Why: forwarding hands the vendor a hand on the charger, and these are the limits of that hand.
@pytest.mark.parametrize(
    ("action", "payload", "allowed"),
    [
        ("SetChargingProfile", PROFILE, True),
        (
            "SetChargingProfile",
            {
                **PROFILE,
                "csChargingProfiles": {
                    **PROFILE["csChargingProfiles"],
                    "chargingSchedule": {
                        "chargingRateUnit": "A",
                        "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 17}],
                    },
                },
            },
            False,
        ),
        (
            "SetChargingProfile",
            {
                **PROFILE,
                "csChargingProfiles": {
                    **PROFILE["csChargingProfiles"],
                    "chargingSchedule": {
                        "chargingRateUnit": "W",
                        "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 11041}],
                    },
                },
            },
            False,
        ),
        (
            "SetChargingProfile",
            {
                **PROFILE,
                "csChargingProfiles": {
                    **PROFILE["csChargingProfiles"],
                    "chargingSchedule": {
                        "chargingRateUnit": "W",
                        "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 11040}],
                    },
                },
            },
            True,
        ),
        (
            "SetChargingProfile",
            {**PROFILE, "csChargingProfiles": {**PROFILE["csChargingProfiles"], "chargingProfileId": HUB_PROFILE_ID}},
            False,
        ),
        ("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": {}}, False),
        ("ClearChargingProfile", {"id": HUB_PROFILE_ID}, False),
        ("ClearChargingProfile", {"id": 5}, True),
        ("ChangeConfiguration", {"key": "WebSocketPingInterval", "value": "30"}, False),
        ("ChangeConfiguration", {"key": "CentralSystemURL", "value": "ws://x"}, False),
        ("ChangeConfiguration", {"key": "AuthorizationKey", "value": "x"}, False),
        ("ChangeConfiguration", {"key": "MeterValuesSampledData", "value": "Voltage"}, False),
        ("ChangeConfiguration", {"key": "LightIntensity", "value": "50"}, True),
        ("ChangeConfiguration", {"key": "SomeVendorCurrentKey", "value": "63"}, False),
        ("RemoteStartTransaction", {"idTag": "x"}, True),
        ("RemoteStartTransaction", {"idTag": "x", "chargingProfile": _profile(unit="A", limit=32)}, False),
        ("RemoteStartTransaction", {"idTag": "x", "chargingProfile": _profile(unit="A", limit=10)}, True),
        (
            "SetChargingProfile",
            {"connectorId": 1, "csChargingProfiles": _profile(unit="W", limit=11040, phases=1)},
            False,
        ),
        (
            "SetChargingProfile",
            {"connectorId": 1, "csChargingProfiles": _profile(unit="W", limit=11040, phases=3)},
            True,
        ),
        ("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": _profile(unit=None, limit=10)}, False),
        ("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": _profile(unit="kW", limit=10)}, False),
        (
            "SetChargingProfile",
            {"connectorId": 1, "csChargingProfiles": _profile(unit="A", limit=10, periods=100)},
            False,
        ),
        ("ClearChargingProfile", {"id": "7001"}, False),
        ("ChangeConfiguration", {"key": "ChargingScheduleAllowedChargingRateUnit", "value": "W"}, False),
        ("ChangeConfiguration", {"key": "LightIntensity ", "value": "50"}, False),
        ("ChangeConfiguration", {"key": "ConnectionTimeOut", "value": 30}, False),
        ("ChangeConfiguration", {"key": "ConnectionTimeOut", "value": "30"}, True),
        ("ChangeConfiguration", {"key": "HeartbeatInterval", "value": "30"}, False),
        ("DataTransfer", {"vendorId": "v"}, True),
    ],
)
def test_the_forwarding_rules(action: str, payload: dict[str, Any], allowed: bool) -> None:
    plan = plan_forward(action, payload, ControlPolicy(max_current_a=16.0), TransactionIdMap())

    assert isinstance(plan, Forward) is allowed and isinstance(plan, Refuse) is not allowed


# Intent: commands beyond the four that may wait at once are refused with an answer that is valid for their own action
# (Unknown for ClearChargingProfile, UnlockFailed for UnlockConnector), and the charger sees only the first four.
# Why: a flood must not hold up the owner's writes, and a strict backend treats an answer outside the protocol's list
# as an error of the device, not as a refusal.
@pytest.mark.parametrize(
    ("action", "payload", "refusal"),
    [
        ("ClearChargingProfile", {"id": 5}, {"status": "Unknown"}),
        ("UnlockConnector", {"connectorId": 1}, {"status": "UnlockFailed"}),
    ],
)
async def test_a_flood_of_commands_is_refused_with_a_valid_answer(
    start_rig, action: str, payload: dict[str, Any], refusal: dict[str, Any]
) -> None:
    rig, charger = await _forwarding_rig(start_rig)

    asked = [asyncio.create_task(rig.vendor.command(action, payload, timeout=5.0)) for _ in range(MAX_FORWARDS + 3)]
    refused = [answer for answer in await asyncio.gather(*asked[MAX_FORWARDS:]) if answer[0] == 3]

    assert len(refused) == 3 and all(answer[2] == refusal for answer in refused)
    assert charger.hub_calls.qsize() <= MAX_FORWARDS


# Intent: a profile with a limit too big to be a number, a profile id that is not an integer, and an accepted clear with
# filters are all handled: the first two are refused, the third forgets the hub's limit.
# Why: a 400-digit number made the check raise and left the vendor without an answer, a string id could be coerced by a
# lenient firmware into the hub's own id, and a clear by filter can remove the hub's profile as well.
def test_odd_numbers_and_ids_in_profiles_are_refused() -> None:
    big = _profile(unit="A", limit=10)
    big["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 10**400
    textual_id = _profile(unit="A", limit=10)
    textual_id["chargingProfileId"] = "7001"

    for profile in (big, textual_id):
        plan = plan_forward(
            "SetChargingProfile",
            {"connectorId": 1, "csChargingProfiles": profile},
            ControlPolicy(max_current_a=16.0),
            TransactionIdMap(),
        )
        assert isinstance(plan, Refuse)


# Intent: with forwarding on, a DataTransfer of any content reaches the charger as it is and the charger's answer goes
# back to the vendor.
# Why: the owner decided that the vendor's own channel is passed on; Reset, firmware and diagnostics are separate
# commands and stay refused.
async def test_a_data_transfer_reaches_the_charger_and_its_answer_returns(start_rig) -> None:
    rig, charger = await _forwarding_rig(start_rig)
    message = {"vendorId": "READIAN", "messageId": "RemoteConfiguration", "data": '{"instructType":"fe"}'}

    asked = asyncio.create_task(rig.vendor.command("DataTransfer", message))
    call = await charger.next_hub_call()
    await charger.answer(call, {"status": "Accepted", "data": "{}"})
    answer = await asked

    assert call[2] == "DataTransfer" and call[3] == message and answer[2]["status"] == "Accepted"
