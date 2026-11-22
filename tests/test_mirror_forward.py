"""Vendor commands that reach the charger when forwarding is on, and the rules that still stop them."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from ocpp_hub_core.control import HUB_PROFILE_ID, ControlError, ControlPolicy
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


# Intent: a sent profile update still settles when only the vendor connection changes.
# Why: the actual charger can apply and acknowledge it while the original vendor socket is gone.
async def test_sent_profile_result_survives_vendor_disconnect(start_rig) -> None:
    rig, charger = await _forwarding_rig(start_rig)
    caller = asyncio.create_task(rig.vendor.command("SetChargingProfile", PROFILE))
    sent = await charger.next_hub_call()
    assert sent[2] == "SetChargingProfile"
    before = rig.vendor.connection_count
    await rig.vendor.stop()
    caller.cancel()
    await asyncio.gather(caller, return_exceptions=True)
    await rig.vendor.start()
    await eventually(lambda: rig.vendor.connection_count > before, message="vendor connection was not replaced")
    await charger.answer(sent, {"status": "Accepted"})
    await eventually(lambda: rig.hub.listener.latest.vendor_limit_a == 10.0, message="charger profile reply was lost")


# Intent: a late per-transaction limit reply applies only to the original still-current transaction.
# Why: another Start can replace the original transaction before the real charger answers.
@pytest.mark.parametrize("source", ["local", "vendor"])
async def test_old_transaction_profile_reply_cannot_set_replacement_limit(start_rig, source):
    rig, charger = await _forwarding_rig(start_rig)
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "A", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    first = started[2]["transactionId"]
    session = rig.hub.central.session(CHARGER)
    if source == "vendor":
        await eventually(lambda: len(rig.mirror._transactions.get(CHARGER, ())) == 1, message="ids not paired")
        mapped = rig.mirror._transactions[CHARGER].to_vendor(first)
        profile = {**PROFILE["csChargingProfiles"], "chargingProfilePurpose": "TxProfile", "transactionId": mapped}
        task = asyncio.create_task(rig.vendor.command("SetChargingProfile", {**PROFILE, "csChargingProfiles": profile}))
    else:
        session.record_limit(None, "TxProfile")
        task = asyncio.create_task(rig.hub.central.control(CHARGER).set_current_limit(10))
    sent = await charger.next_hub_call()
    assert sent[2] == "SetChargingProfile" and sent[3]["csChargingProfiles"]["transactionId"] == first
    replacement = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "B", "meterStart": 100, "timestamp": "2026-10-21T08:30:00Z"}
    )
    assert replacement[2]["transactionId"] != first
    await charger.answer(sent, {"status": "Accepted"})
    if source == "vendor":
        assert (await task)[2] == {"status": "Accepted"}
    else:
        with pytest.raises(ControlError, match="requested charge ended"):
            await task
    assert session.snapshot.connectors[1].transaction_id == replacement[2]["transactionId"]
    assert session.snapshot.vendor_limit_a is None and session.snapshot.charge_limit_a is None


# Intent: a vendor TxProfile without an ID belongs to the session running when the charger receives it.
# Why: a real acceptance can update that session, but a late reply cannot update its replacement.
@pytest.mark.parametrize("replace_session", [False, True])
@pytest.mark.parametrize("null_id", [False, True])
async def test_forwarded_profile_without_id_keeps_its_session(start_rig, replace_session, null_id):
    rig, charger = await _forwarding_rig(start_rig)
    first = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "A", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    profile = {**PROFILE["csChargingProfiles"], "chargingProfilePurpose": "TxProfile"}
    if null_id:
        profile["transactionId"] = None
    task = asyncio.create_task(rig.vendor.command("SetChargingProfile", {**PROFILE, "csChargingProfiles": profile}))
    sent = await charger.next_hub_call()
    assert sent[3]["csChargingProfiles"] == profile
    if replace_session:
        second = await charger.call(
            "StartTransaction", {"connectorId": 1, "idTag": "B", "meterStart": 100, "timestamp": "2026-10-21T08:30:00Z"}
        )
        assert second[2]["transactionId"] != first[2]["transactionId"]
    await charger.answer(sent, {"status": "Accepted"})
    assert (await task)[2] == {"status": "Accepted"}
    assert rig.hub.central.session(CHARGER).snapshot.vendor_limit_a == (None if replace_session else 10)


# Intent: accepted vendor profile changes retire only the hub profile the charger actually replaced.
# Why: a same-stack edit must clear a stale hub limit, while a clear of an absent profile must keep a dormant plan.
@pytest.mark.parametrize("change", ["same-stack", "another-stack", "clear-absent"])
async def test_forwarded_profile_change_tracks_actual_hub_profile(start_rig, change):
    rig, charger = await _forwarding_rig(start_rig)
    session = rig.hub.central.session(CHARGER)
    if change == "clear-absent":
        task = asyncio.create_task(rig.vendor.command("ClearChargingProfile", {}))
    else:
        session.record_limit(0, "TxDefaultProfile", from_plan=True)
        profile = {**PROFILE["csChargingProfiles"], "stackLevel": int(change == "another-stack")}
        task = asyncio.create_task(rig.vendor.command("SetChargingProfile", {**PROFILE, "csChargingProfiles": profile}))
    await charger.answer(await charger.next_hub_call(), {"status": "Accepted"})
    assert (await task)[2] == {"status": "Accepted"}
    assert session.snapshot.charge_limit_a == (0 if change == "another-stack" else None)
    assert session.snapshot.vendor_limit_a == (None if change == "clear-absent" else 10)
    assert session.snapshot.external_profile_writes == int(change == "same-stack")


# Intent: a successful clear forgets only the known profiles matching its ID, purpose, connector and stack.
# Why: retiring an unrelated hub profile would also discard the saved HA plan.
@pytest.mark.parametrize(
    ("payload", "hub_cleared", "vendor_cleared"),
    [
        ({}, True, True),
        ({"id": 9, "connectorId": 0}, False, False),
        ({"id": 5, "connectorId": 0}, False, True),
        ({"chargingProfilePurpose": "TxDefaultProfile"}, False, True),
        ({"chargingProfilePurpose": "ChargePointMaxProfile"}, True, False),
        ({"connectorId": 1}, False, True),
        ({"connectorId": 0, "stackLevel": 1}, False, False),
        ({"connectorId": 0, "stackLevel": 0}, True, False),
    ],
)
async def test_forwarded_clear_ack_updates_exact_profile_scope(start_rig, payload, hub_cleared, vendor_cleared):
    rig, charger = await _forwarding_rig(start_rig)
    session = rig.hub.central.session(CHARGER)
    session.record_limit(None, "ChargePointMaxProfile", from_plan=True)
    profile = {**PROFILE, "csChargingProfiles": {**PROFILE["csChargingProfiles"], "stackLevel": 1}}
    setting = asyncio.create_task(rig.vendor.command("SetChargingProfile", profile))
    await charger.answer(await charger.next_hub_call(), {"status": "Accepted"})
    assert (await setting)[2] == {"status": "Accepted"}
    assert session.snapshot.vendor_profile_id == 5

    task = asyncio.create_task(rig.vendor.command("ClearChargingProfile", payload))
    request = await charger.next_hub_call()
    assert request[2:] == ["ClearChargingProfile", payload]
    await charger.answer(request, {"status": "Accepted"})
    assert (await task)[2] == {"status": "Accepted"}
    assert session.snapshot.limit_purpose == (None if hub_cleared else "ChargePointMaxProfile")
    assert session.snapshot.external_profile_writes == int(hub_cleared)
    assert session.snapshot.vendor_limit_a == (None if vendor_cleared else 10.0)


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
