"""The charger session and the central system, driven by a fake charger over a real websocket."""

from __future__ import annotations

import asyncio

import pytest
from ocpp_hub_core.session import CallRejectedError, CallTimeoutError


# Intent: a BootNotification is accepted with the hub's time and interval, and the identity lands in the snapshot.
# Why: the charger only starts working normally after an Accepted boot, and Home Assistant shows this identity.
async def test_boot_is_accepted_and_identity_is_recorded(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    answer = await charger.boot()

    assert answer[0] == 3 and answer[2]["status"] == "Accepted" and answer[2]["interval"] == 300
    assert answer[2]["currentTime"].endswith("Z")
    info = hub.listener.latest.info
    assert (info.vendor, info.model, info.firmware) == ("RAEDIAN", "NEO_22T_A", "s8100000001131&2.0.5_134")
    assert hub.listener.latest.connected and hub.listener.connected == ["TEST0001"]


# Intent: malformed optional boot identity fields stay unknown and valid fields remain available.
# Why: the accepted charger identity is also used in Home Assistant's registry and display code.
async def test_boot_identity_skips_unusable_fields(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    result = await charger.call(
        "BootNotification",
        {"chargePointVendor": 42, "chargePointModel": "VALID", "firmwareVersion": {"version": "unknown"}},
    )
    assert result[0] == 3
    info = hub.listener.latest.info
    assert info.vendor is None and info.model == "VALID" and info.firmware is None


# Intent: a heartbeat is answered with the current time.
# Why: chargers set their clock from it and treat a missing answer as a dead link.
async def test_heartbeat_returns_time(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    answer = await charger.call("Heartbeat", {})

    assert answer[0] == 3 and answer[2]["currentTime"].endswith("Z")


# Intent: a status notification updates that connector and leaves the others alone.
# Why: connector 0 is the charger itself and connector 1 the socket; Home Assistant shows connector 1.
async def test_status_notification_updates_one_connector(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    await charger.call("StatusNotification", {"connectorId": 0, "status": "Available", "errorCode": "NoError"})
    answer = await charger.call(
        "StatusNotification",
        {"connectorId": 1, "status": "Preparing", "errorCode": "NoError", "info": '{"current": 12}'},
    )

    connectors = hub.listener.latest.connectors
    assert answer[0] == 3
    assert connectors[0].status == "Available" and connectors[1].status == "Preparing"
    assert connectors[1].info == '{"current": 12}'


# Intent: MeterValues land as one sample with per-phase current.
# Why: the per-phase current is the input of load balancing against the mains limit.
async def test_meter_values_update_the_sample(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    await charger.call(
        "MeterValues",
        {
            "connectorId": 1,
            "meterValue": [
                {
                    "timestamp": "2026-10-20T18:00:00Z",
                    "sampledValue": [
                        {"measurand": "Current.Import", "phase": "L1", "value": "10.5", "unit": "A"},
                        {"measurand": "Power.Active.Import", "value": "2.4", "unit": "kW"},
                    ],
                }
            ],
        },
    )

    meter = hub.listener.latest.meter
    assert meter.current_a == {"L1": 10.5} and meter.power_w == 2400.0


# Intent: a transaction gets a unique, increasing id that is cleared again when it stops.
# Why: ids must never repeat while a charger may still hold the old one, and a stopped session must not look active.
async def test_transaction_lifecycle_and_unique_ids(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    first = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "tag", "meterStart": 100, "timestamp": "t"}
    )
    assert hub.listener.latest.connectors[1].transaction_id == first[2]["transactionId"]
    assert first[2]["idTagInfo"]["status"] == "Accepted" and hub.listener.latest.connectors[1].meter_start_wh == 100.0
    stop = await charger.call(
        "StopTransaction", {"transactionId": first[2]["transactionId"], "meterStop": 200, "timestamp": "t"}
    )
    second = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "tag", "meterStart": 200, "timestamp": "t"}
    )

    assert stop[0] == 3 and hub.listener.latest.connectors[1].transaction_id == second[2]["transactionId"]
    assert second[2]["transactionId"] > first[2]["transactionId"]


# Intent: unknown actions, bad payloads and bad field types get an error frame and the link stays up.
# Why: a charger must always get an answer, and one bad frame must not end the connection.
async def test_bad_requests_get_errors_and_the_link_survives(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    unknown = await charger.call("MakeCoffee", {})
    bad_type = await charger.call("StatusNotification", {"connectorId": "one", "status": "Available"})
    bad_payload = await charger.call("Heartbeat", [])
    await charger.send_raw("this is not json")
    await charger.send_raw(b"\x00\x01".decode("latin-1"))
    alive = await charger.call("Heartbeat", {})

    assert unknown[:3] == [4, unknown[1], "NotImplemented"]
    assert bad_type[2] == "FormationViolation" and bad_payload[2] == "FormationViolation"
    assert alive[0] == 3


# Answer the hub's one-key GetConfiguration requests from a table of values, as the NEO does, one request per key.
# A key the table does not hold is reported as unknown; a key outside the hub's list is added to the first answer so a
# test can check that only the asked keys are kept.
async def _answer_configuration(charger, values: dict[str, str], extra: dict[str, str] | None = None) -> list[str]:
    from ocpp_hub_core.session import CONFIG_KEYS

    asked: list[str] = []
    for _ in CONFIG_KEYS:
        request = await charger.next_hub_call()
        assert request[2] == "GetConfiguration" and len(request[3]["key"]) == 1
        key = request[3]["key"][0]
        asked.append(key)
        found = [{"key": key, "readonly": False, "value": values[key]}] if key in values else []
        found += [{"key": k, "readonly": False, "value": v} for k, v in (extra or {}).items()] if not asked[1:] else []
        await charger.answer(request, {"configurationKey": found, **({} if found else {"unknownKey": [key]})})
    return asked


# Intent: after boot the hub reads the configuration, stores the feature profiles and keeps only the asked keys.
# Why: whether smart charging is offered at all decides if a current limit can ever be set.
async def test_post_boot_setup_reads_configuration_and_features(start_hub) -> None:
    hub = await start_hub(setup_delay_s=0.0)
    charger = await hub.charger()
    await charger.boot()

    asked = await _answer_configuration(
        charger,
        {
            "SupportedFeatureProfiles": "Core,FirmwareManagement,SmartCharging",
            "MeterValuesSampledData": "Energy.Active.Import.Register",
            "MeterValueSampleInterval": "15",
        },
        extra={"AuthorizationKey": "not-asked-for"},
    )
    change = await charger.next_hub_call()
    await charger.answer(change, {"status": "Accepted"})

    assert asked[0] == "SupportedFeatureProfiles" and "MeterValuesSampledData" in asked
    assert change[2] == "ChangeConfiguration" and change[3]["key"] == "MeterValuesSampledData"
    assert "Current.Import" in change[3]["value"]
    await asyncio.sleep(0.05)
    assert hub.listener.latest.features == {"Core", "FirmwareManagement", "SmartCharging"}
    assert hub.listener.latest.configuration["MeterValuesSampledData"] == change[3]["value"]
    assert "AuthorizationKey" not in hub.listener.latest.configuration


# Intent: a malformed configuration entry leaves later configuration reads usable on the same connection.
# Why: a charger can return null for one key while supplying valid data for the next.
async def test_malformed_configuration_does_not_end_the_read_sequence(start_hub) -> None:
    from ocpp_hub_core.session import CONFIG_KEYS

    hub = await start_hub()
    charger = await hub.charger()
    session = hub.central.session("TEST0001")
    assert session is not None
    read = asyncio.create_task(session._read_configuration())
    for index, key in enumerate(CONFIG_KEYS):
        request = await charger.next_hub_call()
        assert request[2:] == ["GetConfiguration", {"key": [key]}]
        value = None if index == 0 else [{"key": key, "value": "fixture"}]
        await charger.answer(request, {"configurationKey": value})
    await read
    assert CONFIG_KEYS[0] not in session.snapshot.configuration
    assert session.snapshot.configuration[CONFIG_KEYS[-1]] == "fixture"


# Intent: a metering setting the charger already has is not written again.
# Why: every needless write to a charger is a risk and wears its flash; the hub only changes what differs.
async def test_matching_metering_settings_are_left_alone(start_hub) -> None:
    from ocpp_hub_core.session import SessionSettings

    wanted = SessionSettings()
    hub = await start_hub(setup_delay_s=0.0)
    charger = await hub.charger()
    await charger.boot()

    await _answer_configuration(
        charger,
        {"MeterValuesSampledData": wanted.measurands, "MeterValueSampleInterval": str(wanted.meter_sample_interval_s)},
    )

    with pytest.raises(TimeoutError):
        await charger.next_hub_call(timeout=0.3)


# Intent: a request the charger does not answer times out, and an error answer is raised with its code.
# Why: callers (limits, start, stop) must be able to tell a silent charger from a refusing one.
async def test_call_timeout_and_rejection(start_hub) -> None:
    hub = await start_hub(call_timeout_s=0.3)
    charger = await hub.charger()
    await charger.boot()
    session = hub.central.session("TEST0001")

    silent = asyncio.create_task(session.call("Reset", {"type": "Soft"}))
    await charger.next_hub_call()
    with pytest.raises(CallTimeoutError):
        await silent

    refused = asyncio.create_task(session.call("Reset", {"type": "Soft"}))
    request = await charger.next_hub_call()
    await charger.answer_error(request, "NotSupported")
    with pytest.raises(CallRejectedError) as error:
        await refused
    assert error.value.code == "NotSupported"


# Intent: the same charger reconnecting replaces the old connection, and a disconnect is reported.
# Why: after a Wi-Fi drop the charger returns before the hub notices the old link is dead.
async def test_reconnect_replaces_the_old_session_and_disconnect_is_reported(start_hub) -> None:
    hub = await start_hub()
    first = await hub.charger()
    await first.boot()

    second = await hub.charger()
    await second.boot()
    await first.wait_closed()
    await second.close()
    await asyncio.sleep(0.1)

    assert hub.central.session("TEST0001") is None
    assert hub.listener.disconnected.count("TEST0001") == 2
    assert hub.listener.latest.connected is False


# Intent: listeners see the charger's frames and the hub's answers, each labelled with its sender.
# Why: the mirror forwards the charger's frames and needs the hub's StartTransaction answer to map ids.
async def test_listener_sees_both_directions(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    await charger.call("Heartbeat", {})

    senders = [(sender, frame[0]) for _, sender, frame in hub.listener.frames]
    assert senders == [("CP", 2), ("CSMS", 3)]


# Intent: a connection without a charger identity is refused.
# Why: the identity is the only way to tell chargers apart; without it nothing can be tracked.
async def test_connection_without_identity_is_refused(start_hub) -> None:
    from tests.fake_charger import FakeCharger

    hub = await start_hub()
    charger = FakeCharger(hub.central.port, cp_id="")
    await charger.connect()

    await charger.wait_closed()

    assert charger.close_code == 4001


# Intent: a charger that reconnects without a BootNotification is asked for its boot, and then for its status.
# Why: after a restart of the hub the Raedian NEO sends neither, and without them the hub knows no model and no state.
async def test_charger_that_skips_its_boot_is_asked_to_introduce_itself(start_hub) -> None:
    hub = await start_hub(boot_wait_s=0.0, trigger_settle_s=0.2, setup_retry_delays_s=(0.2,))
    charger = await hub.charger()

    boot_request = await charger.next_hub_call()
    await charger.answer(boot_request, {"status": "Accepted"})
    await charger.boot()
    status_request = await charger.next_hub_call()
    await charger.answer(status_request, {"status": "Accepted"})

    assert boot_request[2] == "TriggerMessage" and boot_request[3] == {"requestedMessage": "BootNotification"}
    assert status_request[2] == "TriggerMessage"
    assert status_request[3] == {"requestedMessage": "StatusNotification", "connectorId": 1}
    assert hub.listener.latest.info.model == "NEO_22T_A"


# Intent: a charger that introduces itself on its own is not asked again.
# Why: every extra request to a charger is a risk, and a normal charger sends its boot and status without being asked.
async def test_charger_that_introduces_itself_is_not_asked(start_hub) -> None:
    hub = await start_hub(boot_wait_s=0.3, trigger_settle_s=0.1, setup_retry_delays_s=(0.2,))
    charger = await hub.charger()

    await charger.boot()
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"})

    with pytest.raises(TimeoutError):
        await charger.next_hub_call(timeout=0.8)


# Intent: stopping the hub drops every charger connection without a closing handshake, and the sessions end cleanly.
# Why: the Raedian NEO waits for minutes after any proper close and returns in seconds after a lost connection.
async def test_stopping_the_hub_drops_chargers_without_a_closing_handshake(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    await charger.boot()

    await hub.central.stop()
    await charger.wait_closed()
    await asyncio.sleep(0.05)

    assert charger.close_code == 1006
    assert hub.listener.disconnected == ["TEST0001"] and hub.listener.latest.connected is False


# Answer every request the hub sends until the charger has been quiet for a moment, as a charger that knows no key and
# accepts everything would; the requests are returned in the order they came.
async def _serve_hub_calls(charger, statuses: dict[str, str] | None = None) -> list[list]:
    seen: list[list] = []
    while True:
        try:
            request = await charger.next_hub_call(timeout=0.4)
        except TimeoutError:
            return seen
        seen.append(request)
        if request[2] == "GetConfiguration":
            key = request[3]["key"][0]
            value = {"MeterValuesSampledData": "X"}.get(key)
            found = [{"key": key, "readonly": False, "value": value}] if value is not None else []
            await charger.answer(request, {"configurationKey": found, **({} if found else {"unknownKey": [key]})})
        else:
            await charger.answer(request, {"status": (statuses or {}).get(request[2], "Accepted")})


# Intent: a charger that boots again while its setup is still running does not get a second setup.
# Why: a setup lives for minutes and asks for seven keys, so a charger in a boot loop would otherwise be flooded.
async def test_repeated_boots_run_one_setup(start_hub) -> None:
    hub = await start_hub(setup_delay_s=0.0, setup_retry_delays_s=())
    charger = await hub.charger()

    for _ in range(5):
        await charger.boot()
    seen = await _serve_hub_calls(charger)

    keys = [request[3]["key"][0] for request in seen if request[2] == "GetConfiguration"]
    assert keys.count("SupportedFeatureProfiles") == 1


# Intent: a metering setting the charger rejected is not written again on the same connection.
# Why: the write is limited to two keys, but a charger that keeps rejecting must not be asked again at every boot.
async def test_rejected_metering_setting_is_not_written_again(start_hub) -> None:
    hub = await start_hub(setup_delay_s=0.0, setup_retry_delays_s=())
    charger = await hub.charger()

    await charger.boot()
    first = await _serve_hub_calls(charger, {"ChangeConfiguration": "Rejected"})
    await charger.boot()
    second = await _serve_hub_calls(charger, {"ChangeConfiguration": "Rejected"})

    writes = [request for request in first + second if request[2] == "ChangeConfiguration"]
    assert len(writes) == 1
