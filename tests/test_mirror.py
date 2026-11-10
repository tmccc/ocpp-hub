"""The vendor mirror, driven end to end: a fake charger, the real hub, the real mirror and a fake vendor backend."""

from __future__ import annotations

import base64
import json
import logging
import shutil
import ssl
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.fake_vendor import FakeVendor, eventually

CHARGER = "TEST0001"
METER_VALUE = [
    {"timestamp": "2026-10-20T18:00:00Z", "sampledValue": [{"measurand": "Power.Active.Import", "value": "7"}]}
]


# Intent: everything the charger asks the hub reaches the vendor in order, except its heartbeats.
# Why: the vendor must see the whole picture, and its own heartbeat interval, not the charger's, decides its timeouts.
async def test_charger_traffic_is_forwarded_in_order_without_heartbeats(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()

    await charger.boot()
    await charger.call("Heartbeat", {})
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"})
    await charger.call("Authorize", {"idTag": "tag"})
    await charger.call("DataTransfer", {"vendorId": "x", "data": "y"})
    await eventually(lambda: len(rig.vendor.calls) >= 4, message="frames did not arrive")

    assert rig.vendor.actions() == ["BootNotification", "StatusNotification", "Authorize", "DataTransfer"]
    assert rig.vendor.calls[2][3] == {"idTag": "tag"}


# Intent: the vendor URL gets the charger identity as the last path segment and the OCPP 1.6 subprotocol.
# Why: the vendor recognises the device by that segment, and a different subprotocol is refused.
async def test_vendor_connection_carries_the_charger_identity(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()

    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    assert rig.vendor.paths == [f"/ocpp/{CHARGER}"] and rig.vendor.subprotocols == ["ocpp1.6"]


# Intent: Basic authentication is sent when a user and password are configured, and not otherwise.
# Why: a vendor without authentication must not receive an empty header, and one with it must receive the right one.
@pytest.mark.parametrize(("user", "password"), [(None, None), ("mirror-user", "s3cret")])
async def test_basic_auth_only_when_configured(start_rig: Any, user: str | None, password: str | None) -> None:
    rig = await start_rig(user=user, password=password)
    charger = await rig.hub.charger()

    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    expected = f"Basic {base64.b64encode(b'mirror-user:s3cret').decode()}" if user else None
    assert rig.vendor.authorizations == [expected]


# Intent: the vendor sees its own transaction id in meter values and the stop, and a finished transaction is forgotten.
# Why: the vendor only matches frames to a session by the id it handed out, and an id kept forever would leak.
async def test_transaction_ids_are_translated_and_forgotten_after_the_stop(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()

    start = await charger.call("StartTransaction", {"connectorId": 1, "idTag": "t", "meterStart": 1, "timestamp": "t"})
    hub_id = start[2]["transactionId"]
    await charger.call("MeterValues", {"connectorId": 1, "transactionId": hub_id, "meterValue": METER_VALUE})
    await charger.call("StopTransaction", {"transactionId": hub_id, "meterStop": 2, "timestamp": "t"})
    await charger.call("StopTransaction", {"transactionId": hub_id, "meterStop": 2, "timestamp": "t"})
    await eventually(lambda: rig.mirror.status(CHARGER).unmapped_transactions == 1, message="second stop not seen")

    vendor_id = 7001
    assert hub_id != vendor_id
    assert rig.vendor.calls_of("MeterValues")[0][3]["transactionId"] == vendor_id
    assert [call[3]["transactionId"] for call in rig.vendor.calls_of("StopTransaction")] == [vendor_id]


# Intent: a stop for a transaction the vendor never saw is not sent, and meter values for it go without an id.
# Why: this happens after a Home Assistant restart mid-charge; guessing an id could end someone else's session.
async def test_unknown_transaction_is_not_stopped_at_the_vendor(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()

    await charger.call("MeterValues", {"connectorId": 1, "transactionId": 4242, "meterValue": METER_VALUE})
    await charger.call("StopTransaction", {"transactionId": 4242, "meterStop": 2, "timestamp": "t"})
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Finishing", "errorCode": "NoError"})
    await eventually(lambda: len(rig.vendor.calls) >= 3, message="frames did not arrive")

    assert rig.vendor.actions() == ["BootNotification", "MeterValues", "StatusNotification"]
    assert "transactionId" not in rig.vendor.calls[1][3]
    assert rig.mirror.status(CHARGER).unmapped_transactions == 1


REFUSALS: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
    ("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": {}}, {"status": "Rejected"}),
    ("ClearChargingProfile", {}, {"status": "Unknown"}),
    ("RemoteStartTransaction", {"idTag": "x"}, {"status": "Rejected"}),
    ("RemoteStopTransaction", {"transactionId": 1}, {"status": "Rejected"}),
    ("ChangeConfiguration", {"key": "HeartbeatInterval", "value": "10"}, {"status": "Rejected"}),
    ("Reset", {"type": "Soft"}, {"status": "Rejected"}),
    ("ChangeAvailability", {"connectorId": 1, "type": "Inoperative"}, {"status": "Rejected"}),
    ("UnlockConnector", {"connectorId": 1}, {"status": "UnlockFailed"}),
    ("DataTransfer", {"vendorId": "v"}, {"status": "Rejected"}),
    ("UpdateFirmware", {"location": "https://vendor.example/fw", "retrieveDate": "t"}, {}),
    ("GetDiagnostics", {"location": "https://vendor.example/logs"}, {}),
]


# Intent: every command the vendor sends gets an answer, and none of them reaches the charger.
# Why: the vendor is read-only by design; its load management must not override the limits Home Assistant sets.
@pytest.mark.parametrize(("action", "payload", "expected"), REFUSALS)
async def test_vendor_commands_are_refused_and_never_reach_the_charger(
    start_rig: Any, action: str, payload: dict[str, Any], expected: dict[str, Any]
) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    answer = await rig.vendor.command(action, payload)

    assert answer[0] == 3 and answer[2] == expected
    assert charger.hub_calls.empty()


# Intent: a command the mirror does not know is answered with a NotSupported error, not ignored.
# Why: a backend that gets no answer stops talking to the device.
async def test_unknown_vendor_command_gets_an_error(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    answer = await rig.vendor.command("MakeCoffee", {})

    assert answer[0] == 4 and answer[2] == "NotSupported"
    assert charger.hub_calls.empty()


# Intent: TriggerMessage makes the mirror send the remembered Boot or Status again, under a new unique id.
# Why: the vendor refreshes its picture of the device with it, and a repeated unique id looks like a retransmission.
async def test_trigger_message_sends_remembered_frames_again(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Preparing", "errorCode": "NoError"})
    await eventually(lambda: len(rig.vendor.calls) >= 2, message="frames did not arrive")

    status = await rig.vendor.command("TriggerMessage", {"requestedMessage": "StatusNotification", "connectorId": 1})
    boot = await rig.vendor.command("TriggerMessage", {"requestedMessage": "BootNotification"})
    unknown_connector = await rig.vendor.command(
        "TriggerMessage", {"requestedMessage": "StatusNotification", "connectorId": 7}
    )
    meter = await rig.vendor.command("TriggerMessage", {"requestedMessage": "MeterValues"})
    await eventually(lambda: len(rig.vendor.calls) >= 4, message="triggered frames did not arrive")

    assert [answer[2]["status"] for answer in (status, boot, unknown_connector, meter)] == [
        "Accepted",
        "Accepted",
        "Rejected",
        "NotImplemented",
    ]
    again_status, again_boot = rig.vendor.calls[2], rig.vendor.calls[3]
    assert again_status[2] == "StatusNotification" and again_status[3]["status"] == "Preparing"
    assert again_boot[2] == "BootNotification" and again_boot[3]["chargePointModel"] == "NEO_22T_A"
    assert {again_status[1], again_boot[1]}.isdisjoint({rig.vendor.calls[0][1], rig.vendor.calls[1][1]})


# Intent: GetConfiguration from the vendor is answered with the settings the hub read from the charger.
# Why: the vendor then sees the real device, while the keys stay read-only.
async def test_get_configuration_is_answered_from_the_hub_snapshot(start_rig: Any) -> None:
    rig = await start_rig(hub={"setup_delay_s": 0.0})
    charger = await rig.hub.charger()
    await charger.boot()
    request = await charger.next_hub_call()
    profiles = {"key": "SupportedFeatureProfiles", "readonly": True, "value": "Core,SmartCharging"}
    await charger.answer(request, {"configurationKey": [profiles]})
    await eventually(lambda: rig.hub.listener.latest.configuration, message="hub did not read the configuration")

    answer = await rig.vendor.command("GetConfiguration", {"key": ["SupportedFeatureProfiles", "Nope"]})

    item = {"key": "SupportedFeatureProfiles", "readonly": True, "value": "Core,SmartCharging"}
    assert answer[2] == {"configurationKey": [item], "unknownKey": ["Nope"]}


# Intent: after the vendor was away, the new connection starts with Boot and the connector states, then the missed
# frames in their original order.
# Why: the vendor sees a new charge point and must learn its state first; reordering shows a session before its start.
async def test_vendor_outage_is_bridged_in_order(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()
    for connector in (0, 1):
        await charger.call(
            "StatusNotification", {"connectorId": connector, "status": "Available", "errorCode": "NoError"}
        )
    await eventually(lambda: len(rig.vendor.calls) >= 3, message="first frames did not arrive")
    await eventually(lambda: rig.mirror.status(CHARGER).queue_length == 0, message="first frames not delivered")
    await rig.vendor.stop()
    await eventually(lambda: not rig.mirror.status(CHARGER).connected, message="outage not noticed")
    first_connection_calls = len(rig.vendor.calls)

    await charger.call("StatusNotification", {"connectorId": 1, "status": "Preparing", "errorCode": "NoError"})
    await charger.call("StartTransaction", {"connectorId": 1, "idTag": "t", "meterStart": 1, "timestamp": "t"})
    await charger.call("MeterValues", {"connectorId": 1, "meterValue": METER_VALUE})
    await rig.vendor.start()
    await eventually(lambda: len(rig.vendor.calls) >= first_connection_calls + 5, message="frames not replayed")
    # Peer receipt precedes processing its ACK and retiring the last queued frame.
    await eventually(lambda: rig.mirror.status(CHARGER).queue_length == 0, message="replayed frames not acknowledged")

    replayed = rig.vendor.calls[first_connection_calls:]
    assert [(call[2], call[3].get("connectorId"), call[3].get("status")) for call in replayed] == [
        ("BootNotification", None, None),
        ("StatusNotification", 0, "Available"),
        ("StatusNotification", 1, "Preparing"),
        ("StartTransaction", 1, None),
        ("MeterValues", 1, None),
    ]
    assert rig.mirror.status(CHARGER).queue_length == 0


# Intent: a vendor that is not reachable is reported in the status and does not disturb the charger.
# Why: the mirror is a courtesy copy; the hub must keep serving the charger however the vendor behaves.
async def test_unreachable_vendor_is_reported_and_the_charger_is_unaffected(start_hub: Any, start_mirror: Any) -> None:
    mirror = await start_mirror("ws://127.0.0.1:1/ocpp")
    hub = await start_hub(mirror)
    charger = await hub.charger()

    boot = await charger.boot()
    heartbeat = await charger.call("Heartbeat", {})
    await eventually(lambda: mirror.status(CHARGER).last_error is not None, message="no error reported")

    status = mirror.status(CHARGER)
    assert boot[2]["status"] == "Accepted" and heartbeat[0] == 3
    assert status.connected is False and status.queue_length == 1 and status.last_success is None


# Intent: a vendor that accepts the connection but never answers makes the link reconnect and send the frame again.
# Why: only one request may be outstanding, so a silent vendor must not freeze the link or lose the frame.
async def test_silent_vendor_leads_to_a_reconnect_and_a_resend(start_rig: Any) -> None:
    vendor = FakeVendor()
    vendor.hold()
    rig = await start_rig(vendor, reply_timeout_s=0.3)
    charger = await rig.hub.charger()

    boot = await charger.boot()
    await eventually(lambda: rig.vendor.connection_count >= 2, message="no reconnect after the timeout")
    vendor.release()
    await eventually(lambda: rig.mirror.status(CHARGER).last_success is not None, message="never delivered")

    status = rig.mirror.status(CHARGER)
    assert boot[2]["status"] == "Accepted" and vendor.actions().count("BootNotification") >= 2
    assert status.connected and status.queue_length == 0


# Intent: a BootNotification the vendor does not accept is retried after the interval, and nothing else is sent before.
# Why: OCPP forbids other messages from a charge point whose boot was not accepted.
async def test_boot_that_is_not_accepted_is_retried_after_the_interval(start_rig: Any) -> None:
    vendor = FakeVendor(boot_status="Pending", interval=1)
    rig = await start_rig(vendor)
    charger = await rig.hub.charger()

    await charger.boot()
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"})
    await eventually(lambda: "Pending" in (rig.mirror.status(CHARGER).last_error or ""), message="refusal not seen")
    assert vendor.actions() == ["BootNotification"]
    vendor.boot_status = "Accepted"
    await eventually(lambda: "StatusNotification" in vendor.actions(), timeout=4.0, message="boot not retried")

    assert vendor.actions() == ["BootNotification", "BootNotification", "StatusNotification"]
    assert rig.mirror.status(CHARGER).connected


# Intent: when the vendor is idle, the mirror sends a heartbeat at the interval the vendor asked for.
# Why: a charge point that goes silent is shown as offline, and the charger's own heartbeats are not forwarded.
async def test_idle_link_sends_heartbeats_at_the_vendors_interval(start_rig: Any) -> None:
    rig = await start_rig(FakeVendor(interval=1))
    charger = await rig.hub.charger()

    await charger.boot()
    await eventually(lambda: "Heartbeat" in rig.vendor.actions(), timeout=4.0, message="no heartbeat")

    heartbeat = rig.vendor.calls_of("Heartbeat")[0]
    assert heartbeat[1].startswith("mirror-") and heartbeat[3] == {}


# Intent: when the charger leaves, the vendor connection is closed and the link disappears.
# Why: the vendor should see the device go offline, and a link for a charger that is gone would keep heartbeating.
async def test_charger_leaving_closes_the_vendor_link(start_rig: Any) -> None:
    rig = await start_rig()
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    await charger.close()
    await eventually(lambda: rig.mirror.status(CHARGER) is None, message="link not closed")
    await eventually(lambda: rig.vendor.closed_count == 1, message="vendor connection not closed")

    assert rig.mirror.statuses() == {}


# Intent: frames still waiting for an offline vendor are delivered after the charger has left, then the link closes.
# Why: a StopTransaction lost this way would leave a session open at the vendor for good.
async def test_waiting_frames_survive_the_charger_leaving(start_rig: Any) -> None:
    rig = await start_rig()
    await rig.vendor.stop()
    charger = await rig.hub.charger()
    await charger.boot()
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"})

    await charger.close()
    await eventually(lambda: rig.hub.listener.disconnected, message="disconnect not seen")
    assert rig.mirror.status(CHARGER).queue_length == 2
    await rig.vendor.start()
    await eventually(lambda: rig.mirror.status(CHARGER) is None, message="link not closed after delivery")

    assert rig.vendor.actions() == ["BootNotification", "StatusNotification"]


# Intent: the same charger reconnecting keeps its transactions mapped.
# Why: a Wi-Fi drop in the middle of a charge must not leave the vendor with a session that never ends.
async def test_stop_after_the_charger_reconnected_still_uses_the_vendors_id(start_rig: Any) -> None:
    rig = await start_rig()
    first = await rig.hub.charger()
    await first.boot()
    start = await first.call("StartTransaction", {"connectorId": 1, "idTag": "t", "meterStart": 1, "timestamp": "t"})
    await eventually(lambda: "StartTransaction" in rig.vendor.actions(), message="start did not arrive")
    await eventually(lambda: rig.mirror.status(CHARGER).queue_length == 0, message="start not delivered")

    second = await rig.hub.charger()
    await second.boot()
    await second.call("StopTransaction", {"transactionId": start[2]["transactionId"], "meterStop": 2, "timestamp": "t"})
    await eventually(lambda: rig.vendor.calls_of("StopTransaction"), message="stop did not arrive")

    assert rig.vendor.calls_of("StopTransaction")[0][3]["transactionId"] == 7001


# Intent: a wss vendor is reached with the SSL context given to the mirror.
# Why: the real vendor backend is probably TLS-only, and the context is built outside the event loop on purpose.
async def test_wss_vendor_is_reached_with_the_given_ssl_context(
    tmp_path: Path, start_hub: Any, start_mirror: Any
) -> None:
    if shutil.which("openssl") is None:
        pytest.skip("openssl is needed to make a test certificate")
    certificate, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=vendor.test"]
        + ["-addext", "subjectAltName=IP:127.0.0.1", "-keyout", str(key), "-out", str(certificate)],
        check=True,
        capture_output=True,
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certificate, key)
    vendor = FakeVendor()
    await vendor.start(server_context)
    try:
        mirror = await start_mirror(vendor.url, ssl_context=ssl.create_default_context(cafile=str(certificate)))
        charger = await (await start_hub(mirror)).charger()

        await charger.boot()
        await eventually(lambda: vendor.calls, message="no frame arrived over TLS")
    finally:
        await vendor.stop()

    assert vendor.url.startswith("wss://") and vendor.actions() == ["BootNotification"]


# Intent: two StartTransaction requests that share a unique id and wait for an offline vendor each keep their own pair.
# Why: a charger that restarts its message counter would otherwise give a transaction the vendor id of another one,
# and a stop or meter value could then end or pollute the wrong session at the vendor.
async def test_reused_start_ids_still_pair_with_the_right_vendor_transaction(start_rig: Any) -> None:
    rig = await start_rig()
    await rig.vendor.stop()
    charger = await rig.hub.charger()
    start = {"connectorId": 1, "idTag": "t", "meterStart": 1, "timestamp": "t"}

    # The transaction ids the hub handed out for the requests with unique id X, in order.
    def hub_transactions() -> list[int]:
        return [f[2]["transactionId"] for _, sender, f in rig.hub.listener.frames if sender == "CSMS" and f[1] == "X"]

    await charger.send_raw(json.dumps([2, "X", "StartTransaction", start]))
    await eventually(lambda: len(hub_transactions()) == 1, message="first start not answered by the hub")
    await charger.send_raw(json.dumps([2, "X", "StartTransaction", start]))
    await eventually(lambda: len(hub_transactions()) == 2, message="second start not answered by the hub")
    first, second = hub_transactions()
    await rig.vendor.start()
    await eventually(lambda: len(rig.vendor.calls_of("StartTransaction")) == 2, message="starts not delivered")
    await eventually(lambda: rig.mirror.status(CHARGER).queue_length == 0, message="starts not answered")
    await charger.call("MeterValues", {"connectorId": 1, "transactionId": second, "meterValue": METER_VALUE})
    await charger.call("StopTransaction", {"transactionId": first, "meterStop": 2, "timestamp": "t"})
    await eventually(lambda: rig.vendor.calls_of("StopTransaction"), message="stop did not arrive")

    assert first != second
    assert rig.vendor.calls_of("MeterValues")[0][3]["transactionId"] == 7002
    assert rig.vendor.calls_of("StopTransaction")[0][3]["transactionId"] == 7001


# Intent: the vendor password and its encoded form never appear in a log line, even with debug logging on everywhere.
# Why: the websockets library logs every request header at debug level, and Home Assistant logs get pasted into issues.
async def test_vendor_password_never_reaches_the_log(start_rig: Any, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    rig = await start_rig(user="mirror-user", password="s3cret")
    charger = await rig.hub.charger()

    await charger.boot()
    await eventually(lambda: rig.vendor.calls, message="no frame arrived")

    encoded = base64.b64encode(b"mirror-user:s3cret").decode()
    # The fake vendor's own websocket server logs what it receives; only our side of the connection is under test.
    ours = "\n".join(
        record.getMessage() for record in caplog.records if not record.name.startswith("websockets.server")
    )
    assert rig.vendor.authorizations == [f"Basic {encoded}"]
    assert "Authorization" in ours, "the header must be logged (redacted), or this test proves nothing"
    assert "s3cret" not in ours and encoded not in ours
