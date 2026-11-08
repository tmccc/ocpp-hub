"""Real sanitized iCS wire shapes against the isolated engine; all peers remain simulated.

The complete transformed capture is retained, including unmatched requests. A synthetic
Boot introduces the test peers because the real export contains no BootNotification.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.ics import TransactionQuery
from ocpp_hub_core.prototype.policy import DeniedError, Policy
from ocpp_hub_core.prototype.rpc import Peer
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_prototype import Rig

CAPTURE_PATH = Path(__file__).parent / "fixtures" / "ics-log7628.sanitized.json"
CAPTURE = json.loads(CAPTURE_PATH.read_text())
ROWS = CAPTURE["frames"]
CALLS = [row for row in ROWS if row["frame"][0] == 2]
REPLIES = {(row["direction"], row["frame"][1]): row["frame"] for row in ROWS if row["frame"][0] != 2}
STARTS = [row["frame"][3] for row in CALLS if row["frame"][2] == "StartTransaction"]
TAG = STARTS[0]["idTag"]
ICS_POLICY = Policy(
    max_current_a=32,
    allowed_tags=frozenset({TAG}),
    extensions={("IoCharger", "getTransaction"): "read"},
)


# Find a recorded response on the opposite leg; missing responses stay explicitly missing.
def recorded_reply(row: dict[str, Any]) -> list[Any] | None:
    opposite = "cloud_to_charger" if row["direction"] == "charger_to_cloud" else "charger_to_cloud"
    return REPLIES.get((opposite, row["frame"][1]))


# Compare the known embedded JSON semantically because translated data is deliberately reserialized.
def semantic_result(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    if isinstance(result.get("data"), str):
        result["data"] = json.loads(result["data"])
    return result


# Intent: retain and parse every real frame rather than cherry-picking easy exchanges.
# Why: this is transformed actual wire evidence, distinct from reconstructed LAN transaction history.
def test_ics_complete_capture_shapes_and_provenance() -> None:
    assert CAPTURE["frame_count"] == len(ROWS) == 517
    assert CAPTURE["transforms"]["sampled_frames_dropped"] == 0
    assert "NOT byte-for-byte" in CAPTURE["provenance"]
    for row in ROWS:
        assert decode(json.dumps(row["frame"])) == row["frame"]
    counts = Counter(row["frame"][2] for row in CALLS)
    assert counts == {
        "Authorize": 1,
        "DiagnosticsStatusNotification": 2,
        "Heartbeat": 10,
        "MeterValues": 194,
        "StartTransaction": 2,
        "StatusNotification": 47,
        "StopTransaction": 1,
        "DataTransfer": 2,
        "GetConfiguration": 2,
        "GetDiagnostics": 1,
        "RemoteStartTransaction": 1,
    }
    assert sum(recorded_reply(row) is None for row in CALLS) == 9
    assert {row["frame"][2] for row in CALLS if recorded_reply(row) is None} == {"MeterValues"}


# Intent: exercise every captured request through the real journal, mapping and command boundary.
# Why: payload compatibility alone cannot show that the two different transaction namespaces stay aligned.
async def test_ics_capture_requests_and_lifecycle_with_explicit_policy(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", ICS_POLICY)
    rig.next_vendor = 6000000
    await rig.boot()  # Synthetic setup; the capture contains no actual boot/authentication handshake.
    await rig.cloud()
    matched = 0
    missing_meter_replies = 0
    blocked_diagnostics = 0
    for row in CALLS:
        frame = row["frame"]
        payload = json.loads(json.dumps(frame[3]))
        reply = recorded_reply(row)
        if row["direction"] == "charger_to_cloud":
            if "transactionId" in payload:
                local = rig.store.historical_local_id(payload["transactionId"])
                assert local is not None
                payload["transactionId"] = local
            answer = await rig.charger(frame[1], frame[2], payload)
            assert answer[0] == 3, frame[2]
            if reply is not None and frame[2] not in ("StartTransaction", "Heartbeat", "StopTransaction", "Authorize"):
                assert answer[2] == reply[2]
            if frame[2] == "Authorize":
                assert answer[2]["idTagInfo"]["status"] == reply[2]["idTagInfo"]["status"]
            if frame[2] == "StopTransaction":
                assert answer[2] == {}  # Cloud's optional idTagInfo is not part of the earlier local ACK.
            rig.vendor_result = reply[2] if reply is not None else {}
            if reply is None:
                missing_meter_replies += 1  # Simulated empty ACK, explicitly absent from the real recording.
            before = len(rig.vendor_frames)
            await rig.gateway.flush()
            outgoing = [sent for sent in rig.vendor_frames[before:] if sent[0] == 2]
            if frame[2] in ("Authorize", "Heartbeat"):
                assert outgoing == []  # Upstream heartbeat maintenance is a documented missing runtime feature.
            else:
                assert len(outgoing) == 1
                assert outgoing[0][2:] == frame[2:]
        else:
            assert reply is not None
            rig.charger_result = json.loads(json.dumps(reply[2]))
            if frame[2] == "DataTransfer":
                data = json.loads(rig.charger_result["data"])
                data["transactionId"] = rig.store.historical_local_id(data["transactionId"])
                rig.charger_result["data"] = json.dumps(data)
            answer = await rig.vendor(frame[1], frame[2], payload)
            if frame[2] == "GetDiagnostics":
                assert answer[0] == 4  # A real support feature intentionally not approved by this policy.
                blocked_diagnostics += 1
            else:
                assert answer[0] == 3
                assert semantic_result(answer[2]) == semantic_result(reply[2])
        matched += 1
    assert (matched, missing_meter_replies, blocked_diagnostics) == (263, 9, 1)
    assert rig.store.next_delivery() is None
    assert rig.store.active() == 1002
    completed = rig.store.db.execute("SELECT start,stop FROM transactions WHERE local=1001").fetchone()
    assert json.loads(completed[1])["meterStop"] - json.loads(completed[0])["meterStart"] == 43683
    assert not rig.gateway.failures
    await rig.close()


# Intent: test the historical metadata mapping after Stop without allowing a stopped-session control.
# Why: iCS's nested transaction query needs tombstones, but RemoteStop must remain active-only.
async def test_ics_completed_transaction_query_preserves_control_boundary(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", ICS_POLICY)
    rig.next_vendor = 6000000
    await rig.boot()
    await rig.charger("start", "StartTransaction", STARTS[0])
    await rig.cloud()
    stop = next(row["frame"][3] for row in CALLS if row["frame"][2] == "StopTransaction")
    await rig.charger("stop", "StopTransaction", {**stop, "transactionId": 1001})
    await rig.gateway.flush()
    assert rig.store.local_id(6000001) is None
    assert rig.store.historical_local_id(6000001) == 1001
    query = next(row["frame"][3] for row in CALLS if row["frame"][2] == "DataTransfer")
    rig.charger_result = {"status": "Accepted", "data": '{"transactionId":1001,"eco":"Fast"}'}
    answer = await rig.vendor("history", "DataTransfer", query)
    assert json.loads(answer[2]["data"]) == {"transactionId": 6000001, "eco": "Fast"}
    assert json.loads(rig.charger_frames[-1][3]["data"])["transactionId"] == 1001
    assert (await rig.vendor("stale-stop", "RemoteStopTransaction", {"transactionId": 6000001}))[0] == 4
    assert (await rig.charger("reverse-query", "DataTransfer", query))[0] == 4
    await rig.close()


# Intent: defaults deliberately refuse the real tag and 32 A settings until explicitly configured offline.
# Why: compatibility evidence is not authorization to expand the live controller's permissions or limits.
async def test_ics_policy_defaults_and_explicit_32_amp_ceiling(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    assert (await rig.charger("auth", "Authorize", {"idTag": TAG}))[2]["idTagInfo"]["status"] == "Invalid"
    assert (await rig.charger("start", "StartTransaction", STARTS[0]))[0] == 4
    profile = commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 32, 10)
    with pytest.raises(DeniedError):
        rig.gateway.policy.validate("SetChargingProfile", profile)
    ICS_POLICY.validate("SetChargingProfile", profile)
    profile["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 33
    with pytest.raises(DeniedError):
        ICS_POLICY.validate("SetChargingProfile", profile)
    query = next(row["frame"][3] for row in CALLS if row["frame"][2] == "DataTransfer")
    with pytest.raises(DeniedError):
        rig.gateway.policy.kind("DataTransfer", query)
    await rig.close()


# Intent: raw pre-existing ids and overlapping unresolved Starts cannot silently become trusted local sessions.
# Why: a future cutover needs verified reconciliation instead of guessing from the connector or current cloud id.
async def test_ics_cold_orphan_and_overlapping_history_stay_blocked(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", ICS_POLICY)
    meter = next(row["frame"][3] for row in CALLS if row["frame"][2] == "MeterValues")
    assert (await rig.charger("orphan-meter", "MeterValues", meter))[0] == 4
    await rig.charger("old-start", "StartTransaction", STARTS[0])
    assert rig.store.db.execute("SELECT live FROM transactions").fetchone()[0] == 0
    assert (await rig.charger("overlap", "StartTransaction", STARTS[1]))[0] == 4
    assert rig.store.active() == 1001
    await rig.close()


# Intent: enforce OCPP-J's 36-character messageId limit at both incoming and generated boundaries.
# Why: the original prototype emitted 38/39-character ids that the real iCS server need not accept.
@pytest.mark.parametrize("length", [0, 1, 19, 32, 36, 37, 64])
def test_ics_message_identifier_boundary(length: int) -> None:
    raw = build_call("a" * length, "Heartbeat", {})
    if 1 <= length <= 36:
        assert decode(raw)[1] == "a" * length
    else:
        with pytest.raises(DeniedError):
            decode(raw)


# Intent: test actual Peer generation and persisted outbox ids rather than inspecting a format string.
# Why: retries must preserve a protocol-valid id without executing send hooks for invalid tokens.
@pytest.mark.parametrize("name", ["local", "vendor"])
async def test_ics_generated_identifiers_fit_protocol(tmp_path: Path, name: str) -> None:
    peer = Peer(name, 0.1)
    sent: list[list[Any]] = []

    # Resolve the real peer's generated request using its actual id.
    async def send(raw: str) -> None:
        frame = decode(raw)
        sent.append(frame)
        peer.receive(decode(build_call_result(frame[1], {})))

    peer.sender = send
    await peer.call("Heartbeat", {})
    assert len(sent[0][1]) == 34
    with pytest.raises(RecoveryRequiredError):
        await peer.call("Heartbeat", {}, token="x" * 37)
    assert len(sent) == 1
    rig = Rig(tmp_path / "journal.db")
    await rig.charger("start", "StartTransaction", {**STARTS[0], "idTag": "LOCAL_TEST"})
    assert len(rig.store.next_delivery().token) == 34
    await rig.close()


# Intent: reject unsupported or ambiguous nested request shapes, including unknown transaction mappings.
# Why: generic opaque forwarding would expose cloud ids directly to a charger using local ids.
@pytest.mark.parametrize(
    "data",
    [
        '{"transactionId":6000002}',
        '{"transactionId":true}',
        '{"transactionId":"6000001"}',
        '{"transactionId":6000001,"extra":1}',
        '{"transactionId":6000001,"transactionId":6000002}',
        '{"transactionId":NaN}',
        "[]",
        {"transactionId": 6000001},
        "\ud800",
    ],
)
def test_ics_query_rejects_unknown_request_shapes(data: Any) -> None:
    payload = {"vendorId": "IoCharger", "messageId": "getTransaction", "data": data}
    with pytest.raises(DeniedError):
        TransactionQuery.from_request(payload, lambda vendor: 1001 if vendor == 6000001 else None)


# Intent: preserve actual refusals/errors while rejecting an Accepted answer for a different local transaction.
# Why: a successful response must never attach another session's metadata to the requester's transaction.
def test_ics_query_response_validation_and_real_refusals() -> None:
    query = TransactionQuery(6000001, 1001)
    for raw in (build_call_result("test", {"status": "Rejected"}), build_call_error("test", "InternalError", "actual")):
        frame = decode(raw)
        assert query.response(frame) == frame
    for data in (
        '{"transactionId":1002,"eco":"Fast"}',
        '{"transactionId":1001,"eco":"UnknownMode"}',
        '{"transactionId":1001,"eco":"Fast","extra":1}',
        '{"transactionId":1001,"transactionId":1002,"eco":"Fast"}',
    ):
        with pytest.raises(DeniedError):
            query.response(decode(build_call_result("test", {"status": "Accepted", "data": data})))


# Intent: malformed nested text and duplicate envelope keys receive a refusal without hanging a request handler.
# Why: an authenticated peer can still send malformed data, and must not bypass the translation boundary.
async def test_ics_invalid_text_and_ambiguous_envelope_fail_closed(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", ICS_POLICY)
    rig.gateway.attach_vendor(rig.vendor_send)
    payload = {"vendorId": "IoCharger", "messageId": "getTransaction", "data": "\ud800"}
    assert (await rig.vendor("bad-text", "DataTransfer", payload))[0] == 4
    assert not rig.gateway.failures
    for raw in (
        '[2,"duplicate","DataTransfer",{"vendorId":"IoCharger","vendorId":"Other"}]',
        "\ud800",
    ):
        with pytest.raises(DeniedError):
            decode(raw)
    query = TransactionQuery(6000001, 1001)
    for result in (
        {"status": "Rejected", "data": '{"transactionId":1001,"eco":"Fast"}'},
        {"data": '{"transactionId":1001,"eco":"Fast"}'},
    ):
        with pytest.raises(DeniedError):
            query.response(decode(build_call_result("refused", result)))
    await rig.close()


# Intent: never regenerate or send a pre-fix persisted oversized outbox id automatically.
# Why: a retained retry id may already have reached the remote server and requires explicit reconciliation.
async def test_ics_legacy_oversized_journal_id_requires_reconciliation(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", ICS_POLICY)
    await rig.boot()
    await rig.charger("start", "StartTransaction", STARTS[0])
    legacy = "event-" + "a" * 32
    rig.store.db.execute("UPDATE outbox SET token=?", (legacy,))
    with pytest.raises(RecoveryRequiredError):
        await rig.cloud()
    assert rig.store.next_delivery().token == legacy
    assert rig.store.next_delivery().state == "queued"
    assert not [frame for frame in rig.vendor_frames if frame[0] == 2 and frame[2] == "StartTransaction"]
    await rig.close()
