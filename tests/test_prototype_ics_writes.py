"""Captured iCS control schemas with different namespaces and real durable engine state."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.ics_writes import validate_write
from ocpp_hub_core.prototype.policy import DeniedError, Policy

from tests.test_prototype import START, Rig

CAPTURE = json.loads((Path(__file__).parent / "fixtures/ics-app-controls.sanitized.json").read_text())
CALLS = [row["frame"] for row in CAPTURE["frames"] if row["frame"][0] == 2]
WRITES = [
    f[3] for f in CALLS if f[2] == "DataTransfer" and f[3]["messageId"] in ("setPlan", "clearPlans", "setTransaction")
]
PLAN = next(p for p in WRITES if p["messageId"] == "setPlan")
PLAN_DATA = json.loads(PLAN["data"])
TAG = PLAN_DATA["idTag"]
POLICY = Policy(
    max_current_a=32,
    allowed_tags=frozenset({"LOCAL_TEST", TAG}),
    extensions={("IoCharger", name): "write" for name in ("setPlan", "clearPlans", "setTransaction")},
)


# Build one modified request without changing the actual fixture.
def plan(**changes: Any) -> dict[str, Any]:
    return {**PLAN, "data": json.dumps({**PLAN_DATA, **changes})}


# Introduce an open mapped synthetic transaction for captured mode commands.
async def ready(path: Path) -> Rig:
    rig = Rig(path, POLICY)
    rig.next_vendor = 6000001
    rig.charger_result = {"status": "Accepted", "data": ""}
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    await rig.cloud()
    await rig.gateway.controls.ensure_ceiling()
    return rig


# Intent: retain every new control frame and exercise all actual writes with explicit, verified initial ownership.
# Why: the capture begins by clearing a pre-existing plan; cold-journal adoption must never be guessed.
async def test_ics_actual_writes_translate_and_commit(tmp_path: Path) -> None:
    assert CAPTURE["frame_count"] == len(CAPTURE["frames"]) == 551
    assert len(WRITES) == 12
    for row in CAPTURE["frames"]:
        assert decode(json.dumps(row["frame"])) == row["frame"]
    rig = await ready(tmp_path / "journal.db")
    rig.gateway.plans.adopt(8000001, 2000001)
    expected_ids = {8000001: 2000001, 8000002: 2000002}
    for index, payload in enumerate(WRITES):
        response = await rig.vendor(f"capture-{index}", "DataTransfer", payload)
        assert response[2] == {"status": "Accepted", "data": ""}
        translated = json.loads(rig.charger_frames[-1][3]["data"])
        original = json.loads(payload["data"])
        if payload["messageId"] == "setTransaction":
            assert translated == {**original, "transactionId": 1001}
        elif payload["messageId"] == "clearPlans":
            assert translated == [expected_ids[value] for value in original]
        else:
            assert translated == {**original, "planId": expected_ids[original["planId"]]}
            assert translated["validTime"] == original["validTime"]
    assert rig.store.db.execute("SELECT state FROM plans WHERE vendor=8000001").fetchone()[0] == "active"
    assert rig.store.db.execute("SELECT state FROM plans WHERE vendor=8000002").fetchone()[0] == "cleared"
    assert not rig.gateway.failures
    await rig.close()


# Intent: known mutation names cannot be misclassified as reads by an external mutable policy dictionary.
# Why: otherwise local ownership and a persisted stop could be bypassed after policy construction.
async def test_ics_intrinsic_write_classification_and_owner(tmp_path: Path) -> None:
    rules = {("IoCharger", "setPlan"): "write"}
    policy = Policy(max_current_a=32, allowed_tags=frozenset({TAG}), extensions=rules)
    rig = Rig(tmp_path / "journal.db", policy)
    rig.gateway.attach_vendor(rig.vendor_send)
    rules[("IoCharger", "setPlan")] = "read"
    assert (await rig.vendor("misclassified", "DataTransfer", PLAN))[0] == 4
    rules[("IoCharger", "setPlan")] = "write"
    rig.store.put("control", {"owner": "local", "inhibit": True})
    assert (await rig.vendor("local-owner", "DataTransfer", PLAN))[0] == 4
    assert not rig.charger_frames
    assert rig.store.db.execute("SELECT COUNT(*) FROM plans").fetchone()[0] == 0
    assert (await rig.charger("reverse", "DataTransfer", PLAN))[0] == 4
    await rig.close()


# Intent: reject cold, mixed, locally-owned and unconfirmed plan clears before any charger request.
# Why: a cloud plan id is never permission to clear a local resource with a numerically matching id.
async def test_ics_plan_ownership_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = await ready(path)
    clear = {"vendorId": "IoCharger", "messageId": "clearPlans", "data": "[8000001]"}
    assert (await rig.vendor("cold-clear", "DataTransfer", clear))[0] == 4
    rig.gateway.plans.protect_local(8000001)
    assert (await rig.vendor("local-id", "DataTransfer", clear))[0] == 4
    rig.charger_result = {"status": "Rejected", "data": ""}
    assert (await rig.vendor("refused", "DataTransfer", PLAN))[2]["status"] == "Rejected"
    assert (await rig.vendor("reserved-clear", "DataTransfer", clear))[0] == 4
    rig.charger_result = {"status": "Accepted", "data": ""}
    assert (await rig.vendor("install", "DataTransfer", PLAN))[0] == 3
    mapped = json.loads(rig.charger_frames[-1][3]["data"])["planId"]
    assert mapped != 8000001
    await rig.close()
    rig = Rig(path, POLICY)
    rig.gateway.attach_vendor(rig.vendor_send)
    rig.charger_result = {"status": "Accepted", "data": ""}
    assert (await rig.vendor("after-restart", "DataTransfer", clear))[0] == 3
    assert json.loads(rig.charger_frames[-1][3]["data"]) == [mapped]
    count = len(rig.charger_frames)
    mixed = {**clear, "data": "[8000001,999]"}
    assert (await rig.vendor("mixed", "DataTransfer", mixed))[0] == 4
    assert len(rig.charger_frames) == count
    assert (await rig.vendor("repeat-tombstone", "DataTransfer", clear))[0] == 3
    await rig.close()


# Intent: uncertainty blocks changed request bodies and different writes until explicit reconciliation.
# Why: hash-only dedupe cannot determine whether a timed-out prior command changed the device.
async def test_ics_write_uncertainty_blocks_changed_bodies_across_restart(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = await ready(path)
    rig.drop_charger = True
    assert (await rig.vendor("lost", "DataTransfer", PLAN))[0] == 4
    await rig.close()
    rig = Rig(path, POLICY)
    rig.gateway.attach_vendor(rig.vendor_send)
    rig.charger_result = {"status": "Accepted", "data": ""}
    assert (await rig.vendor("changed", "DataTransfer", plan(limitCurrent=6)))[0] == 4
    assert not rig.charger_frames
    token = rig.store.db.execute("SELECT token FROM mutations").fetchone()[0]
    rig.store.reconcile_mutation(token)
    assert (await rig.vendor("reconciled", "DataTransfer", plan(limitCurrent=6)))[0] == 3
    await rig.close()


# Intent: ambiguous success bodies retain uncertainty while actual peer errors/refusals keep their exact content.
# Why: an invented or untranslatable success must not publish a plan effect or be replayed automatically.
async def test_ics_write_peer_results_and_errors(tmp_path: Path) -> None:
    rig = await ready(tmp_path / "journal.db")
    rig.charger_result = {"status": "Accepted", "data": "unobserved"}
    assert (await rig.vendor("malformed-success", "DataTransfer", PLAN))[0] == 4
    assert rig.store.db.execute("SELECT state FROM plans").fetchone()[0] == "reserved"
    token = rig.store.db.execute("SELECT token FROM mutations").fetchone()[0]
    rig.store.reconcile_mutation(token)

    # Return an actual peer error rather than using the rig's normal synthetic result.
    async def peer_error(raw: str) -> None:
        f = decode(raw)
        rig.charger_frames.append(f)
        if f[2] == "DataTransfer":
            await rig.gateway.receive("charger", build_call_error(f[1], "InternalError", "actual failure", {"peer": 1}))
        else:
            result = rig.firmware.answer(f[2], f[3])
            from ocpp_hub_core.frames import build_call_result

            await rig.gateway.receive("charger", build_call_result(f[1], result))

    rig.gateway.attach_charger(peer_error)
    answer = await rig.vendor("peer-error", "DataTransfer", PLAN)
    assert answer[2:] == ["InternalError", "actual failure", {"peer": 1}]
    assert rig.store.db.execute("SELECT COUNT(*) FROM mutations WHERE state='uncertain'").fetchone()[0] == 1
    await rig.close()


# Intent: write mapping must not revive a completed session even though historical reads can still find it.
# Why: a delayed cloud mode change must not change whichever session happens to be on the connector now.
async def test_ics_mode_writes_are_active_only(tmp_path: Path) -> None:
    rig = await ready(tmp_path / "journal.db")
    await rig.charger(
        "stop", "StopTransaction", {"transactionId": 1001, "meterStop": 1000, "timestamp": START["timestamp"]}
    )
    payload = next(p for p in WRITES if p["messageId"] == "setTransaction")
    assert (await rig.vendor("historical-mode", "DataTransfer", payload))[0] == 4
    assert rig.store.historical_local_id(6000002) == 1001
    await rig.close()


# Intent: protect every observed plan boundary, including activation time precision and non-finite coercions.
# Why: these nested proprietary writes bypass ordinary SetChargingProfile validation without a real codec.
@pytest.mark.parametrize(
    "changes",
    [
        {"limitCurrent": 33},
        {"limitCurrent": True},
        {"limitCurrent": "6"},
        {"limitCurrent": 5},
        {"stopValue": 0},
        {"stopValue": 0.1},
        {"stopValue": 1441},
        {"stopValue": True},
        {"startTime": -1},
        {"startTime": 1440},
        {"startTime": True},
        {"connectorId": 2},
        {"recurring": 0},
        {"recurring": 128},
        {"recurring": True},
        {"idTag": "UNKNOWN"},
        {"kindType": "Unknown"},
        {"stopModel": "Current"},
        {"eco": "Eco"},
        {"type": "Unknown"},
        {"validTime": "2030-01-01T00:00:00"},
        {"validTime": "bad"},
        {"validTime": []},
        {"extra": 1},
        {"planId": False},
        {"planId": -1},
    ],
)
def test_ics_write_validation_boundaries(changes: dict[str, Any]) -> None:
    with pytest.raises(DeniedError):
        validate_write(plan(**changes), POLICY)


# Intent: reject malformed JSON-string boundaries and unknown mode/clear shapes without handler crashes.
# Why: authenticated peers can still send duplicate keys, surrogates, nested values or conflicting identifiers.
@pytest.mark.parametrize("data", ["[]", "{}", "[true]", "[1,1]", "[0]", "[1,999999999999]", "[NaN]", "\ud800", [1]])
def test_ics_clear_invalid_data(data: Any) -> None:
    with pytest.raises(DeniedError):
        validate_write({"vendorId": "IoCharger", "messageId": "clearPlans", "data": data}, POLICY)
