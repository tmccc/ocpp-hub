"""The retirement owner preserves exact SQL filters, preimages and historical bytes."""

import json
import sqlite3
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.managed_start_cancellation import unprepared_start
from ocpp_hub_core.prototype.start_settlement import RetirementRule as Rule
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_start_lifecycle import closed_owner as closed_owner


# Make the two zero rules simultaneously eligible while leaving the actual closed current receipt intact.
def unsent(j):
    j.starts.set_lease(None)
    j.put(
        "managed_test_charge",
        {
            **j.get("managed_test_charge"),
            "revision": 3,
            "start_rpc_accepted": False,
            "start_attempted": False,
            "local_transaction": None,
        },
    )
    j.db.execute("UPDATE managed_requests SET state='uncertain'")


# Intent: superficially similar zero rules preserve their distinct exact result bytes and metadata projection.
# Why: consolidation must not silently replace uncertain-only cleanup with generic terminal completion.
@pytest.mark.parametrize("rule", [Rule.UNISSUED, Rule.GENERIC])
def test_zero_rules_keep_original_encoding_and_local_projection(closed_owner, rule):
    j, owner, local = closed_owner
    unsent(j)
    # An unrelated historical receipt leaves both paths unbound, with explicit never-attempted provenance.
    j.put("local_start_receipt", {**j.get("local_start_receipt"), "request_id": "9" * 32})
    candidate = j.starts.retirement(rule)
    assert candidate is not None
    if rule is Rule.GENERIC:
        with j.atomic():
            j.starts.retire(candidate)
    else:
        j.starts.retire(candidate)
    row = j.db.execute("SELECT state,result FROM managed_requests WHERE id=?", (owner.identifier,)).fetchone()
    expected = {"classification": "start_not_accepted_at_verified_zero"}
    assert row[0] == "failed" and json.loads(row[1]) == expected
    assert (
        row[1] == json.dumps(expected, separators=(",", ":"))
        if rule is Rule.GENERIC
        else row[1] == json.dumps(expected)
    )
    state = j.get("managed_test_charge")
    assert state["state"] == "completed" and state["recovery_reason"] == expected["classification"]
    assert j.get("local_start_receipt")["local"] == local


# Intent: generic candidates pin the selected ledger and exact lease while ignoring unrelated global observations.
# Why: widening a candidate across awaited release recovery can strand otherwise settled operations.
@pytest.mark.parametrize("change,refused", [("pending", False), ("lease", True), ("ledger", True)])
def test_generic_preimage_has_its_original_scope(closed_owner, change, refused):
    j, owner, _ = closed_owner
    candidate = j.starts.retirement(Rule.GENERIC)
    assert candidate is not None
    if change == "pending":
        j.put("control", {**j.get("control"), "start_pending": True})
    elif change == "lease":
        j.starts.set_lease(replace(owner))
    else:
        j.db.execute("UPDATE transactions SET vendor=999")
    if refused:
        with pytest.raises(RecoveryRequiredError, match="changed"):
            j.starts.require(candidate)
    else:
        j.starts.require(candidate)


# Intent: cancellation reads its explicit lease and exactly the raw metadata/request/receipt scope.
# Why: a prior cloud mapping or pending-bit update must not become new cancellation provenance.
def test_cancellation_candidate_keeps_raw_scope_and_explicit_lease(closed_owner):
    j, owner, _ = closed_owner
    j.starts.set_lease(None)
    j.put(
        "managed_test_charge",
        {
            **j.get("managed_test_charge"),
            "state": "starting",
            "revision": 3,
            "start_rpc_accepted": False,
            "start_attempted": False,
            "local_transaction": None,
            "tag_digest": None,
        },
    )
    j.db.execute("UPDATE managed_requests SET state='accepted',result='opaque old bytes'")
    j.db.execute("DELETE FROM meta WHERE key='local_start_receipt'")
    candidate = unprepared_start(j, replace(owner, revision=500))
    assert candidate is not None
    j.db.execute("UPDATE transactions SET vendor=777")
    j.put("control", {**j.get("control"), "start_pending": True})
    assert unprepared_start(j, owner) == candidate
    j.put("local_start_receipt", {})
    assert unprepared_start(j, owner) is None


# Intent: unsupported raw shapes are refused before cancellation forces a compatibility receipt or SQL binding.
# Why: the shared loader must preserve the older cancellation boundary's safe shape rejection.
@pytest.mark.parametrize(
    "key,value", [("managed_test_charge", [1]), ("local_start_receipt", [1]), ("managed_test_charge", {"id": []})]
)
def test_cancelled_malformed_raw_shapes_refuse(closed_owner, key, value):
    j, owner, _ = closed_owner
    j.put(key, value)
    assert unprepared_start(j, owner) is None


# Intent: normal consumers and closed-zero selection retain the old string-identifier admission fence.
# Why: broader generic metadata loading must not broaden normal ownership.
@pytest.mark.parametrize("identifier", [123, [], {}])
def test_normal_identifier_shape_stays_strict(closed_owner, identifier):
    j, _, _ = closed_owner
    j.put("managed_test_charge", {**j.get("managed_test_charge"), "id": identifier})
    assert j.starts.normal() is None
    assert j.starts.retirement(Rule.CLOSED) is None


# Intent: unissued zero retirement retains its own atomic boundary and cannot join a caller's SQL transaction.
# Why: only the original caller-transaction rules may transfer COMMIT ownership.
def test_unissued_requires_its_own_transaction(closed_owner):
    j, _, _ = closed_owner
    unsent(j)
    candidate = j.starts.retirement(Rule.UNISSUED)
    assert candidate is not None
    with j.atomic():
        with pytest.raises(sqlite3.OperationalError, match="transaction"):
            j.starts.retire(candidate)


# Intent: the common writer retains each caller's historical state filter and exact encoding.
# Why: sharing SQL must not complete an uncertain live request or rewrite an original terminal result.
@pytest.mark.parametrize("state", ["accepted", "running", "uncertain", "failed", "completed"])
@pytest.mark.parametrize("caller", ["preflight", "expiry", "normal", "normal-uncertain", "operator"])
def test_outcome_writer_caller_contract(closed_owner, state, caller):
    j, owner, local = closed_owner
    original = ' {"original": "bytes kept"} '
    j.db.execute("UPDATE managed_requests SET state=?,result=?", (state, original))
    metadata = j.get("managed_test_charge")
    if caller == "preflight":
        j.starts.fail_preflight(owner.identifier, {"cause": "synthetic"})
        outcome, result = "failed", {"classification": "preflight_refused_unsent"}
    elif caller == "expiry":
        with j.atomic():
            j.starts.expire(owner, "deadline", True, None)
        outcome, result = "uncertain", {"classification": "deadline"}
    elif caller.startswith("normal"):
        j.starts.complete_normal(metadata, local, allow_uncertain=caller == "normal-uncertain", request_revision=3)
        outcome, result = "completed", {"local_session_verified": True, "charging": False, "timed": False}
    else:
        with j.atomic():
            j.starts.operator_no_pending(j.get("local_start_receipt"), metadata, "4" * 32, owner.identifier)
        outcome, result = "failed", {"classification": "operator_reconciled_at_zero", "charging_started": False}
    current = j.db.execute("SELECT state,result FROM managed_requests").fetchone()
    allowed = (
        state in ("accepted", "running")
        or caller == "normal-uncertain"
        and state == "uncertain"
        or caller == "operator"
    )
    expected = json.dumps(result, sort_keys=True, **({"separators": (",", ":")} if caller == "operator" else {}))
    assert tuple(current) == (outcome, expected) if allowed else tuple(current) == (state, original)


# Intent: optional action and revision fences stay effective after the normal completion writer is shared.
# Why: a stored result cannot be applied to a different action or management generation.
@pytest.mark.parametrize("column,value", [("action", "pause"), ("revision", 9)])
def test_normal_writer_keeps_action_revision_fences(closed_owner, column, value):
    j, _, local = closed_owner
    j.db.execute(f"UPDATE managed_requests SET {column}=?", (value,))
    before = tuple(j.db.execute("SELECT * FROM managed_requests").fetchone())
    j.starts.complete_normal(j.get("managed_test_charge"), local, allow_uncertain=True, request_revision=3)
    assert tuple(j.db.execute("SELECT * FROM managed_requests").fetchone()) == before


# Intent: a known-open normal session uses the same accepted/running-only request filter and truthful measured result.
# Why: open-session bookkeeping is separate from confirmed closure and uncertain-request recovery.
@pytest.mark.parametrize("state", ["accepted", "running", "uncertain", "failed", "completed"])
def test_open_observation_writer_contract(closed_owner, state):
    from types import SimpleNamespace

    j, owner, local = closed_owner
    j.db.execute("UPDATE transactions SET stop=NULL")
    j.db.execute("UPDATE managed_requests SET state=?,result='opaque original'", (state,))
    native = SimpleNamespace(physical=SimpleNamespace(transaction_open=True, native_transaction_id=local, power_w=20))
    assert j.starts.observe_normal(native, allow_open=True)
    row = j.db.execute("SELECT state,result FROM managed_requests").fetchone()
    expected = json.dumps({"local_session_verified": True, "charging": True, "timed": False}, sort_keys=True)
    assert (
        tuple(row) == ("completed", expected)
        if state in ("accepted", "running")
        else tuple(row) == (state, "opaque original")
    )
    assert j.get("managed_test_charge")["state"] == "running" and j.starts.lease is None
