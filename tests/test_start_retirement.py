"""Pure retirement rules keep evidence strength and caller-selected outcomes distinct."""

import json
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.start_settlement import (
    ChargeStart,
    LeaseIdentity,
    RetirementCandidate,
    RetirementDecision,
    StartReceipt,
    StartRequest,
    StartSession,
    idle_receipt_ended,
    previous_receipt,
    retire_start,
)
from ocpp_hub_core.prototype.start_settlement import (
    RetirementRule as Rule,
)
from ocpp_hub_core.prototype.store import fingerprint

ID, PRIOR = "1" * 32, "2" * 32
DIGEST = fingerprint("owner")
LEASE = LeaseIdentity(ID, "local", True)


# Name real stored facts independently of the policy's expected result.
def charge(**changes):
    receipt = StartReceipt("{}", ID, "3" * 32, DIGEST, "accepted", 1)
    row = StartSession(1, json.dumps({"idTag": "OWNER"}), '{"actual":"stop"}', None)
    base = ChargeStart(
        "{}",
        ID,
        4,
        "interrupted",
        True,
        True,
        DIGEST,
        1,
        receipt,
        StartRequest(ID, "start", 3, "uncertain", ' {"original": true} '),
        False,
        "normal",
        "local",
        None,
        "null",
        (row,),
    )
    return replace(base, **changes)


# Supply the three explicit current-version no-dispatch markers, without treating absent flags as false.
def unissued(**changes):
    return charge(revision=3, published=False, attempted=False, reported_local=None, **changes)


# Intent: caller-named rules preserve genuinely different outcomes for the same overlapping stored evidence.
# Why: a priority picker would turn proven no-dispatch retirement into bound-session completion or vice versa.
def test_named_rule_preserves_overlapping_unissued_and_bound_paths():
    record = unissued()
    assert retire_start(record, Rule.UNISSUED) == RetirementDecision(Rule.UNISSUED)
    assert retire_start(record, Rule.GENERIC) == RetirementDecision(Rule.GENERIC, 1)
    assert retire_start(record, Rule.CLOSED) == RetirementDecision(Rule.CLOSED, 1)


# Intent: explicit False and exact unsent receipt are independent supported proofs; missing markers remain unknown.
# Why: legacy absence must not manufacture proof that no Start was dispatched.
@pytest.mark.parametrize(
    "attempted,stage,own,local,expected",
    [
        (False, "issued", False, 1, True),
        (False, "accepted", True, 1, True),
        (None, "unsent", True, None, True),
        (True, "rejected", True, None, True),
        (None, "accepted", True, None, False),
        (None, "unsent", False, None, False),
        (True, "rejected", True, 1, False),
        (True, "prepared", True, None, False),
    ],
)
def test_unissued_provenance(attempted, stage, own, local, expected):
    record = charge(revision=3, published=False, attempted=attempted, reported_local=None)
    record = replace(record, receipt=replace(record.receipt, request_id=ID if own else PRIOR, stage=stage, local=local))
    assert (retire_start(record, Rule.UNISSUED) is not None) is expected


# Intent: each stored/request/runtime fence independently revokes unissued retirement.
# Why: valid no-dispatch evidence does not settle a different request or borrow another lease.
@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", "other"),
        ("source", "cloud"),
        ("published", None),
        ("published", True),
        ("reported_local", 1),
        ("revision", 99),
    ],
)
def test_unissued_record_fences(field, value):
    assert retire_start(replace(unissued(), **{field: value}), Rule.UNISSUED) is None
    assert retire_start(unissued(), Rule.UNISSUED, LEASE) is None


# Intent: refused preflight cleanup preserves its older weaker bookkeeping contract.
# Why: it must not acquire kind/source/revision/tag/local restrictions belonging to native-zero retirement.
def test_refusal_is_metadata_only_and_accepts_older_bound_receipt():
    record = charge(
        phase="failed",
        published=False,
        kind="legacy",
        source="cloud",
        revision=99,
        request=StartRequest(ID, "start_test", 8, "failed", '{"classification":"preflight_refused_unsent","extra":1}'),
    )
    assert retire_start(record, Rule.REFUSED) == RetirementDecision(Rule.REFUSED)
    assert retire_start(replace(record, pending=True), Rule.REFUSED) is None
    assert retire_start(record, Rule.REFUSED, uncertain_vendor=True) is None
    assert retire_start(record, Rule.REFUSED, LEASE) is None


# Intent: only refusal parsing interprets result JSON; generic/cancellation preserve opaque original bytes.
# Why: terminal receipt history is a preimage, not a new schema to normalize during retirement.
@pytest.mark.parametrize("body", [' ["historical"] ', "not a new result schema", ' {"kept":true} '])
def test_generic_result_stays_opaque(body):
    record = charge(request=StartRequest(ID, "start", 3, "failed", body))
    assert retire_start(record, Rule.GENERIC) == RetirementDecision(Rule.GENERIC, 1)
    assert record.request.result == body


# Intent: generic legacy binding remains weaker than current normal closed-zero ownership.
# Why: replacing both with the strict normal identity would strand supported old timed/normal metadata.
@pytest.mark.parametrize("stage", ["issued", "accepted"])
def test_generic_legacy_binding_is_not_normal_ownership(stage):
    record = charge(revision=3, published=False)
    record = replace(record, receipt=replace(record.receipt, request_id=None, stage=stage))
    assert retire_start(record, Rule.GENERIC) == RetirementDecision(Rule.GENERIC, 1)
    assert retire_start(record, Rule.CLOSED) is None


# Intent: normal closure may settle accepted or issued publication lag, including absent attempted flags for Accepted.
# Why: a failed app handoff cannot erase the genuine later local lifecycle.
@pytest.mark.parametrize(
    "stage,attempted,published",
    [
        ("issued", True, False),
        ("accepted", None, None),
        ("accepted", False, False),
    ],
)
def test_closed_publication_lag(stage, attempted, published):
    record = charge(revision=3, reported_local=None, attempted=attempted, published=published)
    record = replace(record, receipt=replace(record.receipt, stage=stage))
    assert retire_start(record, Rule.CLOSED) == RetirementDecision(Rule.CLOSED, 1)
    assert retire_start(replace(record, pending=True), Rule.CLOSED) is None


# Intent: an actual closed/tag-matched session remains required for bound local settlement.
# Why: neither an Accepted reply nor a stale metadata local is a physical closure oracle.
@pytest.mark.parametrize(
    "fault", ["open", "missing", "tag", "local", "request", "source", "worker", "active", "vendor", "mutation"]
)
def test_generic_bound_fences(fault):
    record, flags = charge(), {}
    if fault == "open":
        record = replace(record, sessions=(replace(record.sessions[0], stop=None),))
    elif fault == "missing":
        record = replace(record, sessions=())
    elif fault == "tag":
        record = replace(record, projection_digest=fingerprint("other"))
    elif fault == "local":
        record = replace(record, reported_local=2)
    elif fault == "request":
        record = replace(record, request=replace(record.request, action="pause"))
    elif fault == "source":
        record = replace(record, source="other")
    else:
        flags = {
            "worker_pending"
            if fault == "worker"
            else "active"
            if fault == "active"
            else "vendor_pending"
            if fault == "vendor"
            else "mutation_pending": 1 if fault == "active" else True
        }
    assert retire_start(record, Rule.GENERIC, **flags) is None


# Intent: cloud-test settlement preserves its independent source proof and arm-only result semantics.
# Why: local receipt/tag ownership cannot substitute for genuine cloud attribution.
@pytest.mark.parametrize("bound", [False, True])
def test_cloud_test_source_rules(bound):
    record = charge(
        kind="test",
        source="cloud",
        revision=3,
        published=None,
        reported_local=1 if bound else None,
        source_verified=True if bound else None,
        request=StartRequest(ID, "arm_cloud_test", 3, "completed", "opaque"),
    )
    assert retire_start(record, Rule.GENERIC) == RetirementDecision(Rule.GENERIC, 1 if bound else None)
    if bound:
        assert retire_start(replace(record, source_verified=False), Rule.GENERIC) is None
    else:
        assert retire_start(record, Rule.GENERIC, LeaseIdentity(ID, "cloud", False)) is None
        assert retire_start(replace(record, phase="armed"), Rule.GENERIC) is None


# Intent: raw missing, empty, and historical receipt shapes keep cancellation's original distinctions.
# Why: an older receipt is preserved, but an unresolved or malformed command cannot become a new unsent Start.
@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, True),
        ({}, False),
        ([], False),
        ({"request_id": PRIOR, "ticket": "3" * 32, "digest": DIGEST, "stage": "unsent", "local": None}, True),
        ({"request_id": PRIOR, "ticket": "3" * 32, "digest": DIGEST, "stage": "issued", "local": None}, False),
        ({"request_id": PRIOR, "ticket": "3" * 32, "digest": DIGEST, "stage": "accepted", "local": 1}, True),
        ({"request_id": ID, "ticket": "3" * 32, "digest": DIGEST, "stage": "rejected", "local": None}, False),
    ],
)
def test_cancelled_prior_receipt(raw, expected):
    record = charge(
        revision=3,
        phase="starting",
        attempted=False,
        published=False,
        projection_digest=None,
        reported_local=None,
        raw_receipt=json.dumps(raw),
        request=StartRequest(ID, "start", 3, "accepted", "opaque"),
    )
    assert previous_receipt(record) is expected
    assert (retire_start(record, Rule.CANCELLED, LEASE) is not None) is expected


# Intent: cancellation refuses every changed current-request marker and unrelated lease.
# Why: no old cancelled waiter may retire a newly prepared or differently owned request.
@pytest.mark.parametrize(
    "field,value",
    [
        ("phase", "awaiting_power"),
        ("attempted", None),
        ("attempted", True),
        ("published", None),
        ("projection_digest", DIGEST),
        ("reported_local", 1),
        ("revision", True),
        ("kind", "test"),
    ],
)
def test_cancelled_request_fences(field, value):
    record = charge(
        revision=3,
        phase="starting",
        attempted=False,
        published=False,
        projection_digest=None,
        reported_local=None,
        request=StartRequest(ID, "start", 3, "accepted", "opaque"),
    )
    assert retire_start(record, Rule.CANCELLED, LEASE) is not None
    assert retire_start(replace(record, **{field: value}), Rule.CANCELLED, LEASE) is None
    assert retire_start(record, Rule.CANCELLED, LeaseIdentity(PRIOR, "local", True)) is None


# Intent: expiry's old slot-level fact stays distinct from any current-request retirement permission.
# Why: a historical ended receipt may retire metadata while the management request and cloud debt stay outstanding.
def test_historical_receipt_end_is_only_an_expiry_fact():
    record = charge(
        pending=True,
        source="cloud",
        kind="legacy",
        request=None,
        receipt=StartReceipt("{}", PRIOR, "3" * 32, "other", "accepted", 1),
    )
    assert idle_receipt_ended(record)
    assert retire_start(record, Rule.CLOSED) is None
    assert retire_start(record, Rule.GENERIC) is None
    assert not idle_receipt_ended(replace(record, sessions=()))


# Intent: the pure decision owns each durable outcome before the executor chooses filters and byte encoding.
# Why: a shared eligibility check must not leave a second competing classification in the SQL owner.
@pytest.mark.parametrize(
    "rule,kind,source,local,state,result,reason",
    [
        (
            Rule.UNISSUED,
            "normal",
            "local",
            None,
            "failed",
            {"classification": "start_not_accepted_at_verified_zero"},
            "start_not_accepted_at_verified_zero",
        ),
        (
            Rule.GENERIC,
            "normal",
            "local",
            None,
            "failed",
            {"classification": "start_not_accepted_at_verified_zero"},
            "start_not_accepted_at_verified_zero",
        ),
        (
            Rule.GENERIC,
            "test",
            "local",
            1,
            "failed",
            {"classification": "test_interrupted_at_verified_zero"},
            "owned_operation_settled_at_verified_zero",
        ),
        (
            Rule.GENERIC,
            "test",
            "cloud",
            None,
            "completed",
            {"armed": True, "charging_started": False},
            "owned_operation_settled_at_verified_zero",
        ),
        (
            Rule.CLOSED,
            "normal",
            "local",
            1,
            "completed",
            {"local_session_verified": True, "charging": False, "timed": False},
            "owned_session_closed_at_verified_zero",
        ),
        (
            Rule.GENERIC,
            "normal",
            "local",
            1,
            "completed",
            {"local_session_verified": True, "charging": False, "timed": False},
            "owned_session_closed_at_verified_zero",
        ),
        (
            Rule.CANCELLED,
            "normal",
            "local",
            None,
            "failed",
            {"classification": "cancelled_before_start_dispatch"},
            "cancelled_before_start_dispatch",
        ),
        (Rule.REFUSED, "normal", "local", None, None, {}, "preflight_refused_unsent"),
    ],
)
def test_retirement_outcome_contract(rule, kind, source, local, state, result, reason):
    record = charge(kind=kind, source=source)
    update = RetirementDecision(rule, local).update(record)
    assert (update.state, update.result, update.reason) == (state, result, reason)


# Intent: every rule freezes its own original persisted scope without freezing unrelated history or permission.
# Why: a universal wider snapshot can strand recovery, while a narrower one could settle a replaced operation.
@pytest.mark.parametrize("rule", [Rule.CLOSED, Rule.GENERIC, Rule.CANCELLED])
@pytest.mark.parametrize("changed", ["body", "request", "view", "raw", "pending", "selected", "unrelated", "lease"])
def test_candidate_snapshot_scope(rule, changed):
    record, lease = charge(), object()
    decision = RetirementDecision(rule, 1 if rule in (Rule.CLOSED, Rule.GENERIC) else None)
    candidate = RetirementCandidate.capture(record, decision, lease)
    current = record
    if changed == "body":
        current = replace(record, body='{"changed":"metadata"}')
    elif changed == "request":
        current = replace(record, request=replace(record.request, result=' {"different":"bytes"} '))
    elif changed == "view":
        current = replace(record, receipt=replace(record.receipt, body='{"changed":"view"}'))
    elif changed == "raw":
        current = replace(record, raw_receipt="{}")
    elif changed == "pending":
        current = replace(record, pending=True)
    elif changed == "selected":
        current = replace(record, sessions=(replace(record.sessions[0], vendor=9000),))
    elif changed == "unrelated":
        current = replace(record, sessions=(*record.sessions, StartSession(2, "opaque history", "closed", 999)))
    else:
        lease = object()
    same = RetirementCandidate.capture(current, decision, lease) == candidate
    expected_change = changed in ("body", "request") or (
        changed == "raw"
        and rule is Rule.CANCELLED
        or changed == "view"
        and rule is not Rule.CANCELLED
        or changed == "pending"
        and rule is Rule.CLOSED
        or changed == "selected"
        and rule is not Rule.CANCELLED
        or changed == "lease"
        and rule is Rule.GENERIC
    )
    assert same is not expected_change
    assert candidate.key == "managed_test_charge" and candidate.identifier == ID


# Intent: the selected rule retains its exact request-state whitelist and explicit-local projection contract.
# Why: generic zero and uncertain-only cleanup must not silently share a broader update policy.
@pytest.mark.parametrize(
    "rule,states,writes_local",
    [
        (Rule.REFUSED, ("accepted", "running", "uncertain"), False),
        (Rule.UNISSUED, ("uncertain",), False),
        (Rule.CLOSED, ("accepted", "running", "uncertain"), True),
        (Rule.GENERIC, ("accepted", "running", "uncertain"), True),
        (Rule.CANCELLED, ("accepted", "running"), False),
    ],
)
def test_decision_update_scope(rule, states, writes_local):
    decision = RetirementDecision(rule)
    assert decision.states == states and decision.writes_local is writes_local


# Intent: malformed or still-open historical receipts cannot justify cancellation of a different admitted Start.
# Why: valid-looking tags alone do not prove that the previous command is settled.
@pytest.mark.parametrize("fault", ["id", "ticket", "digest", "local", "open", "missing"])
def test_prior_receipt_negative_boundaries(fault):
    raw = {"request_id": PRIOR, "ticket": "3" * 32, "digest": DIGEST, "stage": "accepted", "local": 1}
    record = charge()
    if fault in ("id", "ticket", "digest", "local"):
        raw[{"id": "request_id", "ticket": "ticket", "digest": "digest", "local": "local"}[fault]] = []
    elif fault == "open":
        record = replace(record, sessions=(replace(record.sessions[0], stop=None),))
    else:
        record = replace(record, sessions=())
    assert not previous_receipt(replace(record, raw_receipt=json.dumps(raw)))


# Intent: generic and closed-zero rules retain their different live-lease relationships.
# Why: generic settlement checks id/source, while closed normal retirement also requires a normal lease.
@pytest.mark.parametrize("rule", [Rule.GENERIC, Rule.CLOSED])
@pytest.mark.parametrize(
    "identity,expected_generic,expected_closed",
    [
        (LEASE, True, True),
        (LeaseIdentity(PRIOR, "local", True), False, False),
        (LeaseIdentity(ID, "cloud", True), False, False),
        (LeaseIdentity(ID, "local", False), True, False),
    ],
)
def test_rule_specific_lease_fences(rule, identity, expected_generic, expected_closed):
    expected = expected_generic if rule is Rule.GENERIC else expected_closed
    assert (retire_start(charge(), rule, identity) is not None) is expected


# Intent: truly unbound generic retirement requires explicit unpublished and independent no-dispatch provenance.
# Why: the bound-receipt branch must not mask missing witnesses for the generic no-session path.
@pytest.mark.parametrize(
    "published,attempted,stage,own,expected",
    [
        (False, False, "prepared", True, True),
        (True, False, "prepared", True, False),
        (None, False, "prepared", True, False),
        (False, None, "prepared", True, False),
        (False, True, "rejected", True, True),
        (False, None, "unsent", True, True),
        (False, None, "unsent", False, False),
    ],
)
def test_generic_truly_unbound_provenance(published, attempted, stage, own, expected):
    record = charge(revision=3 + int(published is True), published=published, attempted=attempted, reported_local=None)
    record = replace(record, receipt=replace(record.receipt, local=None, stage=stage, request_id=ID if own else PRIOR))
    assert (retire_start(record, Rule.GENERIC) is not None) is expected


# Intent: matching metadata/receipt copies cannot replace the genuine ledger's Start tag.
# Why: agreeing duplicated projections can still identify a different physical session.
@pytest.mark.parametrize("rule", [Rule.GENERIC, Rule.CLOSED])
def test_ledger_tag_is_independent_of_matching_projections(rule):
    record = charge(projection_digest=fingerprint("wrong-tag"))
    record = replace(record, receipt=replace(record.receipt, digest=record.projection_digest))
    assert retire_start(record, rule) is None


# Intent: preflight refusal keeps its exact unbound-stage fence without imposing bound closure ownership.
# Why: a pending command cannot become proven refusal merely because its management row already failed.
@pytest.mark.parametrize(
    "stage,expected",
    [
        ("prepared", False),
        ("issued", False),
        ("accepted", False),
        ("unsent", True),
        ("rejected", True),
        ("reconciled", True),
    ],
)
def test_refused_unbound_receipt_fence(stage, expected):
    record = charge(
        phase="failed",
        published=False,
        request=StartRequest(ID, "start", 3, "failed", '{"classification":"preflight_refused_unsent"}'),
    )
    record = replace(record, receipt=replace(record.receipt, local=None, stage=stage))
    assert (retire_start(record, Rule.REFUSED) is not None) is expected


# Intent: shared normal correlation rejects unissued preparation, impossible issued publication and open lifecycles.
# Why: a weaker helper cannot be hidden by the generic path's independent downstream fences.
@pytest.mark.parametrize("fault", ["prepared", "issued-published", "open"])
def test_closed_shared_correlation_fences(fault):
    record = charge()
    if fault == "prepared":
        record = replace(record, receipt=replace(record.receipt, stage="prepared"))
    elif fault == "issued-published":
        record = replace(record, receipt=replace(record.receipt, stage="issued"))
    else:
        record = replace(record, sessions=(replace(record.sessions[0], stop=None),))
    assert retire_start(record, Rule.CLOSED) is None
