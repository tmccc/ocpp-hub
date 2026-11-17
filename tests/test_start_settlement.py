"""Independent failure-settlement rows and exact boundary folds exercise production decisions without sockets."""

from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.connection_scope import CloudAttemptScope
from ocpp_hub_core.prototype.control_slice_state import StartOutcome
from ocpp_hub_core.prototype.managed_intent import ControlIntent
from ocpp_hub_core.prototype.recovery_contracts import AuthorityStamp, FailureKind, WireReply
from ocpp_hub_core.prototype.start_handoff import (
    OwnedStartResult,
    StartBoundary,
    StartBoundaryObservation,
    StartHandoffContext,
    StartPublication,
)
from ocpp_hub_core.prototype.start_settlement import (
    StartFailureAction,
    StartFailureDecision,
    StartFailureFacts,
    StartReceiptFacts,
    StartSettlementTrace,
    settle_start,
)


# Name input facts without deriving expected actions from the production decision.
def facts(**changes):
    base = StartFailureFacts("request-A", "start", "request-A", "local", "starting", "start", "accepted",
                             True, False, "a" * 64)
    return replace(base, **changes)


# An exact prepared and unbound receipt may retire even after its original lease was replaced.
def receipt(**changes):
    return replace(StartReceiptFacts("request-A", "a" * 64, "prepared", None), **changes)


# Intent: source/action/request provenance controls whether settlement belongs to this worker at all.
# Why: an unrelated or absent receipt must get neither a diagnostic nor a borrowed restrictive action.
@pytest.mark.parametrize("changes", [
    {"record_id": "other"}, {"record_id": None}, {"record_source": "cloud"},
    {"request_action": None}, {"request_action": "start_test"},
])
def test_unrelated_work_has_no_settlement_or_retirement(changes):
    assert settle_start(facts(**changes), receipt()) == StartFailureDecision(StartFailureAction.UNRELATED)


# Intent: an already replaced or terminal operation records diagnostics without altering the winner.
# Why: exact prepared evidence may still need retirement before this diagnostic-only exit.
@pytest.mark.parametrize("changes", [
    {"lease_current": False}, {"record_state": "stopping"}, {"record_state": "completed"},
    {"record_state": "failed"}, {"record_state": "interrupted"}, {"record_state": "expired"},
    {"record_state": None}, {"request_state": "completed"}, {"request_state": "failed"},
])
@pytest.mark.parametrize("effects", [False, True])
def test_newer_or_terminal_owner_gets_diagnostic_only(changes, effects):
    decision = settle_start(facts(effects_started=effects, **changes), receipt())
    assert decision == StartFailureDecision(StartFailureAction.DIAGNOSTIC_ONLY, True)


# Intent: all live phases retain conservative initial-zero effects for normal and timed Start.
# Why: unsent RemoteStart is insufficient to claim the whole worker had no effects.
@pytest.mark.parametrize("phase", ["armed", "starting", "awaiting_power", "charging", "running"])
@pytest.mark.parametrize("action", ["start", "start_test"])
@pytest.mark.parametrize("effects,expected", [
    (False, StartFailureAction.FAIL_PREFLIGHT), (True, StartFailureAction.STOP_REQUIRED),
])
def test_current_failure_separates_preflight_from_initial_or_positive_effects(phase, action, effects, expected):
    inputs = facts(record_state=phase, request_action=action, expected_action=action, effects_started=effects)
    assert settle_start(inputs, receipt()) == StartFailureDecision(expected, True)


# Intent: every provenance component independently fences prepared-receipt retirement.
# Why: known current request identity cannot retire an older tag, bound session, or issued command.
@pytest.mark.parametrize("other", [None, receipt(request_id="other"), receipt(tag_digest="b" * 64),
                                      receipt(stage="issued"), receipt(stage="accepted"), receipt(stage="unsent"),
                                      receipt(stage="rejected"), receipt(local=41)])
def test_retirement_requires_the_exact_unbound_prepared_receipt(other):
    assert settle_start(facts(), other) == StartFailureDecision(StartFailureAction.FAIL_PREFLIGHT)


# Build exact synthetic child context; it is never installed in the engine or used as outgoing permission.
def trace_prepared():
    trace = StartSettlementTrace(ControlIntent("paused", 7))
    context = StartHandoffContext("request-A", "ticket-A", CloudAttemptScope(2, "boot-A", 3), 11, "a" * 64)
    trace.observe(StartBoundaryObservation(StartBoundary.PREPARED, context=context))
    return trace, context


# Intent: physical Accepted, its evidence COMMIT, and final publication COMMIT remain separate observations.
# Why: refusal at the evidence COMMIT cannot be silently upgraded from an in-memory reply.
@pytest.mark.parametrize("boundary", ["success", "evidence_commit", "publication_commit"])
def test_exact_start_boundary_observations_preserve_commit_distinctions(boundary):
    trace, context = trace_prepared()
    trace.observe(StartBoundaryObservation(StartBoundary.DISPATCHED))
    trace.observe(StartBoundaryObservation(StartBoundary.REPLIED, reply=WireReply.ACCEPTED))
    if boundary != "evidence_commit":
        trace.observe(StartBoundaryObservation(StartBoundary.EVIDENCE_COMMITTED, reply=WireReply.ACCEPTED))
    publication = StartPublication("request-A", ControlIntent("app", 8), 2) if boundary == "success" else None
    if publication is not None:
        trace.observe(StartBoundaryObservation(StartBoundary.PUBLICATION_COMMITTED, publication=publication))
    result = OwnedStartResult(True, [3, "wire-A", {"status": "Accepted"}], publication, boundary != "success", None)
    trace.observe(StartBoundaryObservation(StartBoundary.RETIRED, result=result,
                                           failure_kind=FailureKind.STORAGE if boundary != "success" else None))
    assert not trace.errors and trace.state is not None
    record = trace.state.start(context.ticket)
    assert record.outcome is StartOutcome.ACCEPTED and record.retired
    assert record.evidence_committed == (boundary != "evidence_commit")
    assert (record.publication is not None) == (boundary == "success")


# Intent: child retirement retains unsent versus possibly dispatched uncertainty without a new async owner.
# Why: finishing a cancelled waiter cannot manufacture a complete unsent result for a still-owned wire child.
@pytest.mark.parametrize("dispatched,expected", [(False, StartOutcome.NOT_SENT), (True, StartOutcome.UNKNOWN)])
def test_owned_retirement_keeps_actual_dispatch_uncertainty(dispatched, expected):
    trace, context = trace_prepared()
    if dispatched:
        trace.observe(StartBoundaryObservation(StartBoundary.DISPATCHED))
    trace.observe(StartBoundaryObservation(StartBoundary.RETIRED,
                                           result=OwnedStartResult(dispatched, None, None, False, None)))
    assert not trace.errors and trace.state.start(context.ticket).outcome is expected


# Intent: settlement observes current ownership independently of the exact earlier child boundaries.
# Why: a new Pause revokes publication without erasing the old actual Accepted evidence.
def test_current_owner_observation_retains_late_accepted_truth():
    trace, context = trace_prepared()
    trace.observe(StartBoundaryObservation(StartBoundary.DISPATCHED))
    trace.observe(StartBoundaryObservation(StartBoundary.REPLIED, reply=WireReply.ACCEPTED))
    trace.observe(StartBoundaryObservation(StartBoundary.EVIDENCE_COMMITTED, reply=WireReply.ACCEPTED))
    newer = AuthorityStamp(ControlIntent("paused", 8), context.scope, 11, "pause-B")
    trace.owner_observed(newer)
    assert not trace.errors and trace.state.authority == newer
    assert trace.state.start(context.ticket).outcome is StartOutcome.ACCEPTED
    assert trace.state.start(context.ticket).publication is None
