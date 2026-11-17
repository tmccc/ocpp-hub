"""Contract rows exercise the candidate pure core, without claiming any physical or SQL evidence."""

from dataclasses import FrozenInstanceError, replace

import pytest
from ocpp_hub_core.prototype.connection_scope import CloudAttemptScope
from ocpp_hub_core.prototype.managed_intent import ControlIntent
from ocpp_hub_core.prototype.recovery_contracts import (
    AuthorityStamp,
    FailureKind,
    OwnedReleaseResult,
    ReleaseEffects,
    WireReply,
)
from ocpp_hub_core.prototype.recovery_policy import ReleaseDisposition

from tests_model.control_slice import step
from tests_model.control_slice_state import (
    AuthorityChanged,
    CloseStarted,
    CloudSettled,
    ConsiderPublication,
    LifecycleBound,
    LifecycleClosed,
    OutcomeCommitted,
    PortRetired,
    PublicationCommitted,
    PublicationFailed,
    RecordOutcome,
    RestrictiveRecovery,
    ReturnCompleted,
    SliceState,
    StartAnswered,
    StartDebt,
    StartDispatched,
    StartIdentity,
    StartOutcome,
    StartPrepared,
)


# Use synthetic independent identities; these records are never installed in an engine journal.
def authority():
    return AuthorityStamp(ControlIntent("paused", 7), CloudAttemptScope(2, "boot-A", 3), 11, "ticket-A")


# The grammar supplies facts only; each test states its own expected consequences.
def prepared():
    state = SliceState(authority())
    identity = StartIdentity("wire-A", "request-A", "a" * 64, state.authority)
    return step(state, StartPrepared(identity)).state


# Observe a real-send boundary abstractly, without granting the experiment any outgoing port.
def dispatched():
    return step(prepared(), StartDispatched("wire-A")).state


# Supply actual Accepted separately from its durable recording boundary.
def accepted(*, committed=False):
    state = step(dispatched(), StartAnswered("wire-A", StartOutcome.ACCEPTED)).state
    return step(state, OutcomeCommitted("wire-A", StartOutcome.ACCEPTED)).state if committed else state


# Intent: a late real Accepted retains both debts while a newer committed Pause keeps authority.
# Why: supersession revokes permission, not the physical history of a prior dispatch.
@pytest.mark.parametrize("change", ["pause", "boot", "connection", "control_revision", "ticket"])
def test_late_accepted_keeps_truth_without_republishing_authority(change):
    state = dispatched()
    before = state
    changes = {
        "pause": {"intent": ControlIntent("paused", 8), "ticket": "pause-B"},
        "boot": {"scope": replace(state.authority.scope, boot="boot-B")},
        "connection": {"scope": replace(state.authority.scope, epoch=3)},
        "control_revision": {"control_revision": 12},
        "ticket": {"ticket": "ticket-B"},
    }
    newer = replace(state.authority, **changes[change])
    state = step(state, AuthorityChanged(state.authority, newer)).state
    transition = step(state, StartAnswered("wire-A", StartOutcome.ACCEPTED))
    assert transition.effects == (RecordOutcome("wire-A", StartOutcome.ACCEPTED), RestrictiveRecovery("wire-A"))
    state = step(transition.state, OutcomeCommitted("wire-A", StartOutcome.ACCEPTED)).state
    assert state.authority == newer
    assert state.start("wire-A").outcome is StartOutcome.ACCEPTED
    assert state.start("wire-A").debts == frozenset(StartDebt)
    assert state.start("wire-A").publication is None
    assert before.authority == authority() and before.start("wire-A").outcome is StartOutcome.ISSUED


# Intent: Accepted evidence commits before publication is even considered; failed publication retains it.
# Why: request/authority publication failure must not erase a definite physical reply or confer app control.
@pytest.mark.parametrize("failure", [FailureKind.STORAGE, FailureKind.SUPERSEDED, FailureKind.STRICT])
def test_outcome_and_publication_have_separate_commit_boundaries(failure):
    observed = accepted()
    assert not observed.start("wire-A").evidence_committed
    stored = step(observed, OutcomeCommitted("wire-A", StartOutcome.ACCEPTED))
    assert stored.effects == (ConsiderPublication("wire-A", authority()),)
    failed = step(stored.state, PublicationFailed("wire-A", failure))
    assert failed.effects == (RestrictiveRecovery("wire-A"),)
    assert failed.state.authority == authority()
    assert failed.state.start("wire-A").evidence_committed
    assert failed.state.start("wire-A").publication is None
    assert failed.state.start("wire-A").debts == frozenset(StartDebt)
    assert not observed.start("wire-A").evidence_committed


# Intent: the same current handoff can publish once; later Pause and delayed notifications remain separate.
# Why: a committed old publication stays truthful without overwriting the later owner.
@pytest.mark.parametrize("pause_first", [False, True])
def test_historical_publication_never_reclaims_newer_pause(pause_first):
    state = accepted(committed=True)
    published = replace(authority(), intent=ControlIntent("app", 8))
    newer = replace(authority(), intent=ControlIntent("paused", 9), ticket="pause-B")
    if pause_first:
        state = step(state, AuthorityChanged(state.authority, newer)).state
    state = step(state, PublicationCommitted("wire-A", published)).state
    assert state.start("wire-A").publication == published
    if not pause_first:
        assert state.authority == published
        state = step(state, AuthorityChanged(state.authority, newer)).state
    assert state.authority == newer and state.start("wire-A").debts == frozenset(StartDebt)
    assert step(state, PublicationCommitted("wire-A", published)).state == state


# Intent: possible send, rejected reply and unsent have distinct independent debt consequences.
# Why: unknown delivery cannot be replayed or relabelled merely because a later read was not sent.
@pytest.mark.parametrize(
    "outcome,expected",
    [(StartOutcome.UNKNOWN, frozenset(StartDebt)), (StartOutcome.REJECTED, frozenset()),
     (StartOutcome.ERROR, frozenset(StartDebt))],
)
def test_terminal_wire_outcomes_do_not_share_one_failure_flag(outcome, expected):
    state = step(dispatched(), StartAnswered("wire-A", outcome)).state
    assert state.start("wire-A").debts == expected
    with pytest.raises(ValueError, match="cannot become unsent"):
        step(state, StartAnswered("wire-A", StartOutcome.NOT_SENT))
    unsent = step(prepared(), StartAnswered("wire-A", StartOutcome.NOT_SENT)).state
    assert not unsent.start("wire-A").debts


# Intent: a delayed Unknown storage notification cannot certify a newer actual Accepted reply.
# Why: COMMIT evidence needs its outcome identity as well as the operation identity.
def test_late_reply_and_delayed_storage_ack_keep_distinct_outcome_identities():
    state = step(dispatched(), StartAnswered("wire-A", StartOutcome.UNKNOWN)).state
    state = step(state, StartAnswered("wire-A", StartOutcome.ACCEPTED)).state
    delayed = step(state, OutcomeCommitted("wire-A", StartOutcome.UNKNOWN))
    assert not delayed.effects and not delayed.state.start("wire-A").evidence_committed
    stored = step(delayed.state, OutcomeCommitted("wire-A", StartOutcome.ACCEPTED))
    assert stored.effects == (ConsiderPublication("wire-A", authority()),)
    assert step(stored.state, OutcomeCommitted("wire-A", StartOutcome.ACCEPTED)).effects == ()


# Intent: lifecycle/native closure and cloud settlement discharge only their own exact transaction duties.
# Why: zero or local Stop alone cannot settle a still-pending vendor acknowledgement.
@pytest.mark.parametrize("native,ledger", [(False, False), (True, False), (False, True), (True, True)])
def test_local_and_cloud_debts_are_independent(native, ledger):
    state = step(accepted(committed=True), LifecycleBound("wire-A", 41)).state
    closed = step(state, LifecycleClosed("wire-A", 41, native, ledger)).state
    expected = {StartDebt.CLOUD_START, StartDebt.CLOUD_STOP} if native and ledger else set(StartDebt)
    assert closed.start("wire-A").debts == expected
    settled = step(closed, CloudSettled("wire-A", 41, "StartTransaction")).state
    assert settled.start("wire-A").debts == expected - {StartDebt.CLOUD_START}
    settled = step(settled, CloudSettled("wire-A", 41, "StopTransaction")).state
    assert settled.start("wire-A").debts == expected - {StartDebt.CLOUD_START, StartDebt.CLOUD_STOP}
    for event in [CloudSettled("wire-A", 42, "StartTransaction"), LifecycleClosed("wire-A", 42, True, True)]:
        with pytest.raises(ValueError, match="different local"):
            step(state, event)


# Intent: an observed real lifecycle survives even an earlier explicit rejection.
# Why: RPC classification cannot erase independent physical session evidence.
def test_lifecycle_evidence_is_not_erased_by_rejected_reply():
    state = step(dispatched(), LifecycleBound("wire-A", 41)).state
    rejected = step(state, StartAnswered("wire-A", StartOutcome.REJECTED)).state
    assert rejected.start("wire-A").debts == frozenset(StartDebt)
    late = step(dispatched(), StartAnswered("wire-A", StartOutcome.REJECTED)).state
    late = step(late, LifecycleBound("wire-A", 41)).state
    assert late.start("wire-A").debts == frozenset(StartDebt)


# Intent: a real cloud Start ACK cannot consume the separate Stop delivery debt of that same session.
# Why: local closure followed by a pending Stop ACK must still prevent another positive operation.
def test_start_ack_does_not_erase_later_stop_ack_obligation():
    state = step(accepted(committed=True), LifecycleBound("wire-A", 41)).state
    state = step(state, CloudSettled("wire-A", 41, "StartTransaction")).state
    state = step(state, LifecycleClosed("wire-A", 41, True, True)).state
    state = step(state, PortRetired("wire-A")).state
    assert state.start("wire-A").debts == {StartDebt.CLOUD_STOP}
    with pytest.raises(ValueError, match="indebted"):
        step(state, StartPrepared(StartIdentity("wire-B", "request-B", "b" * 64, state.authority)))
    state = step(state, CloudSettled("wire-A", 41, "StopTransaction")).state
    assert not state.start("wire-A").debts


# Intent: shutdown closes positive admission, while actual late outcomes and port retirement remain owned.
# Why: replacing a single active-operation slot cannot abandon journal users or accepted debt.
def test_close_revokes_candidates_but_keeps_outcomes_until_ports_retire():
    state = step(dispatched(), CloseStarted()).state
    assert state.closing and not state.ports_retired
    with pytest.raises(ValueError, match="retirement"):
        step(state, PortRetired("wire-A"))
    state = step(state, StartAnswered("wire-A", StartOutcome.ACCEPTED)).state
    stored = step(state, OutcomeCommitted("wire-A", StartOutcome.ACCEPTED))
    assert stored.effects == (RestrictiveRecovery("wire-A"),)
    state = step(stored.state, PortRetired("wire-A")).state
    assert state.ports_retired and state.start("wire-A").debts == frozenset(StartDebt)
    with pytest.raises(ValueError, match="non-closing"):
        step(state, StartPrepared(StartIdentity("wire-B", "request-B", "b" * 64, state.authority)))


# Intent: the Return branch retains production whole-operation interpretation and historical physical facts.
# Why: a later Pause makes successful old sharing stale, and rollback zero cannot turn prior positive into unsent.
@pytest.mark.parametrize("case,expected", [("shared", ReleaseDisposition.SHARED),
                                         ("new_pause", ReleaseDisposition.STALE),
                                         ("rollback", ReleaseDisposition.FAULT)])
def test_return_uses_existing_whole_operation_policy(case, expected):
    before = authority()
    published = replace(before, intent=ControlIntent("app", 8))
    result = OwnedReleaseResult(
        before, True, True, published, ReleaseEffects(True, WireReply.ACCEPTED, True, False, False),
        None, None, None, None, False, False,
    )
    current = published
    if case == "new_pause":
        current = replace(before, intent=ControlIntent("paused", 9), ticket="pause-B")
    elif case == "rollback":
        result = replace(result, published=False, published_authority=None,
                         effects=ReleaseEffects(True, WireReply.ACCEPTED, False, True, True))
        current = before
    transition = step(SliceState(current), ReturnCompleted("return-A", result, True, False))
    assert transition.state.returns[-1].disposition is expected
    assert transition.state.returns[-1].result.effects.positive_dispatched
    assert transition.state.authority == current and not transition.effects


# Intent: malformed causal events fail instead of fabricating dispatch, publication or a reused request.
# Why: cheap execution does not justify accepting impossible input grammars.
def test_impossible_events_and_identity_reuse_are_rejected():
    state = prepared()
    for event in [StartAnswered("wire-A", StartOutcome.ACCEPTED), OutcomeCommitted("wire-A", StartOutcome.ACCEPTED),
                  LifecycleBound("wire-A", 41), PublicationCommitted("wire-A", authority()),
                  PublicationFailed("wire-A", FailureKind.STORAGE)]:
        with pytest.raises(ValueError):
            step(state, event)
    with pytest.raises(ValueError, match="twice"):
        step(dispatched(), StartDispatched("wire-A"))
    with pytest.raises(ValueError, match="different observed"):
        step(accepted(), OutcomeCommitted("wire-A", StartOutcome.REJECTED))
    with pytest.raises(ValueError, match="live or indebted"):
        step(state, StartPrepared(StartIdentity("wire-B", "request-A", "b" * 64, state.authority)))
    with pytest.raises(FrozenInstanceError):
        state.closing = True


# Intent: lifecycle identities use positive integer transaction numbers rather than booleans or sentinels.
# Why: a malformed binding must never discharge debt or authorize a later session association.
@pytest.mark.parametrize("local", [0, -1, True, None, "41"])
def test_invalid_local_identity_cannot_bind_start(local):
    with pytest.raises(ValueError, match="exact local"):
        step(dispatched(), LifecycleBound("wire-A", local))
