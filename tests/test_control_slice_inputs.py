"""Independent causal and identity admission rows for the bounded boundary-event grammar."""

from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.managed_intent import ControlIntent
from ocpp_hub_core.prototype.recovery_contracts import FailureKind, OwnedReleaseResult, ReleaseEffects, WireReply
from ocpp_hub_core.prototype.recovery_policy import ReleaseDisposition

from tests.test_control_slice import accepted, authority, dispatched, prepared
from tests_model.control_slice import step
from tests_model.control_slice_state import (
    AuthorityChanged,
    CloseStarted,
    CloudSettled,
    LifecycleBound,
    LifecycleClosed,
    OutcomeCommitted,
    PortRetired,
    PublicationCommitted,
    PublicationFailed,
    RecordOutcome,
    ReturnCompleted,
    SliceState,
    StartAnswered,
    StartDispatched,
    StartIdentity,
    StartOutcome,
    StartPrepared,
)


# Supply a completely observed closed lifecycle, without using the reducer as an expected-result oracle.
def settled():
    state = step(accepted(committed=True), LifecycleBound("wire-A", 41)).state
    state = step(state, LifecycleClosed("wire-A", 41, True, True)).state
    state = step(state, CloudSettled("wire-A", 41, "StartTransaction")).state
    state = step(state, CloudSettled("wire-A", 41, "StopTransaction")).state
    return step(state, PortRetired("wire-A")).state


# Intent: each independently scoped publication field must identify the same committed Start handoff.
# Why: one matching ticket cannot replace Boot, connection, intent and control revision proof.
@pytest.mark.parametrize("field", ["boot", "epoch", "metadata", "ticket", "control", "mode", "revision"])
def test_publication_requires_every_original_scope_component(field):
    current = authority()
    published = replace(current, intent=ControlIntent("app", 8))
    changes = {
        "boot": {"scope": replace(current.scope, boot="other-boot")},
        "epoch": {"scope": replace(current.scope, epoch=3)},
        "metadata": {"scope": replace(current.scope, metadata_revision=4)},
        "ticket": {"ticket": "other-ticket"},
        "control": {"control_revision": 12},
        "mode": {"intent": ControlIntent("paused", 8)},
        "revision": {"intent": ControlIntent("app", 9)},
    }
    with pytest.raises(ValueError, match="exact original"):
        step(accepted(committed=True), PublicationCommitted("wire-A", replace(published, **changes[field])))


# Intent: a committed old publication remains a fact even if a duplicate completion changes its other fields.
# Why: a later notification cannot rewrite an already published historical authority.
def test_committed_publication_cannot_be_replaced_or_reported_failed():
    published = replace(authority(), intent=ControlIntent("app", 8))
    state = step(accepted(committed=True), PublicationCommitted("wire-A", published)).state
    with pytest.raises(ValueError, match="committed publication"):
        step(state, PublicationFailed("wire-A", FailureKind.STORAGE))
    assert step(state, PublicationCommitted("wire-A", published)).state == state


# Intent: empty request, effect and tag identities are rejected independently.
# Why: missing provenance cannot be repaired by borrowing the current operation identity.
@pytest.mark.parametrize("field", ["effect_id", "request_id", "tag_digest"])
def test_each_start_identity_is_required(field):
    identity = StartIdentity("wire-A", "request-A", "a" * 64, authority())
    with pytest.raises(ValueError, match="explicit"):
        step(SliceState(authority()), StartPrepared(replace(identity, **{field: ""})))


# Intent: historical effect and request identities cannot be reused after all their duties settle.
# Why: idempotency must survive retirement without preventing a distinct later request.
@pytest.mark.parametrize("effect_id,request_id", [("wire-A", "request-B"), ("wire-B", "request-A")])
def test_retired_identity_is_not_reusable(effect_id, request_id):
    state = settled()
    with pytest.raises(ValueError, match="replace"):
        step(state, StartPrepared(StartIdentity(effect_id, request_id, "b" * 64, state.authority)))
    later = step(state, StartPrepared(StartIdentity("wire-B", "request-B", "b" * 64, state.authority))).state
    assert later.starts[0] == state.starts[0] and len(later.starts) == 2


# Intent: stale preparation and outdated committed-authority observations are refused.
# Why: old observations cannot reacquire authority, including when their intent revision goes backwards.
@pytest.mark.parametrize("case", ["prepared", "previous", "revision"])
def test_authority_observations_cannot_overwrite_a_newer_owner(case):
    state = SliceState(authority())
    older = replace(authority(), intent=ControlIntent("paused", 6))
    event = (
        StartPrepared(StartIdentity("wire-A", "request-A", "a" * 64, older))
        if case == "prepared"
        else AuthorityChanged(older, authority())
        if case == "previous"
        else AuthorityChanged(authority(), older)
    )
    with pytest.raises(ValueError):
        step(state, event)


# Intent: no reply, storage notification or retirement can invent a missing causal predecessor.
# Why: raw grammar products contain impossible orders and must be rejected explicitly.
@pytest.mark.parametrize("outcome", [StartOutcome.PREPARED, StartOutcome.ISSUED])
def test_nonterminal_answers_and_commits_are_invalid(outcome):
    with pytest.raises(ValueError, match="answer must"):
        step(dispatched(), StartAnswered("wire-A", outcome))
    with pytest.raises(ValueError, match="terminal"):
        step(dispatched(), OutcomeCommitted("wire-A", outcome))
    with pytest.raises(ValueError, match="retirement"):
        step(prepared(), PortRetired("wire-A"))


# Intent: duplicate wire observations are idempotent, while conflicting real replies remain invalid.
# Why: event delivery retries must neither repeat effects nor reinterpret a definite peer answer.
@pytest.mark.parametrize(
    "outcome", [StartOutcome.ACCEPTED, StartOutcome.REJECTED, StartOutcome.ERROR, StartOutcome.UNKNOWN]
)
def test_duplicate_reply_does_not_repeat_persistence_or_recovery(outcome):
    state = step(dispatched(), StartAnswered("wire-A", outcome)).state
    repeated = step(state, StartAnswered("wire-A", outcome))
    assert repeated.state == state and repeated.effects == ()
    if outcome is not StartOutcome.UNKNOWN:
        other = StartOutcome.REJECTED if outcome is StartOutcome.ACCEPTED else StartOutcome.ACCEPTED
        with pytest.raises(ValueError, match="different actual"):
            step(state, StartAnswered("wire-A", other))


# Intent: only a bound exact positive local ID and named lifecycle ACK may settle an attempt.
# Why: a terminal wire reply does not authorize attribution to an unrelated session or action.
def test_binding_and_cloud_ack_preserve_exact_lifecycle_identity():
    state = step(accepted(committed=True), LifecycleBound("wire-A", 41)).state
    with pytest.raises(ValueError, match="exact local"):
        step(state, LifecycleBound("wire-A", 42))
    with pytest.raises(ValueError, match="exact lifecycle action"):
        step(state, CloudSettled("wire-A", 41, "BootNotification"))
    complete = settled()
    assert step(complete, LifecycleBound("wire-A", 41)).state == complete
    unsent = step(prepared(), StartAnswered("wire-A", StartOutcome.NOT_SENT)).state
    assert step(unsent, StartAnswered("wire-A", StartOutcome.NOT_SENT)).state == unsent
    with pytest.raises(ValueError, match="unissued"):
        step(unsent, LifecycleBound("wire-A", 41))


# Intent: positive admission is closed during shutdown even when an old Return actually published successfully.
# Why: historical sharing does not grant current publication permission to a closing owner.
@pytest.mark.parametrize("closing,allowed", [(True, True), (False, False)])
def test_closing_or_revoked_publication_keeps_return_result_stale(closing, allowed):
    published = replace(authority(), intent=ControlIntent("app", 8))
    result = OwnedReleaseResult(
        authority(),
        True,
        True,
        published,
        ReleaseEffects(True, WireReply.ACCEPTED, True, False, False),
        None,
        None,
        None,
        None,
        False,
        False,
    )
    state = SliceState(published)
    if closing:
        state = step(state, CloseStarted()).state
    state = step(state, ReturnCompleted("return-A", result, True, False, publication_allowed=allowed)).state
    assert state.returns[-1].disposition is ReleaseDisposition.STALE


# Intent: a physical dispatch observation remains true even if its owner event is delivered later than Pause.
# Why: revocation changes permission while the outcome ledger still records what actually reached dispatch.
def test_delayed_dispatch_observation_is_not_erased_by_newer_pause():
    state = prepared()
    newer = replace(authority(), intent=ControlIntent("paused", 8), ticket="pause-B")
    state = step(state, AuthorityChanged(state.authority, newer)).state
    state = step(state, StartDispatched("wire-A")).state
    assert state.authority == newer and state.start("wire-A").outcome is StartOutcome.ISSUED


# Intent: current actual acceptance records evidence before considering publication, without asking for rollback.
# Why: a definite current success must not be confused with stale or uncertain physical outcomes.
def test_current_accepted_reply_only_requests_its_evidence_record():
    transition = step(dispatched(), StartAnswered("wire-A", StartOutcome.ACCEPTED))
    assert transition.effects == (RecordOutcome("wire-A", StartOutcome.ACCEPTED),)


# Intent: Accepted observed only in memory cannot be reported as a final authority-publication attempt.
# Why: failure or success at the evidence COMMIT precedes, and differs from, final handoff COMMIT.
def test_uncommitted_acceptance_cannot_enter_final_publication():
    state = accepted()
    for event in [
        PublicationFailed("wire-A", FailureKind.STORAGE),
        PublicationCommitted("wire-A", replace(authority(), intent=ControlIntent("app", 8))),
    ]:
        with pytest.raises(ValueError, match="separately committed"):
            step(state, event)


# Intent: live ports block replacement even when the first effect has no physical or cloud debt yet.
# Why: a prepared native validation still belongs to the current attempt and must be retired first.
def test_debt_free_live_preparation_cannot_be_replaced():
    state = prepared()
    with pytest.raises(ValueError, match="live or indebted"):
        step(state, StartPrepared(StartIdentity("wire-B", "request-B", "b" * 64, state.authority)))


# Intent: a publication cannot be invented at the same revision already consumed by a newer Pause.
# Why: if Start published first, that Pause would increment the revision again; if Pause won, Start cannot publish.
@pytest.mark.parametrize("replacement", ["pause", "boot_before_publication"])
def test_historical_publication_requires_a_possible_committed_revision_order(replacement):
    state = accepted(committed=True)
    newer = (
        replace(authority(), intent=ControlIntent("paused", 8), ticket="pause-B")
        if replacement == "pause"
        else replace(authority(), scope=replace(authority().scope, boot="boot-B"))
    )
    state = step(state, AuthorityChanged(state.authority, newer)).state
    with pytest.raises(ValueError, match="committed owner revision"):
        step(state, PublicationCommitted("wire-A", replace(authority(), intent=ControlIntent("app", 8))))


# Intent: a real publication before a later identical Boot keeps its historical scope and current app intent.
# Why: changed proof scope at the same app revision does not invalidate a genuinely earlier COMMIT.
def test_historical_publication_at_same_app_revision_can_precede_later_boot():
    published = replace(authority(), intent=ControlIntent("app", 8))
    current = replace(published, scope=replace(published.scope, boot="boot-B"))
    state = accepted(committed=True)
    state = step(state, AuthorityChanged(state.authority, current)).state
    state = step(state, PublicationCommitted("wire-A", published)).state
    assert state.authority == current and state.start("wire-A").publication == published
