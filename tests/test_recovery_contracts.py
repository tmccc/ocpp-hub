"""Pure recovery policy and explicitly isolated Peer unit collaborators.

Synthetic result facts below prove policy, never real managed authority or physical effects.
Existing genuine managed regressions must separately prove production fact recording, native proof,
owned zero, SQL atomicity and the Start child handoff. No model dimensions are changed here.
"""

import asyncio
import json
import sqlite3
from dataclasses import FrozenInstanceError, replace

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.connection_scope import CloudAttemptScope
from ocpp_hub_core.prototype.managed_intent import ControlIntent
from ocpp_hub_core.prototype.recovery_contracts import (
    ALLOW,
    AuthorityStamp,
    FailureKind,
    NotSent,
    OwnedReleaseResult,
    Pending,
    Prerequisite,
    ReleaseEffects,
    Reply,
    RestoreProgress,
    RestoreState,
    WireReply,
    require_reply,
)
from ocpp_hub_core.prototype.recovery_policy import (
    PreflightDisposition,
    RecoveryFacts,
    RecoveryObligation,
    RecoveryWork,
    ReleaseDisposition,
    next_recovery_work,
    preflight_disposition,
    release_disposition,
)
from ocpp_hub_core.prototype.rpc import Peer
from ocpp_hub_core.prototype.start_handoff import StartAttempt, StartPublication
from ocpp_hub_core.prototype.store import ReleaseDeferredError


# Construct an exact pure identity; these synthetic fields are never installed in a journal.
def authority(purpose="automatic", **changes):
    original = AuthorityStamp(
        intent=ControlIntent("app" if purpose == "automatic" else "paused", 4),
        scope=CloudAttemptScope(2, "b" * 32, 3),
        control_revision=7,
        ticket="owned-release-A",
    )
    return replace(original, **changes)


# Name independent effect facts without deriving their expected disposition from the implementation.
def effects(**changes):
    original = ReleaseEffects(
        positive_dispatched=False,
        positive_reply=WireReply.MISSING,
        positive_verified=False,
        zero_dispatched=True,
        zero_verified=True,
    )
    return replace(original, **changes)


# Supply a canonical owned zero wait; each table changes only the boundary under discussion.
def owned(purpose="automatic", **changes):
    original = OwnedReleaseResult(
        authority=authority(purpose),
        claimed=True,
        published=False,
        published_authority=None,
        effects=effects(),
        pending=Pending(Prerequisite.NATIVE, "unit prerequisite"),
        failure=None,
        initial_failure_kind=None,
        failure_kind=None,
        cleanup_failed=False,
        cancelled=False,
    )
    return replace(original, **changes)


# Keep the expected result at each call site explicit; a current stamp is a fact, not cached permission.
def disposition(result, purpose="automatic", **changes):
    inputs = {
        "zero_ready": True,
        "faulted": False,
        "timer_superseded": False,
        "publication_allowed": True,
    }
    current = changes.pop("current", result.published_authority if result.published else result.authority)
    inputs.update(changes)
    return release_disposition(purpose, result, current, **inputs)


# Intent: each canonical prerequisite may retain automatic zero, while an already-owned explicit attempt fails.
# Why: typed domain reasons replace exception guessing without silently granting explicit Return replay.
@pytest.mark.parametrize(
    "reason",
    [
        Prerequisite.NATIVE,
        Prerequisite.SOURCE,
        Prerequisite.LIFECYCLE,
        Prerequisite.CLOUD,
        Prerequisite.CLOUD_ACK,
        Prerequisite.ZERO_PROOF,
        Prerequisite.RECONCILIATION,
        Prerequisite.TIMER_CLEANUP,
        Prerequisite.OWNER_RETURN,
        Prerequisite.SUPERSEDED,
        Prerequisite.RESTRICTIVE_OPERATION,
        Prerequisite.OWNED_OPERATION,
    ],
)
@pytest.mark.parametrize(
    "purpose,expected", [("automatic", ReleaseDisposition.WAIT), ("explicit", ReleaseDisposition.FAULT)]
)
def test_owned_pending_policy_is_distinct_from_explicit_attempt_failure(reason, purpose, expected):
    result = owned(purpose, pending=Pending(reason, "same diagnostic wording"))
    assert disposition(result, purpose) is expected


# Intent: a prerequisite before any claim/effect preserves the existing verified hold for either caller.
# Why: pre-attempt waiting must not be confused with retrying an already-owned explicit positive operation.
@pytest.mark.parametrize("purpose", ["automatic", "explicit"])
def test_preclaim_wait_has_no_borrowed_operation_effects(purpose):
    result = owned(
        purpose,
        authority=authority(purpose, ticket="prior-owned-zero"),
        claimed=False,
        pending=Pending(Prerequisite.SOURCE, "source waiting before actual ownership"),
        effects=effects(zero_dispatched=False, zero_verified=False),
    )
    assert disposition(result, purpose) is ReleaseDisposition.WAIT
    assert disposition(result, purpose, zero_ready=False) is ReleaseDisposition.FAULT
    assert disposition(replace(result, effects=effects()), purpose) is ReleaseDisposition.FAULT


# Intent: cancellation, incomplete zero, an existing fault or cleanup failure defeats an automatic wait.
# Why: a canonical Pending reason is insufficient proof that the complete operation is safe to retry.
@pytest.mark.parametrize(
    "changes,inputs",
    [
        ({"cancelled": True}, {}),
        ({"cleanup_failed": True}, {}),
        ({"effects": effects(zero_verified=False)}, {}),
        ({"authority": authority(ticket=None)}, {}),
        ({}, {"zero_ready": False}),
        ({}, {"faulted": True}),
        ({"failure": RuntimeError("same diagnostic wording"), "failure_kind": FailureKind.UNKNOWN}, {}),
    ],
)
def test_owned_wait_requires_its_complete_zero_and_failure_contract(changes, inputs):
    assert disposition(owned(**changes), **inputs) is ReleaseDisposition.FAULT


# Intent: every exact ownership component still fences waiting and successful publication.
# Why: an earlier scope/ticket check cannot authorize a later result after a new owner, Boot or claim.
@pytest.mark.parametrize(
    "current",
    [
        None,
        authority(intent=ControlIntent("paused", 4)),
        authority(intent=ControlIntent("app", 5)),
        authority(scope=CloudAttemptScope(3, "b" * 32, 3)),
        authority(scope=CloudAttemptScope(2, "c" * 32, 3)),
        authority(scope=CloudAttemptScope(2, "b" * 32, 4)),
        authority(control_revision=8),
        authority(ticket="newer-claim-B"),
    ],
)
def test_current_authority_is_required_for_wait_and_sharing(current):
    waiting = owned()
    shared = owned(
        published=True,
        published_authority=authority(),
        pending=None,
        effects=effects(
            positive_dispatched=True,
            positive_reply=WireReply.ACCEPTED,
            positive_verified=True,
            zero_dispatched=False,
            zero_verified=False,
        ),
    )
    assert disposition(waiting, current=current) is ReleaseDisposition.FAULT
    assert disposition(shared, current=current) is ReleaseDisposition.STALE


# Intent: any earlier positive dispatch remains a failed positive attempt, including a definite refusal.
# Why: negative reply knowledge and physical uncertainty differ, but both retain the selected owner-recovery policy.
@pytest.mark.parametrize("reply", [WireReply.ACCEPTED, WireReply.REJECTED, WireReply.MISSING, WireReply.ERROR])
def test_recorded_positive_dispatch_cannot_become_a_later_unsent_wait(reply):
    pending = Pending(Prerequisite.SOURCE, "later call did not send")
    later = NotSent(pending)
    result = owned(effects=effects(positive_dispatched=True, positive_reply=reply), pending=later.pending)
    assert result.effects.zero_verified and disposition(result) is ReleaseDisposition.FAULT
    verified = replace(result, effects=replace(result.effects, positive_verified=True))
    assert disposition(verified) is ReleaseDisposition.FAULT


# Intent: an independently recorded positive verification also defeats the no-positive wait contract.
# Why: a missing or reset dispatch bit must not turn a stronger earlier positive fact into permission to retry.
def test_positive_verification_alone_is_not_zero_wait_evidence():
    assert disposition(owned(effects=effects(positive_verified=True))) is ReleaseDisposition.FAULT


# Intent: real complete success may share, while pending or failed publication remains unconfirmed.
# Why: an Accepted reply alone is distinct from verified profile and successful final authority publication.
@pytest.mark.parametrize("purpose", ["automatic", "explicit"])
def test_complete_success_requires_dispatch_accepted_verification_and_publication(purpose):
    result = owned(
        purpose,
        published=True,
        published_authority=authority(purpose, intent=ControlIntent("app", 5 if purpose == "explicit" else 4)),
        pending=None,
        effects=effects(
            positive_dispatched=True,
            positive_reply=WireReply.ACCEPTED,
            positive_verified=True,
            zero_dispatched=False,
            zero_verified=False,
        ),
    )
    assert disposition(result, purpose) is ReleaseDisposition.SHARED
    assert disposition(result, purpose, publication_allowed=False) is ReleaseDisposition.STALE
    for changed in (
        replace(result, claimed=False),
        replace(result, published=False),
        replace(result, published_authority=None),
        replace(result, effects=replace(result.effects, positive_dispatched=False)),
        replace(result, effects=replace(result.effects, positive_reply=WireReply.REJECTED)),
        replace(result, effects=replace(result.effects, positive_verified=False)),
        replace(result, cleanup_failed=True),
        replace(result, effects=replace(result.effects, zero_dispatched=True)),
        replace(result, effects=replace(result.effects, zero_verified=True)),
    ):
        assert disposition(changed, purpose) is ReleaseDisposition.FAULT


# Intent: a technical error can never gain ordinary Pending permission from its class, text or cause chain.
# Why: domain prerequisites are explicit input data, while strict/storage/unknown failures remain failures.
@pytest.mark.parametrize(
    "kind", [FailureKind.STRICT, FailureKind.STORAGE, FailureKind.CONTROL, FailureKind.TRANSPORT, FailureKind.UNKNOWN]
)
def test_failure_kinds_do_not_create_business_waits(kind):
    error = RuntimeError("temporary native source not sent")
    error.__cause__ = OSError("unit cause which policy must not classify")
    result = owned(failure=error, initial_failure_kind=kind, failure_kind=kind)
    assert disposition(result) is ReleaseDisposition.FAULT
    assert result.failure is error and error.__cause__ is not None


# Intent: only exact verified timer supersession may yield even when the old rollback lost its own claim.
# Why: a pending newer zero is not the old attempt's zero proof, and strict/storage failures still dominate it.
@pytest.mark.parametrize(
    "initial,final,proof,expected",
    [
        (FailureKind.SUPERSEDED, FailureKind.SUPERSEDED, True, ReleaseDisposition.YIELD),
        (FailureKind.SUPERSEDED, FailureKind.SUPERSEDED, False, ReleaseDisposition.FAULT),
        (FailureKind.STRICT, FailureKind.SUPERSEDED, True, ReleaseDisposition.FAULT),
        (FailureKind.SUPERSEDED, FailureKind.STRICT, True, ReleaseDisposition.FAULT),
        (FailureKind.STORAGE, FailureKind.SUPERSEDED, True, ReleaseDisposition.FAULT),
        (FailureKind.SUPERSEDED, FailureKind.STORAGE, True, ReleaseDisposition.FAULT),
        (FailureKind.TRANSPORT, FailureKind.SUPERSEDED, True, ReleaseDisposition.FAULT),
        (FailureKind.SUPERSEDED, FailureKind.CONTROL, True, ReleaseDisposition.FAULT),
        (FailureKind.UNKNOWN, FailureKind.SUPERSEDED, True, ReleaseDisposition.FAULT),
    ],
)
def test_timer_yield_requires_both_typed_supersession_verdicts(initial, final, proof, expected):
    # The boolean stands for the separately verified provider; this unit test cannot prove that provider's identity.
    result = owned(
        effects=effects(zero_dispatched=False, zero_verified=False),
        pending=Pending(Prerequisite.RESTRICTIVE_OPERATION, "actual newer timer claim"),
        failure=RuntimeError("identical technical diagnostic for every row"),
        initial_failure_kind=initial,
        failure_kind=final,
        cleanup_failed=True,
    )
    current = authority(ticket="actual-new-timer-claim")
    assert disposition(result, current=current, zero_ready=False, timer_superseded=proof) is expected
    if expected is ReleaseDisposition.YIELD:
        assert not result.effects.zero_verified
        assert disposition(result, "explicit", current=current, timer_superseded=True) is ReleaseDisposition.FAULT
        assert disposition(replace(result, cancelled=True), timer_superseded=True) is ReleaseDisposition.FAULT
        assert disposition(result, faulted=True, timer_superseded=True) is ReleaseDisposition.FAULT
        positive = replace(result, effects=replace(result.effects, positive_dispatched=True))
        assert disposition(positive, timer_superseded=True) is ReleaseDisposition.FAULT


# Intent: published effect snapshots cannot be changed, and a new operation does not inherit an old failure.
# Why: same-attempt monotonic provenance must coexist with legitimate fresh recovery after an earlier operation.
def test_effect_snapshot_is_immutable_and_fresh_operation_policy_is_independent():
    prior = owned(effects=effects(positive_dispatched=True, positive_reply=WireReply.ACCEPTED))
    with pytest.raises(FrozenInstanceError):
        prior.effects.positive_dispatched = False
    assert disposition(prior) is ReleaseDisposition.FAULT
    fresh = owned(authority=authority(ticket="fresh-operation-B"))
    assert disposition(fresh) is ReleaseDisposition.WAIT
    assert prior.effects.positive_dispatched and prior.effects.positive_reply is WireReply.ACCEPTED


# Intent: WAITING and YIELDED progress cannot settle a domain's positive restoration duty.
# Why: callers must inspect the named shared fact rather than truthiness of a returned data object.
@pytest.mark.parametrize(
    "state,expected",
    [
        (RestoreState.WAITING, False),
        (RestoreState.YIELDED, False),
        (RestoreState.REQUEST_UNCONFIRMED, False),
        (RestoreState.SHARED, True),
    ],
)
def test_restore_progress_has_an_explicit_publication_fact(state, expected):
    pending = None if expected else Pending(Prerequisite.SOURCE, "explicit progress blocker")
    assert RestoreProgress(state=state, pending=pending).shared is expected


# Intent: simultaneous duties follow a declared order and remain present after selection.
# Why: selecting foreground work must not erase a different ledger, current Pause or independent cutoff duty.
@pytest.mark.parametrize(
    "paused,expected",
    [
        (
            False,
            [
                RecoveryWork.CURRENT,
                RecoveryWork.EXPIRED_START,
                RecoveryWork.TIMER_DEADLINE,
                RecoveryWork.RETURN,
                RecoveryWork.ORDINARY,
            ],
        ),
        (
            True,
            [
                RecoveryWork.CURRENT,
                RecoveryWork.EXPIRED_START,
                RecoveryWork.TIMER_DEADLINE,
                RecoveryWork.RETURN,
                RecoveryWork.PAUSE,
            ],
        ),
    ],
)
def test_simultaneous_work_precedence_is_explicit_and_non_destructive(paused, expected):
    obligations = (
        RecoveryObligation(RecoveryWork.CONNECTION, "connection-B"),
        RecoveryObligation(RecoveryWork.TIMER_DEADLINE, "timer-older"),
        RecoveryObligation(RecoveryWork.PAUSE, "pause-C"),
        RecoveryObligation(RecoveryWork.CURRENT, "current-A"),
        RecoveryObligation(RecoveryWork.RETURN, "return-D"),
        RecoveryObligation(RecoveryWork.ORDINARY, "daily-E"),
        RecoveryObligation(RecoveryWork.EXPIRED_START, "start-F"),
        RecoveryObligation(RecoveryWork.TIMER_DEADLINE, "timer-newer"),
    )
    facts = RecoveryFacts(obligations=obligations, paused=paused)
    visited = frozenset()
    for work in expected:
        assert next_recovery_work(facts, visited) is work
        visited = visited | {work}
    assert next_recovery_work(facts, visited) is None
    assert facts.obligations == obligations and len(facts.obligations) == 8


# Intent: a completed awaited duty requires a new projection before choosing new Pause or connection work.
# Why: an old snapshot is neither permission to skip a newer restriction nor proof that ordinary work still exists.
def test_recomputed_facts_observe_new_pause_and_released_ordinary_obligation():
    before = RecoveryFacts((RecoveryObligation(RecoveryWork.CURRENT, "old-current"),), paused=False)
    assert next_recovery_work(before, frozenset()) is RecoveryWork.CURRENT
    visited = frozenset({RecoveryWork.CURRENT})
    changed = RecoveryFacts(
        (RecoveryObligation(RecoveryWork.PAUSE, "new-Pause"), RecoveryObligation(RecoveryWork.CONNECTION, "epoch-B")),
        paused=True,
    )
    assert next_recovery_work(changed, visited) is RecoveryWork.PAUSE
    assert next_recovery_work(changed, visited | {RecoveryWork.PAUSE}) is None
    ordinary = RecoveryFacts(
        (
            RecoveryObligation(RecoveryWork.ORDINARY, "old-daily"),
            RecoveryObligation(RecoveryWork.CONNECTION, "epoch-B"),
        ),
        paused=False,
    )
    assert next_recovery_work(ordinary, frozenset()) is RecoveryWork.ORDINARY
    current = RecoveryFacts((RecoveryObligation(RecoveryWork.CONNECTION, "epoch-B"),), paused=False)
    assert next_recovery_work(current, frozenset({RecoveryWork.ORDINARY})) is RecoveryWork.CONNECTION
    assert before.obligations[0].identity == "old-current" and not before.paused


# Intent: accepted Start evidence survives failed authority publication and partial cleanup snapshots.
# Why: a compatibility exception cannot erase the genuine reply, and an unfinished child is not terminal proof.
def test_start_result_keeps_actual_reply_distinct_from_publication_and_cleanup():
    answer = [3, "start-wire", {"status": "Accepted"}]
    failure = sqlite3.IntegrityError("unit publication COMMIT refused")
    child = StartAttempt(remote_start_dispatched=True, answer=answer, failure=failure)
    with pytest.raises(RuntimeError, match="not finished"):
        child.result()
    child.finished = True
    first = child.result()
    assert first.answer is answer and first.publication is None and not first.zero_verified
    with pytest.raises(sqlite3.IntegrityError) as observed:
        first.require_reply()
    assert observed.value is failure and first.answer is answer
    child.zero_verified = True
    after_zero = child.result()
    assert after_zero.zero_verified and after_zero.answer is answer and after_zero.failure is failure
    assert not first.zero_verified
    with pytest.raises(FrozenInstanceError):
        first.failure = None
    publication = StartPublication("request-A", ControlIntent("app", 5), 2)
    child.publication, child.failure = publication, None
    completed = child.result()
    assert completed.publication is publication and completed.require_reply() is answer
    assert first.publication is None and first.failure is failure


# Build one positive profile for isolated Peer tests; no real charger or managed authority is implied.
def unit_profile():
    return commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001)


# Invoke each public typed entry without replacing its actual gate, validator or locked transport implementation.
async def invoke_typed(peer, api, payload, *, check, before=None, on_dispatch=None, on_reply=None):
    if api == "call":
        return await peer.call_result(
            "SetChargingProfile", payload, 1, check, before_send=before, on_dispatch=on_dispatch
        )
    assert api == "sequence" and before is None
    async with peer.sequence_results(1, check, on_dispatch=on_dispatch, on_reply=on_reply) as call:
        return await call("SetChargingProfile", payload)


# Intent: async/final/resource prerequisites stop the actual Peer before resource intent or dispatch.
# Why: these explicit unit collaborators prove transport routing, while real managed tests prove domain facts.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api,boundary",
    [
        ("call", "async"),
        ("call", "entry"),
        ("call", "final"),
        ("call", "before"),
        ("call", "resource"),
        ("sequence", "async"),
        ("sequence", "final"),
        ("sequence", "resource"),
    ],
)
async def test_peer_typed_not_sent_skips_resource_intent_dispatch_and_result(api, boundary):
    peer = Peer("unit", 1)
    pending = Pending(Prerequisite.SOURCE, "explicit unit collaborator", OSError("diagnostic only"))
    events, cuts = [], []
    checks = 0

    # Make the exact selected prerequisite observable before returning it to the real Peer.
    def stop_at(phase):
        cuts.append(phase)
        return pending

    # Entry and final synchronous checks are different call boundaries, not fabricated Peer state.
    def check():
        nonlocal checks
        checks += 1
        if boundary == "entry" or boundary == "final" and checks == 2:
            return stop_at(boundary)
        return ALLOW

    # The unit's async collaborator yields once; no production engine decision is replaced in a managed test.
    async def validate(action, payload):
        events.append("validate")
        await asyncio.sleep(0)
        return stop_at("async") if boundary == "async" else ALLOW

    # A pending resource prerequisite must stop before the collaborator would record its resource intent.
    def resource_gate(action, payload):
        events.append("resource_gate")
        if boundary == "resource":
            return stop_at("resource")
        events.append("resource_intent")
        return ALLOW

    # Sending or publishing a result in a NotSent row is always a failed transport contract.
    async def sender(raw):
        events.append("send")
        raise AssertionError("NotSent reached the unit sender")

    peer.sender, peer.validate_send, peer.before_send = sender, validate, resource_gate
    peer.after_result = lambda *args: events.append("resource_result")
    before = (lambda: stop_at("before")) if boundary == "before" else None
    result = await invoke_typed(
        peer,
        api,
        unit_profile(),
        check=check,
        before=before,
        on_dispatch=lambda action: events.append("dispatch"),
        on_reply=lambda *args: events.append("reply"),
    )
    assert isinstance(result, NotSent) and result.pending is pending and cuts == [boundary]
    assert not ({"resource_intent", "send", "dispatch", "resource_result", "reply"} & set(events))
    assert not peer._pending and not peer.gate.busy and not peer.gate.restrictive_waiting.is_set()


# Intent: correlated negative CALLRESULT and CALLERROR remain real Reply values through both typed APIs.
# Why: definite refusal, error reply and absence of dispatch are distinct inputs to later operation policy.
@pytest.mark.asyncio
@pytest.mark.parametrize("api", ["call", "sequence"])
@pytest.mark.parametrize("kind", ["accepted", "rejected", "call-error"])
async def test_peer_typed_reply_preserves_actual_negative_and_error_frames(api, kind):
    peer = Peer("unit", 1)
    frames, actual, observed, events = [], [], [], []

    # Feed a correlated unit wire response through the actual Peer receiver, preserving every original field.
    async def sender(raw):
        frame = json.loads(raw)
        frames.append(frame)
        text = (
            build_call_error(frame[1], "InternalError", "unit peer error")
            if kind == "call-error"
            else build_call_result(frame[1], {"status": "Accepted" if kind == "accepted" else "Rejected"})
        )
        answer = json.loads(text)
        actual.append(answer)
        peer.receive(answer)

    peer.sender = sender
    peer.before_send = lambda *args: events.append("intent")
    peer.after_result = lambda *args: events.append("result")
    result = await invoke_typed(
        peer,
        api,
        unit_profile(),
        check=lambda: ALLOW,
        on_dispatch=lambda action: events.append("dispatch"),
        on_reply=lambda action, answer: observed.append(answer),
    )
    assert len(frames) == len(actual) == 1 and isinstance(result, Reply)
    assert result.frame is actual[0] and require_reply(result) is actual[0]
    assert events == ["intent", "dispatch", "result"]
    assert observed == ([actual[0]] if api == "sequence" else [])
    assert not peer._pending and not peer.gate.busy


# Intent: an actual Accepted reply reaches the owned operation before after_result suffers real SQLite refusal.
# Why: cleanup/storage failure cannot retroactively rewrite a received positive ACK as missing or NotSent.
@pytest.mark.asyncio
async def test_peer_on_reply_retains_accepted_frame_before_real_after_result_sql_failure():
    peer = Peer("unit", 1)
    db = sqlite3.connect(":memory:", isolation_level=None)
    db.execute("CREATE TABLE acknowledgements(token TEXT)")
    db.execute("CREATE TRIGGER refuse_ack BEFORE INSERT ON acknowledgements BEGIN SELECT RAISE(ABORT,'unit SQL'); END")
    frames, answers, replies, failures, events = [], [], [], [], []

    # This is a real Peer wire exchange with a unit sender, not a TLS/native managed-engine reproduction.
    async def sender(raw):
        frame = json.loads(raw)
        frames.append(frame)
        answer = json.loads(build_call_result(frame[1], {"status": "Accepted"}))
        answers.append(answer)
        peer.receive(answer)

    # Observe the exact frame before any resource acknowledgement publication runs.
    def on_reply(action, answer):
        replies.append(answer)
        events.append("reply")

    # Fail an actual SQLite statement after receipt; retain the original exception identity for the assertion.
    def after_result(action, payload, answer):
        events.append("resource_result")
        try:
            db.execute("INSERT INTO acknowledgements VALUES(?)", (answer[1],))
        except sqlite3.Error as error:
            failures.append(error)
            raise

    peer.sender, peer.after_result = sender, after_result
    try:
        with pytest.raises(sqlite3.IntegrityError) as observed:
            await invoke_typed(peer, "sequence", unit_profile(), check=lambda: ALLOW, on_reply=on_reply)
        assert len(frames) == len(answers) == len(replies) == len(failures) == 1
        assert replies[0] is answers[0] and replies[0][2] == {"status": "Accepted"}
        assert observed.value is failures[0] and events == ["reply", "resource_result"]
        assert db.execute("SELECT COUNT(*) FROM acknowledgements").fetchone()[0] == 0
        assert not peer._pending and not peer.gate.busy
    finally:
        db.close()


# Intent: the public Frame compatibility edge preserves the original Pending diagnostic cause.
# Why: legacy callers keep their API without making the typed policy inspect exception wording or causes.
def test_legacy_frame_adapter_unwraps_only_the_typed_pending_edge():
    cause = OSError("unit diagnostic")
    pending = Pending(Prerequisite.NATIVE, "wait for current native proof", cause)
    with pytest.raises(ReleaseDeferredError) as observed:
        require_reply(NotSent(pending))
    assert observed.value.__cause__ is cause


# Intent: explicit Return native failure retains its existing one-shot policy before any physical claim.
# Why: sharing typed prerequisites must not silently add automatic retries to an accepted positive request.
@pytest.mark.parametrize(
    "purpose,reason,expected",
    [
        ("automatic", Prerequisite.NATIVE, PreflightDisposition.WAIT),
        ("explicit", Prerequisite.NATIVE, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.SOURCE, PreflightDisposition.WAIT),
        ("explicit", Prerequisite.CLOUD_ACK, PreflightDisposition.WAIT),
        ("explicit", Prerequisite.TIMER_CLEANUP, PreflightDisposition.WAIT),
        ("explicit", Prerequisite.OWNER_RETURN, PreflightDisposition.WAIT),
        ("explicit", Prerequisite.CLOUD, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.ZERO_PROOF, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.LIFECYCLE, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.RECONCILIATION, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.SUPERSEDED, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.RESTRICTIVE_OPERATION, PreflightDisposition.REQUEST_UNCONFIRMED),
        ("explicit", Prerequisite.OWNED_OPERATION, PreflightDisposition.REQUEST_UNCONFIRMED),
    ],
)
def test_preflight_native_failure_does_not_grant_explicit_return_replay(purpose, reason, expected):
    assert preflight_disposition(purpose, Pending(reason, "same diagnostic", OSError("unit cause"))) is expected
