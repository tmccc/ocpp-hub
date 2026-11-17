"""Independent finite restore sequences and terminal outcomes, without native, SQL or wire execution."""

from itertools import product

import pytest
from ocpp_hub_core.prototype.recovery_contracts import Pending, Prerequisite, RestoreState
from ocpp_hub_core.prototype.recovery_policy import ReleaseDisposition
from ocpp_hub_core.prototype.restore_machine import RestoreStep, restore_progress

from tests.test_recovery_contracts import owned

EDGES = [
    (RestoreStep.READ_INITIAL, False, RestoreStep.CHECK_INITIAL),
    (RestoreStep.READ_INITIAL, True, RestoreStep.CHECK_INITIAL),
    (RestoreStep.CHECK_INITIAL, False, RestoreStep.VERIFY),
    (RestoreStep.CHECK_INITIAL, True, RestoreStep.RESUME),
    (RestoreStep.RESUME, False, RestoreStep.VERIFY),
    (RestoreStep.RESUME, True, RestoreStep.VERIFY),
    (RestoreStep.VERIFY, False, RestoreStep.RELEASE),
    (RestoreStep.VERIFY, True, RestoreStep.CLEAN_TIMER),
    (RestoreStep.CLEAN_TIMER, False, RestoreStep.RECHECK),
    (RestoreStep.CLEAN_TIMER, True, RestoreStep.RECHECK),
    (RestoreStep.RECHECK, False, RestoreStep.RELEASE),
    (RestoreStep.RECHECK, True, RestoreStep.RELEASE),
]


# Intent: only initial recovery and verified timer cleanup can add an optional effect to this attempt.
# Why: later observations must not repeat resume or timer cleanup after another owner changes durable state.
@pytest.mark.parametrize("step,needed,expected", EDGES)
def test_restore_successor_table(step, needed, expected):
    assert step.advance(needed=needed) is expected


# Intent: every ordinary prerequisite ends every pre-release phase with its original cause and no operation.
# Why: native/source waits cannot silently retry or manufacture an already-claimed release result.
@pytest.mark.parametrize("step", list(RestoreStep)[:-1])
@pytest.mark.parametrize("reason", list(Prerequisite))
@pytest.mark.parametrize("needed", [False, True])
def test_pending_ends_current_attempt(step, reason, needed):
    cause = TimeoutError("independent leaf failure")
    pending = Pending(reason, "original domain detail", cause)
    progress = step.advance(pending, needed=needed)
    assert progress.state is RestoreState.WAITING and not progress.shared
    assert progress.pending is pending and progress.pending.cause is cause
    assert progress.operation is None


# Intent: all four allowed paths perform initial proof, post-resume verification and any post-cleanup recheck once.
# Why: the transition core must never create an internal retry or recapture authority after timer cleanup.
@pytest.mark.parametrize("recovering,cleanup", list(product([False, True], repeat=2)))
def test_complete_sequences(recovering, cleanup):
    expected = [RestoreStep.READ_INITIAL, RestoreStep.CHECK_INITIAL]
    if recovering:
        expected.append(RestoreStep.RESUME)
    expected.append(RestoreStep.VERIFY)
    if cleanup:
        expected.extend([RestoreStep.CLEAN_TIMER, RestoreStep.RECHECK])
    expected.append(RestoreStep.RELEASE)
    actual, step = [], RestoreStep.READ_INITIAL
    for _ in range(8):
        actual.append(step)
        if step is RestoreStep.RELEASE:
            break
        needed = recovering if step is RestoreStep.CHECK_INITIAL else cleanup if step is RestoreStep.VERIFY else False
        step = step.advance(needed=needed)
    assert actual == expected and len(actual) == len(set(actual))


# Intent: release has no sequencer successor, even if supplied another prerequisite or optional-work flag.
# Why: actual owned-result policy and effects must settle release before any further attempt is admitted.
@pytest.mark.parametrize("pending", [None, Pending(Prerequisite.NATIVE, "late input")])
@pytest.mark.parametrize("needed", [False, True])
def test_release_cannot_restart(pending, needed):
    with pytest.raises(ValueError, match="owned result"):
        RestoreStep.RELEASE.advance(pending, needed=needed)


# Intent: terminal classification retains exact pending detail and operation identity without granting effects.
# Why: later Pause/Boot supersession is a yielded result, distinct from zero waiting and committed sharing.
@pytest.mark.parametrize(
    "disposition,state,reason,detail",
    [
        (
            ReleaseDisposition.STALE,
            RestoreState.YIELDED,
            Prerequisite.SUPERSEDED,
            "a subsequent owner event retired completed release authority",
        ),
        (
            ReleaseDisposition.YIELD,
            RestoreState.YIELDED,
            Prerequisite.RESTRICTIVE_OPERATION,
            "release yielded to its exact timer zero operation",
        ),
        (ReleaseDisposition.WAIT, RestoreState.WAITING, Prerequisite.NATIVE, "unit prerequisite"),
        (ReleaseDisposition.SHARED, RestoreState.SHARED, None, None),
    ],
)
@pytest.mark.parametrize("claimed", [False, True])
def test_terminal_progress(disposition, state, reason, detail, claimed):
    result = owned(claimed=claimed)
    progress = restore_progress(disposition, result)
    assert progress.state is state and progress.operation is result
    assert progress.shared is (state is RestoreState.SHARED)
    if reason is None:
        assert progress.pending is None
    else:
        assert progress.pending.reason is reason and progress.pending.detail == detail
        if disposition is ReleaseDisposition.WAIT:
            assert progress.pending is result.pending


# Intent: failure classification cannot become ordinary progress before the driver records its exact fault.
# Why: a failed or possibly applied positive release still requires diagnostic/uncertainty and guarded zero handling.
@pytest.mark.parametrize("disposition", [ReleaseDisposition.FAULT, None, "shared"])
def test_fault_requires_its_effect(disposition):
    with pytest.raises(ValueError, match="fault effect"):
        restore_progress(disposition, owned())
