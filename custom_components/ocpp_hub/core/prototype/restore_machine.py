"""Pure per-attempt restore sequencing; phases never grant permission to execute an effect."""

from __future__ import annotations

from enum import Enum

from .recovery_contracts import OwnedReleaseResult, Pending, Prerequisite, RestoreProgress, RestoreState
from .recovery_policy import ReleaseDisposition


class RestoreStep(Enum):
    READ_INITIAL = "read_initial"
    CHECK_INITIAL = "check_initial"
    RESUME = "resume"
    VERIFY = "verify"
    CLEAN_TIMER = "clean_timer"
    RECHECK = "recheck"
    RELEASE = "release"

    # Pending ends this attempt; needed selects only this phase's optional resume or timer cleanup.
    def advance(self, pending: Pending | None = None, *, needed: bool = False) -> RestoreStep | RestoreProgress:
        if self is RestoreStep.RELEASE:
            raise ValueError("release completion belongs to its owned result policy")
        if pending is not None:
            return RestoreProgress(RestoreState.WAITING, pending)
        if needed and self is RestoreStep.CHECK_INITIAL:
            return RestoreStep.RESUME
        if needed and self is RestoreStep.VERIFY:
            return RestoreStep.CLEAN_TIMER
        return _NEXT[self]


_NEXT = {
    RestoreStep.READ_INITIAL: RestoreStep.CHECK_INITIAL,
    RestoreStep.CHECK_INITIAL: RestoreStep.VERIFY,
    RestoreStep.RESUME: RestoreStep.VERIFY,
    RestoreStep.VERIFY: RestoreStep.RELEASE,
    RestoreStep.CLEAN_TIMER: RestoreStep.RECHECK,
    RestoreStep.RECHECK: RestoreStep.RELEASE,
}


# Map the existing owned-result policy to progress; SQL acknowledgement and fault effects stay with the driver.
def restore_progress(disposition: ReleaseDisposition, result: OwnedReleaseResult) -> RestoreProgress:
    if disposition is ReleaseDisposition.STALE:
        pending = Pending(Prerequisite.SUPERSEDED, "a subsequent owner event retired completed release authority")
    elif disposition is ReleaseDisposition.YIELD:
        pending = Pending(Prerequisite.RESTRICTIVE_OPERATION, "release yielded to its exact timer zero operation")
    elif disposition is ReleaseDisposition.WAIT:
        return RestoreProgress(RestoreState.WAITING, result.pending, result)
    elif disposition is ReleaseDisposition.SHARED:
        return RestoreProgress(RestoreState.SHARED, operation=result)
    else:
        raise ValueError("a failed release requires its fault effect")
    return RestoreProgress(RestoreState.YIELDED, pending, result)
