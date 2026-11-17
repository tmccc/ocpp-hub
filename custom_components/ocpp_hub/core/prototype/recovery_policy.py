"""One pure recovery policy selects work and interprets owned results without inspecting exception chains."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from .recovery_contracts import AuthorityStamp, FailureKind, OwnedReleaseResult, Pending, Prerequisite, WireReply


class PreflightDisposition(Enum):
    """Return can wait for named prerequisites before ownership; other refusals end the accepted request."""

    WAIT = "wait"
    REQUEST_UNCONFIRMED = "request_unconfirmed"


# Only the named source, cleanup and actual acknowledgement waits retain the same explicit Return request.
def preflight_disposition(purpose: Literal["automatic", "explicit"], pending: Pending) -> PreflightDisposition:
    if purpose == "automatic" or pending.reason in (
        Prerequisite.SOURCE,
        Prerequisite.CLOUD_ACK,
        Prerequisite.TIMER_CLEANUP,
        Prerequisite.OWNER_RETURN,
    ):
        return PreflightDisposition.WAIT
    return PreflightDisposition.REQUEST_UNCONFIRMED


class ReleaseDisposition(Enum):
    """Successful sharing, ordinary waiting and failure are separate terminal decisions."""

    SHARED = "shared"
    WAIT = "wait"
    FAULT = "fault"
    YIELD = "yield"
    STALE = "stale"


# Whole-operation effects and current authority determine retry; neither source kind nor error wording does.
def release_disposition(
    purpose: Literal["automatic", "explicit"],
    result: OwnedReleaseResult,
    current: AuthorityStamp | None,
    *,
    zero_ready: bool,
    faulted: bool,
    timer_superseded: bool,
    publication_allowed: bool = True,
) -> ReleaseDisposition:
    effects = result.effects
    if (
        not result.claimed
        and result.pending is not None
        and result.failure is None
        and not result.cancelled
        and not result.cleanup_failed
        and not effects.positive_dispatched
        and not effects.positive_verified
        and not effects.zero_dispatched
        and not effects.zero_verified
        and current == result.authority
        and zero_ready
        and not faulted
    ):
        return ReleaseDisposition.WAIT
    if (
        result.claimed
        and result.published
        and result.published_authority is not None
        and not result.cancelled
        and not result.cleanup_failed
        and result.failure is None
        and result.pending is None
        and effects.positive_dispatched
        and effects.positive_reply is WireReply.ACCEPTED
        and effects.positive_verified
        and not effects.zero_dispatched
        and not effects.zero_verified
    ):
        return (
            ReleaseDisposition.SHARED
            if current == result.published_authority and not faulted and publication_allowed
            else ReleaseDisposition.STALE
        )
    if (
        timer_superseded
        and purpose == "automatic"
        and not effects.positive_dispatched
        and not effects.positive_verified
        and not result.cancelled
        and not faulted
        and result.initial_failure_kind is FailureKind.SUPERSEDED
        and result.failure_kind is FailureKind.SUPERSEDED
    ):
        return ReleaseDisposition.YIELD
    if (
        purpose == "automatic"
        and result.claimed
        and result.pending is not None
        and result.failure is None
        and not result.cancelled
        and not result.cleanup_failed
        and not effects.positive_dispatched
        and not effects.positive_verified
        and effects.zero_verified
        and current == result.authority
        and result.authority.ticket is not None
        and zero_ready
        and not faulted
    ):
        return ReleaseDisposition.WAIT
    return ReleaseDisposition.FAULT


class RecoveryWork(Enum):
    """Domain owners retain their ledgers; this ordering is shared by foreground recovery."""

    CURRENT = "current"
    EXPIRED_START = "expired_start"
    TIMER_DEADLINE = "timer_deadline"
    RETURN = "return"
    PAUSE = "pause"
    ORDINARY = "ordinary"
    CONNECTION = "connection"


@dataclass(frozen=True)
class RecoveryObligation:
    """Simultaneous obligations retain their distinct durable identities instead of overwriting one global phase."""

    work: RecoveryWork
    identity: str


@dataclass(frozen=True)
class RecoveryFacts:
    """A short-lived domain projection chooses the next step and must be rebuilt after every awaited action."""

    obligations: tuple[RecoveryObligation, ...]
    paused: bool


# Fixed precedence is explicit and testable; completion here means only visited during this bounded poll.
def next_recovery_work(facts: RecoveryFacts, visited: frozenset[RecoveryWork]) -> RecoveryWork | None:
    present = {item.work for item in facts.obligations}
    for work in RecoveryWork:
        if work not in present or work in visited:
            continue
        if work in (RecoveryWork.ORDINARY, RecoveryWork.CONNECTION) and facts.paused:
            continue
        if work is RecoveryWork.PAUSE and not facts.paused:
            continue
        if work is RecoveryWork.CONNECTION and RecoveryWork.ORDINARY in present:
            continue
        return work
    return None
