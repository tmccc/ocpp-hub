"""Task-owned release facts retain possible positive effects across every later RPC."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal

from ..frames import Frame
from .connection_scope import CloudAttemptScope
from .managed_intent import ControlIntent
from .recovery_contracts import (
    AuthorityStamp,
    FailureKind,
    Guard,
    OwnedReleaseResult,
    Pending,
    ReleaseEffects,
    WireReply,
)


@dataclass(frozen=True)
class TimerRestriction:
    """An actual owned timer zero claimed this exact newer ticket; physical confirmation remains separate."""

    scope: CloudAttemptScope
    control_revision: int
    intent: ControlIntent
    old_ticket: str
    new_ticket: str
    occurrence: str
    identity_digest: str
    allowed_local: tuple[int | None, ...]
    operation: asyncio.Task[Frame]


@dataclass
class ReleaseAttempt:
    """Only a finished owned operation can establish that its positive ceiling was never dispatched."""

    purpose: Literal["automatic", "explicit"]
    intent: ControlIntent
    scope: CloudAttemptScope
    control_revision: int
    positive_guard: Guard
    ticket: str | None = None
    positive_may_have_dispatched: bool = False
    positive_confirmed: bool = False
    zero_may_have_dispatched: bool = False
    zero_confirmed: bool = False
    finished: bool = False
    cancelled: bool = False
    failure: BaseException | None = None
    positive_failure: BaseException | None = None
    restriction: TimerRestriction | None = None
    pending: Pending | None = None
    positive_reply: WireReply = WireReply.MISSING
    cleanup_failed: bool = False
    before_ticket: str | None = None
    initial_failure_kind: FailureKind | None = None
    failure_kind: FailureKind | None = None
    published: bool = False
    published_authority: AuthorityStamp | None = None

    # Freeze actual owned facts after cleanup; current authority is checked independently by the coordinator.
    def result(self) -> OwnedReleaseResult:
        if not self.finished:
            raise RuntimeError("owned release has not finished")
        return OwnedReleaseResult(
            AuthorityStamp(self.intent, self.scope, self.control_revision, self.ticket or self.before_ticket),
            self.ticket is not None,
            self.published,
            self.published_authority,
            ReleaseEffects(
                self.positive_may_have_dispatched,
                self.positive_reply,
                self.positive_confirmed,
                self.zero_may_have_dispatched,
                self.zero_confirmed,
            ),
            self.pending,
            self.failure,
            self.initial_failure_kind,
            self.failure_kind,
            self.cleanup_failed,
            self.cancelled,
        )

    # A later unsent read or confirmed zero cannot erase an earlier possibly dispatched positive mutation.
    def known_unsent(self) -> bool:
        return (
            self.finished
            and not self.cancelled
            and not self.positive_may_have_dispatched
            and not self.positive_confirmed
        )
