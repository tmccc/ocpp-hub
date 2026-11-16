"""Task-owned release facts retain possible positive effects across every later RPC."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from ..frames import Frame
from .connection_scope import CloudAttemptScope
from .managed_intent import ControlIntent
from .native_http import NativeTransportUnavailableError
from .rpc import UnavailableError
from .security import StaleEvidenceError
from .store import ReleaseDeferredError


# Only typed native availability causes permit this retry policy; strict and storage refusals stay distinct.
def native_availability(error: BaseException) -> bool:
    if type(error) is ReleaseDeferredError and error.__cause__ is not None:
        error = error.__cause__
    if type(error) is UnavailableError and error.__cause__ is not None:
        error = error.__cause__
    return isinstance(error, (NativeTransportUnavailableError, StaleEvidenceError))


class ReleaseOutcome(Enum):
    """One whole-operation result distinguishes safe zero waits from failures with unconfirmed effects."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    POSSIBLE_POSITIVE = "possible_positive"
    ZERO_UNCONFIRMED = "zero_unconfirmed"
    NATIVE_UNAVAILABLE = "native_unavailable"
    PREREQUISITE_PENDING = "prerequisite_pending"
    FAILED = "failed"


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
    positive_guard: Callable[[], None]
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

    # A later unsent read or confirmed zero cannot erase an earlier possibly dispatched positive mutation.
    def known_unsent(self) -> bool:
        return self.finished and not self.cancelled and not self.positive_may_have_dispatched

    # Callers consume one result after owned cleanup; the original verdict alone never proves unsent or zero.
    def outcome(self) -> ReleaseOutcome:
        if not self.finished:
            return ReleaseOutcome.PENDING
        if self.cancelled:
            return ReleaseOutcome.CANCELLED
        error = self.failure
        if error is None:
            return ReleaseOutcome.SUCCEEDED if self.positive_confirmed else ReleaseOutcome.FAILED
        if self.positive_may_have_dispatched or self.positive_confirmed:
            return ReleaseOutcome.POSSIBLE_POSITIVE
        if not self.zero_confirmed:
            return ReleaseOutcome.ZERO_UNCONFIRMED
        if error is not self.positive_failure:
            return ReleaseOutcome.FAILED
        if native_availability(error):
            return ReleaseOutcome.NATIVE_UNAVAILABLE
        if type(error) is ReleaseDeferredError and error.__cause__ is None:
            return ReleaseOutcome.PREREQUISITE_PENDING
        return ReleaseOutcome.FAILED
