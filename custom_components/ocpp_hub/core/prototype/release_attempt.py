"""Task-owned release facts retain possible positive effects across every later RPC."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
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
