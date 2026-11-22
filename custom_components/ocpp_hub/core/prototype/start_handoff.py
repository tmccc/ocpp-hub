"""A truthful Start reply and its atomic authority publication remain independent owned facts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from ..frames import Frame
from .connection_scope import CloudAttemptScope
from .managed_intent import ControlIntent


@dataclass(frozen=True)
class NativeStartReport:
    """A held lifecycle report, with an optional verified native source, committed with the actual transaction."""

    native_id: int | None
    plan_id: int | None
    attempt: tuple[str, int] | None
    record: Callable[[int], None] = field(compare=False)


@dataclass(frozen=True)
class StartHandoffContext:
    """The publisher joins this actual accepted request and current claim, never a guessed Start identity."""

    request_id: str | None
    ticket: str
    scope: CloudAttemptScope
    control_revision: int
    tag_digest: str


@dataclass(frozen=True)
class StartPublication:
    """Memory publication follows successful COMMIT with no intervening await."""

    request_id: str
    intent: ControlIntent
    epoch: int


@dataclass(frozen=True)
class StartHandoff:
    """The child owns the transaction; the domain supplies synchronous SQL and an infallible memory notification."""

    write_in_transaction: Callable[[StartHandoffContext], StartPublication]
    committed: Callable[[StartPublication], None]


@dataclass(frozen=True)
class OwnedStartResult:
    """Actual remote acceptance survives a failed authority COMMIT and even a failed restrictive cleanup."""

    remote_start_dispatched: bool
    answer: Frame | None
    publication: StartPublication | None
    zero_verified: bool
    failure: BaseException | None

    # Frame compatibility belongs only at the old edge; managed parents consume the explicit result.
    def require_reply(self) -> Frame:
        if self.failure is not None:
            raise self.failure
        if self.answer is None:
            raise RuntimeError("owned Start has no actual reply")
        return self.answer


@dataclass
class StartAttempt:
    """Only the shielded child records its wire facts, cleanup and successful publication."""

    remote_start_dispatched: bool = False
    answer: Frame | None = None
    publication: StartPublication | None = None
    zero_verified: bool = False
    failure: BaseException | None = None
    finished: bool = False

    # A parent cannot freeze partial child evidence into a terminal outcome.
    def result(self) -> OwnedStartResult:
        if not self.finished:
            raise RuntimeError("owned Start has not finished")
        return OwnedStartResult(
            self.remote_start_dispatched, self.answer, self.publication, self.zero_verified, self.failure
        )
