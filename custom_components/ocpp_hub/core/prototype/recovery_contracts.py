"""Recovery prerequisites and effects are data; an observation never grants permission across an await."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ..frames import Frame
from .connection_scope import CloudAttemptScope
from .managed_intent import ControlIntent
from .store import ReleaseDeferredError


class Prerequisite(Enum):
    """Each pending duty has one domain owner and can coexist with other restrictions."""

    NATIVE = "native"
    SOURCE = "source"
    LIFECYCLE = "lifecycle"
    CLOUD = "cloud"
    CLOUD_ACK = "cloud_ack"
    ZERO_PROOF = "zero_proof"
    RECONCILIATION = "reconciliation"
    OWNER_RETURN = "owner_return"
    SUPERSEDED = "superseded"
    RESTRICTIVE_OPERATION = "restrictive_operation"
    OWNED_OPERATION = "owned_operation"


@dataclass(frozen=True)
class Allowed:
    """The current check passed; its caller must check again at the next effect boundary."""


@dataclass(frozen=True)
class Pending:
    """An ordinary prerequisite stopped this step before dispatch, retaining its original diagnostic cause."""

    reason: Prerequisite
    detail: str
    cause: BaseException | None = None


ALLOW = Allowed()
type GateDecision = Allowed | Pending
type Guard = Callable[[], GateDecision | None]


# Only legacy Frame/require APIs translate an ordinary pending prerequisite into their historical exception.
def require_allowed(decision: GateDecision | None) -> None:
    if isinstance(decision, Pending):
        raise ReleaseDeferredError(decision.detail) from decision.cause


@dataclass(frozen=True)
class Reply:
    """An actual correlated wire reply, including negative results and CALLERROR."""

    frame: Frame


@dataclass(frozen=True)
class NotSent:
    """This particular call did not reach dispatch; earlier calls in its operation remain independent."""

    pending: Pending


type CallResult = Reply | NotSent
type ResultCall = Callable[[str, dict[str, Any]], Awaitable[CallResult]]


@dataclass(frozen=True)
class CapPending:
    """Ceiling preparation or verification stopped; earlier Set effects remain in the owned operation record."""

    pending: Pending


# Preserve the public Frame API without adding another transport or applying policy at that compatibility edge.
def require_reply(result: CallResult) -> Frame:
    if isinstance(result, NotSent):
        require_allowed(result.pending)
        raise AssertionError("pending prerequisite did not stop the legacy call")
    return result.frame


@dataclass(frozen=True)
class AuthorityStamp:
    """Exact observed ownership identifies a result; it is never a reusable permission token."""

    intent: ControlIntent
    scope: CloudAttemptScope
    control_revision: int
    ticket: str | None


@dataclass(frozen=True)
class ReleaseHandoff:
    """The wire child owns final SQL and invokes the memory notification only after successful COMMIT."""

    write_in_transaction: Callable[[AuthorityStamp], AuthorityStamp | Pending]
    committed: Callable[[AuthorityStamp], None]


class WireReply(Enum):
    """A definite refusal differs from an absent reply even when both require owner reconciliation."""

    MISSING = "missing"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    ERROR = "error"


class FailureKind(Enum):
    """Technical boundary failures remain distinct; domain prerequisites use Pending instead."""

    SUPERSEDED = "superseded"
    TRANSPORT = "transport"
    STRICT = "strict"
    STORAGE = "storage"
    CONTROL = "control"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ReleaseEffects:
    """Dispatch may have reached the peer; verification proves its ceiling, not measured charging current."""

    positive_dispatched: bool
    positive_reply: WireReply
    positive_verified: bool
    zero_dispatched: bool
    zero_verified: bool


@dataclass(frozen=True)
class OwnedReleaseResult:
    """Only the owned child freezes its result after all required cleanup has finished."""

    authority: AuthorityStamp
    claimed: bool
    published: bool
    published_authority: AuthorityStamp | None
    effects: ReleaseEffects
    pending: Pending | None
    failure: BaseException | None
    initial_failure_kind: FailureKind | None
    failure_kind: FailureKind | None
    cleanup_failed: bool
    cancelled: bool


class RestoreState(Enum):
    """Restoration progress cannot be mistaken for permission to settle a domain duty."""

    SHARED = "shared"
    WAITING = "waiting"
    YIELDED = "yielded"
    REQUEST_NOT_SENT = "request_not_sent"


@dataclass(frozen=True)
class RestoreProgress:
    """Every domain caller checks shared before completing its own durable obligation."""

    state: RestoreState
    pending: Pending | None = None
    operation: OwnedReleaseResult | None = None

    # An incomplete operation always names its blocker, so a caller cannot silently treat missing progress as success.
    def __post_init__(self) -> None:
        if (self.state is RestoreState.SHARED) != (self.pending is None):
            raise ValueError("restoration progress must distinguish sharing from its pending prerequisite")

    # A waiting or superseding restrictive operation never certifies positive restoration.
    @property
    def shared(self) -> bool:
        return self.state is RestoreState.SHARED
