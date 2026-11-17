"""Immutable Start/Pause/Return observation records; no record grants wire permission."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from .recovery_contracts import AuthorityStamp, FailureKind, OwnedReleaseResult
from .recovery_policy import ReleaseDisposition


class StartOutcome(Enum):
    """Physical uncertainty is distinct from both a definite refusal and a command known unsent."""

    PREPARED = "prepared"
    ISSUED = "issued"
    NOT_SENT = "not_sent"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    ERROR = "error"
    UNKNOWN = "unknown"


class StartDebt(Enum):
    """Local closure and cloud delivery have independent evidence and lifetimes."""

    LIFECYCLE = "lifecycle"
    CLOUD_START = "cloud_start"
    CLOUD_STOP = "cloud_stop"


@dataclass(frozen=True)
class StartIdentity:
    """A retired attempt retains its exact request, effect, tag and original authority."""

    effect_id: str
    request_id: str
    tag_digest: str
    authority: AuthorityStamp


@dataclass(frozen=True)
class StartRecord:
    """Observed outcomes, committed evidence, publication and debts never share one completion flag."""

    identity: StartIdentity
    outcome: StartOutcome = StartOutcome.PREPARED
    observed_outcomes: frozenset[StartOutcome] = frozenset()
    committed_outcomes: frozenset[StartOutcome] = frozenset()
    publication: AuthorityStamp | None = None
    publication_failure: FailureKind | None = None
    debts: frozenset[StartDebt] = frozenset()
    local: int | None = None
    retired: bool = False

    # A delayed COMMIT acknowledgement for Unknown cannot certify a subsequently observed Accepted reply.
    @property
    def evidence_committed(self) -> bool:
        return self.outcome in self.committed_outcomes


@dataclass(frozen=True)
class ReturnRecord:
    """An actual owned Return result keeps its physical facts even when a newer owner wins."""

    request_id: str
    result: OwnedReleaseResult
    disposition: ReleaseDisposition


@dataclass(frozen=True)
class SliceState:
    """Only the experiment's caller replaces this aggregate; existing runtime ledgers remain authoritative."""

    authority: AuthorityStamp
    starts: tuple[StartRecord, ...] = ()
    returns: tuple[ReturnRecord, ...] = ()
    closing: bool = False

    # Find historical attempts by effect identity rather than borrowing the current operation slot.
    def start(self, effect_id: str) -> StartRecord:
        for record in self.starts:
            if record.identity.effect_id == effect_id:
                return record
        raise ValueError("unknown Start effect identity")

    # Debts may outlive every port; this is only a journal-user retirement condition, not a release permit.
    @property
    def ports_retired(self) -> bool:
        return all(record.retired for record in self.starts)


@dataclass(frozen=True)
class StartPrepared:
    identity: StartIdentity


@dataclass(frozen=True)
class StartDispatched:
    """The dispatch boundary may have sent; this fact alone does not prove reception by the peer."""

    effect_id: str


@dataclass(frozen=True)
class StartAnswered:
    effect_id: str
    outcome: StartOutcome


@dataclass(frozen=True)
class OutcomeCommitted:
    effect_id: str
    outcome: StartOutcome


@dataclass(frozen=True)
class PublicationCommitted:
    effect_id: str
    authority: AuthorityStamp


@dataclass(frozen=True)
class PublicationFailed:
    effect_id: str
    failure: FailureKind


@dataclass(frozen=True)
class AuthorityChanged:
    """The observer names both sides of a real committed owner/Boot/connection change."""

    previous: AuthorityStamp
    current: AuthorityStamp


@dataclass(frozen=True)
class LifecycleBound:
    effect_id: str
    local: int


@dataclass(frozen=True)
class LifecycleClosed:
    effect_id: str
    local: int
    native_closed: bool
    ledger_closed: bool


@dataclass(frozen=True)
class CloudSettled:
    """Each actual committed cloud acknowledgement names its action and already bound local identity."""

    effect_id: str
    local: int
    action: Literal["StartTransaction", "StopTransaction"]


@dataclass(frozen=True)
class PortRetired:
    effect_id: str


@dataclass(frozen=True)
class CloseStarted:
    pass


@dataclass(frozen=True)
class ReturnCompleted:
    """Return interpretation delegates to the existing production policy with actual owned operation facts."""

    request_id: str
    result: OwnedReleaseResult
    zero_ready: bool
    faulted: bool
    timer_superseded: bool = False
    publication_allowed: bool = True


type SliceEvent = (
    StartPrepared
    | StartDispatched
    | StartAnswered
    | OutcomeCommitted
    | PublicationCommitted
    | PublicationFailed
    | AuthorityChanged
    | LifecycleBound
    | LifecycleClosed
    | CloudSettled
    | PortRetired
    | CloseStarted
    | ReturnCompleted
)


@dataclass(frozen=True)
class RecordOutcome:
    effect_id: str
    outcome: StartOutcome


@dataclass(frozen=True)
class ConsiderPublication:
    """A candidate still requires the existing live guard and one atomic SQL handoff; it cannot send anything."""

    effect_id: str
    expected: AuthorityStamp


@dataclass(frozen=True)
class RestrictiveRecovery:
    """Ask the existing owner to resolve the duty; this grants neither profile nor Stop permission."""

    effect_id: str


type SliceEffect = RecordOutcome | ConsiderPublication | RestrictiveRecovery


@dataclass(frozen=True)
class SliceTransition:
    state: SliceState
    effects: tuple[SliceEffect, ...] = ()
