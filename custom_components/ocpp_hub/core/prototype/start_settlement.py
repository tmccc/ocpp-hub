"""Pure Start failure settlement and a bounded observation of the existing child's actual boundaries."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

from .control_slice import step
from .control_slice_state import (
    AuthorityChanged,
    OutcomeCommitted,
    PortRetired,
    PublicationCommitted,
    PublicationFailed,
    SliceState,
    StartAnswered,
    StartDispatched,
    StartIdentity,
    StartOutcome,
    StartPrepared,
)
from .managed_intent import ControlIntent
from .recovery_contracts import AuthorityStamp, WireReply
from .start_handoff import StartBoundary, StartBoundaryObservation


class StartFailureAction(Enum):
    """Only the existing owner executes receipt writes or restrictive recovery."""

    UNRELATED = "unrelated"
    DIAGNOSTIC_ONLY = "diagnostic_only"
    FAIL_PREFLIGHT = "fail_preflight"
    STOP_REQUIRED = "stop_required"


@dataclass(frozen=True)
class StartFailureFacts:
    request_id: str
    expected_action: str
    record_id: str | None
    record_source: str | None
    record_state: str | None
    request_action: str | None
    request_state: str | None
    lease_current: bool
    effects_started: bool
    tag_digest: str | None


@dataclass(frozen=True)
class StartReceiptFacts:
    request_id: str | None
    tag_digest: str | None
    stage: str | None
    local: int | None


@dataclass(frozen=True)
class StartFailureDecision:
    action: StartFailureAction
    retire_prepared: bool = False


# Select settlement from exact current identities; initial zero work counts as effects even before RemoteStart.
def settle_start(facts: StartFailureFacts, receipt: StartReceiptFacts | None = None) -> StartFailureDecision:
    if (
        facts.record_id != facts.request_id
        or facts.record_source != "local"
        or facts.request_action != facts.expected_action
    ):
        return StartFailureDecision(StartFailureAction.UNRELATED)
    retire = bool(
        receipt is not None
        and receipt.request_id == facts.request_id
        and receipt.tag_digest == facts.tag_digest
        and receipt.stage == "prepared"
        and receipt.local is None
    )
    if (
        not facts.lease_current
        or facts.record_state not in ("armed", "starting", "awaiting_power", "charging", "running")
        or facts.request_state in ("completed", "failed")
    ):
        return StartFailureDecision(StartFailureAction.DIAGNOSTIC_ONLY, retire)
    action = StartFailureAction.STOP_REQUIRED if facts.effects_started else StartFailureAction.FAIL_PREFLIGHT
    return StartFailureDecision(action, retire)


@dataclass
class StartSettlementTrace:
    """One existing worker owns this optional in-memory observation; it has no tasks, writes or effect ports."""

    intent: ControlIntent
    state: SliceState | None = None
    current_authority: AuthorityStamp | None = None
    boundaries: tuple[StartBoundaryObservation, ...] = ()
    errors: tuple[str, ...] = ()

    # Fold exact child observations without allowing an experimental grammar error to alter physical completion.
    def observe(self, observation: StartBoundaryObservation) -> None:
        self.boundaries = (*self.boundaries[-7:], observation)
        try:
            self._observe(observation)
        except ValueError as error:
            self.errors = (*self.errors[-3:], str(error))

    # Translate only observed commits; a refused evidence COMMIT never becomes a stored Accepted event.
    def _observe(self, observation: StartBoundaryObservation) -> None:
        if observation.boundary is StartBoundary.PREPARED:
            context = observation.context
            if context is None or context.request_id is None:
                raise ValueError("managed Start observation needs its committed request context")
            stamp = AuthorityStamp(self.intent, context.scope, context.control_revision, context.ticket)
            identity = StartIdentity(context.ticket, context.request_id, context.tag_digest, stamp)
            self.state = step(SliceState(stamp), StartPrepared(identity)).state
            return
        state = self.state
        if state is None:
            return  # A refusal before the prepared COMMIT has no correlated child effect to fold.
        identifier = state.starts[0].identity.effect_id
        if observation.boundary is StartBoundary.DISPATCHED:
            state = step(state, StartDispatched(identifier)).state
        elif observation.boundary in (StartBoundary.REPLIED, StartBoundary.EVIDENCE_COMMITTED):
            outcome = {
                WireReply.ACCEPTED: StartOutcome.ACCEPTED,
                WireReply.REJECTED: StartOutcome.REJECTED,
                WireReply.ERROR: StartOutcome.ERROR,
            }.get(observation.reply)
            if outcome is None:
                raise ValueError("a real Start reply must name its actual classification")
            event = (
                StartAnswered(identifier, outcome)
                if observation.boundary is StartBoundary.REPLIED
                else OutcomeCommitted(identifier, outcome)
            )
            state = step(state, event).state
        elif observation.boundary is StartBoundary.PUBLICATION_COMMITTED:
            publication = observation.publication
            if publication is None:
                raise ValueError("publication observation needs its committed memory notification")
            stamp = replace(state.starts[0].identity.authority, intent=publication.intent)
            state = step(state, PublicationCommitted(identifier, stamp)).state
        elif observation.boundary is StartBoundary.RETIRED:
            result = observation.result
            if result is None:
                raise ValueError("retirement observation needs its finished owned result")
            record = state.start(identifier)
            if record.outcome in (StartOutcome.PREPARED, StartOutcome.ISSUED):
                outcome = StartOutcome.UNKNOWN if result.remote_start_dispatched else StartOutcome.NOT_SENT
                state = step(state, StartAnswered(identifier, outcome)).state
            record = state.start(identifier)
            if (
                record.outcome is StartOutcome.ACCEPTED
                and record.evidence_committed
                and record.publication is None
                and observation.failure_kind is not None
            ):
                state = step(state, PublicationFailed(identifier, observation.failure_kind)).state
            state = step(state, PortRetired(identifier)).state
        self.state = state

    # Observe current ownership at settlement; this is not a reconstruction of the owner's earlier event timing.
    def owner_observed(self, authority: AuthorityStamp | None) -> None:
        self.current_authority = authority
        if self.state is not None and authority is not None and self.state.authority != authority:
            try:
                self.state = step(self.state, AuthorityChanged(self.state.authority, authority)).state
            except ValueError as error:
                self.errors = (*self.errors[-3:], str(error))
