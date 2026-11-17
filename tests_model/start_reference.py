"""Preserved pilot observations for reference tests; production uses the persisted Start lifecycle owner."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

from ocpp_hub_core.prototype.managed_intent import ControlIntent
from ocpp_hub_core.prototype.recovery_contracts import AuthorityStamp, FailureKind, WireReply
from ocpp_hub_core.prototype.start_handoff import OwnedStartResult, StartHandoffContext, StartPublication

from tests_model.control_slice import step
from tests_model.control_slice_state import (
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


class StartBoundary(Enum):
    """Observers run synchronously after the actual boundary, within the same owned child."""

    PREPARED = "prepared"
    DISPATCHED = "dispatched"
    REPLIED = "replied"
    EVIDENCE_COMMITTED = "evidence_committed"
    PUBLICATION_COMMITTED = "publication_committed"
    RETIRED = "retired"


@dataclass(frozen=True)
class StartBoundaryObservation:
    boundary: StartBoundary
    context: StartHandoffContext | None = None
    reply: WireReply = WireReply.MISSING
    publication: StartPublication | None = None
    result: OwnedStartResult | None = None
    failure_kind: FailureKind | None = None


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
