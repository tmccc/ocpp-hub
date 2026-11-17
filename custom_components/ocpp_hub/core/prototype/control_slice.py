"""Pure, effect-free evidence/authority pilot; the standalone service does not load or execute this experiment."""

from __future__ import annotations

from dataclasses import replace

from .control_slice_state import (
    AuthorityChanged,
    CloseStarted,
    CloudSettled,
    ConsiderPublication,
    LifecycleBound,
    LifecycleClosed,
    OutcomeCommitted,
    PortRetired,
    PublicationCommitted,
    PublicationFailed,
    RecordOutcome,
    RestrictiveRecovery,
    ReturnCompleted,
    ReturnRecord,
    SliceEvent,
    SliceState,
    SliceTransition,
    StartAnswered,
    StartDebt,
    StartDispatched,
    StartOutcome,
    StartPrepared,
    StartRecord,
)
from .recovery_policy import release_disposition


# Replacing one immutable attempt cannot discard independently retained historical attempts.
def _updated(state: SliceState, record: StartRecord) -> SliceState:
    return replace(
        state,
        starts=tuple(record if item.identity.effect_id == record.identity.effect_id else item for item in state.starts),
    )


# Only actual terminal wire evidence can advance the outcome; timeout uncertainty may gain a real late reply.
def _answered(state: SliceState, event: StartAnswered) -> SliceTransition:
    record = state.start(event.effect_id)
    if event.outcome in (StartOutcome.PREPARED, StartOutcome.ISSUED):
        raise ValueError("an answer must distinguish unsent, an actual reply and unknown delivery")
    if event.outcome is StartOutcome.NOT_SENT:
        if record.outcome not in (StartOutcome.PREPARED, StartOutcome.NOT_SENT):
            raise ValueError("a possible dispatch cannot become unsent")
    elif record.outcome not in (StartOutcome.ISSUED, StartOutcome.UNKNOWN, event.outcome):
        raise ValueError("a reply requires dispatch and cannot replace a different actual reply")
    if record.outcome is event.outcome:
        return SliceTransition(state)
    debts = record.debts
    if event.outcome in (StartOutcome.REJECTED, StartOutcome.NOT_SENT) and record.local is None:
        debts = frozenset()
    updated = replace(
        record,
        outcome=event.outcome,
        observed_outcomes=record.observed_outcomes | {event.outcome},
        debts=debts,
    )
    effects = (RecordOutcome(event.effect_id, event.outcome),)
    if event.outcome is not StartOutcome.ACCEPTED or state.authority != record.identity.authority or state.closing:
        return SliceTransition(_updated(state, updated), (*effects, RestrictiveRecovery(event.effect_id)))
    return SliceTransition(_updated(state, updated), effects)


# Every event is a boundary fact, not a command or a substitute for the executor's final live permission check.
def step(state: SliceState, event: SliceEvent) -> SliceTransition:
    if isinstance(event, StartPrepared):
        identity = event.identity
        if not identity.effect_id or not identity.request_id or not identity.tag_digest:
            raise ValueError("Start identity must be explicit")
        if state.closing or identity.authority != state.authority:
            raise ValueError("a new Start needs the current non-closing owner")
        if any(
            item.identity.effect_id == identity.effect_id
            or item.identity.request_id == identity.request_id
            or not item.retired
            or item.debts
            for item in state.starts
        ):
            raise ValueError("a new Start cannot replace a live or indebted attempt")
        return SliceTransition(replace(state, starts=(*state.starts, StartRecord(identity))))
    if isinstance(event, AuthorityChanged):
        if event.previous != state.authority or event.current.intent.revision < event.previous.intent.revision:
            raise ValueError("an outdated authority observation cannot overwrite the current owner")
        return SliceTransition(replace(state, authority=event.current))
    if isinstance(event, CloseStarted):
        return SliceTransition(replace(state, closing=True))
    if isinstance(event, ReturnCompleted):
        disposition = release_disposition(
            "explicit",
            event.result,
            state.authority,
            zero_ready=event.zero_ready,
            faulted=event.faulted,
            timer_superseded=event.timer_superseded,
            publication_allowed=event.publication_allowed and not state.closing,
        )
        returned = ReturnRecord(event.request_id, event.result, disposition)
        return SliceTransition(replace(state, returns=(*state.returns, returned)))
    if isinstance(event, StartAnswered):
        return _answered(state, event)

    record = state.start(event.effect_id)
    if isinstance(event, StartDispatched):
        if record.outcome is not StartOutcome.PREPARED or record.retired:
            raise ValueError("a Start effect cannot be dispatched twice")
        # A delayed observation remains physical truth even if a newer owner has already revoked permission.
        record = replace(record, outcome=StartOutcome.ISSUED, debts=frozenset(StartDebt))
    elif isinstance(event, OutcomeCommitted):
        if event.outcome in (StartOutcome.PREPARED, StartOutcome.ISSUED):
            raise ValueError("outcome storage requires an observed terminal result")
        if event.outcome not in record.observed_outcomes:
            raise ValueError("outcome storage cannot manufacture a different observed reply")
        if event.outcome in record.committed_outcomes:
            return SliceTransition(state)
        record = replace(record, committed_outcomes=record.committed_outcomes | {event.outcome})
        if event.outcome is StartOutcome.ACCEPTED:
            effect = (
                ConsiderPublication(event.effect_id, record.identity.authority)
                if state.authority == record.identity.authority and not state.closing and not record.retired
                else RestrictiveRecovery(event.effect_id)
            )
            return SliceTransition(_updated(state, record), (effect,))
    elif isinstance(event, PublicationCommitted):
        expected = record.identity.authority
        if record.outcome is not StartOutcome.ACCEPTED or not record.evidence_committed:
            raise ValueError("authority publication requires separately committed actual acceptance")
        if (
            event.authority.scope != expected.scope
            or event.authority.ticket != expected.ticket
            or event.authority.control_revision != expected.control_revision
            or event.authority.intent.mode != "app"
            or event.authority.intent.revision != expected.intent.revision + 1
            or record.publication not in (None, event.authority)
        ):
            raise ValueError("publication must identify the exact original handoff")
        if state.authority != expected and (
            state.authority.intent.revision < event.authority.intent.revision
            or state.authority.intent.revision == event.authority.intent.revision
            and state.authority.intent != event.authority.intent
        ):
            raise ValueError("historical publication conflicts with the committed owner revision")
        record = replace(record, publication=event.authority, publication_failure=None)
        if state.authority == expected and not state.closing:
            state = replace(state, authority=event.authority)
    elif isinstance(event, PublicationFailed):
        if record.outcome is not StartOutcome.ACCEPTED or not record.evidence_committed:
            raise ValueError("final publication failure requires separately committed actual acceptance")
        if record.publication is not None:
            raise ValueError("a committed publication cannot become a failed publication")
        record = replace(record, publication_failure=event.failure)
        return SliceTransition(_updated(state, record), (RestrictiveRecovery(event.effect_id),))
    elif isinstance(event, LifecycleBound):
        if record.outcome in (StartOutcome.PREPARED, StartOutcome.NOT_SENT):
            raise ValueError("an unissued Start cannot own a lifecycle")
        if type(event.local) is not int or event.local <= 0 or record.local not in (None, event.local):
            raise ValueError("lifecycle evidence must keep its exact local transaction identity")
        record = replace(
            record,
            local=event.local,
            debts=record.debts | frozenset(StartDebt) if record.local is None else record.debts,
        )
    elif isinstance(event, LifecycleClosed):
        if record.local is None or event.local != record.local:
            raise ValueError("closure cannot settle a different local transaction")
        if event.native_closed and event.ledger_closed:
            record = replace(record, debts=record.debts - {StartDebt.LIFECYCLE})
    elif isinstance(event, CloudSettled):
        if record.local is None or event.local != record.local:
            raise ValueError("cloud acknowledgement cannot settle a different local transaction")
        if event.action not in ("StartTransaction", "StopTransaction"):
            raise ValueError("cloud acknowledgement must identify its exact lifecycle action")
        debt = StartDebt.CLOUD_START if event.action == "StartTransaction" else StartDebt.CLOUD_STOP
        record = replace(record, debts=record.debts - {debt})
    elif isinstance(event, PortRetired):
        if record.outcome in (StartOutcome.PREPARED, StartOutcome.ISSUED):
            raise ValueError("retirement must first retain unsent or unknown outcome evidence")
        record = replace(record, retired=True)
    else:
        raise AssertionError("unhandled control slice event")
    return SliceTransition(_updated(state, record))
