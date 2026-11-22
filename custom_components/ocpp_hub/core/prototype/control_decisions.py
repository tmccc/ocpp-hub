"""The shared decision owner for Start, Pause, Stop and protective charging control."""

from __future__ import annotations

import asyncio
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Literal, Protocol

from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .command_processor import CommandPriority
from .connection_scope import ControlScope, control_scope_matches, current_control_scope, current_scope
from .control_state import ControlState
from .current_policy import (
    CurrentSelection,
    attempt_key,
    current_refusal,
    current_session_decision,
    current_source_decision,
    original_current_plan,
)
from .managed_facts import ManagedFacts
from .managed_intent import (
    ControlIntent,
    ManagedRequests,
    PauseRequest,
    ReturnSettlement,
    request_pending,
    write_intent,
)
from .managed_records import CurrentFailure
from .managed_release_wait import awaiting_known_stops, awaiting_observations
from .managed_settlement import (
    charge_test_candidate,
    complete_record,
    current_candidate,
    internal_pause_matches,
    require_record,
    require_settled_debt,
)
from .managed_sources import awaiting_owner_start, recorded_station_profile
from .native_bounded import NativeReadinessError, readiness_decision
from .native_faults import STATION_CEILING_LOST, _NativeMeasurementError
from .native_http import NativeTransportUnavailableError
from .native_start_attempts import matching_unassigned_start
from .policy import DeniedError, integer, number
from .recovery_contracts import (
    ALLOW,
    AuthorityStamp,
    CapPending,
    GateDecision,
    Guard,
    OwnedReleaseResult,
    Pending,
    Prerequisite,
    ReleaseHandoff,
    Reply,
    RestoreProgress,
    RestoreState,
    require_allowed,
)
from .recovery_control import RecoveryControl
from .recovery_policy import ReleaseDisposition, release_disposition, restore_progress
from .release_attempt import ReleaseAttempt
from .rpc import UnavailableError
from .schedule_control import ScheduleControl
from .security import SecurityError, StaleEvidenceError
from .start_handoff import NativeStartReport, OwnedStartResult, StartHandoff, StartHandoffContext, StartPublication
from .start_settlement import RetirementCandidate, StartFailureAction, StartFailureFacts, settle_start
from .store import RecoveryRequiredError, StationZeroStorageError, encoded, fingerprint
from .trial_evidence import Resource, profile_projection

if TYPE_CHECKING:
    from .gateway import Gateway
    from .local_control import LocalControl
    from .managed import ManagedController
    from .managed_charging import ChargeLease, ManagedCharging
    from .managed_current import ManagedCurrentLimits
    from .managed_records import TerminalCurrent
    from .native_bounded_handover import NativeBoundedHandover
    from .recovery_coordinator import RecoveryCoordinator
    from .trial_handover_evidence import HandoverEvidence


type ProveStop = Callable[[int, bool, bool], Awaitable[StopProof]]
ACTIVE = frozenset({"armed", "starting", "awaiting_power", "charging", "running", "stopping"})


@dataclass(frozen=True)
class StopProof:
    """Independent evidence required to Stop an exact target when zero is in doubt."""

    check: Callable[[], None]
    flowing: Callable[[], bool] | None = None


@dataclass
class PauseStopClaim:
    """The exact Pause, charger and session that the foreground Stop may act on."""

    intent: ControlIntent
    request: PauseRequest | None
    local: int | None
    connection: ControlScope | None
    stop_epoch: int
    previous_ticket: object
    ticket: str | None = None


@dataclass(frozen=True)
class ExpiredStartStop:
    """The exact expired Start and observation that may still need a protective Stop."""

    local: int
    intent: ControlIntent
    identifier: object
    tag_digest: object
    connection: ControlScope | None
    native: HandoverEvidence
    sequence: int


class StopZeroEvidence(Protocol):
    """Independent zero observations for reusing a previous charger Stop reply."""

    # Refresh only when a prior Stop may already cover this session.
    async def refresh_stop_zero(self, local: int) -> None: ...

    # Report whether the latest evidence still covers the exact open session at zero.
    def stop_zero_verified(self, local: int) -> bool: ...


class StopDecision(Enum):
    """A Stop can use an exact acknowledgement, wait for its report, or need a physical send."""

    SEND = auto()
    ACCEPTED = auto()
    WAIT_FOR_REPORT = auto()
    CLOSED = auto()


class VendorStopAlreadyAcceptedError(Exception):
    """The exact Stop is already accepted and the same session still has verified zero."""


class StopReplyUnconfirmedError(UnavailableError):
    """The station confirmed zero but the transaction Stop reply did not establish acceptance."""


class ControlDecisions:
    """Make charging decisions while the existing executors own device and cloud work."""

    # Attach the decision owner to the local executor's existing journal and peer.
    def __init__(self, control: LocalControl) -> None:
        self.control, self.journal, self.peer = control, control.journal, control.peer
        self._protective_stop: tuple[int, str | None, int, str] | None = None
        self.schedule = ScheduleControl(self)
        self.recovery = RecoveryControl(self)
        self.shutdown_requested = False
        self._fault_zero_retry: dict[str, float] = {}
        self._start_renewals: dict[tuple[str, int, int, str | None, str | None], asyncio.Future[bool]] = {}

    # Admit an explicit Return without giving it authority to send or leaving Pause.
    def accept_return(self, controller: ManagedController, identifier: str, revision: int) -> dict[str, Any]:
        previous = controller.requests.existing(identifier, "app_control", revision)
        if previous is not None:
            return previous
        if controller.charging.return_blocked():
            raise RecoveryRequiredError("return awaits timed test settlement")
        if controller.current.pending():
            raise RecoveryRequiredError("return awaits the current-setting outcome")
        if self.journal.fault.latched():
            raise RecoveryRequiredError("a latched fault requires independent operator reconciliation")
        if controller.requests.intent().mode != "paused":
            raise RecoveryRequiredError("return requires a current paused intent")
        return controller.requests.accept(identifier, "app_control", revision)

    # Keep resource observations and their zero verdict together.
    def release_zero_decision(
        self, controller: ManagedController, native: HandoverEvidence, allow_offline: bool, continuing_local: int | None
    ) -> GateDecision:
        from .trial_rollback import lifecycle_decision

        s, g, j = controller.session, controller.session.gateway, self.journal
        controller.native.require(native)
        decision = readiness_decision(native, continuing_local is None, controller.native.allow_unplugged)
        if isinstance(decision, Pending):
            return decision
        if continuing_local is not None:
            decision = controller.continuation_decision(native, continuing_local)
            if isinstance(decision, Pending):
                return decision
            if native.output_limit_a != 0 or any(v != 0 for v in native.currents_a + native.powers_w):
                return Pending(Prerequisite.LIFECYCLE, "the identified original session awaits a verified zero hold")
        decision = controller.native.session_decision(native)
        if isinstance(decision, Pending):
            return decision
        present = s.book.owned_present(native.physical)
        pending_stop = continuing_local is None and awaiting_known_stops(j)
        if not pending_stop:
            decision = lifecycle_decision(
                g,
                continuing_local=continuing_local,
                preserve_expired_start=controller.charging.preserve_debt(native, continuing_local),
            )
            if isinstance(decision, Pending):
                return decision
        pending_delivery = pending_stop or (not allow_offline and awaiting_observations(j))
        refusal = self._release_zero_refusal(
            controller, present, allow_offline=allow_offline, pending_delivery=pending_delivery
        )
        if refusal is not None:
            return Pending(Prerequisite.ZERO_PROOF, refusal)
        if pending_delivery or g.vendor._pending:
            # The original Return can wait without claiming a release or dispatching a positive ceiling.
            return Pending(Prerequisite.CLOUD_ACK, "shared control awaits the current cloud acknowledgement")
        return ALLOW

    # Release needs an owned zero receipt and no conflicting work after the native and lifecycle checks.
    def _release_zero_refusal(
        self,
        controller: ManagedController,
        present: list[Resource],
        *,
        allow_offline: bool,
        pending_delivery: bool,
    ) -> str | None:
        s, g, j = controller.session, controller.session.gateway, self.journal
        facts = ManagedFacts(controller)
        if not j.phase.read().operating:
            return "installation is not operating"
        if j.get("native_bounded_handover", {}).get("stage") != "admitted":
            return "native handover is not admitted"
        if j.fault.latched():
            return "managed fault is latched"
        if not facts.boot_current():
            return "current Boot is unconfirmed"
        if not facts.introduction_current():
            return "current introduction is unconfirmed"
        if g.charger.sender is None:
            return "charger sender is unavailable"
        if not allow_offline and (
            g.vendor.sender is None or g.vendor_registration != "Accepted" or not s.runtime.status.vendor_connected
        ):
            return "cloud registration is unavailable"
        if not j.get("control", {}).get("inhibit"):
            return "local inhibit is missing"
        if (refusal := controller.native.station_zero_refusal(present)) is not None:
            return refusal
        if not allow_offline and not pending_delivery and not facts.cloud_queue_empty():
            return "cloud delivery is pending"
        if g.controls._operations:
            return "control operation is pending"
        if g.charger._pending:
            return "charger RPC is pending"
        if g.vendor_write_evidence().unresolved:
            return "vendor command is uncertain"
        return None

    # One accepted return receives one guarded attempt; uncertainty never changes persisted paused intent to app.
    async def run_requested_return(self, controller: ManagedController, identifier: str, revision: int) -> None:
        expected = controller.commands.decide(lambda: controller.requests.check_return_revision(identifier, revision))
        if expected is None:
            return
        if controller.charging.return_blocked():
            return  # An already accepted Return survives until observation completes the timed test under Pause.
        j = self.journal
        control = j.get("control", {})
        if control.get("uncertain") and controller.requests.stop_owns_ticket(control.get("ticket")):
            # Verify this exact protective Stop before treating Return as a positive release attempt.
            try:
                await controller.maintain_pause()
            except RecoveryRequiredError, UnavailableError, TimeoutError:
                return  # Closed zero and selective cleanup can finish while the same Return remains accepted.
            control = j.get("control", {})
            if controller.requests.intent() != expected or control.get("uncertain"):
                return
        if controller.commands.decide(lambda: controller.requests.return_waits_for_pause(expected)):
            return  # The same Return survives the real Start and both closure reports under its Pause.
        try:
            await controller.recovery.restore(expected, return_request=identifier, claim_return=True)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            return  # The owned completion retains the exact request disposition when its caller disappears.

    # The same decision handles the first write and any retained storage retry.
    def write_return_settlement(
        self, coordinator: RecoveryCoordinator, settlement: ReturnSettlement, retry: bool
    ) -> None:
        try:
            requests = coordinator.c.requests
            if not retry or requests.return_current(settlement.identifier, settlement.intent):
                requests.settle_return(
                    settlement.identifier,
                    settlement.intent,
                    failure=settlement.failure,
                    await_cloud=settlement.await_cloud,
                )
        except sqlite3.Error:
            coordinator._return_settlement = settlement
        else:
            coordinator._return_settlement = None

    # Claim the accepted Return before the worker starts and leave busy requests accepted.
    def claim_restore_request(
        self,
        coordinator: RecoveryCoordinator,
        expected_intent: ControlIntent | None,
        return_request: str | None,
        claim_return: bool,
        offline_pause: bool,
    ) -> ControlIntent | RestoreProgress:
        if coordinator.c.stopping:
            return RestoreProgress(
                RestoreState.WAITING, Pending(Prerequisite.SUPERSEDED, "managed retirement retains its hold")
            )
        if coordinator.pending():
            return RestoreProgress(
                RestoreState.WAITING,
                Pending(Prerequisite.OWNED_OPERATION, "the current owned recovery completion remains pending"),
            )
        expected = expected_intent or coordinator.c.requests.intent()
        if offline_pause and coordinator.c.requests.return_in_progress():
            return RestoreProgress(
                RestoreState.WAITING, Pending(Prerequisite.OWNED_OPERATION, "an explicit Return already owns recovery")
            )
        # The request driver claims running; the existing direct restore API retains accepted until publication.
        if claim_return and return_request is not None:
            try:
                claimed = coordinator.c.requests.return_started(return_request, expected)
            except sqlite3.Error:
                return RestoreProgress(
                    RestoreState.WAITING, Pending(Prerequisite.RECONCILIATION, "Return awaits journal storage")
                )
            if not claimed:
                return RestoreProgress(
                    RestoreState.WAITING, Pending(Prerequisite.SUPERSEDED, "Return no longer owns its accepted request")
                )
        return expected

    # Bind release to its original request, cloud connection and permissible source across physical reads.
    def release_guard(
        self,
        c: ManagedController,
        expected_intent: ControlIntent,
        continuing_local: int | None,
        return_request: str | None,
        offline_pause: bool,
    ) -> Guard:
        g, j = c.session.gateway, self.journal
        vendor_epoch = g._vendor_epoch
        lifecycle = j.lifecycle_position() if offline_pause else None

        # A replacement vendor connection cannot authorize a release prepared under the previous one.
        def cloud_current() -> bool:
            return (
                g._vendor_epoch == vendor_epoch
                and g.vendor.sender is not None
                and g.vendor_registration == "Accepted"
                and c.session.runtime.status.vendor_connected
            )

        can_wait_for_cloud = cloud_current() or (
            return_request is not None and c.requests.return_awaiting_cloud(return_request, expected_intent)
        )

        # This guard also runs before LocalControl claims and again inside each actual outgoing RPC gate.
        def guard() -> GateDecision:
            current = (
                c.requests.offline_pause_current(expected_intent)
                if offline_pause
                else c.requests.return_current(return_request, expected_intent)
            )
            if c.stopping or not current:
                return Pending(Prerequisite.SUPERSEDED, "release awaits its exact current owner Return")
            if offline_pause:
                if j.lifecycle_position() != lifecycle:
                    return Pending(Prerequisite.LIFECYCLE, "charging records changed during offline Pause verification")
                decision = self.recovery.offline_return_decision(c)
                if isinstance(decision, Pending):
                    return decision
            if continuing_local is not None and j.outcomes.awaits_stop_report(continuing_local):
                return Pending(Prerequisite.LIFECYCLE, "continuation awaits the Stop's lifecycle report")
            if not cloud_current():
                reason = (
                    Prerequisite.CLOUD
                    if return_request is None
                    else (Prerequisite.CLOUD_ACK if can_wait_for_cloud else Prerequisite.ZERO_PROOF)
                )
                return Pending(reason, "release awaits its current registered cloud connection")
            if c.ordinary.active():
                decision = c.ordinary.authority_decision(continuing_local)
                if isinstance(decision, Pending):
                    return decision
            current_recovery = j.current_change.read().recovering
            if current_recovery:
                decision = current_source_decision(g, continuing_local)
                if isinstance(decision, Pending):
                    return decision
            if (
                continuing_local is not None
                and not current_recovery
                and not c.ordinary.active()
                and not (c.charging.scoped_normal_stop() and c.charging.current_daily(continuing_local))
            ):
                return Pending(Prerequisite.SOURCE, "source-specific continuation was superseded")
            return ALLOW

        return guard

    # Bind Return only after the latest native sample still proves the original hold.
    def prepare_release_attempt(
        self, c: ManagedController, attempt: ReleaseAttempt, native: HandoverEvidence | Pending
    ) -> RestoreProgress | ReleaseAttempt:
        g, j = c.session.gateway, self.journal
        guard, expected_intent = attempt.positive_guard, attempt.intent
        connection = (attempt.scope, attempt.control_revision)
        ticket, return_request, continuing_local = attempt.before_ticket, attempt.request_id, attempt.continuing_local
        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if isinstance(native, Pending):
            return RestoreProgress(RestoreState.WAITING, native)
        decision = c.zero_decision(native, continuing_local=continuing_local)
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if (
            c.stopping
            or not control_scope_matches(g, connection)
            or attempt.scope.epoch != g._charger_epoch
            or attempt.control_revision != g.controls._revision
            or ticket != j.get("control", {}).get("ticket")
            or not j.phase.read().active
            or j.get("control", {}).get("uncertain")
        ):
            raise UnavailableError("managed release authority changed during its native check")
        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if expected_intent.mode == "paused":
            c.complete_pause_observation(native, expected_intent, connection)
            if return_request is not None:
                if not isinstance(ticket, str):
                    raise RecoveryRequiredError("Return requires its current zero hold ticket")
                c.requests.bind_return(return_request, expected_intent, ticket, connection[0].boot)
        return attempt

    # Rollback is still tied to the original Boot after the positive permission expires.
    def require_release_boot(self, gateway: Gateway, attempt: ReleaseAttempt) -> None:
        if not control_scope_matches(gateway, (attempt.scope, attempt.control_revision)):
            raise UnavailableError("release rollback belongs to a superseded Boot observation")

    # Publish app intent, current retirement and its receipt inside the owned wire transaction.
    def publish_release(
        self, c: ManagedController, attempt: ReleaseAttempt, offline_pause: bool, authority: AuthorityStamp
    ) -> AuthorityStamp | Pending:
        j = self.journal
        return_request, expected_intent = attempt.request_id, attempt.intent
        if not c.publication_is_current(authority):
            return Pending(Prerequisite.SUPERSEDED, "release publication was superseded")
        decision = attempt.positive_guard()
        if isinstance(decision, Pending):
            return decision
        j.put("managed_owner_epoch", attempt.scope.epoch)
        if return_request is not None:
            c.requests.returned(return_request, expected_intent, c.current.returned_to_previous)
        elif offline_pause:
            write_intent(j, expected_intent, "app")
        published = c.authority()
        if published is None:
            raise RecoveryRequiredError("release publication lost its exact Boot")
        return published

    # Resolve the exact Return using the positive write and its actual rollback outcome.
    def settle_release(
        self, c: ManagedController, attempt: ReleaseAttempt, result: OwnedReleaseResult
    ) -> RestoreProgress:
        g, j = c.session.gateway, self.journal
        expected_intent, return_request = attempt.intent, attempt.request_id
        decision = ALLOW if result.published else attempt.positive_guard()
        disposition = release_disposition(
            attempt.purpose,
            result,
            c.authority(),
            zero_ready=g.controls.ready and g.controls.desired() == 0,
            faulted=j.fault.latched(),
            publication_allowed=not isinstance(decision, Pending),
        )
        if (
            disposition in (ReleaseDisposition.WAIT, ReleaseDisposition.NOT_SENT)
            and result.claimed
            and result.effects.zero_verified
        ):
            with j.atomic():
                if c.authority() != result.authority or not g.controls.ready or g.controls.desired() != 0:
                    raise RecoveryRequiredError("owned release zero acknowledgement was superseded")
                assert attempt.ticket is not None
                g.controls._complete(attempt.ticket, uncertain=False)
        if disposition is ReleaseDisposition.FAULT:
            if not c.stopping and c.requests.intent() == expected_intent:
                c.fault("owner_release_unconfirmed")
                failure = result.failure
                try:
                    j.put(
                        "managed_release_failure",
                        {
                            "type": type(failure).__name__ if failure is not None else "prerequisite_after_release",
                            "detail": str(failure)[:1024]
                            if failure is not None
                            else result.pending.detail
                            if result.pending
                            else "release unconfirmed",
                            "at_epoch": time.time(),
                            "charger_rpc": g.charger.diagnostics.snapshot(),
                        },
                    )
                except sqlite3.Error:
                    pass  # The required fault already committed and the command still reports its actual failure.
            if (
                return_request is not None
                and isinstance(result.failure, sqlite3.Error)
                and result.effects.zero_verified
                and not result.cleanup_failed
                and not j.db.in_transaction
                and j.fault.read({}).get("classification") == "owner_release_unconfirmed"
            ):
                raise RecoveryRequiredError(
                    "Return kept the verified zero hold after a storage failure"
                ) from result.failure
            if result.failure is not None:
                raise result.failure
            raise RecoveryRequiredError("owned release did not establish shared authority")
        return restore_progress(disposition, result)

    # Claim only the exact preflight hold, including its current Boot and positive guard.
    def claim_release(
        self,
        acknowledge_uncertainty: bool,
        guard: Guard | None,
        rollback_guard: Callable[[], None] | None,
        attempt: ReleaseAttempt | None,
    ) -> tuple[int, str, dict[str, Any]] | Pending:
        if guard is not None:
            pending = guard()  # A newer Pause must be checked before this child claims any ownership.
            if isinstance(pending, Pending):
                return pending
        if self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if attempt is not None and rollback_guard is not None:
            # A task can start after a new Boot or hold has replaced the verified preflight.
            try:
                rollback_guard()
            except UnavailableError as error:
                return Pending(Prerequisite.SUPERSEDED, "release Boot changed before the claim", error)
            if previous.get("ticket") != attempt.before_ticket:
                return Pending(Prerequisite.SUPERSEDED, "release hold changed before the claim")
        if previous.get("uncertain") and not acknowledge_uncertainty:
            raise RecoveryRequiredError("previous local outcome requires reconciliation")
        try:
            epoch, ticket = self.control._claim(previous.get("inhibit", False))
        except sqlite3.Error as error:
            if attempt is None or self.journal.db.in_transaction:
                raise
            # The coordinator still compares the original verified zero, Boot and unchanged ticket.
            return Pending(Prerequisite.RECONCILIATION, "release claim could not be saved", error)
        if attempt is not None:
            attempt.ticket = ticket
        return epoch, ticket, previous

    # Only the verified ceiling may publish shared control within the exact owner's transaction.
    def complete_release(
        self,
        ticket: str,
        check: Callable[[], GateDecision | None],
        result: Reply | CapPending,
        attempt: ReleaseAttempt | None,
        handoff: ReleaseHandoff | None,
    ) -> Pending | None:
        pending = result.pending if isinstance(result, CapPending) else check()
        if attempt is not None and isinstance(result, Reply):
            attempt.positive_confirmed = True
        if not isinstance(pending, Pending):
            publication = None
            with self.journal.atomic():
                pending = check()
                if not isinstance(pending, Pending) and handoff is not None:
                    if attempt is None:
                        raise RecoveryRequiredError("managed release publication lacks owned identity")
                    publication = handoff.write_in_transaction(
                        AuthorityStamp(attempt.intent, attempt.scope, attempt.control_revision, ticket)
                    )
                    if isinstance(publication, Pending):
                        pending = publication
                if not isinstance(pending, Pending):
                    self.control._complete(ticket, owner="shared", inhibit=False, uncertain=False)
            if not isinstance(pending, Pending):
                if attempt is not None:
                    attempt.published = True
                    attempt.published_authority = publication if isinstance(publication, AuthorityStamp) else None
                if handoff is not None:
                    assert isinstance(publication, AuthorityStamp)
                    handoff.committed(publication)
                return None
        if attempt is not None:
            attempt.pending = pending
        return pending

    # Asserting zero belongs to the same release claim and cannot overwrite a newer owner.
    def begin_release_rollback(self, epoch: int, ticket: str) -> None:
        self.control._complete(ticket, inhibit=True)
        self.control._check(epoch, ticket)

    # Status and command admission use the same prerequisite check.
    def current_refusal(self, current: ManagedCurrentLimits, native: HandoverEvidence | None) -> str | None:
        return current_refusal(current, native)

    # Idempotency binds the complete current argument as well as the request identifier, action and observed revision.
    def accept_current(
        self, current: ManagedCurrentLimits, identifier: str, revision: int, current_a: int
    ) -> CurrentSelection | dict[str, Any]:
        value = integer(current_a, 6, 32)
        c, j = current.controller, self.journal
        previous = c.requests.existing(identifier, "set_current", revision)
        if previous is not None:
            bound = j.db.execute("SELECT current_a FROM managed_current_requests WHERE id=?", (identifier,)).fetchone()
            if bound is None or bound[0] != value:
                raise SecurityError("request identifier already binds a different current")
            return previous
        intent = c.requests.intent()
        g = c.session.gateway
        connection, native = current_control_scope(g), c.current_native_sample()
        if (
            connection is None
            or intent.revision != revision
            or native is None
            or self.current_refusal(current, native) is not None
        ):
            raise RecoveryRequiredError("current selection requires fresh idle authority and the current revision")
        previous_value = current.selected()
        # This verified idle sample already saw these attempts, regardless of when their records were created.
        prior_attempts = [attempt_key(record) for record in native.history if record["transactionId"] == -1]
        with j.atomic():
            expected = write_intent(j, intent, intent.mode)
            j.current_change.accept(
                identifier, value, previous_value, intent, j.get("next_transaction"), prior_attempts
            )
            control = j.get("control", {})
            j.control.select(control, value, app=intent.mode == "app")
            ticket = j.get("control", {}).get("ticket")
            if not isinstance(ticket, str):
                raise RecoveryRequiredError("current selection requires an exact existing station claim")
            j.current_change.bind(identifier, ticket, connection[0].boot)
        return CurrentSelection(identifier, value, previous_value, expected, connection, ticket)

    # Recheck the selection, Boot and current ticket at the actual send and publication.
    def require_current_selection(self, current: ManagedCurrentLimits, selection: CurrentSelection) -> None:
        c, j = current.controller, self.journal
        g = c.session.gateway
        change = j.current_change.read()
        if (
            c.stopping
            or c.requests.intent() != selection.intent
            or not control_scope_matches(g, selection.connection)
            or not request_pending(j, selection.identifier, "set_current", selection.intent.revision - 1)
            or j.get("control", {}).get("ticket") != selection.ticket
            or j.fault.latched()
            or not j.phase.read().active
            or j.active() is not None
            or change.value.get("native_open") is True
            or j.get("control", {}).get("start_pending")
            or j.get("vendor_start_grant")
            or current.selected() != selection.value
            or change.identifier != selection.identifier
            or change.value.get("ticket") != selection.ticket
        ):
            raise UnavailableError("current update was superseded by another control operation")

    # A changed claim and its exact receipt bind inside the wire operation's transaction.
    def bind_current_selection(self, selection: CurrentSelection, ticket: str) -> None:
        self.journal.current_change.bind(selection.identifier, ticket, selection.connection[0].boot)
        selection.ticket = ticket

    # Confirm the native profile before finishing the exact current request.
    def complete_current(
        self, current: ManagedCurrentLimits, selection: CurrentSelection, native: HandoverEvidence
    ) -> None:
        self.require_current_selection(current, selection)
        present = current.controller.session.book.owned_present(native.physical)
        target = 0 if selection.intent.mode == "paused" else selection.value
        if not self.control.ready or not any(
            r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == target for r in present
        ):
            raise RecoveryRequiredError("selected current has not been independently verified")
        with self.journal.atomic():
            self.require_current_selection(current, selection)
            self.journal.current_change.complete(selection.identifier, selection.value, selection.intent.revision - 1)

    # A failed receipt write cannot keep an applied but unverified current at the charger.
    async def protect_unsettled_current(
        self,
        worker: ManagedCurrentLimits,
        identifier: str,
        previous: int,
        expected: ControlIntent,
        hold_ticket: str | None,
    ) -> str | None:
        c, j = worker.controller, self.journal
        g = c.session.gateway
        connection = current_control_scope(g)

        # Keep the restrictive operation tied to the same current change and current commissioned connection.
        def guard(ticket: str | None) -> None:
            change = j.current_change.read()
            if (
                c.stopping
                or c.requests.intent() != expected
                or change.identifier != identifier
                or not change.selecting
                or not ticket
                or j.control.read().values.get("ticket") != ticket
                or not control_scope_matches(g, connection)
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("failed current hold was superseded")

        # Commit the zero claim separately from the command's failed receipt, preserving its previous selection.
        def claim() -> str:
            with j.atomic():
                guard(j.current_change.read().value.get("ticket"))
                j.control.project(j.control.read().values, resume_limit=previous)
                _, ticket = g.controls._claim(True, stopping=True)
                return ticket

        try:
            if hold_ticket is None:
                hold_ticket = c.commands.decide(claim, priority=CommandPriority.RESTRICT)
            await g.controls.ensure_ceiling(guard=lambda: guard(hold_ticket), request_id=identifier)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            pass  # The same current worker retries while an exact owner still permits zero.
        return hold_ticket

    # Decide which hold or overlapping session still owns this failed current selection.
    def settle_failed_current(
        self,
        worker: ManagedCurrentLimits,
        identifier: str,
        value: int,
        previous: int,
        expected: ControlIntent,
        cause: dict[str, Any],
    ) -> CurrentFailure | None:
        c, j = worker.controller, self.journal
        change = j.current_change.read()
        if change.identifier != identifier or not change.selecting:
            return None
        intent = c.requests.intent()
        outcome = change.failure_outcome(expected, intent, j.active() is not None)
        if outcome is CurrentFailure.PROTECT_OPEN_SESSION:
            self.cancel_current_overlap(worker, identifier, previous, "session_opened", failure=cause)
        else:
            with j.atomic():
                if outcome is CurrentFailure.HOLD_IDLE:
                    write_intent(j, intent, "paused")
                    self._claim_internal_pause(c, "current", identifier)
                control = j.get("control", {})
                if control.get("inhibit") is True and j.current_change.read().identifier == identifier:
                    j.control.project(control, resume_limit=previous)
                j.current_change.fail(identifier, value, previous, expected.mode, cause, outcome)
        return outcome

    # Persist the failed operation's zero hold without manufacturing durable owner Pause intent or Stop authority.
    def cancel_current_overlap(
        self,
        worker: ManagedCurrentLimits,
        identifier: str,
        previous: int,
        reason: str,
        *,
        failure: dict[str, Any] | None = None,
    ) -> None:
        c, j = worker.controller, self.journal

        # Keep the zero ticket and current outcome in the same transaction.
        def claim_hold() -> None:
            j.control.project(j.get("control", {}), resume_limit=previous)
            c.session.gateway.controls._claim(True, stopping=True)
            c.released_epoch = None

        with j.atomic():
            j.current_change.recover_overlap(
                identifier, previous, reason, c.requests.intent().revision, claim_hold, failure
            )

    # Apply only the already owned station zero, including before native source identification or reintroduction.
    async def protect_current_overlap(self, worker: ManagedCurrentLimits) -> None:
        c, j = worker.controller, self.journal
        g = c.session.gateway
        change = j.current_change.read().value
        connection = current_control_scope(g)
        ticket = j.get("control", {}).get("ticket")

        # A newer owner Pause or another Boot revokes this operation's restrictive ticket too.
        def guard() -> None:
            current = j.current_change.read()
            if (
                c.stopping
                or not current.recovering
                or current.identifier != change["id"]
                or c.requests.intent().mode != "app"
                or c.requests.intent().revision != change["recovery_revision"]
                or not control_scope_matches(g, connection)
                or j.get("control", {}).get("ticket") != ticket
                or fingerprint(j.get("boot")) != c.native.spec.boot_digest
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
                or g.controls.desired() != 0
            ):
                raise RecoveryRequiredError("current recovery zero was superseded")

        await g.controls.ensure_ceiling(guard=guard, request_id=change["id"])

        # This zero verifies only the hold that requested it.
        def settle() -> None:
            guard()
            if not g.controls.ready:
                raise RecoveryRequiredError("current recovery zero no longer belongs to its hold")
            if j.phase.read().active and isinstance(ticket, str):
                g.controls._complete(ticket, uncertain=False)

        c.commands.decide(settle, priority=CommandPriority.RESTRICT)

    # A replacement retains the previous selection or the exact newer Pause.
    def restore_current_request(self, worker: ManagedCurrentLimits) -> None:
        controller = worker.controller
        pending = self.journal.current_change.read()
        if pending.selecting:
            change = pending.value
            previous = change.get("previous_a")
            if previous is None:
                # Earlier pending receipts stored the target only. Recover the most recent completed selection.
                row = self.journal.db.execute(
                    "SELECT c.current_a FROM managed_current_requests c JOIN managed_requests r ON r.id=c.id "
                    "WHERE r.state='completed' ORDER BY r.rowid DESC LIMIT 1"
                ).fetchone()
                previous = row["current_a"] if row else 6
            previous = integer(previous, 6, 32)
            change = {**change, "previous_a": previous}
            ownership = controller.requests.pause_ownership()
            intent = ownership.intent
            outcome = pending.replacement_state(intent, ownership.requested)
            if outcome == "cancelled":
                # Current admission advances intent, so a matching owner Pause was accepted after this selection.
                with self.journal.atomic():
                    self.journal.control.project(self.journal.get("control"), resume_limit=previous)
                    self.journal.current_change.cancel(change)
                return
            if outcome == "recovering":
                self.cancel_current_overlap(worker, change["id"], previous, "process_replaced")
                return
            with self.journal.atomic():
                write_intent(self.journal, intent, "paused")
                self._claim_internal_pause(controller, "current", change["id"])
                control = self.journal.get("control")
                self.journal.control.project(control, resume_limit=previous)
                self.journal.current_change.replace({**change, "state": outcome})

    # Retry only the closed zero hold of this exact failed idle update; its current receipt stays uncertain.
    def failed_idle_current(self, worker: ManagedCurrentLimits) -> bool:
        c, j = worker.controller, self.journal
        g, control = c.session.gateway, j.get("control", {})
        receipt = j.starts.receipt()
        return bool(
            not c.stopping
            and (worker.worker is None or worker.worker.done())
            and j.current_change.failed_idle_at_zero(worker.selected, g.boot_observation)
            and g.current_boot_seen(g._charger_epoch)
            and j.active() is None
            and ControlState(control).explicit_local_hold
            and control.get("uncertain") is True
            and isinstance(control.get("ticket"), str)
            and re.fullmatch(r"[a-f0-9]{32}", control["ticket"]) is not None
            and not control.get("start_pending")
            and not j.fault.latched()
            and not j.get("vendor_start_grant")
            and not g.vendor_write_evidence().unresolved
            and not c.requests.return_in_progress()
            and c.charging.lease is None
            and c.charging.state().get("state") in (None, "completed", "failed", "cancelled")
            and receipt.get("stage") not in ("prepared", "issued")
            and (
                receipt.get("stage") != "accepted"
                or j.db.execute(
                    "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (receipt.get("local"),)
                ).fetchone()
            )
            and not j.db.execute(
                "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') LIMIT 1"
            ).fetchone()
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        )

    # Verified restoration retires only its exact abandoned target, in the caller's authority transaction.
    def restore_previous_current(self, worker: ManagedCurrentLimits, *, recovery: dict[str, Any] | None = None) -> None:
        c = worker.controller
        self.journal.current_change.restore_previous(worker.selected, c.stopping, recovery)

    # Retire a recovery whose exact intent is obsolete before reading any native evidence.
    def prepare_current_recovery(self, worker: ManagedCurrentLimits) -> tuple[dict[str, Any], ControlIntent] | None:
        c, j = worker.controller, self.journal
        change = j.current_change.read()
        if change.recovering:
            expected = ControlIntent("app", change.value["recovery_revision"])
            if c.requests.intent() == expected:
                return change.value, expected
            j.current_change.cancel(change.value)
        worker._settling_overlap = None
        return None

    # Reclaim only the current operation's own zero if earlier restoration failed to settle.
    def reclaim_current_recovery(
        self, worker: ManagedCurrentLimits, change: dict[str, Any], expected: ControlIntent
    ) -> bool:
        c, j = worker.controller, self.journal
        g = c.session.gateway
        current = j.current_change.read()
        if (
            c.stopping
            or c.requests.intent() != expected
            or not current.recovering
            or current.identifier != change["id"]
        ):
            return False
        if g.controls.desired() != 0:
            with j.atomic():
                g.controls._claim(True, stopping=True)
                c.released_epoch = None
        return True

    # Accept native recovery evidence only while it belongs to the exact pending selection.
    def assess_current_recovery(
        self,
        worker: ManagedCurrentLimits,
        change: dict[str, Any],
        expected: ControlIntent,
        native: HandoverEvidence,
        native_authority: str,
        sequence: int,
    ) -> tuple[bool, int | Pending | None]:
        c, j = worker.controller, self.journal
        if c.native.authority() != native_authority or c.native.last_resolved_read != sequence:
            return False, Pending(Prerequisite.SUPERSEDED, "current recovery observation was superseded")
        current = j.current_change.read()
        if (
            c.stopping
            or c.requests.intent() != expected
            or not current.recovering
            or current.identifier != change["id"]
        ):
            return False, None
        decision = c.native.session_decision(native)
        if isinstance(decision, Pending):
            return False, decision
        if change.get("recovery_reason") == "process_replaced" and not current.value["observed_open"]:
            # Preserve the existing fail-closed policy when replacement confirms there was no overlapping session.
            with j.atomic():
                write_intent(j, expected, "paused")
                self._claim_internal_pause(c, "current", change["id"])
                j.control.project(j.get("control"), resume_limit=change["previous_a"])
                j.current_change.replace({**current.value, "state": "interrupted"})
            return False, None
        local = j.active()
        if local is not None:
            decision = current_session_decision(c.session.gateway, native, local)
            if isinstance(decision, Pending):
                return False, decision
        return True, local

    # Retry a failed receipt write only after the restored limit and exact session are still verified.
    async def retry_current_recovery(
        self, worker: ManagedCurrentLimits, change: dict[str, Any], expected: ControlIntent
    ) -> tuple[bool, Pending | None]:
        c, j = worker.controller, self.journal
        g = c.session.gateway
        if worker._settling_overlap is not None and not c.stopping:
            identifier, authority, local = worker._settling_overlap
            if identifier == change["id"] and c.authority() == authority and j.active() == local and g.controls.ready:
                native = await c.native.read_result()
                if c.stopping or c.requests.intent() != expected:
                    return True, Pending(Prerequisite.SUPERSEDED, "current recovery yielded to newer control")
                if (
                    not isinstance(native, Pending)
                    and c.authority() == authority
                    and g.controls.ready
                    and g.controls.desired() == change["previous_a"]
                    and c.native.session_decision(native) is ALLOW
                    and (
                        current_source_decision(g, None)
                        if local is None
                        else current_session_decision(g, native, local)
                    )
                    is ALLOW
                ):
                    return True, self.complete_current_recovery(worker, change, authority, local)
            worker._settling_overlap = None
        return False, None

    # Settle only the previous current and authority this recovery verified.
    def complete_current_recovery(
        self, worker: ManagedCurrentLimits, change: dict[str, Any], authority: AuthorityStamp, local: int | None
    ) -> Pending | None:
        # The restored receipt may be retried only under the authority and session it verified.
        def settle() -> Pending | None:
            if worker.controller.authority() != authority or self.journal.active() != local:
                return Pending(Prerequisite.SUPERSEDED, "current recovery lost its confirmed authority")
            try:
                with self.journal.atomic():
                    self.restore_previous_current(worker, recovery=change)
            except sqlite3.Error as error:
                worker._settling_overlap = (change["id"], authority, local)
                return Pending(Prerequisite.RECONCILIATION, "current recovery awaits its receipt write", error)
            worker._settling_overlap = None
            return None

        return worker.controller.commands.decide(settle)

    # Accept a delayed genuine original-plan report under this operation's zero, without inventing its lifecycle.
    async def prepare_current_report(
        self, worker: ManagedCurrentLimits, payload: dict[str, Any], parse_timestamp: Callable[[Any], datetime]
    ) -> NativeStartReport | None:
        c, j = worker.controller, self.journal
        change = j.current_change.read().value
        g = c.session.gateway
        intent, connection = c.requests.intent(), current_control_scope(g)
        ticket, tag = j.get("control", {}).get("ticket"), payload.get("idTag")
        if (
            change.get("state") != "recovering"
            or intent != ControlIntent("app", change["recovery_revision"])
            or c.stopping
            or connection is None
            or payload.get("connectorId") != 1
            or not isinstance(tag, str)
            or j.active() is not None
            or not g.controls.ready
            or g.controls.desired() != 0
        ):
            return None
        native = await c.native.read(owned=True)
        local = native.physical.native_transaction_id
        plan = original_current_plan(native, c.native.spec.saved_schedule_digest)
        if (
            plan is None
            or plan["idTag"].casefold() != tag.casefold()
            or native.output_limit_a != 0
            or any(v != 0 for v in native.currents_a + native.powers_w)
            or not c.native.commissioned_binding_matches(native)
        ):
            return None
        attempt = None
        if local is None:
            c.native.require(native, closed=True)
            attempt = matching_unassigned_start(native.history, payload, parse_timestamp(payload.get("timestamp")))
            if attempt is None:
                return None
        elif (
            not native.physical.transaction_open
            or local <= change.get("floor", 0)
            or j.db.execute("SELECT 1 FROM transactions WHERE local=?", (local,)).fetchone()
        ):
            return None

        selected_change = j.current_change.read().value
        sequence = c.native.last_resolved_read

        # Bind only the current recovery's hold; a newer selection, Boot or observation revokes this allocation.
        def guard(identifier: int | None) -> None:
            native.physical.require_fresh(j.identity, 5)
            control = j.get("control", {})
            if (
                c.stopping
                or c.requests.intent() != intent
                or not control_scope_matches(g, connection)
                or j.fault.latched()
                or selected_change.get("id") != change.get("id")
                or selected_change.get("state") != "recovering"
                or j.current_change.read().value != selected_change
                or j.active() != identifier
                or control.get("ticket") != ticket
                or not ControlState(control).explicit_local_hold
                or not g.controls.ready
                or g.controls.desired() != 0
                or c.native.last_resolved_read != sequence
            ):
                raise RecoveryRequiredError("current recovery report was superseded before allocation")

        guard(None)

        # Allocation records the real Start; positive restoration still needs a later numbered native session.
        def accepted(identifier: int) -> None:
            guard(identifier)
            if identifier <= change.get("floor", 0) or local is not None and identifier != local:
                raise RecoveryRequiredError("current recovery report no longer matches the next transaction")
            j.current_change.replace({**selected_change, "reported_local": identifier, "reported_plan": plan["planId"]})

        return NativeStartReport(local, plan["planId"], attempt, accepted)

    # The ordinary local limit uses the same commissioned bounds as the wire executor.
    def bounded_current_limit(self, value: Any) -> float:
        limit = number(value, self.control.policy.max_current_a)
        return 0.0 if limit < 6 else limit

    # Commit the local limit claim and, when present, its owning request in one transaction.
    def claim_current_limit(
        self,
        limit: float,
        guard: Callable[[], None] | None,
        on_restrictive_claim: Callable[[str | None, str], None] | None,
        on_claim: Callable[[str], None] | None,
    ) -> tuple[int, str]:
        if guard is not None:
            guard()
        if limit > 0 and self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if previous.get("inhibit") and limit > 0:
            raise DeniedError("resume or release explicitly before raising a stopped limit")
        with self.journal.atomic() if on_claim is not None else nullcontext():
            epoch, ticket = self.control._claim(previous.get("inhibit", False) or limit == 0)
            if on_claim is not None:
                on_claim(ticket)
            if limit == 0 and on_restrictive_claim is not None:
                on_restrictive_claim(previous.get("ticket"), ticket)
            if limit > 0:
                self.control._complete(ticket, resume_limit=limit)
        return epoch, ticket

    # Leave the verified physical result with its current ticket.
    def complete_current_limit(self, ticket: str, check: Callable[[], None], shared: bool) -> None:
        check()
        self.control._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))

    # A vendor write must respect the current local hold, recovery and verified station ceiling.
    def require_vendor_control(
        self, gateway: Gateway, action: str, *, write: bool, require_cap: bool, own_mutation: str | None
    ) -> None:
        if (
            write
            and action != "RemoteStopTransaction"
            and (gateway.retiring() or not self.journal.phase.read().legacy_active or self.journal.phase.starting)
        ):
            raise DeniedError("trial vendor controls are frozen")
        control = self.journal.get("control", {})
        if write and action != "RemoteStopTransaction":
            if self.journal.current_change.read().recovering:
                raise DeniedError("cloud writes await settlement of the current selection")
            if own_mutation is None:
                self.journal.require_vendor_writes_reconciled()
            elif self.journal.db.execute(
                "SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed' AND token!=? LIMIT 1",
                (own_mutation,),
            ).fetchone():
                raise RecoveryRequiredError("another vendor write still requires reconciliation")
            if require_cap and not self.control.ready:
                raise UnavailableError("station ceiling is not confirmed for this charger connection")
        if write and action != "RemoteStopTransaction" and (control.get("owner") == "local" or control.get("inhibit")):
            raise DeniedError("explicit local control has priority")
        if action == "RemoteStartTransaction":
            self.require_vendor_start_idle()

    # A selected-current write and profile removal need the independently observed idle session.
    def profile_requires_closed(self, action: str) -> bool:
        selecting = self.journal.current_change.read().selecting
        return action == "ClearChargingProfile" or selecting and action == "SetChargingProfile"

    # A recovering selection can change a profile only for its proved session or proved idle state.
    def current_profile_decision(self, gateway: Gateway, action: str, native: HandoverEvidence) -> GateDecision:
        if action == "SetChargingProfile" and self.journal.current_change.read().recovering:
            local = self.journal.active()
            if local is not None:
                return current_session_decision(gateway, native, local)
            if native.physical.transaction_open:
                return Pending(Prerequisite.LIFECYCLE, "current recovery awaits actual lifecycle reconciliation")
        return ALLOW

    # Report the first failed prerequisite in admission order.
    def start_refusal(
        self,
        charging: ManagedCharging,
        native: HandoverEvidence | None = None,
        *,
        local: bool = True,
        normal: bool = False,
    ) -> str | None:
        c, j = charging.controller, self.journal
        g = c.session.gateway
        facts = ManagedFacts(c)
        if c.stopping:
            return "service_stopping"
        if not (c.requests.intent().mode == "app" or normal and c.requests.intent().mode == "paused"):
            return "intent_not_startable"
        if j.fault.latched():
            return "fault_latched"
        if not j.phase.read().active:
            return "installation_not_active"
        if not g.controls.ready:
            return "station_ceiling_unconfirmed"
        if g.controls.desired() != (0 if c.requests.intent().mode == "paused" else c.current.selected()):
            return "station_ceiling_differs_from_selection"
        if not normal and c.current.selected() != 6:
            return "timed_test_requires_six_amperes"
        if c.current.pending():
            return "current_update_pending"
        if g.policy.allow_local_start:
            return "legacy_start_enabled"
        if local and not any(g._local_start_allowed(tag) for tag in g.policy.allowed_tags):
            if not normal or charging.authorization_status() != "renewal_required":
                return "start_authorization_unavailable"
        if j.get("control", {}).get("uncertain"):
            return "control_uncertain"
        if j.get("control", {}).get("start_pending"):
            return "start_pending"
        if j.active() is not None:
            return "transaction_open"
        if j.get("vendor_start_grant"):
            return "vendor_start_pending"
        if not facts.cloud_queue_empty(lifecycle_only=normal):
            return "cloud_delivery_pending"
        if not facts.mutations_confirmed():
            return "mutation_unconfirmed"
        if c.requests.return_in_progress():
            return "return_pending"
        if not c.requests.has_capacity():
            return "request_capacity_exhausted"
        if charging.state().get("state") in ACTIVE:
            return "managed_start_active"
        if not facts.boot_current():
            return "boot_unconfirmed"
        if not normal and not (c.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted"):
            return "timed_test_requires_cloud"
        if native is not None:
            if c.native.last_verified_at == 0:
                return "native_observation_stale"
            if native.physical.connection != "Plug":
                return "vehicle_not_plugged_in"
            if native.physical.transaction_open:
                return "native_transaction_open"
            if native.physical.power_w != 0:
                return "native_power_nonzero"
        return None

    # Acceptance and its one-shot deadline commit before any positive effect; duplicate identifiers return receipts.
    def accept_start(
        self, charging: ManagedCharging, identifier: str, action: str, revision: int, startup: float, duration: float
    ) -> dict[str, Any]:
        c, j = charging.controller, self.journal
        previous = c.requests.existing(identifier, action, revision)
        if previous is not None:
            return previous
        native = c.current_native_sample()
        if (
            revision != c.requests.intent().revision
            or native is None
            or not 0 <= time.time() - native.physical.observed_at <= 15
            or not charging.available(native, local=action in ("start", "start_test"), normal=action == "start")
        ):
            raise RecoveryRequiredError(
                "Start requires fresh idle authority, current approval or online renewal, and no pending work"
                if action == "start"
                else "test requires current shared six-ampere authority and no pending work"
            )
        lease = charging._start_lease(identifier, action, revision)
        j.starts.admit(lease, action, startup, duration)
        charging._launch_start(lease)
        return c.requests.receipt(identifier)

    # Pause, deadline, shutdown, introduction, permission changes and a replaced request all revoke positive sends.
    def require_start_lease(
        self, charging: ManagedCharging, lease: ChargeLease, monotonic: Callable[[], float], *, stopping: bool = False
    ) -> None:
        c, j = charging.controller, self.journal
        g = c.session.gateway
        state = charging.state()
        if (
            charging.lease is not lease
            or c.stopping
            or monotonic() >= lease.deadline
            or c.requests.intent() != ControlIntent(lease.mode, lease.revision)
            or g._charger_epoch != lease.epoch
            or g.controls._revision != lease.control_revision
            or not g.current_boot_seen(lease.epoch)
            or lease.accepted_boot is not None
            and g.boot_observation != lease.accepted_boot
            or fingerprint(j.identity) != lease.identity
            or c.native.spec.digest != lease.spec
            or g.policy.allow_local_start
            or not 6 <= g.policy.max_current_a <= 32
            or c.current.selected() != lease.current_a
            or c.current.pending()
            or j.fault.latched()
            or not j.phase.read().active
            or state.get("id") != lease.identifier
            or state.get("state") not in ACTIVE
            or state.get("state") == "stopping"
            or lease.source == "local"
            and state.get("state") == "starting"
            and not request_pending(j, lease.identifier, "start" if lease.normal else "start_test", lease.revision)
            or c.requests.return_in_progress()
            or not stopping
            and j.get("control", {}).get("stop_epoch", 0) != lease.stop_epoch
            or lease.source == "local"
            and j.db.execute(
                "SELECT 1 FROM mutations WHERE rowid>? AND origin='vendor' AND action='RemoteStartTransaction' LIMIT 1",
                (lease.first_mutation,),
            ).fetchone()
        ):
            raise DeniedError("managed test Start permission expired or was superseded")

    # Bind only the exact initial hold for the admitted lease on the current Boot.
    def bind_initial_start_zero(self, lease: ChargeLease) -> None:
        j = self.journal
        control, boot = j.get("control", {}), self.control.boot()
        if lease.initial_zero_ticket is None and control.get("stop_epoch", 0) == lease.stop_epoch:
            return  # LocalControl checks once before synchronously making its own claim.
        if (
            control.get("stop_epoch") != lease.stop_epoch + 1
            or not ControlState(control).explicit_local_hold
            or not isinstance(control.get("ticket"), str)
            or lease.initial_zero_ticket not in (None, control["ticket"])
            or lease.initial_zero_boot not in (None, boot)
        ):
            raise DeniedError("initial Start hold was superseded")
        lease.initial_zero_ticket, lease.initial_zero_boot = control["ticket"], boot
        lease.initial_zero_stop_epoch = control["stop_epoch"]

    # The attempt metadata joins the exact Start receipt before the child can send.
    def prepare_start_publication(self, charging: ManagedCharging, lease: ChargeLease, tag: str) -> None:
        charging.guard(lease)
        self.journal.starts.prepared_metadata(lease, fingerprint(tag.casefold()))
        lease.start_scope = current_scope(charging.controller.session.gateway)

    # Publish only the Start that still owns the exact request, Boot, ticket and intent.
    def publish_start(
        self,
        charging: ManagedCharging,
        lease: ChargeLease,
        context: StartHandoffContext,
        previous_current: TerminalCurrent | None,
    ) -> StartPublication:
        c, j = charging.controller, self.journal
        charging.guard(lease)
        expected = ControlIntent(lease.mode, lease.revision)
        authority = AuthorityStamp(expected, context.scope, context.control_revision, context.ticket)
        if not c.publication_is_current(authority):
            raise RecoveryRequiredError("owned Start publication was superseded")
        if lease.normal:
            c.requests.start_published(lease.identifier, expected, lease.epoch)
            if previous_current is not None:
                try:
                    require_record(c, previous_current)
                except RecoveryRequiredError:
                    pass  # A newer current request retains its own independently owned outcome.
                else:
                    complete_record(c, previous_current)
        return j.starts.published(lease, context, c.requests.intent())

    # Use the failed reply's lease and actual recorded effects before settling Start.
    def settle_failed_start(
        self, charging: ManagedCharging, lease: ChargeLease, error: Exception, effects: bool
    ) -> None:
        j = self.journal
        record = j.starts.load(wire=False, request_id=lease.identifier)
        facts = StartFailureFacts(
            lease.identifier, "start" if lease.normal else "start_test", charging.lease is lease, effects
        )
        decision = settle_start(record, facts)
        if decision.action is StartFailureAction.UNRELATED:
            return
        assert record is not None and record.request is not None
        cause = {
            "id": lease.identifier,
            "action": record.request.action,
            "revision": record.request.revision,
            "type": type(error).__name__,
            "detail": str(error)[:1024],
            "at_epoch": time.time(),
            "effects_started": effects,
        }
        record = j.starts.load(ledger=False)
        decision = settle_start(record, facts)
        if decision.retire_prepared:
            assert record is not None
            j.starts.retire_prepared(record.receipt.ticket)
        if decision.action is StartFailureAction.DIAGNOSTIC_ONLY:
            charging.record_failure_diagnostic(cause)
            return  # A deadline or later proof already owns the state and immutable terminal receipt.
        if decision.action is StartFailureAction.FAIL_PREFLIGHT:
            j.starts.fail_preflight(lease.identifier, cause)
            charging.changed.set()
            j.starts.retire_refused()
        else:
            charging.stop(lease, "start_not_confirmed", failure=cause)
        charging.record_failure_diagnostic(cause)

    # The held listener consumes an existing receipt under verified zero and cannot authorize another Start.
    def can_record_owner_start(self, charging: ManagedCharging, tag: str) -> bool:
        c, g = charging.controller, charging.controller.session.gateway
        return bool(
            (
                c.stopping
                or c.requests.intent().mode == "paused"
                or charging.scoped_normal_stop()
                and c.requests.intent().mode == "app"
            )
            and charging.matches_pending_start(tag)
            and self.journal.active() is None
            and not self.journal.fault.latched()
            and g.controls.ready
            and g.controls.desired() == 0
            and charging.report_hold_authorized()
        )

    # An internal timed-test Pause can hold zero without stopping a different charging session.
    def timed_pause_leaves_session(self, charging: ManagedCharging, local: int | None) -> bool:
        requests = charging.controller.requests
        if local is None or requests.pause_request() is not None:
            return False
        intent = requests.intent()
        test = charging.state()
        internal = self.journal.get("managed_internal_pause", {})
        return (
            intent.mode == "paused"
            and internal.get("source") == "test"
            and internal.get("revision") == intent.revision
            and internal.get("id") == test.get("id")
            and test.get("kind") != "normal"
            and self.journal.starts.timed_stop_target(test) != local
        )

    # Record an internal restrictive winner in the same transaction as its intent and control claim.
    def _claim_internal_pause(self, c: ManagedController, source: str, identifier: str) -> None:
        g, j = c.session.gateway, c.session.journal
        _, ticket = g.controls._claim(True, stopping=True)
        j.put(
            "managed_internal_pause",
            {
                "ticket": ticket,
                "revision": c.requests.intent().revision,
                "source": source,
                "id": identifier,
                "epoch": g._charger_epoch,
                "boot": g.boot_observation,
            },
        )

    # Restore an interrupted Start's exact receipt and the restrictive claim in its original transaction.
    def restore_charging_request(self, charging: ManagedCharging) -> None:
        controller = charging.controller
        saved = charging.state()
        if saved.get("state") in ACTIVE:
            with self.journal.atomic():
                ownership = controller.requests.pause_ownership()
                intent = ownership.intent
                receipt = self.journal.starts.receipt()
                refused = (
                    receipt.get("request_id") == saved["id"]
                    and receipt.get("stage") == "rejected"
                    and receipt.get("local") is None
                )
                preserve = (
                    saved.get("kind") == "normal"
                    and intent.mode == "app"
                    and self.journal.get("control", {}).get("start_pending") is True
                    and receipt.get("local") is None
                    and receipt.get("stage") in ("issued", "accepted")
                )
                if not ownership.supersedes(saved.get("revision")):
                    if not preserve:
                        write_intent(self.journal, intent, "paused", coalesce=refused)
                    self._claim_internal_pause(controller, "test", saved["id"])
                self.journal.starts.interrupted(saved, preserve)
        self.journal.starts.retire_refused()

    # Record the protective Pause independently of any subsequent timed-test result.
    def _claim_test_pause(self, c: ManagedController, intent: ControlIntent, identifier: str) -> None:
        write_intent(self.journal, intent, "paused")
        self._claim_internal_pause(c, "test", identifier)
        c.requests.charge_stopped()

    # Retire only the timed test's target, or the matching normal Start's own pending evidence.
    def expire_start(
        self, charging: ManagedCharging, lease: ChargeLease, reason: str, failure: dict[str, Any] | None
    ) -> bool:
        state = charging.state()
        if charging.lease is not lease or state.get("state") == "stopping":
            return False
        c, j = charging.controller, self.journal
        if c.stopping:
            return False
        target = None if lease.normal else j.starts.timed_stop_target(state)
        if self.schedule.retire_test_beside_daily(charging, lease, state, target):
            return False
        with j.atomic():
            intent = c.requests.intent()
            preserve = (
                lease.normal
                and intent.mode == "app"
                and intent.revision == lease.revision
                and charging.state().get("independent_seen") is True
            )
            if intent.mode != "paused" and not preserve:
                self._claim_test_pause(c, intent, lease.identifier)
            elif self.failed_start_zero_needed(charging, lease, reason, failure):
                c.session.gateway.controls._claim(True)
            j.starts.expire(lease, reason, preserve, failure, target=target)
        if lease.normal:
            self.journal.starts.set_lease(None)
        return True

    # Claim the known timed session's zero if expiry bookkeeping cannot be saved yet.
    def claim_timed_cutoff(self, charging: ManagedCharging, lease: ChargeLease, monotonic: Callable[[], float]) -> bool:
        c, j = charging.controller, self.journal
        if charging.lease is not lease or lease.normal or c.stopping or monotonic() < lease.deadline:
            return False
        state, intent, local = charging.state(), c.requests.intent(), j.active()
        phase = state.get("state")
        if (
            state.get("id") != lease.identifier
            or phase not in ACTIVE
            or phase == "stopping"
            or local is None
            or local != j.starts.timed_stop_target(state)
        ):
            return False
        if intent == ControlIntent("app", lease.revision):
            with j.atomic():
                self._claim_test_pause(c, intent, lease.identifier)
            return True
        return intent.mode == "paused"

    # Zero authority does not borrow Stop authority from whichever daily transaction happens to be open.
    async def hold_expired_start(self, charging: ManagedCharging) -> None:
        c, j = charging.controller, self.journal
        g, expected = c.session.gateway, c.requests.intent()
        connection, identifier = current_control_scope(g), charging.state().get("id")

        # Restrictive writes remain bound to this request, current intent, commissioned device and actual Boot.
        def guard() -> None:
            if (
                c.stopping
                or charging.state().get("id") != identifier
                or not charging.scoped_normal_stop()
                or c.requests.intent() != expected
                or not control_scope_matches(g, connection)
                or fingerprint(j.get("boot")) != c.native.spec.boot_digest
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("expired Start zero authority was superseded")

        # Take or reuse the current zero claim within the same turn that checks the expired Start's ownership.
        def claim() -> str:
            guard()
            control = j.get("control", {})
            ticket = control.get("ticket")
            if control.get("inhibit") and isinstance(ticket, str):
                return ticket
            return g.controls._claim(True, stopping=True)[1]

        ticket = c.commands.decide(claim, priority=CommandPriority.RESTRICT)

        # A newer hold cannot inherit a verification completed for this particular Start.
        def require_ticket() -> None:
            guard()
            if j.get("control", {}).get("ticket") != ticket:
                raise RecoveryRequiredError("expired Start zero ticket was superseded")

        await g.controls.ensure_ceiling(guard=require_ticket)

        # Confirm only the station zero. The separate Start receipt and real lifecycle remain outstanding.
        def settle() -> None:
            require_ticket()
            g.controls._complete(ticket, uncertain=False)

        c.commands.decide(settle, priority=CommandPriority.RESTRICT)

    # Hold, authorize and fence a reported Start before committing its real lifecycle.
    async def reported_start(
        self, gateway: Gateway, receipt: str, payload: dict[str, Any], epoch: int, boot: str | None, occurred: datetime
    ) -> dict[str, Any]:
        # A replay can return its stored result, while a new Start must retain its original introduction.
        def check() -> None:
            gateway._require_epoch("charger", epoch)
            if (
                gateway.journal.phase.read().supervised
                and (boot is None or gateway.boot_observation != boot)
                and not gateway.journal.known_start(payload)
            ):
                raise UnavailableError("reported Start belongs to a retired charger Boot")

        # Recheck the report and any captured hold at the one turn that can create a lifecycle entry.
        def commit(native: NativeStartReport | None, ticket: str | None) -> dict[str, Any]:
            check()
            if not gateway.journal.known_start(payload) and gateway.journal.get("control", {}).get("ticket") != ticket:
                raise DeniedError("a newer owner operation superseded the reported start")
            return self.record_start_report(gateway, receipt, payload, occurred, native)

        check()
        if gateway.journal.known_start(payload):
            return await gateway.decide_report(lambda: commit(None, None))
        if gateway.start_reports is not None and not gateway.journal.known_start(payload):
            await gateway.start_reports.prepare_hold(payload)
            check()
        ticket = gateway.journal.get("control", {}).get("ticket")
        if gateway.policy.strict_cloud_authorization:
            try:
                await self.renew_reported_start(gateway, payload, epoch, check_origin=check)
            except DeniedError, UnavailableError:
                if not gateway.journal.known_start(payload):
                    raise
            check()
        if not gateway.journal.known_start(payload) and gateway._tag_allowed(payload.get("idTag")):
            await gateway.controls.ensure_ceiling()
            check()
        if gateway.journal.get("control", {}).get("ticket") != ticket and not gateway.journal.known_start(payload):
            raise DeniedError("a newer owner operation superseded the start")
        native_report = None
        if gateway.start_reports is not None and not gateway.journal.known_start(payload):
            native_report = await gateway.start_reports.identify(payload)
            tag = payload.get("idTag")
            if (
                native_report is not None
                and gateway.policy.strict_cloud_authorization
                and isinstance(tag, str)
                and gateway._tag_info(tag)["status"] != "Accepted"
                and gateway.auth_info.accepted_for_delayed_report(tag, occurred.timestamp(), payload["timestamp"])
                is None
            ):
                await self.renew_reported_start(gateway, payload, epoch, held_native=True, check_origin=check)
                refreshed = await gateway.start_reports.identify(payload)
                if refreshed is None or refreshed != native_report:
                    raise DeniedError("the native occurrence changed during cloud authorization")
                native_report = refreshed
            if native_report is None and not gateway.journal.known_start(payload):
                try:
                    return await gateway.start_reports.commit_at_verified_ceiling(lambda: commit(None, ticket))
                except DeniedError, UnavailableError, SecurityError:
                    if not gateway.journal.known_start(payload):
                        raise
            check()
            if gateway.journal.get("control", {}).get("ticket") != ticket and not gateway.journal.known_start(payload):
                raise DeniedError("a newer owner operation superseded the reported start")
        return await gateway.decide_report(lambda: commit(native_report, ticket))

    # Validate the report and commit its command/native binding with the new transaction.
    def record_start_report(
        self,
        gateway: Gateway,
        token: str,
        payload: dict[str, Any],
        occurred: datetime,
        native_report: NativeStartReport | None,
    ) -> dict[str, Any]:
        integer(payload.get("connectorId"), 1, 1)
        number(payload.get("meterStart"), 1e12)
        tag = payload.get("idTag")
        if not isinstance(tag, str):
            raise DeniedError("a reported start requires a tag")
        known = gateway.journal.known_start(payload)
        historical = None
        if (
            not known
            and gateway.policy.strict_cloud_authorization
            and (
                native_report is not None
                or gateway.start_reports is not None
                and gateway.start_reports.permits_held(tag)
            )
        ):
            historical = gateway.auth_info.accepted_for_delayed_report(tag, occurred.timestamp(), payload["timestamp"])
        if not known and not gateway._tag_allowed(
            tag, lifecycle=True, historical=historical is not None, native_report=native_report is not None
        ):
            raise DeniedError("tag is not approved for local authorization")

        # Permission binding and the pending flag belong to the very same commit as the new Start fact.
        def accepted(local: int) -> None:
            if native_report is not None:
                native_report.record(local)
            elif gateway.start_reports is None or not gateway.start_reports.defer_binding(local, payload["idTag"]):
                gateway.authorisation.consume(payload["idTag"], local)
                gateway.controls.bind_start(payload["idTag"], local)
            if gateway.start_reports is not None:
                gateway.start_reports.recorded(local, payload)

        info = historical or (
            gateway._tag_info(tag) if gateway.policy.strict_cloud_authorization and not known else None
        )
        if info is not None:
            if info["status"] != "Accepted":
                raise DeniedError("authorization changed before accepting the new session")
            if historical is None and not gateway._local_tag(tag):
                gateway.auth_info.require_dynamic(info)
        answer = gateway.journal.start(token, payload, accepted, info)
        gateway.journal.outcomes.retain_bound_owner_start()
        return answer

    # One exact-scoped owned Start publishes authority in its child, while retaining truthful failed wire outcomes.
    async def start_managed_result(
        self,
        gateway: Gateway,
        tag: str,
        guard: Callable[[], None],
        *,
        request_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
        handoff: StartHandoff | None = None,
    ) -> OwnedStartResult:
        connection = current_control_scope(gateway)

        # Restrictive rollback retains the exact source even after the positive management grant is revoked.
        def source() -> None:
            if not control_scope_matches(gateway, connection):
                raise UnavailableError("managed Start belongs to a superseded Boot observation")

        # Revocation or a newer Pause can invalidate authorization while native evidence is awaited.
        def check() -> None:
            guard()
            if not gateway._local_start_allowed(tag):
                raise DeniedError("managed test tag authorization is no longer current")

        check()
        return await self.control.start_managed_result(
            tag,
            check,
            request_id=request_id,
            on_prepared=on_prepared,
            scope_guard=source,
            scope=connection[0] if connection is not None else None,
            handoff=handoff,
        )

    # Claim and prepare the exact local Start in one transaction before native or wire work.
    def claim_local_start(
        self,
        tag: str,
        guard: Callable[[], None] | None,
        scope_guard: Callable[[], None] | None,
        request_id: str | None,
        on_prepared: Callable[[], None] | None,
    ) -> tuple[int, str, dict[str, Any]]:
        if scope_guard is not None:
            scope_guard()
        if guard is not None:
            guard()
        if not self.control.policy.allow_local_start and guard is None:
            raise DeniedError("local Start is disabled; charging remains under the app and charger schedule")
        if self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if previous.get("uncertain") or previous.get("start_pending"):
            raise RecoveryRequiredError("previous local outcome requires reconciliation")
        if (
            not any(tag.casefold() == approved.casefold() for approved in self.control.policy.allowed_tags)
            or self.journal.active() is not None
        ):
            raise DeniedError("tag is not allowed or a transaction is unresolved")
        with self.journal.atomic():
            epoch, ticket = self.control._claim(previous.get("inhibit", False))
            self.journal.starts.prepare(ticket, fingerprint(tag.casefold()), request_id)
            if on_prepared is not None:
                on_prepared()
        return epoch, ticket, previous

    # Check the original Boot, active lease and the latest tag approval before positive work.
    def require_local_start(self, tag: str, boot: str | None, guard: Callable[[], None] | None) -> None:
        if self.control.boot() != boot:
            raise UnavailableError("Start lost its original charger Boot")
        if guard is not None:
            guard()
        if self.control.authorize is not None and not self.control.authorize(tag):
            raise DeniedError("authorization changed before forwarding a local start")

    # Arm only the current ticket after its ceiling and current authorization agree.
    def arm_local_start(self, ticket: str, check: Callable[[], None]) -> None:
        check()
        with self.journal.atomic():
            self.journal.starts.armed(ticket)

    # A real lifecycle that arrived in the meantime supersedes the pending outgoing Start.
    def issue_local_start(self, ticket: str) -> None:
        if self.journal.active() is not None:
            raise DeniedError("an actual transaction superseded the pending Start before dispatch")
        self.journal.starts.record(ticket, "issued")

    # A genuine reply may outlive its permission. Publish only within the current ticket's transaction.
    def complete_local_start(
        self,
        ticket: str,
        accepted: bool,
        check: Callable[[], None],
        shared: bool,
        handoff: StartHandoff | None,
        context: StartHandoffContext | None,
    ) -> StartPublication | None:
        if not accepted:
            raise UnavailableError("transaction start was not confirmed")
        check()
        publication = None
        with self.journal.atomic():
            check()
            if handoff is not None:
                if context is None:
                    raise RecoveryRequiredError("managed Start publication lacks its exact Boot scope")
                publication = handoff.write_in_transaction(context)
            self.control._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
        if publication is not None:
            assert handoff is not None
            handoff.committed(publication)
        return publication

    # A vendor Start under strict authorization needs a real accepted authorization before it can prepare.
    async def authorize_vendor_start(self, gateway: Gateway, tag: str, require_origin: Callable[[], None]) -> None:
        if not gateway.policy.strict_cloud_authorization:
            return
        require_origin()
        info = await gateway._cloud_authorize(tag, guard=require_origin)
        if info["status"] != "Accepted":
            raise DeniedError("cloud refused the trial start tag")
        if not gateway._local_tag(tag):
            gateway.auth_info.require_dynamic(info)

    # Both reservation and dispatch depend on the current authorization, not the earlier reply.
    def _require_current_vendor_tag(self, gateway: Gateway, tag: str, refusal: str) -> None:
        if gateway.policy.strict_cloud_authorization:
            info = gateway.auth_info.get(tag)
            if info is None or info["status"] != "Accepted":
                raise DeniedError(refusal)
            if not gateway._local_tag(tag):
                gateway.auth_info.require_dynamic(info)

    # Reserve within the exact command's preparation transaction. Dispatch issues this same grant.
    def reserve_vendor_start(self, gateway: Gateway, tag: str) -> str | None:
        self._require_current_vendor_tag(gateway, tag, "trial authorization expired before forwarding")
        return gateway.authorisation.reserve(tag, prepared=True)

    # After independent validation, the outgoing Start must still own its original tag and reservation.
    def require_vendor_start(self, gateway: Gateway, tag: str, grant: str | None) -> None:
        self._require_current_vendor_tag(gateway, tag, "cloud authorization expired during native validation")
        if grant is not None:
            saved = self.journal.get("vendor_start_grant", {})
            if (
                saved.get("token") != grant
                or saved.get("local") is not None
                or not gateway.authorisation.allows(tag, issuing=grant)
                or not gateway._tag_allowed(tag, issuing_grant=grant)
            ):
                raise DeniedError("this vendor start no longer owns its current tag grant")

    # A vendor Start can wait on neither a session nor an unresolved current selection.
    def require_vendor_start_idle(self) -> None:
        if self.journal.active() is not None or self.journal.get("control", {}).get("start_pending"):
            raise DeniedError("a transaction is already unresolved on this connector")
        if self.journal.current_change.read().selecting:
            raise DeniedError("cloud Start awaits verification of the current selection")

    # Attach the prepared grant to its command before the wire can issue either one.
    def bind_vendor_start(self, mutation: str) -> None:
        current_grant = self.journal.get("vendor_start_grant", {})
        self.journal.put("vendor_start_grant", {**current_grant, "mutation": mutation})

    # Issue the prepared grant in the same transaction that records possible transmission.
    def issue_vendor_start(self, gateway: Gateway, grant: str) -> None:
        gateway.authorisation.issue(grant)

    # A native daily schedule can renew a cloud decision that expired or previously denied permission.
    async def renew_reported_start(
        self,
        gateway: Gateway,
        payload: dict[str, Any],
        epoch: int,
        *,
        held_native: bool = False,
        check_origin: Callable[[], None] | None = None,
    ) -> None:
        tag = payload.get("idTag")
        if (
            not isinstance(tag, str)
            or not gateway._local_tag(tag)
            or gateway.retiring()
            and not held_native
            or gateway.vendor.sender is None
            or gateway.journal.known_start(payload)
            or gateway.journal.active() is not None
            or gateway.journal.get("control", {}).get("inhibit")
            and not held_native
            or gateway.journal.phase.starting
            or not gateway.journal.phase.read().legacy_active
            and not (held_native and gateway.journal.phase.read().value == "recovering")
            or gateway.authorisation.allows(tag)
        ):
            return
        stored = gateway.auth_info.stored(tag)
        if stored is None or gateway._tag_info(tag)["status"] == "Accepted":
            return
        integer(payload.get("connectorId"), 1, 1)
        number(payload.get("meterStart"), 1e12)
        ticket = gateway.journal.get("control", {}).get("ticket")

        # Work from a retired plug connection or superseded owner operation cannot renew current authority.
        def guard() -> None:
            gateway._require_epoch("charger", epoch)
            if check_origin is not None:
                check_origin()
            if (
                gateway.journal.get("control", {}).get("ticket") != ticket
                or gateway.auth_info.stored(tag) != stored
                or gateway.retiring()
                and not held_native
                or held_native
                and (not gateway.controls.ready or gateway.controls.desired() != 0)
            ):
                raise DeniedError("scheduled start authority changed during renewal")

        # Share even a timeout or unchanged denial across this burst of retries. A later burst may retry.
        key = (tag.casefold(), epoch, gateway._vendor_epoch, ticket, gateway.boot_observation)
        pending = self._start_renewals.get(key)
        if pending is not None:
            if not await asyncio.shield(pending):
                raise UnavailableError("scheduled start renewal was unavailable")
            return
        pending = asyncio.get_running_loop().create_future()
        self._start_renewals[key] = pending
        received = False
        try:
            try:
                await gateway._cloud_authorize(tag, guard=guard)
            except DeniedError:
                # An ordinary Authorize can publish a fresh decision while this Start waits for the vendor gate.
                gateway._require_epoch("charger", epoch)
                gateway._require_epoch("vendor", key[2])
                current = gateway.auth_info.get(tag)
                if (
                    gateway.journal.get("control", {}).get("ticket") != ticket
                    or current is None
                    or current["status"] != "Accepted"
                    or not held_native
                    and not gateway._tag_allowed(tag)
                ):
                    raise
            received = True
        finally:
            pending.set_result(received)
            del self._start_renewals[key]

    # A normal Start can request one fresh cloud decision before any charger effect, under exact current authority.
    async def renew_local_start(self, charging: ManagedCharging, lease: ChargeLease) -> str | None:
        c, g = charging.controller, charging.controller.session.gateway
        if any(g._local_start_allowed(tag) for tag in g.policy.allowed_tags):
            return None
        if charging.authorization_status() != "renewal_required":
            raise DeniedError("a current local Start approval is unavailable")
        tag = next(tag for tag in sorted(g.policy.allowed_tags) if g.local_start_expired(tag))
        authority, stored = c.authority(), g.auth_info.stored(tag)
        if authority is None:
            raise DeniedError("Start approval renewal requires current connection authority")

        # A newer owner operation, Boot metadata or tag decision must win over this pending cloud reply.
        def guard() -> None:
            charging.guard(lease)
            if c.authority() != authority or g.auth_info.stored(tag) != stored:
                raise DeniedError("Start approval renewal was superseded")

        guard()
        await g._cloud_authorize(tag, guard=guard)
        charging.guard(lease)
        if c.authority() != authority or not g._local_start_allowed(tag):
            raise DeniedError("the manufacturer did not provide a current local Start approval")
        return tag

    # Accept the fresh idle sample for the exact lease before the worker can take its zero hold.
    def require_idle_start(self, charging: ManagedCharging, lease: ChargeLease, initial: HandoverEvidence) -> None:
        c, j = charging.controller, self.journal
        c.native.require(initial, closed=True)
        if initial.physical.connection != "Plug":
            raise DeniedError("test requires a freshly connected idle vehicle")
        charging.guard(lease)
        if j.active() is not None:
            raise DeniedError("another session began before the Start's idle hold")

    # Select a tag only while its local authorization is still current.
    def choose_start_tag(self, gateway: Gateway, renewed_tag: str | None) -> str:
        tags = sorted(tag for tag in gateway.policy.allowed_tags if gateway._local_start_allowed(tag))
        if not tags:
            raise DeniedError("no current approved local tag is available for this test")
        tag = renewed_tag if renewed_tag is not None else tags[0]
        if not gateway._local_start_allowed(tag):
            raise DeniedError("the selected local Start approval was superseded")
        return tag

    # Bind an actual accepted Start inside its ledger transaction, including sessions ending between native polls.
    def bind_managed_start(self, charging: ManagedCharging, local: int, payload: dict[str, Any]) -> None:
        lease = charging.lease
        if lease is not None:
            try:
                self.journal.starts.managed_bound(lease, local, payload.get("idTag"), lambda: charging.guard(lease))
            except DeniedError:
                return

    # A failed normal Start can reclaim only zero from its own unchanged ticket beneath the original owner Pause.
    def failed_start_zero_needed(
        self, charging: ManagedCharging, lease: ChargeLease, reason: str, failure: dict[str, Any] | None
    ) -> bool:
        c, j, g = charging.controller, self.journal, charging.controller.session.gateway
        intent, state, receipt, control = (
            c.requests.intent(),
            charging.state(),
            j.starts.receipt(),
            j.get("control", {}),
        )
        return bool(
            charging.lease is lease
            and lease.normal
            and reason == "start_not_confirmed"
            and failure is not None
            and failure.get("id") == lease.identifier
            and failure.get("effects_started") is True
            and lease.mode == intent.mode == "paused"
            and lease.revision == intent.revision
            and lease.start_scope is not None
            and control_scope_matches(g, (lease.start_scope, lease.control_revision))
            and state.get("id") == lease.identifier
            and state.get("source") == "local"
            and state.get("start_attempted") is True
            and receipt.get("request_id") == lease.identifier
            and receipt.get("digest") == state.get("tag_digest")
            and receipt.get("stage") in ("issued", "accepted")
            and receipt.get("ticket")
            and control.get("ticket") == receipt["ticket"]
            and control.get("owner") in ("local", "shared")
            and control.get("inhibit") is False
            and j.phase.read().active
            and not j.fault.latched()
            and not c.stopping
        )

    # Stop only the expired owner's verified session, including if the zero write fails first.
    async def stop_expired_start(
        self,
        charging: ManagedCharging,
        scope: ExpiredStartStop,
        read_session: Callable[[int | None], Awaitable[HandoverEvidence]],
    ) -> None:
        c, j = charging.controller, self.journal
        g = c.session.gateway

        # Recheck the original Start, current intent and charger connection at the actual Stop boundary.
        def guard() -> None:
            receipt = j.starts.receipt()
            if (
                c.stopping
                or c.requests.intent() != scope.intent
                or not charging.scoped_normal_stop()
                or charging.state().get("id") != scope.identifier
                or not control_scope_matches(g, scope.connection)
                or receipt.get("local") != scope.local
                or receipt.get("digest") != scope.tag_digest
                or j.active() not in (None, scope.local)
                or not c.native.commissioned_binding_matches(scope.native)
            ):
                raise RecoveryRequiredError("expired owner Stop was superseded")

        # An expired normal Start uses the same draw check as an explicit Pause.
        def station_flowing() -> bool:
            return c.native.still_drawing(scope.native, scope.sequence)

        # If the zero call failed, refresh the exact active session before permitting a protective Stop.
        async def prove_stop(local: int, ceiling_failed: bool, _awaiting_report: bool) -> StopProof:
            if not ceiling_failed:
                return StopProof(guard, station_flowing)
            fresh = await read_session(local)
            fresh_sequence = c.native.last_resolved_read

            # The wire guard may run after a later read, closure or Boot has retired this protection.
            def require_fresh_target() -> None:
                guard()
                fresh.physical.require_fresh(j.identity, 5)
                if (
                    fresh_sequence != c.native.last_resolved_read
                    or not fresh.physical.transaction_open
                    or fresh.physical.native_transaction_id != local
                    or j.active() != local
                    or not c.native.commissioned_binding_matches(fresh)
                ):
                    raise RecoveryRequiredError("expired Start Stop awaits the current active session")

            c.commands.decide(require_fresh_target, priority=CommandPriority.RESTRICT)
            return StopProof(require_fresh_target)

        if self.stop_work_needed(scope.local, station_flowing=station_flowing):
            await self.control.stop(expected_local=scope.local, guard=guard, prove_stop=prove_stop)

    # An accepted known-session Stop waits for its genuine closure instead of repeating after each native poll.
    def stop_acknowledged(self, local: int | None, *, protective: bool = False) -> bool:
        # A Stop sent under a previously sound zero cannot cover a newly observed loss of that protection.
        def accepted() -> bool:
            if not protective:
                return self.journal.outcomes.accepted_stop(local)
            scope = self._protective_stop
            return (
                scope is not None
                and scope[:3] == (local, self.control.boot(), self.control._revision)
                and self.journal.outcomes.accepted_stop(local, ticket=scope[3])
            )

        return self.peer.decide(accepted, priority=CommandPriority.RESTRICT)

    # Apply the same exact-session and prior-reply decision for both local and vendor Stop requests.
    def stop_decision(
        self,
        local: int | None,
        *,
        protective: bool = False,
        station_flowing: Callable[[], bool] | None = None,
        reuse_allowed: bool = True,
    ) -> StopDecision:
        # Read the exact report and reply in the same queued decision as the latest draw evidence.
        def decide() -> StopDecision:
            session = self.journal.outcomes.session(local)
            if session is None or session.closed or self.journal.active() != session.local:
                return StopDecision.CLOSED
            if not reuse_allowed:
                return StopDecision.SEND
            protecting = protective or (station_flowing is not None and station_flowing())
            if self.stop_acknowledged(session.local, protective=protecting):
                return StopDecision.ACCEPTED
            if not protecting and self.journal.outcomes.unanswered_shutdown_stop(session.local):
                return StopDecision.WAIT_FOR_REPORT
            return StopDecision.SEND

        return self.peer.decide(decide, priority=CommandPriority.RESTRICT)

    # Local operations check whether they still need to send, including after a newer receipt or report arrives.
    def stop_needed(
        self, local: int | None, *, protective: bool = False, station_flowing: Callable[[], bool] | None = None
    ) -> bool:
        return self.stop_decision(local, protective=protective, station_flowing=station_flowing) is StopDecision.SEND

    # An accepted Stop can suppress the wire call while the current Boot still needs a zero hold.
    def stop_work_needed(
        self, local: int | None, *, station_absent: bool = False, station_flowing: Callable[[], bool] | None = None
    ) -> bool:
        # Check the station and session against the same live command state.
        def decide() -> bool:
            return (
                not station_absent
                and (not self.control.ready or self.control.desired() != 0)
                or self.stop_needed(local, protective=station_absent, station_flowing=station_flowing)
            )

        return self.peer.decide(decide, priority=CommandPriority.RESTRICT)

    # A fresh Pause owns its hold in the same transaction that saves the request and revokes a prior Return.
    def accept_pause(
        self,
        requests: ManagedRequests,
        identifier: str,
        revision: int,
        capture_release_proof: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any]:
        ticket = None

        # Commit the Pause and its restrictive claim together before any physical work starts.
        def claim() -> None:
            nonlocal ticket
            _, ticket = self.control._claim(True, stopping=True)
            requests.record_pause_claim(requests.intent(), ticket)

        receipt = requests.accept(identifier, "pause", revision, claim, capture_release_proof=capture_release_proof)
        if ticket is not None:
            self.control.observe_incident("pause_committed", identifier, ticket)
        return receipt

    # Pin the Pause and physical target before any awaited native observation.
    def prepare_pause_stop(self, gateway: Gateway, requests: ManagedRequests) -> PauseStopClaim:
        intent, connection = requests.intent(), current_control_scope(gateway)
        request, local = requests.pause_request(), self.journal.active()
        control = self.journal.get("control", {})
        return PauseStopClaim(intent, request, local, connection, control.get("stop_epoch", 0), control.get("ticket"))

    # Revalidate the Pause and its physical claim at both admission and the actual send boundary.
    def require_pause_stop(
        self, gateway: Gateway, requests: ManagedRequests, claim: PauseStopClaim, boot_digest: str
    ) -> None:
        active = self.journal.active()
        # A genuine report can close the exact target after the claim but before its acknowledgement.
        same_target = active == claim.local or active is None and claim.ticket is not None
        if (
            gateway.retiring()
            or claim.intent.mode != "paused"
            or requests.intent() != claim.intent
            or requests.pause_request() != claim.request
            or not control_scope_matches(gateway, claim.connection)
            or fingerprint(self.journal.get("boot")) != boot_digest
            or not same_target
        ):
            raise RecoveryRequiredError("owner Stop awaits the current Pause and known session")
        control = self.journal.get("control", {})
        if claim.ticket is None and (
            control.get("stop_epoch", 0) == claim.stop_epoch and control.get("ticket") == claim.previous_ticket
        ):
            return
        if (
            control.get("stop_epoch") != claim.stop_epoch + 1
            or not ControlState(control).explicit_local_hold
            or not isinstance(control.get("ticket"), str)
            or control["ticket"] != self.control._stop_ticket
            or claim.ticket != control["ticket"]
        ):
            raise RecoveryRequiredError("owner Stop claim was superseded")

    # Bind the exact target to the Pause inside the physical claim transaction.
    def bind_pause_stop(self, requests: ManagedRequests, claim: PauseStopClaim, ticket: str) -> None:
        if claim.connection is None:
            raise RecoveryRequiredError("owner Stop awaits its current Boot")
        requests.bind_pause_stop(claim.intent, ticket, claim.request, claim.local, claim.connection[0].boot)
        claim.ticket = ticket

    # Carry a still-current Pause through introduction while retaining the independent protective Stop.
    async def stop_after_introduction(
        self,
        gateway: Gateway,
        station_flowing: Callable[[], bool] | None,
        requests: ManagedRequests | None,
    ) -> Frame:
        connection, local = current_control_scope(gateway), self.journal.active()
        intent, request = None, None
        if requests is not None:
            try:
                pending, ownership = requests.pause_request(), requests.pause_ownership()
            except sqlite3.Error, SecurityError:
                if self.journal.db.in_transaction:
                    raise
                # An unreadable optional receipt must not block an independently authorized protective Stop.
            else:
                if (
                    pending is not None
                    and pending.revision == ownership.intent.revision
                    and ownership.owns_ticket(pending.hold)
                ):
                    intent, request = ownership.intent, pending

        # The same target may genuinely close before the Stop reply or before acquiring the claim.
        def guard() -> None:
            if (
                gateway.retiring()
                or not control_scope_matches(gateway, connection)
                or self.journal.active() not in (None, local)
                or intent is not None
                and requests is not None
                and requests.intent() != intent
            ):
                raise UnavailableError("local Stop belongs to a superseded Boot, transaction or Pause")

        # Introduction and the foreground Pause use the same persisted binding and join key.
        def bind(ticket: str) -> None:
            if requests is not None and intent is not None and connection is not None:
                requests.bind_pause_stop(intent, ticket, request, local, connection[0].boot)

        return await self.control.stop(
            expected_local=local,
            guard=guard,
            station_flowing=station_flowing,
            on_claim=bind,
            owner=("pause", intent, request, local, connection) if request is not None else None,
            request_id=request.identifier if request is not None else None,
        )

    # Settle the physical hold without a wire Stop when an independent observation already proves closed zero.
    def complete_closed_stop(
        self,
        native: HandoverEvidence,
        require_zero: Callable[[HandoverEvidence], None],
        record_observation: Callable[[HandoverEvidence], None],
    ) -> bool:
        if native.physical.transaction_open or not self.control.ready or self.control.desired() != 0:
            return False
        require_zero(native)
        record_observation(native)
        self.control._complete(self.journal.get("control")["ticket"], uncertain=False)
        return True

    # The current persistent Pause may reclaim only the shared ticket of its exact retired normal Start.
    def reclaim_failed_start_pause(self, c: ManagedController) -> None:
        g, j = c.session.gateway, c.session.journal
        intent, state = c.requests.intent(), c.charging.state()
        failure = state.get("failure")
        if (
            intent.mode != "paused"
            or state.get("kind") != "normal"
            or state.get("source") != "local"
            or state.get("state") != "stopping"
            or state.get("stop_reason") != "start_not_confirmed"
            or state.get("start_attempted") is not True
            or state.get("revision") != intent.revision
            or not isinstance(failure, dict)
            or failure.get("id") != state.get("id")
            or failure.get("action") != "start"
            or failure.get("revision") != intent.revision
            or failure.get("effects_started") is not True
        ):
            return
        receipt, control = j.starts.receipt(), j.get("control", {})
        ownership = c.requests.pause_ownership()
        connection = current_control_scope(g)
        row = j.db.execute(
            "SELECT action,revision,state FROM managed_requests WHERE id=?", (state.get("id"),)
        ).fetchone()
        if (
            connection is None
            or row is None
            or tuple(row) != ("start", intent.revision, "uncertain")
            or receipt.get("request_id") != state.get("id")
            or receipt.get("digest") != state.get("tag_digest")
            or receipt.get("stage") not in ("issued", "accepted")
            or not receipt.get("ticket")
            or control.get("ticket") != receipt["ticket"]
            or not ControlState(control).confirmed_shared
            or not ownership.requested
        ):
            return

        # This is current Pause authority under its own Boot, never adoption of the failed Start's older scope.
        def current() -> bool:
            return (
                not c.stopping
                and not j.fault.latched()
                and c.charging.lease is None
                and not g.controls._operations
                and not g.charger._pending
                and c.requests.intent() == intent
                and control_scope_matches(g, connection)
                and j.phase.read().active
                and c.charging.state() == state
                and j.starts.receipt() == receipt
                and j.get("control") == control
                and c.requests.pause_ownership().pause == ownership.pause
                and fingerprint(j.get("boot")) == c.native.spec.boot_digest
                and recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
                and j.db.execute(
                    "SELECT action,revision,state FROM managed_requests WHERE id=?", (state["id"],)
                ).fetchone()
                == row
            )

        if current():
            with j.atomic():
                if current():
                    g.controls._claim(True)

    # Fence the requested Pause zero to its current hold and commissioned Boot.
    def owner_pause_zero_guard(self, c: ManagedController, intent: ControlIntent) -> Callable[[], None]:
        g, j = c.session.gateway, self.journal
        ticket = j.get("control", {}).get("ticket")
        connection = current_control_scope(g)

        # Recheck the owner's claim and authenticated device immediately before each wire operation.
        def guard() -> None:
            control = j.get("control", {})
            if (
                c.stopping
                or c.requests.intent() != intent
                or not ticket
                or control.get("ticket") != ticket
                or not ControlState(control).explicit_local_hold
                or not control_scope_matches(g, connection)
                or fingerprint(j.get("boot")) != c.native.spec.boot_digest
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("owner Pause awaits the current commissioned station")

        return guard

    # A deliberate hold is confirmed only by actual closed/all-phase zero and the current owned station0 receipt.
    def require_paused(self, c: ManagedController, native: HandoverEvidence, *, local: int | None = None) -> None:
        reason = c.native.stop_zero_refusal(native, local=local) or self._paused_refusal(c)
        if reason is not None:
            error = RecoveryRequiredError("requested pause awaits independently verified closed zero")
            error.add_note(reason)
            raise error

    # A local hold needs its own current authority as well as the common physical Stop proof.
    def _paused_refusal(self, c: ManagedController) -> str | None:
        g, j = c.session.gateway, c.session.journal
        facts = ManagedFacts(c)
        if not facts.introduction_current():
            return "introduction_unconfirmed"
        if not ControlState(j.get("control", {})).local_hold:
            return "local_hold_missing"
        if j.get("control", {}).get("start_pending"):
            return "start_pending"
        if g.vendor_write_evidence().unresolved:
            return "vendor_command_uncertain"
        if g.charger._pending:
            return "charger_rpc_pending"
        if g.controls._operations:
            return "control_operation_pending"
        return None

    # Independently verify the newer owner's closed zero before clearing a superseded Start's pending flag.
    def settle_rejected_start_at_pause(
        self, c: ManagedController, native: HandoverEvidence, intent: ControlIntent, connection: ControlScope | None
    ) -> None:
        g, j = c.session.gateway, c.session.journal

        # Check only the exact newer Pause and its current hold in the Start settlement transaction.
        def require_hold(control: dict[str, Any]) -> None:
            pause, ownership = c.requests.pause_request(), c.requests.pause_ownership()
            if (
                c.stopping
                or ownership.intent != intent
                or pause is None
                or pause.revision != intent.revision
                or not ownership.owns_ticket(pause.hold)
                or not control_scope_matches(g, connection)
                or c.native.introduction_pending
                or c.native.audited_epoch != g._charger_epoch
                or j.get("control", {}) != control
                or not ControlState(control).explicit_local_hold
                or j.active() is not None
                or not g.controls.ready
                or g.controls.desired() != 0
                or g.charger._pending
                or g.controls._operations
                or not c.native.commissioned_binding_matches(native)
            ):
                raise RecoveryRequiredError("rejected Start settlement awaits its current owner Pause")
            c.native.require(native, closed=True)
            key = f"profile:{HUB_PROFILE_ID}"
            station = [resource for resource in native.physical.resources if resource.key == key]
            row = j.db.execute("SELECT state,confirmed FROM trial_resources WHERE key=?", (key,)).fetchone()
            if not c.session.book.confirmed_station_zero(station, row):
                raise RecoveryRequiredError("rejected Start settlement awaits the confirmed station zero")

        try:
            j.starts.settle_rejected_pause(intent.revision, require_hold)
        except sqlite3.Error as error:
            raise RecoveryRequiredError("rejected Start settlement awaits journal storage") from error

    # Settle closure, related receipts and the exact owner Pause together.
    def accept_pause_observation(
        self, c: ManagedController, native: HandoverEvidence, intent: ControlIntent, connection: ControlScope | None
    ) -> bool:
        g = c.session.gateway
        if c.stopping or c.requests.intent() != intent or not control_scope_matches(g, connection):
            raise RecoveryRequiredError("Pause observation no longer belongs to the current request and Boot")
        c.native.require_known_session(native)
        if (
            native.physical.transaction_open
            or not c.session.gateway.controls.ready
            or c.session.gateway.controls.desired() != 0
            or not ControlState(c.session.journal.get("control", {})).local_hold
        ):
            return False
        self.settle_rejected_start_at_pause(c, native, intent, connection)
        if (c.native.introduced_epoch, c.native.introduced_revision) != (g._charger_epoch, g.controls._revision):
            c.native.confirm_paused_introduction(native)
        c.require_paused(native)
        records = tuple(record for record in (current_candidate(c), charge_test_candidate(c)) if record is not None)
        self.settle_pause_records(c, native, records, internal=internal_pause_matches(c))
        if c.current.failed_idle_hold():
            # This fresh closed-zero proof settles only the physical hold, without issuing another Stop.
            g.controls._complete(c.session.journal.get("control")["ticket"], uncertain=False)
            raise RecoveryRequiredError("failed current hold awaits a fresh settled Pause observation")
        # Physical Pause is already proved. Cloud delivery debt can still block reactivation independently.
        c.complete_pause_observation(native, intent, connection)
        return True

    # Foreground Stop and Pause supervision retire exact related records at the same settled zero boundary.
    def settle_pause_records(
        self,
        c: ManagedController,
        native: HandoverEvidence,
        records: tuple[TerminalCurrent | RetirementCandidate, ...],
        *,
        internal: bool = False,
    ) -> None:
        g, j = c.session.gateway, c.session.journal
        if records or internal:
            try:
                require_settled_debt(c)
            except RecoveryRequiredError:
                pass  # Unknown Start and lifecycle facts remain authoritative even at fresh native zero.
            else:
                with j.atomic():
                    c.require_paused(native)
                    require_settled_debt(c)
                    for record in records:
                        complete_record(c, record)
                    if internal:
                        g.controls._complete(j.get("control")["ticket"], uncertain=False)
                if any(isinstance(record, RetirementCandidate) for record in records):
                    j.starts.set_lease(None)
                    c.charging.changed.set()

    # A successful Stop reply alone cannot finish Pause; require an independent fresh closed-zero proof.
    def complete_pause(
        self,
        gateway: Gateway,
        requests: ManagedRequests,
        native: HandoverEvidence,
        intent: ControlIntent,
        connection: ControlScope | None,
        require_zero: Callable[[int | None], None],
    ) -> None:
        if gateway.retiring() or requests.intent() != intent or not control_scope_matches(gateway, connection):
            raise RecoveryRequiredError("Pause observation no longer belongs to the current request and Boot")
        native.physical.require_fresh(self.journal.identity, 5)
        request = requests.pause_request()
        require_zero(request.local if request is not None else None)
        if awaiting_owner_start(self.journal):
            raise RecoveryRequiredError("owner Pause awaits the previously acknowledged Start lifecycle")
        requests.complete_verified_pause()

    # Do not add a native read to a first vendor Stop; refresh only if a prior outcome may cover it.
    async def prepare_vendor_stop(
        self, local: int, evidence: StopZeroEvidence, require_origin: Callable[[], None]
    ) -> None:
        # Read the prior outcome and the current desired hold together after acquiring the wire gate.
        def may_reuse() -> bool:
            require_origin()
            return (
                self.control.ready
                and self.control.desired() == 0
                and self.stop_decision(local) in (StopDecision.ACCEPTED, StopDecision.WAIT_FOR_REPORT)
            )

        if self.peer.decide(may_reuse, priority=CommandPriority.RESTRICT):
            await evidence.refresh_stop_zero(local)

    # The gateway renders this decision with its own request ID and keeps any unsent mutation separate.
    def require_vendor_stop(self, local: int, boot: str, evidence: StopZeroEvidence) -> None:
        decision = self._stop_send_decision(local, boot, reuse_allowed=evidence.stop_zero_verified(local))
        if decision is StopDecision.ACCEPTED:
            raise VendorStopAlreadyAcceptedError
        if decision is StopDecision.WAIT_FOR_REPORT:
            raise RecoveryRequiredError("cloud Stop awaits the charger's original report")
        if decision is StopDecision.CLOSED:
            raise DeniedError("cloud Stop lost its original open session")

    # Both wire callers authorize a Stop against the exact open target, prior reply and original Boot.
    def _stop_send_decision(
        self, local: int, boot: str | None, *, protective: bool = False, reuse_allowed: bool = True
    ) -> StopDecision:
        decision = self.stop_decision(local, protective=protective, reuse_allowed=reuse_allowed)
        if decision is StopDecision.SEND and self.control.boot() != boot:
            raise UnavailableError("Stop lost its original charger Boot or open session")
        return decision

    # Authorize the local executor's send and remember exactly which attempt protects a lost zero.
    def authorize_local_send(self, local: int, boot: str | None, ticket: str, *, protective: bool) -> bool:
        if self._stop_send_decision(local, boot, protective=protective) is not StopDecision.SEND:
            return False
        if protective:
            self._protective_stop = local, boot, self.control._revision, ticket
        return True

    # Revoke positive authority before attempting the durable shutdown claim, even if that write fails.
    def begin_shutdown(self, retiring: Callable[[], bool]) -> None:
        # The process signal, pending positive work, and final Stop share the same decision queue.
        def claim() -> None:
            if retiring():
                return
            self.shutdown_requested = True
            self.control._claim(True, stopping=True)

        self.peer.decide(claim, priority=CommandPriority.SHUTDOWN)

    # Retain the local hold even when shutdown finds no connected charger to verify it.
    def shutdown_target(self) -> int | None:
        if self.peer.sender is None:
            self.journal.control.hold(self.journal.get("control", {}))
            raise UnavailableError("charger disconnected before the final physical hold")
        return self.journal.active()

    # The final shutdown Stop uses its own claim and keeps the receiver alive for genuine closure.
    async def stop_for_shutdown(
        self, gateway: Gateway, native: NativeBoundedHandover, attempt: str, local: int | None
    ) -> None:
        j = self.journal
        connection = current_control_scope(gateway)

        # The shutdown attempt and physical hold claim commit in the same transaction.
        def bind(ticket: str) -> None:
            j.outcomes.claim_shutdown_stop(attempt, local, ticket, connection[0].boot if connection else None)

        # Give the zero write its own authority. A Stop must still belong to the original live Boot.
        async def prove(target: int, ceiling_failed: bool, awaiting_report: bool) -> StopProof:
            # Bind shutdown Stop to the commissioned Boot and the same active transaction.
            def require_scope() -> None:
                if (
                    not control_scope_matches(gateway, connection)
                    or j.active() != target
                    or fingerprint(j.get("boot")) != native.spec.boot_digest
                ):
                    raise RecoveryRequiredError("shutdown Stop lost its original connection or transaction")

            require_scope()
            if not ceiling_failed and not awaiting_report:
                return StopProof(require_scope)

            # A saved Stop can cover a current zero once fresh evidence has checked for resumed draw.
            try:
                async with asyncio.timeout(5):
                    sample = await native.read(closing_local=target, stop_only=True)
            except _NativeMeasurementError as error:
                if error.authority != native.authority() or error.sequence != native.last_resolved_read:
                    raise
                sample = error.native
                require_allowed(readiness_decision(sample, False, native.allow_unplugged, target))
            except (
                NativeTransportUnavailableError,
                NativeReadinessError,
                StaleEvidenceError,
                UnavailableError,
                TimeoutError,
            ):
                if not ceiling_failed:
                    return StopProof(require_scope)
                raise
            sequence = native.last_resolved_read

            # A later reading can supersede this draw evidence before dispatch.
            def station_flowing() -> bool:
                return native.still_drawing(sample, sequence) and native.commissioned_binding_matches(sample)

            if not ceiling_failed and not station_flowing():
                return StopProof(require_scope)
            native.require_known_session(sample)

            # Recheck the independent session proof before sending Stop without a confirmed zero.
            def guard() -> None:
                require_scope()
                sample.physical.require_fresh(j.identity, 5)
                if (
                    not sample.physical.transaction_open
                    or sample.physical.history is None
                    or native.last_resolved_read != sequence
                    or not native.commissioned_binding_matches(sample)
                ):
                    raise RecoveryRequiredError("shutdown Stop awaits current proof of the known transaction")

            return StopProof(guard if ceiling_failed else require_scope, station_flowing)

        try:
            await self.control.stop(
                expected_local=local,
                prove_stop=prove,
                on_claim=bind,
                owner=("shutdown", attempt, local, connection),
                priority=CommandPriority.SHUTDOWN,
            )
        except StopReplyUnconfirmedError:
            # The independent final checks may still establish actual closure and an owned physical zero.
            pass
        except RecoveryRequiredError:
            session = j.outcomes.session(local)
            if session is None or not session.closed:
                raise

    # Both shutdown polling and outcome publication depend on real reports for the exact closed session.
    def _shutdown_reports_settled(self, gateway: Gateway, local: int | None) -> bool:
        return (
            self.journal.outcomes.stop_report_closed(local)
            and not self.journal.control.read().values.get("start_pending")
            and not gateway.authorisation.pending()
        )

    # An accepted Start that reports late gets its own Stop instead of replacing the first target.
    def shutdown_report_progress(
        self, gateway: Gateway, stopping_local: int | None, *, accept_late_start: bool
    ) -> tuple[int | None, bool]:
        local = self.journal.active()
        if accept_late_start and local is not None and local != stopping_local:
            return local, False
        return None, self._shutdown_reports_settled(gateway, stopping_local)

    # Confirm the shutdown hold against a separately observed closed session and station zero.
    def require_shutdown_zero(self, native: NativeBoundedHandover, sample: HandoverEvidence, local: int | None) -> None:
        if self.peer.decide(lambda: native.stop_zero_refusal(sample, local=local), priority=CommandPriority.SHUTDOWN):
            raise RecoveryRequiredError("shutdown awaits the independently verified station zero")

    # Keep uncertainty durable and clear it only after independent shutdown verification.
    def record_shutdown_outcome(self, gateway: Gateway, attempt: str, state: Literal["verified", "uncertain"]) -> None:
        # Publish with the current attempt and the exact lifecycle still in view.
        def record() -> None:
            j = self.journal
            current = j.get("trial_shutdown_attempt", {})
            local = current.get("local") if current.get("id") == attempt else None
            if state == "verified" and not self._shutdown_reports_settled(gateway, local):
                raise RecoveryRequiredError("shutdown still awaits an exact lifecycle report")
            with j.atomic():
                j.outcomes.record_shutdown_stop(attempt, state)
                if state == "verified":
                    j.db.execute("DELETE FROM meta WHERE key='trial_shutdown_uncertain'")
                else:
                    j.put("trial_shutdown_uncertain", True)

        self.peer.decide(record, priority=CommandPriority.SHUTDOWN)

    # Save the first strict fault and withdraw authority atomically, including before the first poll.
    def latch_fault(
        self, reason: str, *, diagnostic: dict[str, Any] | None = None, forwarded_timeout: str | None = None
    ) -> None:
        # A failed write leaves the original observation to be retried by its caller.
        def latch() -> None:
            j = self.journal
            if j.fault.latched():
                return
            with j.atomic():
                fault: dict[str, Any] = {"classification": reason, "at_epoch": time.time()}
                if forwarded_timeout is not None:
                    fault["forwarded_timeout"] = forwarded_timeout
                if diagnostic is not None:
                    j.put("managed_native_failure", diagnostic)
                    fault["native_failure_digest"] = fingerprint(diagnostic)
                j.fault.replace(fault)
                j.phase.fault(j.get("control", {}))
            self.control.invalidate()

        self.peer.decide(latch, priority=CommandPriority.RESTRICT)

    # Maintain only the exact retained owned zero profile while the same fault is latched.
    async def hold_fault_zero(self, gateway: Gateway, native: NativeBoundedHandover) -> None:
        j = self.journal
        fault, control = j.fault.read(), j.get("control", {})
        epoch, boot, revision = self.control.epoch(), self.control.boot(), self.control._revision
        binding, handover = j.get("trial_binding", {}), j.get("native_bounded_handover", {})
        query = "SELECT key,desired,confirmed,state FROM trial_resources WHERE key=?"
        station_key = ("profile:" + str(HUB_PROFILE_ID),)
        station = j.db.execute(query, station_key).fetchone()
        zero = encoded(profile_projection(native.choice.payload(0)))
        # A retained profile can refresh its receipt while verifying the same zero.
        allowed = {tuple(station) if station is not None else None}
        if station is not None and station["state"] != "absent_verified":
            allowed.update(
                {
                    (station["key"], zero, station["confirmed"], "pending"),
                    (station["key"], zero, station["confirmed"], "uncertain"),
                    (station["key"], zero, zero, "confirmed"),
                }
            )

        # Keep the exact current fault, claim, authenticated Boot and commissioned station at every write.
        def guard() -> None:
            row = j.db.execute(query, station_key).fetchone()
            if (
                gateway.retiring()
                or not fault
                or j.fault.read() != fault
                or j.get("control", {}) != control
                or not ControlState(control).explicit_local_hold
                or self.control.desired() != 0
                or not j.phase.read().supervised
                or self.peer.sender is None
                or self.control.epoch() != epoch
                or self.control.boot() != boot
                or self.control._revision != revision
                or not gateway.current_boot_seen(epoch)
                or fingerprint(j.get("boot")) != native.spec.boot_digest
                or j.get("managed_mode", {}).get("spec_digest") != native.spec.digest
                or j.get("trial_binding") != binding
                or binding.get("boot_digest") != native.spec.boot_digest
                or binding.get("station") != native.choice.value()
                or j.get("native_bounded_handover") != handover
                or handover.get("stage") != "admitted"
                or handover.get("spec") != native.spec.value()
                or handover.get("fault")
                or native.book.station_ceiling_lost()
                or row is None
                or row["state"] == "absent_verified"
                or tuple(row) not in allowed
            ):
                raise RecoveryRequiredError("fault zero hold awaits the same admitted owner and current Boot")

        try:
            guard()
            if self.control.ready:
                return
            now = time.monotonic()
            self._fault_zero_retry = {key: until for key, until in self._fault_zero_retry.items() if until > now}
            key = fingerprint({"fault": fault, "epoch": epoch, "boot": boot, "revision": revision, "control": control})
            if key in self._fault_zero_retry:
                return
            self._fault_zero_retry[key] = now + 15
            await self.control.ensure_ceiling(guard=guard)
        except (
            DeniedError,
            SecurityError,
            RecoveryRequiredError,
            UnavailableError,
            TimeoutError,
            StationZeroStorageError,
        ):
            return  # A safety reply alone does not establish measured zero. The fault stays latched.

    # Verified loss of the station ceiling permits a protective Stop for only the exact known session.
    async def stop_for_missing_station(
        self, gateway: Gateway, native: NativeBoundedHandover, requests: ManagedRequests, *, pause_active: bool
    ) -> None:
        j = self.journal
        local, connection = j.active(), current_control_scope(gateway)
        if (
            local is None
            or connection is None
            or not j.phase.read().supervised
            or not (native.book.station_ceiling_lost() or j.phase.read().held)
            or gateway.retiring()
            or self.control._operations
            or self.peer._pending
            or pause_active
            or not self.stop_needed(local, protective=True)
        ):
            return
        intent = requests.intent()
        try:
            try:
                sample = await native.read(owned=True, closing_local=local)
            except _NativeMeasurementError as error:
                if error.authority != native.authority() or error.sequence != native.last_resolved_read:
                    return
                sample = error.native
                require_allowed(readiness_decision(sample, False, native.allow_unplugged, local))
            sequence = native.last_resolved_read

            # Only this observation may withdraw authority before taking its scoped Stop claim.
            def accept_absence() -> bool:
                nonlocal connection
                if (
                    native.last_resolved_read != sequence
                    or not control_scope_matches(gateway, connection)
                    or requests.intent() != intent
                    or gateway.retiring()
                    or j.active() != local
                    or not sample.physical.transaction_open
                    or sample.physical.native_transaction_id != local
                    or sample.physical.history is None
                    or any(resource.key == f"profile:{HUB_PROFILE_ID}" for resource in sample.physical.resources)
                    or not native.book.station_ceiling_lost()
                    or not native.commissioned_binding_matches(sample)
                ):
                    return False
                self.latch_fault(STATION_CEILING_LOST)
                # The fault invalidates old authority. The restrictive Stop needs the current revision.
                connection = current_control_scope(gateway)
                return True

            if not self.peer.decide(accept_absence, priority=CommandPriority.RESTRICT):
                return
            fault, sequence = j.fault.read(), native.last_resolved_read
            control = j.control.read()
            stop_epoch, prior_ticket = control.values.get("stop_epoch", 0), control.values.get("ticket")

            # Revalidate the exact session and the original or newly acquired Stop claim before dispatch.
            def guard() -> None:
                sample.physical.require_fresh(j.identity, 5)
                current = j.control.read()
                if (
                    gateway.retiring()
                    or not control_scope_matches(gateway, connection)
                    or requests.intent() != intent
                    or j.active() != local
                    or native.last_resolved_read != sequence
                    or j.fault.read() != fault
                    or not current.explicit_local_hold
                    or not native.commissioned_binding_matches(sample)
                    or j.get("trial_inventory_fault")
                    or j.get("trial_preservation_fault")
                    or not native.book.station_ceiling_lost()
                ):
                    raise RecoveryRequiredError("missing station Stop lost its current session proof")
                if (current.values.get("stop_epoch", 0), current.values.get("ticket")) not in (
                    (stop_epoch, prior_ticket),
                    (stop_epoch + 1, self.control._stop_ticket),
                ):
                    raise RecoveryRequiredError("missing station Stop claim was superseded")

            await self.control.stop(expected_local=local, guard=guard, station_absent=True)
        except DeniedError, RecoveryRequiredError, SecurityError, UnavailableError, TimeoutError, sqlite3.Error:
            return  # Closure and station zero still require independent proof.
