"""Daily charging supervision with verified native limits and durable owner requests."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from functools import partial
from typing import Any

from ..control import HUB_PROFILE_ID
from .command_processor import CommandPriority
from .composite import STATION_COMPOSITE_FAULT, ExcessiveCompositeError
from .connection_scope import ControlScope, control_scope_matches, current_control_scope, current_scope
from .control_state import ControlState
from .local_control import local_start_receipt
from .managed_charging import ManagedCharging
from .managed_current import (
    ManagedCurrentLimits,
    current_session_decision,
)
from .managed_current_recovery import capture_interrupted_collision
from .managed_facts import ManagedFacts
from .managed_faults import latch_managed_fault, stop_for_missing_station
from .managed_intent import OFFLINE_PAUSE, RELEASE_PAUSE_ACK, ControlIntent, ManagedRequests, write_intent
from .managed_native_repair import NativeRepairAudits
from .managed_release_recovery import (
    reconcile_owner_release,
    release_evidence,
    release_pause_ack,
)
from .managed_release_wait import awaiting_known_stops, awaiting_observations
from .managed_settlement import (
    charge_test_candidate,
    complete_record,
    current_candidate,
    internal_pause_matches,
    require_settled_debt,
)
from .managed_sources import awaiting_owner_start, recorded_station_profile
from .managed_start_binding import ManagedStartBinding, ManagedStartReports
from .managed_view import render_poll
from .native_bounded import NativeReadinessError, readiness_decision
from .native_bounded_handover import AuditSupersededError
from .native_faults import (
    MEASURED_FAULT,
    STATION_CEILING_LOST,
    MeasuredOutputLimitError,
    StationCeilingLostError,
    _NativeMeasurementError,
    diagnostic_type,
)
from .native_history import ORIGINAL_HISTORY_OMISSION, MissingOriginalHistoryError
from .native_http import NativeTransportUnavailableError
from .ordinary_recovery import OrdinaryRecovery
from .policy import DeniedError
from .recovery_contracts import ALLOW, AuthorityStamp, GateDecision, Pending, Prerequisite
from .recovery_coordinator import RecoveryCoordinator
from .rpc import UnavailableError
from .security import SecurityError, StaleEvidenceError
from .start_settlement import RetirementCandidate
from .store import JournalStartBusy, RecoveryRequiredError, StationZeroStorageError, encoded, fingerprint
from .supervision import ControlProof, HealthVerdict, Observation, PollStatus, assess
from .trial_evidence import Resource, profile_projection
from .trial_handover_evidence import HandoverEvidence
from .trial_rollback import lifecycle_decision
from .trial_session import TrialSession, managed_service_mode


class ManagedController:
    """One verified release per actual introduction returns control to the unchanged owner app/native schedule."""

    # Direct session retirement revokes managed positive permission before its independent shutdown zero awaits I/O.
    @property
    def stopping(self) -> bool:
        return self._stopping or self.session.closing

    # Explicit service shutdown and test callers retain the existing synchronous stop flag API.
    @stopping.setter
    def stopping(self, value: bool) -> None:
        self._stopping = value

    # Construct after admission so the allocation bootstrap cannot see unrelated managed-service metadata.
    def __init__(self, session: TrialSession) -> None:
        if session.native_bounded is None or session.gateway.policy.allow_local_start:
            raise SecurityError("managed operation requires bounded native authority and disabled local Start")
        if session.journal.get("trial_allocation") is None:
            raise SecurityError("managed supervision requires completed allocation admission")
        self.session = session
        self.native = session.native_bounded
        self.commands = session.gateway.use_command_processor()
        self.released_epoch: int | None = None
        self.failures = 0
        self.strict_failure_seen = False
        self.history_omission_only = True
        self.native_unavailable = 0
        self.fault_zero_retry: dict[str, float] = {}
        self.stopping = False
        expected = managed_service_mode(self.native.spec)
        saved = session.journal.get("managed_mode")
        if saved is not None and saved != expected:
            raise SecurityError("managed policy changed across process replacement")
        session.journal.put("managed_mode", expected)
        capture_interrupted_collision(session.journal, self.native.spec)
        self.requests = ManagedRequests(session.journal)
        self.pause_task: asyncio.Task[Any] | None = None
        self._pause_generation = 0
        self.latest_native: HandoverEvidence | None = None
        self.latest_native_lifecycle: tuple[int, int | None] | None = None
        self.status_authority: AuthorityStamp | None = None
        self.status_latest_closed: int | None = None
        self.status_vendor_epoch: int | None = None

        self.current = ManagedCurrentLimits(self)
        self.charging = ManagedCharging(self)
        self.ordinary = OrdinaryRecovery(self)
        self.recovery = RecoveryCoordinator(self)
        session.gateway.retiring = lambda: self.stopping
        session.book.retiring = session.gateway.retiring
        session.gateway.controls.retained_ceiling_exceeded = self._retained_ceiling_exceeded
        self.native.ordinary_source_observer = self.ordinary.observe
        self.native.positive_guard = self.positive_decision
        self.native.continuation_guard = self.continuation_decision
        self.native.recovery_zero_only = self.charging.recovery_zero_only
        self.native.stop_after_introduction = partial(session.gateway.stop, requests=self.requests)
        binding = ManagedStartBinding(session.gateway, self.native.spec.saved_schedule_digest, self.charging)
        session.gateway.start_reports = ManagedStartReports(self.charging, binding)
        self.native.command_source_observer = binding.observe
        self.native.repair_audits = NativeRepairAudits(
            session.journal, lambda fault: release_evidence(self, fault) is not None
        )

    # A sample before a recorded Start or Stop cannot prove the current session, even after it closes.
    def current_native_sample(self) -> HandoverEvidence | None:
        if self.latest_native is not None and self.latest_native_lifecycle == self.session.journal.lifecycle_position():
            return self.latest_native
        return None

    # Rebuild full ownership for result/publication checks without caching permission across an await.
    def authority(self) -> AuthorityStamp | None:
        g, j = self.session.gateway, self.session.journal
        scope = current_scope(g)
        if scope is None:
            return None
        return AuthorityStamp(self.requests.intent(), scope, g.controls._revision, j.get("control", {}).get("ticket"))

    # A real reply may be retained even when its owner can no longer publish a change of control.
    def publication_is_current(self, expected: AuthorityStamp) -> bool:
        return self.session.journal.db.in_transaction and self.authority() == expected

    # Accept commands without performing hidden Start, while a newer Pause immediately revokes positive authority.
    def command(self, identifier: str, action: str, revision: int, *, current_a: int | None = None) -> dict[str, Any]:
        if self.stopping:
            raise UnavailableError("managed service is stopping")
        if action != "pause" and self.session._introduction_refusal is not None:
            raise RecoveryRequiredError("strict introduction refusal awaits durable diagnostic")
        if action == "set_current":
            if current_a is None:
                raise SecurityError("a current selection requires an integer current")
            return self.current.accept(identifier, revision, current_a)
        if current_a is not None:
            raise SecurityError("current is only supported by the current-selection command")
        if action in ("start", "start_test", "arm_cloud_test"):
            return self.charging.accept(identifier, action, revision)
        previous = self.requests.existing(identifier, action, revision)
        if previous is not None:
            return previous
        if action == "app_control" and self.charging.return_blocked():
            raise RecoveryRequiredError("return awaits timed test settlement")
        if action == "app_control" and self.current.pending():
            raise RecoveryRequiredError("return awaits the current-setting outcome")
        if action == "app_control" and self.session.journal.fault.latched():
            raise RecoveryRequiredError("a latched fault requires independent operator reconciliation")
        if action == "app_control" and self.requests.intent().mode != "paused":
            raise RecoveryRequiredError("return requires a current paused intent")

        journal = self.session.journal
        controls = self.session.gateway.controls
        ticket = None
        pause_ack = None

        # This claim shares the intent transaction, so no accepted Pause can leave old shared authority committed.
        def claim() -> None:
            nonlocal ticket
            _, ticket = controls._claim(True, stopping=True)
            self.requests.record_pause_claim(self.requests.intent(), ticket)

        # Evidence is optional. A failed read cannot veto a Pause while its transaction remains intact.
        def capture_release_proof() -> dict[str, Any] | None:
            nonlocal pause_ack
            try:
                pause_ack = release_pause_ack(self, identifier)
            except sqlite3.Error, RecoveryRequiredError, SecurityError:
                if not journal.db.in_transaction:
                    raise
            return pause_ack

        receipt = self.requests.accept(identifier, action, revision, claim, capture_release_proof=capture_release_proof)
        if action == "pause":
            if ticket is not None:
                controls.observe_incident("pause_committed", identifier, ticket)
            self.schedule_pause()
            if pause_ack is not None:
                try:
                    journal.put(RELEASE_PAUSE_ACK, pause_ack)
                except sqlite3.Error:
                    if journal.db.in_transaction:
                        raise
                    # The original Pause already contains the proof. This copy serves older readers.
        return receipt

    # A newer accepted hold must get a turn even if the previous Stop is still leaving the wire gate.
    def schedule_pause(self) -> asyncio.Task[None]:
        self._pause_generation += 1
        if self.pause_task is None or self.pause_task.done():
            self.pause_task = self.commands.start(self._run_pause, priority=CommandPriority.RESTRICT)
        return self.pause_task

    # Let each newer hold take a restrictive turn after its predecessor has left the wire gate.
    async def _run_pause(self) -> None:
        while not self.stopping:
            generation = self._pause_generation
            await self._stop_requested()
            if generation == self._pause_generation:
                return
            await self.commands.turn(CommandPriority.RESTRICT)

    # The current persistent Pause may reclaim only the shared ticket of its exact retired normal Start.
    def reclaim_failed_start_pause(self) -> None:
        g, j = self.session.gateway, self.session.journal
        intent, state = self.requests.intent(), self.charging.state()
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
        receipt, control = local_start_receipt(j), j.get("control", {})
        ownership = self.requests.pause_ownership()
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
                not self.stopping
                and not j.fault.latched()
                and self.charging.lease is None
                and not g.controls._operations
                and not g.charger._pending
                and self.requests.intent() == intent
                and control_scope_matches(g, connection)
                and j.phase.read().active
                and self.charging.state() == state
                and local_start_receipt(j) == receipt
                and j.get("control") == control
                and self.requests.pause_ownership().pause == ownership.pause
                and fingerprint(j.get("boot")) == self.native.spec.boot_digest
                and recorded_station_profile(j, self.native.spec.digest, self.native.spec.boot_digest)
                and j.db.execute(
                    "SELECT action,revision,state FROM managed_requests WHERE id=?", (state["id"],)
                ).fetchone()
                == row
            )

        if current():
            with j.atomic():
                if current():
                    g.controls._claim(True)

    # Explicit owner Pause can restrict the commissioned station even while a native HTTP read is unavailable.
    async def ensure_pause_zero(self, *, request_id: str | None = None) -> None:
        g, j = self.session.gateway, self.session.journal
        intent = self.requests.intent()
        if intent.mode != "paused" or g.controls.ready and g.controls.desired() == 0:
            return
        try:
            self.commands.decide(self.reclaim_failed_start_pause, priority=CommandPriority.RESTRICT)
            ticket = j.get("control", {}).get("ticket")
            connection = current_control_scope(g)

            # Recheck the owner's claim and authenticated device immediately before each wire operation.
            def guard() -> None:
                control = j.get("control", {})
                if (
                    self.stopping
                    or self.requests.intent() != intent
                    or not ticket
                    or control.get("ticket") != ticket
                    or not ControlState(control).explicit_local_hold
                    or not control_scope_matches(g, connection)
                    or fingerprint(j.get("boot")) != self.native.spec.boot_digest
                    or not recorded_station_profile(j, self.native.spec.digest, self.native.spec.boot_digest)
                ):
                    raise RecoveryRequiredError("owner Pause awaits the current commissioned station")

            if request_id is None:
                try:
                    request = self.requests.pause_request()
                    request_id = request.identifier if request is not None else None
                except sqlite3.Error:
                    pass  # The already requested zero does not depend on optional receipt correlation.
            await g.controls.ensure_ceiling(guard=guard, request_id=request_id)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            return  # A failed attempt never confirms zero. The saved Pause can try again.

    # Optional observation notes cannot interrupt supervision; required fault proof still commits atomically in fault().
    def record_optional_native_failure(self, diagnostic: dict[str, Any]) -> None:
        try:
            self.session.journal.put("managed_native_failure", diagnostic)
        except sqlite3.Error:
            pass  # Counters and conservative status remain authoritative even when this optional note cannot persist.

    # Keep the physical Stop independent from the requesting websocket's lifetime and retain uncertainty honestly.
    async def _stop_requested(self) -> None:
        try:
            if self.charging.scoped_normal_stop():
                await self.charging.reconcile_expired()
                return
            await self.ensure_pause_zero()
            g, j = self.session.gateway, self.session.journal
            intent, connection = self.requests.intent(), current_control_scope(g)
            request = self.requests.pause_request()
            local = j.active()
            if self.charging.timed_pause_leaves_session(local):
                return  # The zero hold is restrictive; a timed test cannot Stop another session.
            prior_control = j.get("control", {})
            stop_epoch, prior_ticket = prior_control.get("stop_epoch", 0), prior_control.get("ticket")
            stop_ticket: str | None = None

            # A restrictive Stop still belongs to one current owner request and one known local session.
            def guard() -> None:
                active = j.active()
                # After the claim, a genuine closure may arrive before this Stop's acknowledgement.
                same_target = active == local or active is None and stop_ticket is not None
                if (
                    self.stopping
                    or intent.mode != "paused"
                    or self.requests.intent() != intent
                    or self.requests.pause_request() != request
                    or not control_scope_matches(g, connection)
                    or fingerprint(j.get("boot")) != self.native.spec.boot_digest
                    or not same_target
                ):
                    raise RecoveryRequiredError("owner Stop awaits the current Pause and known session")
                control = j.get("control", {})
                if stop_ticket is None and (
                    control.get("stop_epoch", 0) == stop_epoch and control.get("ticket") == prior_ticket
                ):
                    return  # LocalControl checks once before synchronously taking its restrictive claim.
                if (
                    control.get("stop_epoch") != stop_epoch + 1
                    or not ControlState(control).explicit_local_hold
                    or not isinstance(control.get("ticket"), str)
                    or control["ticket"] != g.controls._stop_ticket
                    or stop_ticket != control["ticket"]
                ):
                    raise RecoveryRequiredError("owner Stop claim was superseded")

            # Persist the Stop's target with the hold claim, before the wire worker can run.
            def bind(ticket: str) -> None:
                nonlocal stop_ticket
                if connection is None:
                    raise RecoveryRequiredError("owner Stop awaits its current Boot")
                self.requests.bind_pause_stop(intent, ticket, request, local, connection[0].boot)
                stop_ticket = ticket

            try:
                # Closing the exact locally known session may proceed even after cable removal.
                native = await self.native.read(closing_local=local, stop_only=local is not None)
            except _NativeMeasurementError as candidate:
                if (
                    candidate.authority != self.native.authority()
                    or candidate.sequence != self.native.last_resolved_read
                ):
                    raise AuditSupersededError("owner Stop measurement crossed a newer authority or observation")
                # Only measured excess may be ignored for Stop; identity, history and resources already passed.
                native = candidate.native
            controls = g.controls

            # Interpret the live target or closed-zero report before issuing its exact Stop.
            def accept_stop_observation() -> tuple[bool, bool]:
                nonlocal connection
                guard()
                native.physical.require_fresh(j.identity, 5)
                self.native.require_known_session(native)
                station_absent = local is not None and self.session.book.record_missing_station(native.physical)
                if local is None:
                    self.session.book.owned_present(native.physical)
                elif station_absent and not j.fault.latched():
                    # Own fault publication changes the control revision before taking the exact Stop claim.
                    self.fault(STATION_CEILING_LOST)
                    connection = current_control_scope(g)
                if not native.physical.transaction_open and controls.ready and controls.desired() == 0:
                    self.require_paused(native)
                    self.charging.observed(native)
                    controls._complete(self.session.journal.get("control")["ticket"], uncertain=False)
                    return False, station_absent
                return not controls.stop_acknowledged(local), station_absent

            stop_needed, station_absent = self.commands.decide(
                accept_stop_observation, priority=CommandPriority.RESTRICT
            )
            if stop_needed:
                await controls.stop(
                    expected_local=local,
                    guard=guard,
                    station_absent=station_absent,
                    on_claim=bind,
                    owner=("pause", intent, request, local, connection),
                    request_id=request.identifier if request is not None else None,
                )
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            return  # Intent stays paused; fresh supervision must establish zero before confirmation or any release.

    # A deliberate hold is confirmed only by actual closed/all-phase zero and the current owned station0 receipt.
    def require_paused(self, native: HandoverEvidence) -> None:
        self.native.require(native, closed=True)
        self.native.require_known_session(native)
        present = self.session.book.owned_present(native.physical, record_unchanged=False)
        reason = self._paused_refusal(present)
        if reason is not None:
            error = RecoveryRequiredError("requested pause awaits independently verified closed zero")
            error.add_note(reason)
            raise error

    # This follows the independent native/session/resource checks already performed by require_paused.
    def _paused_refusal(self, present: list[Resource]) -> str | None:
        g, j = self.session.gateway, self.session.journal
        facts = ManagedFacts(self)
        if not facts.boot_current():
            return "boot_unconfirmed"
        if not facts.introduction_current():
            return "introduction_unconfirmed"
        if not g.controls.ready:
            return "station_ceiling_unconfirmed"
        if g.controls.desired() != 0:
            return "station_ceiling_nonzero"
        if not ControlState(j.get("control", {})).local_hold:
            return "local_hold_missing"
        if j.get("control", {}).get("start_pending"):
            return "start_pending"
        if g.vendor_write_evidence().unresolved:
            return "vendor_command_uncertain"
        if not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present):
            return "station_zero_receipt_missing"
        if g.charger._pending:
            return "charger_rpc_pending"
        if g.controls._operations:
            return "control_operation_pending"
        return None

    # Independently verify the newer owner's closed zero before clearing a superseded Start's pending flag.
    def settle_rejected_start_at_pause(
        self, native: HandoverEvidence, intent: ControlIntent, connection: ControlScope | None
    ) -> None:
        g, j = self.session.gateway, self.session.journal

        # Check only the exact newer Pause and its current hold in the Start settlement transaction.
        def require_hold(control: dict[str, Any]) -> None:
            pause, ownership = self.requests.pause_request(), self.requests.pause_ownership()
            if (
                self.stopping
                or ownership.intent != intent
                or pause is None
                or pause.revision != intent.revision
                or not ownership.owns_ticket(pause.hold)
                or not control_scope_matches(g, connection)
                or self.native.introduction_pending
                or self.native.audited_epoch != g._charger_epoch
                or j.get("control", {}) != control
                or not ControlState(control).explicit_local_hold
                or j.active() is not None
                or not g.controls.ready
                or g.controls.desired() != 0
                or g.charger._pending
                or g.controls._operations
                or not self.native.commissioned_binding_matches(native)
            ):
                raise RecoveryRequiredError("rejected Start settlement awaits its current owner Pause")
            self.native.require(native, closed=True)
            key = f"profile:{HUB_PROFILE_ID}"
            station = [resource for resource in native.physical.resources if resource.key == key]
            row = j.db.execute("SELECT state,confirmed FROM trial_resources WHERE key=?", (key,)).fetchone()
            if not self.session.book.confirmed_station_zero(station, row):
                raise RecoveryRequiredError("rejected Start settlement awaits the confirmed station zero")

        try:
            j.starts.settle_rejected_pause(intent.revision, require_hold)
        except sqlite3.Error as error:
            raise RecoveryRequiredError("rejected Start settlement awaits journal storage") from error

    # Reapply only a requested Stop after replacement; this branch never returns authority to the app implicitly.
    async def maintain_pause(self) -> HandoverEvidence:
        if self.pause_task is not None and not self.pause_task.done():
            await asyncio.shield(self.pause_task)
        intent = self.requests.intent()
        connection = current_control_scope(self.session.gateway)
        # A still-open unplugged session must reach the guarded Stop retry below.
        native = await self.native.read_result(closing_local=self.session.journal.active())
        if isinstance(native, Pending):
            if native.reason is Prerequisite.NATIVE and native.cause is not None:
                raise native.cause
            raise RecoveryRequiredError(native.detail) from native.cause
        if not self.commands.decide(
            lambda: self._accept_pause_observation(native, intent, connection), priority=CommandPriority.RESTRICT
        ):
            await asyncio.shield(self.schedule_pause())
            raise RecoveryRequiredError("requested pause awaits actual physical closure")
        g, j = self.session.gateway, self.session.journal
        if j.phase.read().value == "recovering":
            cloud = self.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted"
            if not cloud:
                await self.native.resume(local_only=True)
            elif not j.next_delivery():
                await self.native.resume()
        return native

    # Settle closure, related receipts and the exact owner Pause together.
    def _accept_pause_observation(
        self, native: HandoverEvidence, intent: ControlIntent, connection: ControlScope | None
    ) -> bool:
        g, j = self.session.gateway, self.session.journal
        if self.stopping or self.requests.intent() != intent or not control_scope_matches(g, connection):
            raise RecoveryRequiredError("Pause observation no longer belongs to the current request and Boot")
        self.native.require_known_session(native)
        if (
            native.physical.transaction_open
            or not self.session.gateway.controls.ready
            or self.session.gateway.controls.desired() != 0
            or not ControlState(self.session.journal.get("control", {})).local_hold
        ):
            return False
        self.settle_rejected_start_at_pause(native, intent, connection)
        if (self.native.introduced_epoch, self.native.introduced_revision) != (g._charger_epoch, g.controls._revision):
            self.native.confirm_paused_introduction(native)
        self.require_paused(native)
        records = [record for record in (current_candidate(self), charge_test_candidate(self)) if record is not None]
        internal = internal_pause_matches(self)
        if records or internal:
            try:
                require_settled_debt(self)
            except RecoveryRequiredError:
                pass  # Unknown Start and lifecycle facts remain authoritative even at fresh native zero.
            else:
                with j.atomic():
                    self.require_paused(native)
                    require_settled_debt(self)
                    for record in records:
                        complete_record(self, record)
                    if internal:
                        g.controls._complete(j.get("control")["ticket"], uncertain=False)
                if any(isinstance(record, RetirementCandidate) for record in records):
                    j.starts.set_lease(None)
                    self.charging.changed.set()
        if self.current.failed_idle_hold():
            # This fresh closed-zero proof settles only the physical hold, without issuing another Stop.
            g.controls._complete(j.get("control")["ticket"], uncertain=False)
            raise RecoveryRequiredError("failed current hold awaits a fresh settled Pause observation")
        # Physical Pause is already proved. Cloud delivery debt can still block reactivation independently.
        self.complete_pause_observation(native, intent, connection)
        return True

    # Pause alone settles its receipt, using current independent closure and the original paused generation.
    def complete_pause_observation(
        self, native: HandoverEvidence, intent: ControlIntent, connection: ControlScope | None
    ) -> None:
        j = self.session.journal
        if (
            self.stopping
            or self.requests.intent() != intent
            or not control_scope_matches(self.session.gateway, connection)
        ):
            raise RecoveryRequiredError("Pause observation no longer belongs to the current request and Boot")
        native.physical.require_fresh(j.identity, 5)
        self.require_paused(native)
        if awaiting_owner_start(j):
            raise RecoveryRequiredError("owner Pause awaits the previously acknowledged Start lifecycle")
        self.requests.complete_verified_pause()

    # Offline replacement retires prior app authority before any recovery wait and never auto-Returns on reconnection.
    def prepare_offline_pause(self) -> None:
        g, j = self.session.gateway, self.session.journal
        intent = self.requests.intent()
        scope = current_scope(g)
        if (
            j.phase.read().value == "recovering"
            and not (self.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted")
            and scope is not None
            and self.session.runtime.status.vendor_failed_scope == scope
            and self.native.introduction_lineage == scope.lineage
            and self.native.acknowledged_lineage != scope.lineage
            and intent.mode == "app"
        ):
            with j.atomic(retry_busy=True):
                write_intent(j, intent, "paused")
                j.put(OFFLINE_PAUSE, True)
                g.controls._claim(True, stopping=True)

    # A newer stop ticket invalidates an awaited release before it can send a positive ceiling.
    def request_stop(self) -> None:
        if self.stopping:
            return
        self.stopping = True
        self.session.gateway.controls._claim(True, stopping=True)

    # A persistent failed restoration cannot be silently undone by another poll or a process restart.
    def fault(
        self, reason: str, *, diagnostic: dict[str, Any] | None = None, forwarded_timeout: str | None = None
    ) -> None:
        latch_managed_fault(self.session.gateway, reason, diagnostic=diagnostic, forwarded_timeout=forwarded_timeout)

    # Keep a proved excessive offer even if a command encounters it before the next supervision poll.
    def _retained_ceiling_exceeded(self) -> None:
        if not self.stopping:
            self.fault(STATION_COMPOSITE_FAULT)

    # Reuse only the existing owned zero profile; a fault never creates Stop, cleanup or positive authority.
    async def hold_fault(self) -> None:
        g, j = self.session.gateway, self.session.journal
        fault, control = j.fault.read(), j.get("control", {})
        epoch, boot, revision = g._charger_epoch, g.boot_observation, g.controls._revision
        binding, handover = j.get("trial_binding", {}), j.get("native_bounded_handover", {})
        station = j.db.execute(
            "SELECT * FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        receipt = tuple(station) if station is not None else None
        zero = encoded(profile_projection(self.native.choice.payload(0)))
        # Only this exact owned preimage and ResourceBook's own zero-intent/ACK transitions may cross the RPC waits.
        allowed = {receipt}
        if station is not None and station["state"] != "absent_verified":
            allowed.update(
                {
                    (station["key"], zero, station["confirmed"], "pending", None),
                    (station["key"], zero, station["confirmed"], "uncertain", None),
                    (station["key"], zero, zero, "confirmed", None),
                }
            )

        # Both watchdog and foreground polling must retain the same fault, owner claim and commissioned installation.
        def guard() -> None:
            row = j.db.execute(
                "SELECT * FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
            ).fetchone()
            if (
                self.stopping
                or not fault
                or j.fault.read() != fault
                or j.get("control", {}) != control
                or not ControlState(control).explicit_local_hold
                or g.controls.desired() != 0
                or not j.phase.read().supervised
                or g.charger.sender is None
                or g._charger_epoch != epoch
                or g.boot_observation != boot
                or g.controls._revision != revision
                or not g.current_boot_seen(epoch)
                or fingerprint(j.get("boot")) != self.native.spec.boot_digest
                or j.get("managed_mode", {}).get("spec_digest") != self.native.spec.digest
                or j.get("trial_binding") != binding
                or binding.get("boot_digest") != self.native.spec.boot_digest
                or binding.get("station") != self.native.choice.value()
                or j.get("native_bounded_handover") != handover
                or handover.get("stage") != "admitted"
                or handover.get("spec") != self.native.spec.value()
                or handover.get("fault")
                or self.session.book.station_ceiling_lost()
                or row is None
                or row["state"] == "absent_verified"
                or tuple(row) not in allowed
            ):
                raise RecoveryRequiredError("fault zero hold awaits the same admitted owner and current Boot")

        try:
            guard()
            if g.controls.ready:
                return
            now = time.monotonic()
            self.fault_zero_retry = {key: until for key, until in self.fault_zero_retry.items() if until > now}
            key = fingerprint({"fault": fault, "epoch": epoch, "boot": boot, "revision": revision, "control": control})
            if key in self.fault_zero_retry:
                return
            self.fault_zero_retry[key] = now + 15
            await g.controls.ensure_ceiling(guard=guard)
        except (
            DeniedError,
            SecurityError,
            RecoveryRequiredError,
            UnavailableError,
            TimeoutError,
            StationZeroStorageError,
        ):
            return  # The fault/debts remain latched; a safety ACK alone is never reported as measured physical zero.

    # Choose the domain's current source proof once, identically at preflight and the final native send gate.
    def continuation_decision(self, native: HandoverEvidence, local: int) -> GateDecision:
        j = self.session.journal
        if j.current_change.read().recovering:
            return current_session_decision(self.session.gateway, native, local)
        elif self.ordinary.active():
            return self.ordinary.session_decision(native, local)
        elif not (self.charging.scoped_normal_stop() and self.charging.independent_daily(native, local)):
            return Pending(Prerequisite.SOURCE, "continuation requires an identified daily or owner session")
        return ALLOW

    # Source and Start obligations remain ordinary Pending data; strict introduction refusals remain failures.
    def positive_decision(self, native: HandoverEvidence) -> GateDecision:
        if self.session._introduction_refusal is not None:
            raise RecoveryRequiredError("strict introduction refusal awaits durable diagnostic")
        decision = self.charging.positive_decision(native)
        if isinstance(decision, Pending):
            return decision
        if self.ordinary.active():
            local = self.session.journal.active()
            decision = self.ordinary.authority_decision(local)
            if isinstance(decision, Pending):
                return decision
            if local is not None:
                return self.continuation_decision(native, local)
            elif native.physical.transaction_open:
                return Pending(Prerequisite.LIFECYCLE, "ordinary idle recovery cannot adopt another native session")
        return ALLOW

    # Verified zero and cloud/lifecycle prerequisites are independent from source and physical release effects.
    def zero_decision(
        self, native: HandoverEvidence, *, allow_offline: bool = False, continuing_local: int | None = None
    ) -> GateDecision:
        return self.commands.decide(lambda: self._zero_decision(native, allow_offline, continuing_local))

    # Keep resource observations and their zero verdict together.
    def _zero_decision(
        self, native: HandoverEvidence, allow_offline: bool, continuing_local: int | None
    ) -> GateDecision:
        s, g, j = self.session, self.session.gateway, self.session.journal
        self.native.require(native)
        decision = readiness_decision(native, continuing_local is None, self.native.allow_unplugged)
        if isinstance(decision, Pending):
            return decision
        if continuing_local is not None:
            decision = self.continuation_decision(native, continuing_local)
            if isinstance(decision, Pending):
                return decision
            if native.output_limit_a != 0 or any(v != 0 for v in native.currents_a + native.powers_w):
                return Pending(Prerequisite.LIFECYCLE, "the identified original session awaits a verified zero hold")
        decision = self.native.session_decision(native)
        if isinstance(decision, Pending):
            return decision
        present = s.book.owned_present(native.physical)
        row = j.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        pending_stop = continuing_local is None and awaiting_known_stops(j)
        if not pending_stop:
            decision = lifecycle_decision(
                g,
                continuing_local=continuing_local,
                preserve_expired_start=self.charging.preserve_debt(native, continuing_local),
            )
            if isinstance(decision, Pending):
                return decision
        pending_delivery = pending_stop or (not allow_offline and awaiting_observations(j))
        refusal = self._zero_refusal(present, row, allow_offline=allow_offline, pending_delivery=pending_delivery)
        if refusal is not None:
            return Pending(Prerequisite.ZERO_PROOF, refusal)
        if pending_delivery or g.vendor._pending:
            # The original Return can wait without claiming a release or dispatching a positive ceiling.
            return Pending(Prerequisite.CLOUD_ACK, "shared control awaits the current cloud acknowledgement")
        return ALLOW

    # Release needs an owned zero receipt and no conflicting work after the native and lifecycle checks.
    def _zero_refusal(
        self,
        present: list[Resource],
        row: sqlite3.Row | None,
        *,
        allow_offline: bool,
        pending_delivery: bool,
    ) -> str | None:
        s, g, j = self.session, self.session.gateway, self.session.journal
        facts = ManagedFacts(self)
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
        if not g.controls.ready:
            return "station ceiling is unconfirmed"
        if g.controls.desired() != 0:
            return "station ceiling is not zero"
        if not j.get("control", {}).get("inhibit"):
            return "local inhibit is missing"
        if row is None or row["state"] != "confirmed":
            return "owned station profile is unconfirmed"
        if not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present):
            return "owned station zero receipt is missing"
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
    async def return_requested(self, identifier: str, revision: int) -> None:
        expected = self.requests.check_return_revision(identifier, revision)
        if expected is None:
            return
        if self.charging.return_blocked():
            return  # An already accepted Return survives until observation completes the timed test under Pause.
        j = self.session.journal
        control = j.get("control", {})
        if control.get("uncertain") and self.requests.stop_owns_ticket(control.get("ticket")):
            # Verify this exact protective Stop before treating Return as a positive release attempt.
            try:
                await self.maintain_pause()
            except RecoveryRequiredError, UnavailableError, TimeoutError:
                return  # Closed zero and selective cleanup can finish while the same Return remains accepted.
            control = j.get("control", {})
            if self.requests.intent() != expected or control.get("uncertain"):
                return
        receipt = local_start_receipt(j)
        if (
            control.get("start_pending")
            or receipt.get("pause_revision") == expected.revision
            and receipt.get("local") is not None
            and (
                j.active() == receipt.get("local")
                or j.db.execute(
                    "SELECT 1 FROM transactions WHERE local=? AND stop IS NULL", (receipt.get("local"),)
                ).fetchone()
            )
            or receipt.get("pause_revision") == expected.revision
            and receipt.get("verified_pause_revision") != expected.revision
            or self.requests.pause_ownership().owns_ticket(control.get("ticket"))
            and self.requests.pending_pause()
        ):
            return  # The same Return survives the real Start and both closure reports under its Pause.
        try:
            await self.recovery.restore(expected, return_request=identifier, claim_return=True)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            return  # The owned completion retains the exact request disposition when its caller disappears.

    # Resource settling, a complete native audit and closure agreement each have their own bounded wait.
    async def settled_native(self) -> HandoverEvidence:
        async with asyncio.timeout(15):
            while self.session._resource_mutation_inflight():
                await asyncio.sleep(0.02)
        if self.current.applying():
            raise RecoveryRequiredError("current application owns the pending native verification")
        try:
            native = await self.native.read()
            measured = self.native._selected_measurements(native, self.native._inspect(native))
            authority, sequence = self.native.authority(), self.native.last_resolved_read
        except _NativeMeasurementError as candidate:
            native = candidate.native
            measured = self.native._selected_measurements(native, candidate.measured)
            authority, sequence = candidate.authority, candidate.sequence
        except NativeReadinessError as error:
            physical = error.native.physical
            if not error.unplugged_at_zero or (
                physical.native_transaction_id != self.session.journal.active()
                and not self.ordinary.closure_gap(error.native)
            ):
                raise
            self.latest_native = None
            self.failures = 0
            self.strict_failure_seen = False
            self.history_omission_only = True
            if self.ordinary.ordinary():
                await self.ordinary.hold("unplugged_session_pending")
            raise RecoveryRequiredError("unplugged session awaits exact lifecycle closure") from error
        async with asyncio.timeout(15):
            # Check the audit and its exact authority before the final resource and session checks.
            def accept() -> bool:
                if self.session._resource_mutation_inflight():
                    raise UnavailableError("managed native observation crossed a resource mutation")
                if authority != self.native.authority() or sequence != self.native.last_resolved_read:
                    raise AuditSupersededError("settled native evidence crossed a newer authority or observation")
                if self.ordinary.closure_gap(native):
                    return False
                if (
                    native.physical.transaction_open
                    and native.physical.native_transaction_id != self.session.journal.active()
                ):
                    raise SecurityError("managed native transaction has no exact accepted local counterpart")
                self.native.require_known_session(native)
                present = self.session.book.owned_present(native.physical, record_unchanged=False)
                row = self.session.journal.db.execute(
                    "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
                ).fetchone()
                if (
                    row is None
                    or row["state"] != "confirmed"
                    or not any(
                        r.identifier == HUB_PROFILE_ID
                        and r.canonical.get("limit") == self.session.gateway.controls.desired()
                        for r in present
                    )
                ):
                    if native.physical.transaction_open and self.session.book.station_ceiling_lost():
                        raise StationCeilingLostError("the known charging session lost its owned station ceiling")
                    raise SecurityError("managed local station ceiling is absent or unconfirmed")
                native.physical.require_fresh(self.session.journal.identity, 5)
                self.native.require_selected_output(native)
                if measured.exceeded:
                    raise MeasuredOutputLimitError(native, measured, self.native.spec.digest, self.current.selected())
                return True

            if not self.commands.decide(accept):
                await self.ordinary.hold("native_closure_pending")
                raise RecoveryRequiredError("ordinary zero hold awaits exact lifecycle closure")
            return native

    # A refused archive transaction made no changes. Retry once using a complete fresh observation.
    async def _poll_native(self) -> HandoverEvidence:
        try:
            return await self.settled_native()
        except JournalStartBusy as error:
            repair = self.native.repair_audits
            if repair is not None and repair.pending_storage_error is error:
                raise
            self.latest_native = None
            await asyncio.sleep(0.1)
            return await self.settled_native()

    # Health is measured from a fresh native view and known session; a legitimate owner charge remains healthy.
    async def tick(self) -> dict[str, Any]:
        try:
            return await self._tick()
        except sqlite3.Error as error:
            # Match the error from this attempted publication, including its original subtype and cause.
            repair = self.native.repair_audits
            if repair is None or repair.pending_storage_error is not error:
                raise
            self.latest_native = None
            return render_poll(self, PollStatus("attention", "native_repair_publication_pending"), pending=True)

    # A poll first joins owned work, then supervises, assesses current authority and publishes one view.
    async def _tick(self) -> dict[str, Any]:
        self.commands.decide(self.session.gateway.retry_reply_writes)
        self.commands.decide(self.session.runtime.settle_saved_lifecycle)
        pending = await self._pending_work()
        if pending is not None:
            self.latest_native = None
            return render_poll(self, PollStatus("stopping" if self.stopping else "reconciling", pending), pending=True)
        gate = await self._poll_gate()
        if gate is not None:
            return render_poll(self, gate)
        observed = await self._supervise()
        if isinstance(observed, PollStatus):
            return render_poll(self, observed)
        decided = self.commands.decide(lambda: self._settle_poll(observed))
        return render_poll(self, decided)

    # Decide health and Return completion before rendering the dashboard view.
    def _settle_poll(self, observed: Observation) -> PollStatus | tuple[HandoverEvidence, HealthVerdict]:
        verdict = self._assess_observation(observed)
        if isinstance(verdict, PollStatus):
            return verdict
        if verdict.healthy and not self.ordinary.source_published and self.ordinary.ordinary():
            return PollStatus("degraded", "native_observation_unavailable")
        if verdict.healthy and self.requests.intent().mode == "app":
            self.requests.complete_legacy_return()
        return observed.native, verdict

    # Pending owned completions retain their existing settlement priority before any new native audit.
    async def _pending_work(self) -> str | None:
        pending = None
        if not await self.current.settle_for_poll():
            pending = "current_update_pending"
        elif not await self.charging.settle_for_poll():
            pending = "start_operation_pending"
        elif not self.recovery.settle_for_poll():
            pending = "owner_release_pending"
        return pending

    # Fault maintenance retains its restrictive work before an ordinary observation can establish fresh health.
    async def _poll_gate(self) -> PollStatus | None:
        s, g, j = self.session, self.session.gateway, self.session.journal
        if self.stopping:
            return PollStatus("stopping")
        if s._introduction_refusal is not None:
            classification = s._introduction_refusal.classification
            try:
                s._persist_introduction_refusal()
            except UnavailableError:
                pass  # Retain the proved strict refusal while its atomic diagnostic latch awaits storage.
            return PollStatus("attention", classification)
        if j.fault.latched():
            await self.hold_fault()
        await stop_for_missing_station(self)
        if j.fault.latched():
            if (
                self.requests.pending_pause()
                and (self.pause_task is None or self.pause_task.done())
                and not g.controls._operations
                and not g.charger._pending
            ):
                await asyncio.shield(self.schedule_pause())
            await self.charging.retire_closed_after_fault()
            try:
                await reconcile_owner_release(self)
            except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
                pass  # The exact fault remains latched until a new Pause and every independent zero proof agree.
            if not j.fault.latched():
                return PollStatus("reconciling")
            return PollStatus("attention", j.fault.classification())
        return None

    # Native observation, recovery and diagnostic escalation retain their original await and effect order.
    async def _supervise(self) -> Observation | PollStatus:
        g, j = self.session.gateway, self.session.journal
        if not self.session.runtime.status.charger_connected or not g.current_boot_seen(g._charger_epoch):
            return PollStatus("starting")
        if self.requests.intent().mode == "paused" and (self.pause_task is None or self.pause_task.done()):
            await self.ensure_pause_zero()
        current_hold = j.current_change.read().recovering
        if (
            not g.controls.ready
            and (self.released_epoch != g._charger_epoch)
            and not current_hold
            and (not self.ordinary.needed())
        ):
            return PollStatus("starting")
        try:
            try:
                self.commands.decide(self.prepare_offline_pause, priority=CommandPriority.RESTRICT)
            except JournalStartBusy:
                self.latest_native = None
                await asyncio.sleep(0.1)
                self.commands.decide(self.prepare_offline_pause, priority=CommandPriority.RESTRICT)
            recovery_pending = await self.recovery.advance()
            if j.fault.latched():
                return PollStatus("attention", j.fault.classification())
            if recovery_pending is not None:
                if recovery_pending.reason is Prerequisite.NATIVE:
                    return PollStatus("degraded", "native_observation_unavailable")
                return PollStatus(
                    "waiting_for_cloud" if recovery_pending.reason is Prerequisite.CLOUD else "reconciling"
                )
            native = await self._poll_native()
            if g.controls.ceiling_needs_recheck():
                try:
                    await g.controls.ensure_ceiling()
                except ExcessiveCompositeError:
                    # An actual unsafe offer requires the existing protective zero, even when the car is idle.
                    await self.hold_fault()
                    return PollStatus("attention", STATION_COMPOSITE_FAULT)
                native = await self._poll_native()
            authority, sequence = self.native.authority(), self.native.last_resolved_read
            return self.commands.decide(lambda: self._accept_supervision(native, authority, sequence))
        except (NativeTransportUnavailableError, StaleEvidenceError) as error:
            failure = error
            self.commands.decide(lambda: self._record_unavailable_observation(failure))
            if self.native_unavailable >= 3:
                if self.ordinary.ordinary():
                    try:
                        await self.ordinary.hold("native_observation_unavailable")
                    except SecurityError as failure:
                        j.put(
                            "managed_native_failure",
                            {
                                "type": diagnostic_type(failure),
                                "detail": str(failure)[:1024],
                                "at_epoch": time.time(),
                            },
                        )
                        self.fault("native_authority_or_preservation_failed")
                    except DeniedError, UnavailableError, RecoveryRequiredError:
                        pass  # The admitted zero remains the only retry; no positive authority was created.
            return PollStatus("degraded", "native_observation_unavailable")
        except UnavailableError, TimeoutError:
            return PollStatus("stopping" if self.stopping else "degraded", "native_observation_unavailable")
        except RecoveryRequiredError:
            return PollStatus("reconciling")
        except StationCeilingLostError:
            await stop_for_missing_station(self)
            return PollStatus("attention", j.fault.classification() if j.fault.latched() else STATION_CEILING_LOST)
        except (SecurityError, DeniedError) as error:
            refusal = error
            return self.commands.decide(lambda: self._record_refused_observation(refusal))

    # A temporarily unavailable observation affects freshness without inventing new charger evidence.
    def _record_unavailable_observation(self, error: Exception) -> None:
        self.failures = 0
        self.strict_failure_seen = False
        self.history_omission_only = True
        self.native_unavailable += 1
        self.record_optional_native_failure(
            {
                "type": type(error).__name__,
                "detail": str(error)[:1024],
                "at_epoch": time.time(),
                "availability_only": True,
            },
        )

    # A repeated independent strict refusal can latch a fault alongside its evidence.
    def _record_refused_observation(self, error: Exception) -> PollStatus:
        if self.stopping:
            return PollStatus("stopping")
        # Only a fully settled typed measurement can make this entire hard-failure run recoverable.
        measured_only = isinstance(error, MeasuredOutputLimitError)
        self.strict_failure_seen = self.strict_failure_seen or not measured_only
        self.history_omission_only = self.history_omission_only and isinstance(error, MissingOriginalHistoryError)
        diagnostic: dict[str, Any] = {
            "type": diagnostic_type(error),
            "detail": str(error)[:1024],
            "at_epoch": time.time(),
        }
        if isinstance(error, MeasuredOutputLimitError):
            diagnostic["cause"] = error.cause
        elif self.history_omission_only:
            diagnostic["source"] = ORIGINAL_HISTORY_OMISSION
        self.failures += 1
        classification = (
            MEASURED_FAULT
            if measured_only and not self.strict_failure_seen
            else ("native_authority_or_preservation_failed")
        )
        if self.failures >= 3:
            self.fault(classification, diagnostic=diagnostic)
        else:
            self.record_optional_native_failure(diagnostic)
        return PollStatus("degraded", classification)

    # Apply audited observations to Start and Pause state while the original evidence still owns its turn.
    def _accept_supervision(self, native: HandoverEvidence, authority: str, sequence: int) -> Observation | PollStatus:
        g = self.session.gateway
        if authority != self.native.authority() or sequence != self.native.last_resolved_read:
            raise AuditSupersededError("native supervision crossed a newer authority or observation")
        verified_pause = False
        if (self.requests.intent().mode == "app") and (self.released_epoch != g._charger_epoch):
            raise RecoveryRequiredError("managed release awaits fresh app-control restoration")
        if self.requests.intent().mode == "paused":
            self.require_paused(native)
            verified_pause = True
        previous_intent = self.requests.intent()
        self.charging.observed(native)
        observed_intent = self.requests.intent()
        intent_changed = observed_intent != previous_intent
        self.failures = 0
        self.strict_failure_seen = False
        self.history_omission_only = True
        self.native_unavailable = 0
        self.latest_native_lifecycle = self.session.journal.lifecycle_position()
        self.latest_native = native
        if intent_changed:
            return PollStatus("pausing", intent=observed_intent)
        return Observation(native, verified_pause)

    # The pure verdict requests a latch; the controller still owns its actual write and Return completion.
    def _assess_observation(self, observed: Observation) -> HealthVerdict | PollStatus:
        s, g, j = self.session, self.session.gateway, self.session.journal
        control = j.control.read()

        if (
            self.current.applying()
            and not s.runtime.status.delivery_blocked
            and not g.vendor_write_evidence().unresolved
            and not j.fault.latched()
            and not j.get("trial_preservation_fault")
            and not j.get("trial_inventory_fault")
            and not control.values.get("start_pending")
            and not j.get("vendor_start_grant")
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        ):
            intent = self.requests.intent()
            return PollStatus("reconciling", intent=intent)
        proof = ControlProof(shared=True, hold_verified=observed.verified_pause).retain(control)
        delivery_blocked = s.runtime.status.delivery_blocked
        delivery_uncertain = delivery_blocked and (
            s.runtime.status.delivery_blocked_cause != "storage" or g.delivery_outcome_uncertain
        )
        if not self.stopping:
            # Independent reasons must not be retired when one forwarded answer subsequently arrives.
            if not proof.available or delivery_uncertain:
                self.fault("owner_control_or_delivery_uncertain")
            else:
                evidence = g.vendor_write_evidence()
                if evidence.unresolved:
                    forwarded_timeout = j.outcomes.sole_forwarded_timeout(evidence)
                    self.fault("owner_control_or_delivery_uncertain", forwarded_timeout=forwarded_timeout)
        if (proof.available and j.fault.latched()) or self.stopping:
            proof = ControlProof()
        if proof.shared and not g.controls.ready:
            return PollStatus("reconciling")
        cloud = (
            s.runtime.status.vendor_connected and g.vendor.sender is not None and g.vendor_registration == "Accepted"
        )
        local_ready = proof.locally_ready(
            active=proof.available and j.phase.read().active,
            control_uncertain=bool(control.values.get("uncertain")),
            delivery_blocked=s.runtime.status.delivery_blocked,
        )
        verdict = assess(
            proof,
            stopping=self.stopping,
            locally_ready=local_ready,
            cloud_pending=local_ready and g.authorisation.pending(),
            cloud_ready=cloud,
        )
        return verdict
