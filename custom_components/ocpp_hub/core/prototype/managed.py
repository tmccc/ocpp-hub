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
from .connection_scope import ControlScope, current_control_scope, current_scope
from .local_control import ControlSupersededError
from .managed_charging import ManagedCharging
from .managed_current import ManagedCurrentLimits
from .managed_current_recovery import capture_interrupted_collision
from .managed_intent import RELEASE_PAUSE_ACK, ControlIntent, ManagedRequests
from .managed_native_repair import NativeRepairAudits
from .managed_release_recovery import (
    reconcile_owner_release,
    release_evidence,
    release_pause_ack,
)
from .managed_settlement import (
    charge_test_candidate,
)
from .managed_start_binding import ManagedStartReports
from .managed_view import render_poll
from .native_bounded import NativeReadinessError
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
from .recovery_contracts import AuthorityStamp, GateDecision, Pending, Prerequisite
from .recovery_coordinator import RecoveryCoordinator
from .rpc import UnavailableError
from .security import SecurityError, StaleEvidenceError
from .store import JournalStartBusy, RecoveryRequiredError
from .supervision import ControlProof, HealthVerdict, Observation, PollStatus, assess
from .trial_handover_evidence import HandoverEvidence
from .trial_session import TrialSession, managed_service_mode


class ManagedController:
    """One verified release per actual introduction returns control to the unchanged owner app/native schedule."""

    # Direct session retirement revokes managed positive permission before its independent shutdown zero awaits I/O.
    @property
    def stopping(self) -> bool:
        return self.session.gateway.controls.decisions.shutdown_requested or self.session.closing

    # Explicit service shutdown and test callers retain the existing synchronous stop flag API.
    @stopping.setter
    def stopping(self, value: bool) -> None:
        self.session.gateway.controls.decisions.shutdown_requested = value

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
        self.native.stop_missing_station = self._stop_for_missing_station
        session.gateway.start_reports = ManagedStartReports(self.charging)
        self.native.command_source_observer = partial(
            session.gateway.controls.decisions.schedule.observe, self.charging
        )
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
        if action == "app_control":
            return self.session.gateway.controls.decisions.accept_return(self, identifier, revision)
        previous = self.requests.existing(identifier, action, revision)
        if previous is not None:
            return previous

        if action != "pause":
            return self.requests.accept(identifier, action, revision)

        journal = self.session.journal
        pause_ack = None

        # Evidence is optional. A failed read cannot veto a Pause while its transaction remains intact.
        def capture_release_proof() -> dict[str, Any] | None:
            nonlocal pause_ack
            try:
                pause_ack = release_pause_ack(self, identifier)
            except sqlite3.Error, RecoveryRequiredError, SecurityError:
                if not journal.db.in_transaction:
                    raise
            return pause_ack

        receipt = self.session.gateway.controls.decisions.accept_pause(
            self.requests, identifier, revision, capture_release_proof
        )
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

    # Explicit owner Pause can restrict the commissioned station even while a native HTTP read is unavailable.
    async def ensure_pause_zero(self, *, request_id: str | None = None) -> None:
        g = self.session.gateway
        intent = self.requests.intent()
        if intent.mode != "paused" or g.controls.ready and g.controls.desired() == 0:
            return
        try:
            self.commands.decide(
                lambda: g.controls.decisions.reclaim_failed_start_pause(self), priority=CommandPriority.RESTRICT
            )
            guard = g.controls.decisions.owner_pause_zero_guard(self, intent)

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
            controls = g.controls
            scope = controls.decisions.prepare_pause_stop(g, self.requests)
            local = scope.local
            if self.charging.timed_pause_leaves_session(local):
                return  # The zero hold is restrictive; a timed test cannot Stop another session.
            guard = partial(
                controls.decisions.require_pause_stop, g, self.requests, scope, self.native.spec.boot_digest
            )

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
            sequence = self.native.last_resolved_read

            # Reuse the same short-lived draw evidence at the eventual wire dispatch.
            def station_flowing() -> bool:
                return self.native.still_drawing(native, sequence)

            # Interpret the live target or closed-zero report before issuing its exact Stop.
            def accept_stop_observation() -> tuple[bool, bool]:
                guard()
                native.physical.require_fresh(j.identity, 5)
                self.native.require_known_session(native)
                station_absent = local is not None and self.session.book.record_missing_station(native.physical)
                if local is None:
                    self.session.book.owned_present(native.physical)
                elif station_absent and not j.fault.latched():
                    # Own fault publication changes the control revision before taking the exact Stop claim.
                    self.fault(STATION_CEILING_LOST)
                    scope.connection = current_control_scope(g)
                if controls.decisions.complete_closed_stop(native, self.require_paused, self.charging.observed):
                    timed = charge_test_candidate(self, timed_only=True)
                    if timed is not None:
                        controls.decisions.settle_pause_records(self, native, (timed,))
                    return False, station_absent
                return controls.stop_work_needed(
                    local, station_absent=station_absent, station_flowing=station_flowing
                ), station_absent

            stop_needed, station_absent = self.commands.decide(
                accept_stop_observation, priority=CommandPriority.RESTRICT
            )
            if stop_needed:
                request = scope.request
                await controls.stop(
                    expected_local=local,
                    guard=guard,
                    station_absent=station_absent,
                    station_flowing=station_flowing,
                    on_claim=partial(controls.decisions.bind_pause_stop, self.requests, scope),
                    owner=("pause", scope.intent, request, local, scope.connection),
                    request_id=request.identifier if request is not None else None,
                )
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            return  # Intent stays paused; fresh supervision must establish zero before confirmation or any release.

    # A deliberate hold must pass the shared owner's current closed-zero proof.
    def require_paused(self, native: HandoverEvidence, *, local: int | None = None) -> None:
        self.session.gateway.controls.decisions.require_paused(self, native, local=local)

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
            lambda: self.session.gateway.controls.decisions.accept_pause_observation(self, native, intent, connection),
            priority=CommandPriority.RESTRICT,
        ):
            await asyncio.shield(self.schedule_pause())
            raise RecoveryRequiredError("requested pause awaits actual physical closure")
        local_only = self.session.gateway.controls.decisions.recovery.pause_resume(self)
        if local_only is not None:
            await self.native.resume(local_only=local_only)
        return native

    # Pause alone settles its receipt, using current independent closure and the original paused generation.
    def complete_pause_observation(
        self, native: HandoverEvidence, intent: ControlIntent, connection: ControlScope | None
    ) -> None:
        gateway = self.session.gateway
        gateway.controls.decisions.complete_pause(
            gateway, self.requests, native, intent, connection, lambda local: self.require_paused(native, local=local)
        )

    # Apply the shared owner's outage rule in the existing restrictive turn.
    def prepare_offline_pause(self) -> None:
        self.session.gateway.controls.decisions.recovery.prepare_offline_pause(self)

    # Forward retirement to the same owner that decides and claims the final Stop.
    def request_stop(self) -> None:
        gateway = self.session.gateway
        gateway.controls.decisions.begin_shutdown(gateway.retiring)

    # A persistent failed restoration cannot be silently undone by another poll or a process restart.
    def fault(
        self, reason: str, *, diagnostic: dict[str, Any] | None = None, forwarded_timeout: str | None = None
    ) -> None:
        self.session.gateway.controls.decisions.latch_fault(
            reason, diagnostic=diagnostic, forwarded_timeout=forwarded_timeout
        )

    # Keep a proved excessive offer even if a command encounters it before the next supervision poll.
    def _retained_ceiling_exceeded(self) -> None:
        if not self.stopping:
            self.fault(STATION_COMPOSITE_FAULT)

    # Let the control owner maintain the commissioned station zero for this fault.
    async def hold_fault(self) -> None:
        gateway = self.session.gateway
        await gateway.controls.decisions.hold_fault_zero(gateway, self.native)

    # Report whether a Pause already has a worker when the owner considers a missing-profile Stop.
    async def _stop_for_missing_station(self) -> None:
        gateway = self.session.gateway
        await gateway.controls.decisions.stop_for_missing_station(
            gateway, self.native, self.requests, pause_active=self.pause_task is not None and not self.pause_task.done()
        )

    # Follow the exact source selected by the shared owner during recovery and at the send gate.
    def continuation_decision(self, native: HandoverEvidence, local: int) -> GateDecision:
        return self.session.gateway.controls.decisions.schedule.continuation_decision(self, native, local)

    # Let the shared owner recheck outage and Start debt at the native send gate.
    def positive_decision(self, native: HandoverEvidence) -> GateDecision:
        return self.session.gateway.controls.decisions.recovery.positive_decision(self, native)

    # Verified zero and cloud/lifecycle prerequisites are independent from source and physical release effects.
    def zero_decision(
        self, native: HandoverEvidence, *, allow_offline: bool = False, continuing_local: int | None = None
    ) -> GateDecision:
        return self.commands.decide(
            lambda: self.session.gateway.controls.decisions.release_zero_decision(
                self, native, allow_offline, continuing_local
            )
        )

    # Preserve the caller while the shared owner decides whether this exact Return may proceed.
    async def return_requested(self, identifier: str, revision: int) -> None:
        await self.session.gateway.controls.decisions.run_requested_return(self, identifier, revision)

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
            if not self.session.gateway.controls.decisions.recovery.unplugged_closure(self, error):
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
        await self._stop_for_missing_station()
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
            if g.controls.decisions.recovery.transport_needs_hold(self):
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
        except ControlSupersededError, RecoveryRequiredError:
            return PollStatus("stopping" if self.stopping else "reconciling")
        except StationCeilingLostError:
            await self._stop_for_missing_station()
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
