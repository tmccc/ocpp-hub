"""Daily charging supervision with verified native limits and durable owner requests."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .connection_scope import control_scope_matches, current_control_scope, current_scope
from .control_state import ControlState
from .local_control import local_start_receipt
from .managed_charging import ManagedCharging
from .managed_current import (
    ManagedCurrentLimits,
    current_session_decision,
)
from .managed_current_recovery import capture_interrupted_collision
from .managed_facts import ManagedFacts
from .managed_faults import latch_managed_fault
from .managed_intent import ManagedRequests, write_intent
from .managed_native_repair import NativeRepairAudits
from .managed_release_recovery import reconcile_owner_release, record_release_pause, release_evidence
from .managed_release_wait import awaiting_known_stops
from .managed_settlement import (
    charge_test_candidate,
    complete_record,
    current_candidate,
    internal_pause_matches,
    require_settled_debt,
)
from .managed_sources import awaiting_owner_start, recorded_station_profile
from .managed_start_binding import ManagedStartBinding, ManagedStartReports
from .managed_view import observed_status, poll_base, poll_status
from .native_bounded import NativeReadinessError, readiness_decision
from .native_bounded_handover import AuditSupersededError
from .native_faults import (
    MEASURED_FAULT,
    MeasuredOutputLimitError,
    _NativeMeasurementError,
    diagnostic_type,
)
from .native_http import NativeTransportUnavailableError
from .ordinary_recovery import OrdinaryRecovery
from .policy import DeniedError
from .recovery_contracts import ALLOW, AuthorityStamp, GateDecision, Pending, Prerequisite
from .recovery_coordinator import RecoveryCoordinator
from .rpc import UnavailableError
from .security import SecurityError, StaleEvidenceError
from .start_settlement import RetirementCandidate
from .store import RecoveryRequiredError, encoded, fingerprint
from .supervision import ControlProof, HealthVerdict, Observation, PollStatus, assess
from .trial_evidence import Resource, profile_projection
from .trial_handover_evidence import HandoverEvidence
from .trial_rollback import lifecycle_decision
from .trial_session import TrialSession


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
        self.released_epoch: int | None = None
        self.failures = 0
        self.strict_failure_seen = False
        self.native_unavailable = 0
        self.fault_zero_retry: dict[str, float] = {}
        self.stopping = False
        expected = {"mode": "app_schedule", "spec_digest": self.native.spec.digest, "local_start": False}
        saved = session.journal.get("managed_mode")
        if saved is not None and saved != expected:
            raise SecurityError("managed policy changed across process replacement")
        session.journal.put("managed_mode", expected)
        capture_interrupted_collision(session.journal, self.native.spec)
        self.requests = ManagedRequests(session.journal)
        self.pause_task: asyncio.Task[Any] | None = None
        self.latest_native: HandoverEvidence | None = None
        self.status_authority: AuthorityStamp | None = None
        self.status_vendor_epoch: int | None = None

        self.current = ManagedCurrentLimits(self)
        self.charging = ManagedCharging(self)
        self.ordinary = OrdinaryRecovery(self)
        self.recovery = RecoveryCoordinator(self)
        self.native.ordinary_source_observer = self.ordinary.observe
        self.native.positive_guard = self.positive_decision
        self.native.continuation_guard = self.continuation_decision
        self.native.recovery_zero_only = self.charging.scoped_normal_stop
        binding = ManagedStartBinding(session.gateway, self.native.spec.saved_schedule_digest, self.charging)
        session.gateway.start_reports = ManagedStartReports(self.charging, binding)
        self.native.command_source_observer = binding.observe
        self.native.repair_audits = NativeRepairAudits(
            session.journal, lambda fault: release_evidence(self, fault) is not None
        )

    # Rebuild full ownership for result/publication checks without caching permission across an await.
    def authority(self) -> AuthorityStamp | None:
        g, j = self.session.gateway, self.session.journal
        scope = current_scope(g)
        if scope is None:
            return None
        return AuthorityStamp(self.requests.intent(), scope, g.controls._revision, j.get("control", {}).get("ticket"))

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

        # This claim shares the intent transaction, so no accepted Pause can leave old shared authority committed.
        def claim() -> None:
            _, ticket = self.session.gateway.controls._claim(True, stopping=True)
            self.session.journal.put("managed_owner_pause", self.requests.intent().pause_claim(ticket))
            record_release_pause(self, identifier)

        receipt = self.requests.accept(identifier, action, revision, claim)
        if action == "pause":
            if self.pause_task is None or self.pause_task.done():
                self.pause_task = asyncio.create_task(self._stop_requested())
        return receipt

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
        receipt, control, owner_pause = local_start_receipt(j), j.get("control", {}), j.get("managed_owner_pause", {})
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
            or owner_pause.get("revision") != intent.revision
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
                and j.get("managed_owner_pause") == owner_pause
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
    async def ensure_pause_zero(self) -> None:
        g, j = self.session.gateway, self.session.journal
        intent = self.requests.intent()
        if intent.mode != "paused" or g.controls.ready and g.controls.desired() == 0:
            return
        self.reclaim_failed_start_pause()
        epoch, ticket = g._charger_epoch, j.get("control", {}).get("ticket")
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
                or g._charger_epoch != epoch
                or not control_scope_matches(g, connection)
                or fingerprint(j.get("boot")) != self.native.spec.boot_digest
                or not recorded_station_profile(j, self.native.spec.digest, self.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("owner Pause awaits the current commissioned station")

        try:
            await g.controls.ensure_ceiling(guard=guard)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError:
            return  # A failed zero acknowledgement still permits Stop if independent native verification succeeds.

    # Optional observation notes cannot interrupt supervision; required fault proof still commits atomically in fault().
    def record_optional_native_failure(self, diagnostic: dict[str, Any]) -> None:
        try:
            self.session.journal.put("managed_native_failure", diagnostic)
        except sqlite3.Error:
            pass  # Counters and conservative status remain authoritative even when this optional note cannot persist.

    # Keep the physical Stop independent from the requesting websocket's lifetime and retain uncertainty honestly.
    async def _stop_requested(self) -> None:
        if self.charging.scoped_normal_stop():
            await self.charging.reconcile_expired()
            return
        await self.ensure_pause_zero()
        try:
            g, j = self.session.gateway, self.session.journal
            intent, epoch, boot, revision = (
                self.requests.intent(),
                g._charger_epoch,
                g.boot_observation,
                g.controls._revision,
            )
            local = j.active()
            prior_control = j.get("control", {})
            stop_epoch, prior_ticket = prior_control.get("stop_epoch", 0), prior_control.get("ticket")
            stop_ticket: str | None = None

            # A restrictive Stop still belongs to one current owner request and one known local session.
            def guard() -> None:
                nonlocal stop_ticket
                if (
                    self.stopping
                    or intent.mode != "paused"
                    or self.requests.intent() != intent
                    or g._charger_epoch != epoch
                    or g.boot_observation != boot
                    or g.controls._revision != revision
                    or not g.current_boot_seen(epoch)
                    or fingerprint(j.get("boot")) != self.native.spec.boot_digest
                    or j.active() != local
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
                    or stop_ticket not in (None, control["ticket"])
                ):
                    raise RecoveryRequiredError("owner Stop claim was superseded")
                if stop_ticket is None:
                    stop_ticket = control["ticket"]
                    # Preserve the original Pause identity used by Return and historical Start recovery.
                    j.put("managed_owner_stop", intent.pause_claim(stop_ticket))

            try:
                # Closing the exact locally known session may proceed even after cable removal.
                native = await self.native.read(closing_local=local)
            except _NativeMeasurementError as candidate:
                if (
                    candidate.authority != self.native.authority()
                    or candidate.sequence != self.native.last_resolved_read
                ):
                    raise AuditSupersededError("owner Stop measurement crossed a newer authority or observation")
                # Only measured excess may be ignored for Stop; identity, history and resources already passed.
                native = candidate.native
            guard()
            native.physical.require_fresh(j.identity, 5)
            self.native.require_known_session(native)
            self.session.book.owned_present(native.physical)
            controls = g.controls
            if not native.physical.transaction_open and controls.ready and controls.desired() == 0:
                self.require_paused(native)
                self.charging.observed(native)
                controls._complete(self.session.journal.get("control")["ticket"], uncertain=False)
                return
            if not controls.stop_acknowledged(local):
                await controls.stop(expected_local=local, guard=guard)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            return  # Intent stays paused; fresh supervision must establish zero before confirmation or any release.

    # A deliberate hold is confirmed only by actual closed/all-phase zero and the current owned station0 receipt.
    def require_paused(self, native: HandoverEvidence) -> None:
        self.native.require(native, closed=True)
        self.native.require_known_session(native)
        present = self.session.book.owned_present(native.physical)
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
        if j.get("uncertain_vendor_command"):
            return "vendor_command_uncertain"
        if not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present):
            return "station_zero_receipt_missing"
        if g.charger._pending:
            return "charger_rpc_pending"
        if g.controls._operations:
            return "control_operation_pending"
        return None

    # Reapply only a requested Stop after replacement; this branch never returns authority to the app implicitly.
    async def maintain_pause(self) -> HandoverEvidence:
        if self.pause_task is not None and not self.pause_task.done():
            await asyncio.shield(self.pause_task)
        # A still-open unplugged session must reach the guarded Stop retry below.
        native = await self.native.read(closing_local=self.session.journal.active())
        self.native.require_known_session(native)
        if (
            native.physical.transaction_open
            or not self.session.gateway.controls.ready
            or self.session.gateway.controls.desired() != 0
            or not ControlState(self.session.journal.get("control", {})).local_hold
        ):
            await self._stop_requested()
            raise RecoveryRequiredError("requested pause awaits actual physical closure")
        g = self.session.gateway
        if (self.native.introduced_epoch, self.native.introduced_revision) != (g._charger_epoch, g.controls._revision):
            self.native.confirm_paused_introduction(native)
        self.require_paused(native)
        g, j = self.session.gateway, self.session.journal
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
        if j.phase.read().value == "recovering":
            cloud = self.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted"
            if not cloud:
                await self.native.resume(local_only=True)
            elif not j.next_delivery():
                await self.native.resume()
        if awaiting_owner_start(j):
            raise RecoveryRequiredError("owner Pause awaits the previously acknowledged Start lifecycle")
        control = j.get("control", {})
        receipt = local_start_receipt(j)
        if control.get("uncertain") and (
            j.get("managed_owner_pause") == self.requests.intent().pause_claim(control.get("ticket"))
            or j.get("managed_owner_stop") == self.requests.intent().pause_claim(control.get("ticket"))
            or receipt.get("stage") in ("unsent", "rejected")
            and receipt.get("settled_ticket") == control.get("ticket")
            or receipt.get("bound_ticket") == control.get("ticket")
            and j.db.execute(
                "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (receipt.get("local"),)
            ).fetchone()
        ):
            # This ticket's explicit Pause is now verified, including any previous lifecycle obligation.
            g.controls._complete(control["ticket"], uncertain=False)
        pause_revision = self.requests.intent().revision
        if receipt.get("pause_revision") == pause_revision and receipt.get("verified_pause_revision") != pause_revision:
            j.starts.pause_verified(receipt, pause_revision)
        try:
            self.requests.confirm_pauses()
        except sqlite3.Error as error:
            # Only receipt publication failed. The next poll can verify zero without repeating Stop.
            raise RecoveryRequiredError("verified Pause awaits receipt publication") from error
        return native

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
            with j.atomic():
                write_intent(j, intent, "paused")
                g.controls._claim(True, stopping=True)

    # A newer stop ticket invalidates an awaited release before it can send a positive ceiling.
    def request_stop(self) -> None:
        if self.stopping:
            return
        self.stopping = True
        self.session.gateway.controls._claim(True, stopping=True)

    # A persistent failed restoration cannot be silently undone by another poll or a process restart.
    def fault(self, reason: str, *, diagnostic: dict[str, Any] | None = None) -> None:
        latch_managed_fault(self.session.gateway, reason, diagnostic=diagnostic)

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
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
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
        s, g, j = self.session, self.session.gateway, self.session.journal
        self.native.require(native)
        decision = readiness_decision(native, continuing_local is None, self.native.allow_unplugged)
        if isinstance(decision, Pending):
            return decision
        if continuing_local is not None:
            decision = self.continuation_decision(native, continuing_local)
            if isinstance(decision, Pending):
                return decision
            if any(v != 0 for v in native.currents_a + native.powers_w):
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
        refusal = self._zero_refusal(present, row, allow_offline=allow_offline, pending_stop=pending_stop)
        if refusal is not None:
            return Pending(Prerequisite.ZERO_PROOF, refusal)
        if pending_stop or g.vendor._pending:
            # A known closed Stop or ordinary RPC can finish before any positive release is dispatched.
            return Pending(Prerequisite.CLOUD_ACK, "shared control awaits the current cloud acknowledgement")
        return ALLOW

    # Release needs an owned zero receipt and no conflicting work after the native and lifecycle checks.
    def _zero_refusal(
        self,
        present: list[Resource],
        row: sqlite3.Row | None,
        *,
        allow_offline: bool,
        pending_stop: bool,
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
        if not allow_offline and not pending_stop and not facts.cloud_queue_empty():
            return "cloud delivery is pending"
        if g.controls._operations:
            return "control operation is pending"
        if g.charger._pending:
            return "charger RPC is pending"
        if j.get("uncertain_vendor_command"):
            return "vendor command is uncertain"
        return None

    # One accepted return receives one guarded attempt; uncertainty never changes persisted paused intent to app.
    async def return_requested(self, identifier: str, revision: int) -> None:
        expected = self.requests.intent()
        if expected.revision != revision:
            self.requests.finish(identifier, "failed", {"classification": "stale_revision"})
            return
        if self.charging.return_blocked():
            return  # An already accepted Return survives until observation completes the timed test under Pause.
        j = self.session.journal
        control = j.get("control", {})
        if control.get("uncertain") and j.get("managed_owner_stop") == expected.pause_claim(control.get("ticket")):
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
            or j.get("managed_owner_pause") == expected.pause_claim(control.get("ticket"))
            and j.db.execute(
                "SELECT 1 FROM managed_requests WHERE action='pause' AND state IN ('accepted','uncertain') LIMIT 1"
            ).fetchone()
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
            if self.ordinary.ordinary():
                await self.ordinary.hold("unplugged_session_pending")
            raise RecoveryRequiredError("unplugged session awaits exact lifecycle closure") from error
        async with asyncio.timeout(15):
            if self.session._resource_mutation_inflight():
                raise UnavailableError("managed native observation crossed a resource mutation")
            if authority != self.native.authority() or sequence != self.native.last_resolved_read:
                raise AuditSupersededError("settled native evidence crossed a newer authority or observation")
            if self.ordinary.closure_gap(native):
                await self.ordinary.hold("native_closure_pending")
                raise RecoveryRequiredError("ordinary zero hold awaits exact lifecycle closure")
            if (
                native.physical.transaction_open
                and native.physical.native_transaction_id != self.session.journal.active()
            ):
                raise SecurityError("managed native transaction has no exact accepted local counterpart")
            self.native.require_known_session(native)
            present = self.session.book.owned_present(native.physical)
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
                raise SecurityError("managed local station ceiling is absent or unconfirmed")
            native.physical.require_fresh(self.session.journal.identity, 5)
            if measured.exceeded:
                raise MeasuredOutputLimitError(native, measured, self.native.spec.digest, self.current.selected())
            return native

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
            return poll_status(self, PollStatus("attention", "native_repair_publication_pending"), pending=True)

    # A poll first joins owned work, then supervises, assesses current authority and publishes one view.
    async def _tick(self) -> dict[str, Any]:
        pending = await self._pending_work()
        if pending is not None:
            self.latest_native = None
            return poll_status(self, PollStatus("stopping" if self.stopping else "reconciling", pending), pending=True)
        base = poll_base(self)
        gate = await self._poll_gate()
        if gate is not None:
            return poll_status(self, gate, base=base)
        observed = await self._supervise(base)
        if isinstance(observed, PollStatus):
            return poll_status(self, observed, base=base)
        verdict = self._assess_observation(observed)
        if isinstance(verdict, PollStatus):
            return poll_status(self, verdict, base=base)
        return observed_status(self, base, observed.native, verdict)

    # Pending owned completions retain their existing settlement priority before any new native audit.
    async def _pending_work(self) -> str | None:
        pending = None
        if not await self.current.settle_for_poll():
            pending = "current_update_pending"
        elif not await self.charging.settle_for_poll():
            pending = "start_operation_pending"
        elif self.recovery.pending():
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
            if (
                self.requests.pending_pause()
                and (self.pause_task is None or self.pause_task.done())
                and not g.controls._operations
                and not g.charger._pending
            ):
                await self._stop_requested()
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
    async def _supervise(self, base: dict[str, Any]) -> Observation | PollStatus:
        g, j = self.session.gateway, self.session.journal
        if not base["charger_connected"] or not base["boot_verified"]:
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
        verified_pause = False
        try:
            self.prepare_offline_pause()
            recovery_pending = await self.recovery.advance()
            if recovery_pending is not None:
                if recovery_pending.reason is Prerequisite.NATIVE:
                    return PollStatus("degraded", "native_observation_unavailable")
                return PollStatus(
                    "waiting_for_cloud" if recovery_pending.reason is Prerequisite.CLOUD else "reconciling"
                )
            native = await self.settled_native()
            if (self.requests.intent().mode == "app") and (self.released_epoch != g._charger_epoch):
                raise RecoveryRequiredError("managed release awaits fresh app-control restoration")
            if self.requests.intent().mode == "paused":
                self.require_paused(native)
                verified_pause = True
            previous_intent = self.requests.intent()
            self.charging.observed(native)
            observed_intent = self.requests.intent()
            intent_changed = observed_intent != previous_intent
        except (NativeTransportUnavailableError, StaleEvidenceError) as error:
            self.failures = 0
            self.strict_failure_seen = False
            self.native_unavailable += 1
            self.record_optional_native_failure(
                {
                    "type": type(error).__name__,
                    "detail": str(error)[:1024],
                    "at_epoch": time.time(),
                    "availability_only": True,
                },
            )
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
        except (SecurityError, DeniedError) as error:
            if self.stopping:
                return PollStatus("stopping")
            # Only a fully settled typed measurement can make this entire hard-failure run recoverable.
            measured_only = isinstance(error, MeasuredOutputLimitError)
            self.strict_failure_seen = self.strict_failure_seen or not measured_only
            diagnostic: dict[str, Any] = {
                "type": diagnostic_type(error),
                "detail": str(error)[:1024],
                "at_epoch": time.time(),
            }
            if isinstance(error, MeasuredOutputLimitError):
                diagnostic["cause"] = error.cause
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
        self.failures = 0
        self.strict_failure_seen = False
        self.native_unavailable = 0
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
            and not j.get("uncertain_vendor_command")
            and not j.fault.latched()
            and not j.get("trial_preservation_fault")
            and not j.get("trial_inventory_fault")
            and not control.values.get("start_pending")
            and not j.get("vendor_start_grant")
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        ):
            intent = self.requests.intent()
            return PollStatus("reconciling", intent=intent)
        paused = self.requests.intent().mode == "paused"
        proof = ControlProof(shared=True, hold_verified=observed.verified_pause).retain(control)
        delivery_blocked = s.runtime.status.delivery_blocked
        delivery_uncertain = delivery_blocked and (
            s.runtime.status.delivery_blocked_cause != "storage" or g.delivery_outcome_uncertain
        )
        if (not proof.available or delivery_uncertain or j.get("uncertain_vendor_command")) and not self.stopping:
            self.fault("owner_control_or_delivery_uncertain")
        if (proof.available and j.fault.latched()) or self.stopping:
            proof = ControlProof()
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
        if verdict.healthy and not paused:
            pending_return = self.requests.pending_return()
            if pending_return is not None:
                self.requests.returned(pending_return[0])
        return verdict
