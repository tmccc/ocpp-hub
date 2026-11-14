"""Persistent app/schedule supervision without assistant-initiated Start or fabricated recovery outcomes."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .local_control import local_start_receipt
from .managed_current import (
    ManagedCurrentLimits,
    managed_current_ceiling,
    require_current_session,
    require_current_source,
)
from .managed_current_recovery import capture_interrupted_collision
from .managed_intent import ControlIntent, ManagedRequests
from .managed_native_repair import NativeRepairAudits
from .managed_release_recovery import reconcile_owner_release, record_release_pause, release_evidence
from .managed_release_wait import awaiting_known_stops
from .managed_schedule import (
    KEY,
    ManagedSchedule,
    current_source,
    recorded_station_profile,
    require_continuing_session,
    supervised_timer,
    temporary_continuation,
)
from .managed_start_binding import ManagedStartBinding
from .managed_test_charge import ManagedChargeTests
from .managed_view import summarize_charger_history
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
from .rpc import UnavailableError
from .security import SecurityError, StaleEvidenceError
from .store import RecoveryRequiredError, ReleaseDeferredError, encoded, fingerprint
from .trial_evidence import profile_projection
from .trial_handover_evidence import HandoverEvidence
from .trial_rollback import require_lifecycle_idle
from .trial_session import TrialSession


class ManagedController:
    """One verified release per actual introduction returns control to the unchanged owner app/native schedule."""

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
        self.schedule = ManagedSchedule(self)
        self.native.schedule_changed = self.schedule.changed.set
        self.current = ManagedCurrentLimits(self)
        self.tests = ManagedChargeTests(self)
        self.ordinary = OrdinaryRecovery(self)
        self.native.ordinary_source_observer = self.ordinary.observe
        self.native.positive_guard = self.require_positive
        self.native.continuation_guard = self.require_continuation
        self.native.recovery_zero_only = self.tests.scoped_normal_stop
        binding = ManagedStartBinding(session.gateway, self.native.spec.saved_schedule_digest)
        session.gateway.defer_command_binding = binding.defer
        self.native.command_source_observer = binding.observe
        self.native.repair_audits = NativeRepairAudits(
            session.journal, lambda fault: release_evidence(self, fault) is not None
        )

    # Accept commands without performing hidden Start, while a newer Pause immediately revokes positive authority.
    def command(self, identifier: str, action: str, revision: int, *, current_a: int | None = None) -> dict[str, Any]:
        if self.stopping:
            raise UnavailableError("managed service is stopping")
        if action == "set_current":
            if current_a is None:
                raise SecurityError("a current selection requires an integer current")
            return self.current.accept(identifier, revision, current_a)
        if current_a is not None:
            raise SecurityError("current is only supported by the current-selection command")
        if action in ("start", "start_test", "arm_cloud_test"):
            return self.tests.accept(identifier, action, revision)
        previous = self.requests.existing(identifier, action, revision)
        if previous is not None:
            return previous
        if action == "app_control" and self.current.pending():
            raise RecoveryRequiredError("return awaits the current-setting outcome")
        if action == "app_control" and self.session.journal.get("managed_control_fault"):
            raise RecoveryRequiredError("a latched fault requires independent operator reconciliation")
        if action == "app_control" and self.requests.intent().mode != "paused":
            raise RecoveryRequiredError("return requires a current paused intent")

        # This claim shares the intent transaction, so no accepted Pause can leave old shared authority committed.
        def claim() -> None:
            _, ticket = self.session.gateway.controls._claim(True, stopping=True)
            self.session.journal.put(
                "managed_owner_pause", {"ticket": ticket, "revision": self.requests.intent().revision}
            )
            record_release_pause(self, identifier)

        receipt = self.requests.accept(identifier, action, revision, claim)
        if action == "pause":
            if self.pause_task is None or self.pause_task.done():
                self.pause_task = asyncio.create_task(self._stop_requested())
        return receipt

    # Explicit owner Pause can restrict the commissioned station even while a native HTTP read is unavailable.
    async def ensure_pause_zero(self) -> None:
        g, j = self.session.gateway, self.session.journal
        intent = self.requests.intent()
        if intent.mode != "paused" or g.controls.ready and g.controls.desired() == 0:
            return
        epoch, ticket = g._charger_epoch, j.get("control", {}).get("ticket")

        # Recheck the owner's claim and authenticated device immediately before each wire operation.
        def guard() -> None:
            control = j.get("control", {})
            if (
                self.stopping
                or self.requests.intent() != intent
                or not ticket
                or control.get("ticket") != ticket
                or control.get("owner") != "local"
                or control.get("inhibit") is not True
                or g._charger_epoch != epoch
                or not g.current_boot_seen(epoch)
                or fingerprint(j.get("boot")) != self.native.spec.boot_digest
                or not recorded_station_profile(j, self.native.spec.digest, self.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("owner Pause awaits the current commissioned station")

        try:
            await g.controls.ensure_ceiling(guard=guard)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError:
            return  # A failed zero acknowledgement still permits Stop if independent native verification succeeds.

    # Keep the physical Stop independent from the requesting websocket's lifetime and retain uncertainty honestly.
    async def _stop_requested(self) -> None:
        if self.tests.scoped_normal_stop():
            await self.tests.reconcile_expired()
            return
        await self.ensure_pause_zero()
        try:
            native = await self.native.read()
            self.native.require_known_session(native)
            self.session.book.owned_present(native.physical)
            controls = self.session.gateway.controls
            if not native.physical.transaction_open and controls.ready and controls.desired() == 0:
                self.require_paused(native)
                self.tests.observed(native)
                controls._complete(self.session.journal.get("control")["ticket"], uncertain=False)
                return
            await self.session.gateway.stop()
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            return  # Intent stays paused; fresh supervision must establish zero before confirmation or any release.

    # A deliberate hold is confirmed only by actual closed/all-phase zero and the current owned station0 receipt.
    def require_paused(self, native: HandoverEvidence, *, timer_cleanup: bool = False) -> None:
        g, j = self.session.gateway, self.session.journal
        self.native.require(native, closed=True)
        self.native.require_known_session(native)
        present = self.session.book.owned_present(native.physical)
        if (
            not g.current_boot_seen(g._charger_epoch)
            or self.native.introduced_epoch != g._charger_epoch
            or self.native.introduced_revision != g.controls._revision
            or not g.controls.ready
            or g.controls.desired() != 0
            or j.get("control", {}).get("owner") != "local"
            or not j.get("control", {}).get("inhibit")
            or not timer_cleanup
            and j.get("control", {}).get("start_pending")
            or j.get("uncertain_vendor_command")
            or not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present)
            or g.charger._pending
            or g.controls._operations
        ):
            raise RecoveryRequiredError("requested pause awaits independently verified closed zero")

    # Reapply only a requested Stop after replacement; this branch never returns authority to the app implicitly.
    async def maintain_pause(self) -> HandoverEvidence:
        if self.pause_task is not None and not self.pause_task.done():
            await asyncio.shield(self.pause_task)
        native = await self.native.read()
        await self.schedule.await_closure_agreement(native)
        self.native.require_known_session(native)
        if (
            native.physical.transaction_open
            or not self.session.gateway.controls.ready
            or self.session.gateway.controls.desired() != 0
            or self.session.journal.get("control", {}).get("owner") != "local"
            or not self.session.journal.get("control", {}).get("inhibit")
        ):
            await self._stop_requested()
            raise RecoveryRequiredError("requested pause awaits actual physical closure")
        g = self.session.gateway
        if (self.native.introduced_epoch, self.native.introduced_revision) != (g._charger_epoch, g.controls._revision):
            self.native.confirm_paused_introduction(native)
        if self.schedule.awaiting_start() and self.schedule.revoked(self.session.journal.get(KEY, {})):
            self.require_paused(native, timer_cleanup=True)
            await self.schedule.observe(native)
        self.require_paused(native)
        g, j = self.session.gateway, self.session.journal
        if j.get("trial_phase") == "recovering":
            cloud = self.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted"
            if not cloud:
                await self.native.resume(local_only=True)
            elif not j.next_delivery():
                await self.native.resume()
        if j.get("managed_temporary_schedule", {}).get("state") in ("installing", "armed", "clearing"):
            await self.schedule.before_release(native)
        if self.schedule.awaiting_start():
            raise RecoveryRequiredError("owner Pause awaits the previously acknowledged Start lifecycle")
        control = j.get("control", {})
        receipt = local_start_receipt(j)
        if control.get("uncertain") and (
            j.get("managed_owner_pause")
            == {"ticket": control.get("ticket"), "revision": self.requests.intent().revision}
            or receipt.get("stage") == "unsent"
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
            j.put("local_start_receipt", {**receipt, "verified_pause_revision": pause_revision})
        self.requests.confirm_pauses()
        return native

    # Offline replacement retires prior app authority before any recovery wait and never auto-Returns on reconnection.
    def prepare_offline_pause(self) -> None:
        g, j = self.session.gateway, self.session.journal
        intent = self.requests.intent()
        if (
            j.get("trial_phase") == "recovering"
            and not (self.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted")
            and self.session.runtime.status.vendor_failed_charger_epoch == g._charger_epoch
            and intent.mode == "app"
        ):
            with j.atomic():
                j.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                g.controls._claim(True, stopping=True)

    # A newer stop ticket invalidates an awaited release before it can send a positive ceiling.
    def request_stop(self) -> None:
        if self.stopping:
            return
        self.stopping = True
        self.session.gateway.controls._claim(True, stopping=True)

    # A persistent failed restoration cannot be silently undone by another poll or a process restart.
    def fault(self, reason: str, *, diagnostic: dict[str, Any] | None = None) -> None:
        journal = self.session.journal
        if not journal.get("managed_control_fault"):
            with journal.atomic():
                fault: dict[str, Any] = {"classification": reason, "at_epoch": time.time()}
                if diagnostic is not None:
                    journal.put("managed_native_failure", diagnostic)
                    fault["native_failure_digest"] = fingerprint(diagnostic)
                journal.put("managed_control_fault", fault)
                control = journal.get("control", {})
                journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
                if journal.get("trial_phase") in ("active", "recovering", "blocked"):
                    journal.put("trial_phase", "blocked")
            self.session.gateway.controls.invalidate()

    # Reuse only the existing owned zero profile; a fault never creates Stop, cleanup or positive authority.
    async def hold_fault(self, *, occurrence: str | None = None) -> None:
        g, j = self.session.gateway, self.session.journal
        fault, control = j.get("managed_control_fault"), j.get("control", {})
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
            timer = j.get(KEY, {})
            if (
                self.stopping
                or not fault
                or j.get("managed_control_fault") != fault
                or j.get("control", {}) != control
                or control.get("owner") != "local"
                or control.get("inhibit") is not True
                or g.controls.desired() != 0
                or j.get("trial_phase") not in ("active", "blocked", "recovering")
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
                or occurrence is not None
                and (timer.get("id") != occurrence or not supervised_timer(timer))
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

    # Require the actual source under whichever operation is currently holding this particular open transaction.
    def require_continuation(self, native: HandoverEvidence, local: int) -> None:
        j = self.session.journal
        if j.get("managed_current_change", {}).get("state") == "recovering":
            require_current_session(self.session.gateway, native, local)
        elif self.ordinary.active():
            self.ordinary.require_session(native, local)
        elif not (self.tests.scoped_normal_stop() and self.tests.independent_daily(native, local)):
            require_continuing_session(j, native, local)

    # Preserve scoped test restrictions and recheck ordinary source immediately before any positive physical write.
    def require_positive(self, native: HandoverEvidence) -> None:
        self.tests.check_positive(native)
        if self.ordinary.active():
            local = self.session.journal.active()
            self.ordinary.require_authority(local)
            if local is not None:
                self.require_continuation(native, local)
            elif native.physical.transaction_open:
                raise ReleaseDeferredError("ordinary idle recovery cannot adopt another native session")

    # The present owned zero, live introduction and actual reconciled events are prerequisites to any release.
    def require_zero(
        self, native: HandoverEvidence, *, allow_offline: bool = False, continuing_local: int | None = None
    ) -> None:
        s, g, j = self.session, self.session.gateway, self.session.journal
        self.native.require(native, closed=continuing_local is None)
        if continuing_local is not None:
            self.require_continuation(native, continuing_local)
            if any(v != 0 for v in native.currents_a + native.powers_w):
                raise RecoveryRequiredError("the identified original session awaits a verified zero hold")
        self.native.require_known_session(native)
        present = s.book.owned_present(native.physical)
        row = j.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        pending_stop = continuing_local is None and awaiting_known_stops(j)
        if not pending_stop:
            require_lifecycle_idle(
                g,
                continuing_local=continuing_local,
                preserve_expired_start=self.tests.preserve_debt(native, continuing_local),
            )
        if (
            j.get("trial_phase") not in ("active", "recovering")
            or j.get("native_bounded_handover", {}).get("stage") != "admitted"
            or j.get("managed_control_fault")
            or not g.current_boot_seen(g._charger_epoch)
            or self.native.introduced_epoch != g._charger_epoch
            or self.native.introduced_revision != g.controls._revision
            or g.charger.sender is None
            or not allow_offline
            and (
                g.vendor.sender is None or g.vendor_registration != "Accepted" or not s.runtime.status.vendor_connected
            )
            or not g.controls.ready
            or g.controls.desired() != 0
            or not j.get("control", {}).get("inhibit")
            or row is None
            or row["state"] != "confirmed"
            or not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present)
            or not allow_offline
            and not pending_stop
            and j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or g.controls._operations
            or g.charger._pending
            or j.get("uncertain_vendor_command")
        ):
            raise RecoveryRequiredError("shared control awaits fresh zero and actual history/cloud reconciliation")
        if pending_stop or g.vendor._pending:
            # A known closed Stop or ordinary RPC can finish before any positive release is dispatched.
            raise ReleaseDeferredError("shared control awaits the current cloud acknowledgement")

    # Verify again after recovery acknowledgement; release raises only the ceiling and never sends RemoteStart.
    async def restore_owner_control(
        self, expected_intent: ControlIntent | None = None, *, continuing_local: int | None = None
    ) -> None:
        g, j = self.session.gateway, self.session.journal
        expected_intent = expected_intent or self.requests.intent()

        # This guard also runs before LocalControl claims and again inside each actual outgoing RPC gate.
        def guard() -> None:
            timer = j.get(KEY, {})
            if self.ordinary.active():
                self.ordinary.require_authority(continuing_local)
            current_recovery = j.get("managed_current_change", {}).get("state") == "recovering"
            if current_recovery:
                require_current_source(g, continuing_local)
            if (
                continuing_local is not None
                and not current_recovery
                and not self.ordinary.active()
                and not (self.tests.scoped_normal_stop() and self.tests.current_daily(continuing_local))
                and (
                    not current_source(g, timer)
                    or self.schedule.needs_source(timer)
                    or j.active() != continuing_local
                    or timer.get("unidentified_local") != continuing_local
                    and timer.get("other_local") != continuing_local
                    or (
                        timer.get("other_local") != continuing_local
                        and not temporary_continuation(j, timer, continuing_local)
                    )
                    or timer.get("unreported_native")
                )
            ):
                raise ReleaseDeferredError("source-specific continuation was superseded")
            if self.stopping or self.requests.intent() != expected_intent:
                raise UnavailableError("a newer requested hold superseded managed release")

        guard()
        if self.stopping:
            raise UnavailableError("managed shutdown retains its hold")
        epoch, revision = g._charger_epoch, g.controls._revision
        native = await self.native.read()
        await self.schedule.await_closure_agreement(native)
        self.native.require_known_session(native)
        if native.physical.transaction_open and continuing_local is None:
            raise RecoveryRequiredError("owner release awaits actual transaction closure")
        self.require_zero(native, continuing_local=continuing_local)
        if j.get("trial_phase") == "recovering":
            await self.native.resume(continuing_local=continuing_local, preserve_expired_start=self.tests.preserve_debt)
        ticket = j.get("control", {}).get("ticket")
        native = await self.native.read(closed=continuing_local is None)
        self.require_zero(native, continuing_local=continuing_local)
        if continuing_local is None and j.get("managed_temporary_schedule", {}).get("state") in (
            "installing",
            "armed",
            "clearing",
        ):
            await self.schedule.before_release(native)
            native = await self.native.read(closed=continuing_local is None)
            self.require_zero(native, continuing_local=continuing_local)
        if (
            self.stopping
            or epoch != g._charger_epoch
            or revision != g.controls._revision
            or ticket != j.get("control", {}).get("ticket")
            or j.get("trial_phase") != "active"
            or j.get("control", {}).get("uncertain")
        ):
            raise UnavailableError("managed release authority changed during its native check")
        release_guard = (
            self.schedule.check_release if continuing_local is None and not self.ordinary.active() else guard
        )
        release_guard()
        if expected_intent.mode == "paused":
            guard()
            self.require_paused(native)
            self.requests.confirm_pauses()
        self.session.book.before_positive_release = release_guard
        try:
            await g.controls.release(guard=guard)
            guard()
        except ReleaseDeferredError:
            raise
        except (DeniedError, SecurityError, RecoveryRequiredError, UnavailableError) as error:
            if not self.stopping and self.requests.intent() == expected_intent:
                # Detailed peer/guard errors stay in the private journal; public status exposes classification only.
                j.put(
                    "managed_release_failure",
                    {
                        "type": type(error).__name__,
                        "detail": str(error)[:1024],
                        "at_epoch": time.time(),
                        "charger_rpc": g.charger.diagnostics.snapshot(),
                    },
                )
                if self.ordinary.active() and isinstance(error, UnavailableError):
                    raise ReleaseDeferredError("ordinary recovery retains zero pending a new guarded proof") from error
                if (
                    continuing_local is not None
                    and not isinstance(error, SecurityError)
                    and supervised_timer(j.get(KEY, {}))
                    and j.active() == continuing_local
                ):
                    self.schedule.begin_hold(j.get(KEY))
                    raise ReleaseDeferredError(
                        "independent continuation awaits renewed zero and source proof"
                    ) from error
                self.fault("owner_release_unconfirmed")
            raise
        finally:
            self.session.book.before_positive_release = None
        self.released_epoch = epoch
        j.put("managed_owner_epoch", epoch)
        timer = j.get(KEY, {})
        if timer.get("state") == "complete" and not timer.get("idle_cutoff_hold") and j.active() is None:
            # Inventory cleanup can release this verified idle profile while its separate cutoff remains pending.
            j.put(
                KEY,
                {
                    k: v
                    for k, v in timer.items()
                    if k not in ("hold_since", "next_cap", "next_stop", "unidentified_local")
                },
            )
        if continuing_local is not None:
            self.schedule.clear_idle_cutoff("verified_session")
        elif expected_intent.mode == "paused" and self.schedule.owner_returning():
            self.schedule.clear_idle_cutoff("owner_return")
        timer = j.get(KEY, {})
        if (
            timer.get("state") == "requested"
            and timer.get("idle_cutoff_replacement")
            and timer["revision"] == expected_intent.revision
            and timer["epoch"] == epoch
        ):
            # This newly authorized one-off was accepted under zero; bind its installation to this verified release.
            j.put(KEY, {**timer, "ticket": j.get("control", {}).get("ticket"), "idle_cutoff_replacement": False})

    # One accepted return receives one guarded attempt; uncertainty never changes persisted paused intent to app.
    async def return_requested(self, identifier: str, revision: int) -> None:
        expected = self.requests.intent()
        if expected.revision != revision:
            self.requests.finish(identifier, "failed", {"classification": "stale_revision"})
            return
        j = self.session.journal
        control = j.get("control", {})
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
            or j.get("managed_owner_pause") == {"ticket": control.get("ticket"), "revision": expected.revision}
            and j.db.execute(
                "SELECT 1 FROM managed_requests WHERE action='pause' AND state IN ('accepted','uncertain') LIMIT 1"
            ).fetchone()
        ):
            return  # The same Return survives the real Start and both closure reports under its Pause.
        j.db.execute("UPDATE managed_requests SET state='running' WHERE id=? AND state='accepted'", (identifier,))
        try:
            await self.restore_owner_control(expected)
        except ReleaseDeferredError:
            self.session.journal.db.execute(
                "UPDATE managed_requests SET state='accepted' WHERE id=? AND state='running'", (identifier,)
            )
            return
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            self.requests.finish(identifier, "uncertain", {"classification": "return_not_confirmed"})
            return
        if self.requests.intent() != expected:
            self.requests.finish(identifier, "failed", {"classification": "superseded_by_pause"})
            return
        with self.session.journal.atomic():
            self.session.journal.put("managed_control_intent", {"mode": "app", "revision": revision + 1})
            self.current.returned_to_previous()
            self.requests.finish(identifier, "completed", {"app_control_enabled": True, "charging_started": False})

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
        async with asyncio.timeout(15):
            if self.session._resource_mutation_inflight():
                raise UnavailableError("managed native observation crossed a resource mutation")
            await self.schedule.await_closure_agreement(native)
            if authority != self.native.authority() or sequence != self.native.last_resolved_read:
                raise AuditSupersededError("settled native evidence crossed a newer authority or observation")
            if self.ordinary.closure_gap(native):
                await self.ordinary.hold("native_closure_pending")
                raise RecoveryRequiredError("ordinary zero hold awaits exact lifecycle closure")
            if (
                native.physical.transaction_open
                and native.physical.native_transaction_id != self.session.journal.active()
            ):
                lease = self.session.journal.get(KEY, {})
                if supervised_timer(lease) and lease.get("unreported_native"):
                    await self.schedule.protect_station(lease)
                    raise RecoveryRequiredError("timer holds zero awaiting the genuine transaction or native closure")
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
        status = await self._tick()
        problem = self.schedule.problem()
        if problem and status["state"] not in ("attention", "stopping"):
            status.update(state="degraded", fault=problem, temporary_schedule=self.schedule.summary())
        return status

    # Retain ordinary recovery and owner-control supervision while schedule alarms are reported separately.
    async def _tick(self) -> dict[str, Any]:
        pending = None
        if not await self.current.settle_for_poll():
            pending = "current_update_pending"
        elif not await self.tests.settle_for_poll():
            pending = "start_operation_pending"
        if pending is not None:
            self.latest_native = None
            return {
                "checked_at": time.time(),
                "mode": "app_schedule",
                "state": "stopping" if self.stopping else "reconciling",
                "fault": pending,
                "owner_control_enabled": False,
                "local_hold_verified": False,
                "native_observed_at": None,
                "native_limit_a": None,
                "current_a": None,
                "power_w": None,
                "station_ceiling_a": None,
                "charging_session_open": None,
                "vehicle_drawing_power": None,
            }
        s, g, j = self.session, self.session.gateway, self.session.journal
        base = {
            "checked_at": time.time(),
            "mode": "app_schedule",
            "local_start_disabled": not g.policy.allow_local_start,
            "charger_connected": s.runtime.status.charger_connected,
            "cloud_connected": s.runtime.status.vendor_connected and g.vendor_registration == "Accepted",
            "boot_verified": g.current_boot_seen(g._charger_epoch),
            "phase": j.get("trial_phase"),
            "pending_lifecycle_events": j.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE action IN ('StartTransaction','StopTransaction')"
            ).fetchone()[0],
            "pending_observations": j.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE action NOT IN ('StartTransaction','StopTransaction')"
            ).fetchone()[0],
            "automatic_charge_restart": False,
            "vendor_write_uncertain": j.db.execute(
                "SELECT COUNT(*) FROM mutations WHERE origin='vendor' AND state!='confirmed'"
            ).fetchone()[0],
            "operator_reconciliation_required": bool(j.get("uncertain_vendor_command"))
            or j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed'").fetchone()
            is not None,
            "desired_control": self.requests.intent().mode,
            "control_revision": self.requests.intent().revision,
            "local_hold_verified": False,
            "owner_control_enabled": False,
        }
        if self.stopping:
            return {**base, "state": "stopping", "owner_control_enabled": False}
        if j.get("managed_control_fault"):
            await self.hold_fault()
            await self.tests.retire_closed_after_fault()
            try:
                await reconcile_owner_release(self)
            except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
                pass  # The exact fault remains latched until a new Pause and every independent zero proof agree.
            if not j.get("managed_control_fault"):
                return {**base, "state": "reconciling", "owner_control_enabled": False}
            return {**base, "state": "attention", "fault": j.get("managed_control_fault")["classification"]}
        timer = self.schedule.arm_idle_cutoff()
        pending_timer = supervised_timer(timer)
        cutoff_hold = pending_timer and "hold_since" in timer
        if not base["charger_connected"] or not base["boot_verified"]:
            return {**base, "state": "starting", "owner_control_enabled": False}
        if self.requests.intent().mode == "paused" and (self.pause_task is None or self.pause_task.done()):
            await self.ensure_pause_zero()
        current_hold = j.get("managed_current_change", {}).get("state") == "recovering"
        if (
            not g.controls.ready
            and self.released_epoch != g._charger_epoch
            and not cutoff_hold
            and not current_hold
            and not self.ordinary.needed()
            and not (pending_timer and j.active() is not None)
        ):
            if pending_timer:
                try:
                    await self.schedule.retry_recovery_zero()
                except DeniedError, UnavailableError, RecoveryRequiredError:
                    pass
            return {**base, "state": "starting", "owner_control_enabled": False}
        verified_pause = False
        verified_idle_hold = False
        try:
            self.prepare_offline_pause()
            await self.current.recover_overlap()
            if await self.tests.reconcile_expired() and self.requests.intent().mode == "app":
                return {**base, "state": "reconciling", "owner_control_enabled": False}
            if pending_timer:
                await self.schedule.protect_deadline(timer)
                cutoff_hold = "hold_since" in j.get(KEY, {})
            pending_return = self.requests.pending_return()
            if self.requests.intent().mode == "paused" and pending_return is not None:
                await self.return_requested(*pending_return)
            if self.requests.intent().mode == "paused":
                await self.maintain_pause()
            elif self.ordinary.needed():
                if not base["cloud_connected"]:
                    await self.ordinary.hold("restricted_introduction")
                    return {**base, "state": "waiting_for_cloud", "owner_control_enabled": False}
                await self.ordinary.recover()
            elif self.released_epoch != g._charger_epoch:
                if cutoff_hold:
                    await self.schedule.hold_until_closed()
                    if self.schedule.awaiting_start() and j.active() is None:
                        native = await self.native.read(closed=True)
                        self.require_paused(native, timer_cleanup=True)
                        await self.schedule.observe(native)
                elif pending_timer and j.active() is not None and self.native.introduced_epoch == g._charger_epoch:
                    await self.schedule.observe(await self.native.read())
                if not base["cloud_connected"]:
                    if cutoff_hold and j.get("trial_phase") == "active":
                        native = await self.native.read(closed=True)
                        self.require_paused(native)
                        await self.schedule.before_release(native)
                    return {**base, "state": "waiting_for_cloud", "owner_control_enabled": False}
                if j.get(KEY, {}).get("idle_cutoff_hold"):
                    if j.get("trial_phase") == "recovering":
                        await self.native.resume()
                elif self.released_epoch != g._charger_epoch:
                    await self.restore_owner_control()
            native = await self.settled_native()
            await self.schedule.observe(native)
            if j.get(KEY, {}).get("idle_cutoff_hold"):
                self.require_paused(native, timer_cleanup=True)
                timer = j.get(KEY, {})
                verified_idle_hold = timer.get("state") == "complete" or (
                    timer.get("state") == "armed"
                    and timer.get("idle_cutoff_replacement") is True
                    and not self.schedule.revoked(timer)
                )
            if (
                self.requests.intent().mode == "app"
                and self.released_epoch != g._charger_epoch
                and not verified_idle_hold
            ):
                raise RecoveryRequiredError("timer retirement awaits fresh app-control restoration")
            if self.requests.intent().mode == "paused":
                self.require_paused(native)
                verified_pause = True
        except (NativeTransportUnavailableError, StaleEvidenceError) as error:
            self.failures = 0
            self.strict_failure_seen = False
            self.native_unavailable += 1
            j.put(
                "managed_native_failure",
                {
                    "type": type(error).__name__,
                    "detail": str(error)[:1024],
                    "at_epoch": time.time(),
                    "availability_only": True,
                },
            )
            if self.native_unavailable >= 3:
                lease = j.get(KEY, {})
                if supervised_timer(lease):
                    if not lease.get("retire_reason"):
                        lease = {**lease, "retire_reason": "native_observation_unavailable"}
                        j.put(KEY, lease)
                    await self.schedule.protect_station(lease)
                elif self.ordinary.ordinary():
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
            return {
                **base,
                "state": "degraded",
                "owner_control_enabled": False,
                "fault": "native_observation_unavailable",
            }
        except UnavailableError, TimeoutError:
            return {
                **base,
                "state": "stopping" if self.stopping else "degraded",
                "owner_control_enabled": False,
                "fault": "native_observation_unavailable",
            }
        except RecoveryRequiredError:
            return {**base, "state": "reconciling", "owner_control_enabled": False}
        except (SecurityError, DeniedError) as error:
            if self.stopping:
                return {**base, "state": "stopping", "owner_control_enabled": False}
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
                j.put("managed_native_failure", diagnostic)
            return {**base, "state": "degraded", "fault": classification}
        self.failures = 0
        self.strict_failure_seen = False
        self.native_unavailable = 0
        self.latest_native = native
        previous_intent = self.requests.intent()
        self.tests.observed(native)
        if self.requests.intent() != previous_intent:
            return {
                **base,
                "state": "pausing",
                "owner_control_enabled": False,
                "desired_control": self.requests.intent().mode,
                "control_revision": self.requests.intent().revision,
            }
        control = j.get("control", {})
        if (
            self.current.applying()
            and not s.runtime.status.delivery_blocked
            and not j.get("uncertain_vendor_command")
            and not j.get("managed_control_fault")
            and not j.get("trial_preservation_fault")
            and not j.get("trial_inventory_fault")
            and not control.get("start_pending")
            and not j.get("vendor_start_grant")
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        ):
            intent = self.requests.intent()
            return {
                **base,
                "state": "reconciling",
                "owner_control_enabled": False,
                "desired_control": intent.mode,
                "control_revision": intent.revision,
            }
        shared = control.get("owner") == "shared" and not control.get("inhibit") and not control.get("uncertain")
        paused = self.requests.intent().mode == "paused"
        delivery_uncertain = s.runtime.status.delivery_blocked and (
            s.runtime.status.delivery_blocked_cause != "storage" or g.delivery_outcome_uncertain
        )
        if (
            not (shared or verified_pause or verified_idle_hold)
            or delivery_uncertain
            or j.get("uncertain_vendor_command")
        ):
            if not self.stopping:
                self.fault("owner_control_or_delivery_uncertain")
        shared = shared and not j.get("managed_control_fault") and not self.stopping
        verified_pause = verified_pause and not j.get("managed_control_fault") and not self.stopping
        verified_idle_hold = verified_idle_hold and not j.get("managed_control_fault") and not self.stopping
        cloud = (
            s.runtime.status.vendor_connected and g.vendor.sender is not None and g.vendor_registration == "Accepted"
        )
        local_healthy = (
            (shared or verified_pause or verified_idle_hold)
            and j.get("trial_phase") == "active"
            and not control.get("uncertain")
            and not s.runtime.status.delivery_blocked
        )
        healthy = local_healthy and cloud
        if healthy and not paused:
            pending_return = self.requests.pending_return()
            if pending_return is not None:
                self.requests.finish(
                    pending_return[0], "completed", {"app_control_enabled": True, "charging_started": False}
                )
        history = summarize_charger_history(native.history)
        return {
            **base,
            "local_healthy": bool(local_healthy),
            "checked_at": time.time(),
            "phase": j.get("trial_phase"),
            "cloud_connected": cloud,
            "state": "stopping"
            if self.stopping
            else "healthy"
            if healthy
            else "local_only"
            if local_healthy
            else "degraded",
            "owner_control_enabled": shared,
            "desired_control": self.requests.intent().mode,
            "control_revision": self.requests.intent().revision,
            "local_hold_verified": verified_pause or verified_idle_hold,
            "native_observed_at": native.physical.observed_at,
            "native_limit_a": native.runtime_user_a,
            "requested_current_a": self.current.selected(),
            "current_capability": managed_current_ceiling(j, self.native.spec) == 32,
            "can_set_current": self.current.available(native),
            "station_ceiling_a": g.controls.desired(),
            "current_a": native.physical.current_a,
            "power_w": native.physical.power_w,
            "charging_session_open": native.physical.transaction_open,
            "vehicle_connected": native.physical.connection != "Unplug",
            "history_records": len(native.history),
            "test_charge": self.tests.summary(),
            "test_start_ready": self.tests.available(native),
            "local_charge": self.tests.normal_summary(),
            "normal_start_ready": self.tests.available(native, normal=True),
            "history_archive": j.get("native_history_summary"),
            "history_warning": "records_absent_from_charger"
            if j.get("native_history_summary", {}).get("missing_records", 0)
            else None,
            "charger_sessions": history.sessions,
            "charger_history_complete": history.complete,
            "temporary_schedule": self.schedule.summary(),
            "preservation_failed": bool(j.get("trial_preservation_fault") or j.get("trial_inventory_fault")),
        }
