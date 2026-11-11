"""Persistent app/schedule supervision without assistant-initiated Start or fabricated recovery outcomes."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .local_control import local_start_receipt
from .managed_current import ManagedCurrentLimits, managed_current_ceiling
from .managed_intent import ControlIntent, ManagedRequests
from .managed_schedule import (
    KEY,
    OBSERVABLE,
    ManagedSchedule,
    current_source,
    recorded_station_profile,
    require_continuing_session,
)
from .managed_test_charge import ManagedChargeTests
from .managed_view import charger_history
from .native_http import NativeTransportUnavailableError
from .policy import DeniedError
from .rpc import UnavailableError
from .security import SecurityError, StaleEvidenceError
from .store import RecoveryRequiredError, ReleaseDeferredError, fingerprint
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
        self.stopping = False
        expected = {"mode": "app_schedule", "spec_digest": self.native.spec.digest, "local_start": False}
        saved = session.journal.get("managed_mode")
        if saved is not None and saved != expected:
            raise SecurityError("managed policy changed across process replacement")
        session.journal.put("managed_mode", expected)
        self.requests = ManagedRequests(session.journal)
        self.pause_task: asyncio.Task[Any] | None = None
        self.latest_native: HandoverEvidence | None = None
        self.schedule = ManagedSchedule(self)
        self.current = ManagedCurrentLimits(self)
        self.tests = ManagedChargeTests(self)

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
            self.session.gateway.controls._claim(True, stopping=True)

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
        await self.ensure_pause_zero()
        try:
            native = await self.native.read()
            self.native.require_known_session(native)
            self.session.book.owned_present(native.physical)
            controls = self.session.gateway.controls
            if not native.physical.transaction_open and controls.ready and controls.desired() == 0:
                self.require_paused(native)
                controls._complete(self.session.journal.get("control")["ticket"], uncertain=False)
                return
            await self.session.gateway.stop()
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError:
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
            receipt.get("stage") == "unsent"
            and receipt.get("settled_ticket") == control.get("ticket")
            or receipt.get("bound_ticket") == control.get("ticket")
            and j.db.execute(
                "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (receipt.get("local"),)
            ).fetchone()
        ):
            # Verified physical Pause reconciles this ticket's proven unsent Start or its real lifecycle closure.
            g.controls._complete(control["ticket"], uncertain=False)
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
    def fault(self, reason: str) -> None:
        journal = self.session.journal
        if not journal.get("managed_control_fault"):
            with journal.atomic():
                journal.put("managed_control_fault", {"classification": reason, "at_epoch": time.time()})
                control = journal.get("control", {})
                journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
                if journal.get("trial_phase") in ("active", "recovering", "blocked"):
                    journal.put("trial_phase", "blocked")
            self.session.gateway.controls.invalidate()

    # A recorded existing ceiling may be lowered to zero; unknown native transactions never receive a guessed Stop.
    async def hold_fault(self) -> None:
        g, j = self.session.gateway, self.session.journal
        row = j.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        if (
            row is None
            or row["state"] == "absent_verified"
            or g.charger.sender is None
            or not g.current_boot_seen(g._charger_epoch)
            or fingerprint(j.get("boot")) != self.native.spec.boot_digest
            or g.controls.ready
        ):
            return
        try:
            await g.controls.ensure_ceiling()
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError:
            return  # Attention remains latched; an unconfirmed safety cap is never reported as measured zero.

    # The present owned zero, live introduction and actual reconciled events are prerequisites to any release.
    def require_zero(
        self, native: HandoverEvidence, *, allow_offline: bool = False, continuing_local: int | None = None
    ) -> None:
        s, g, j = self.session, self.session.gateway, self.session.journal
        self.native.require(native, closed=continuing_local is None)
        if continuing_local is not None:
            require_continuing_session(j, native, continuing_local)
            if any(v != 0 for v in native.currents_a + native.powers_w):
                raise RecoveryRequiredError("the identified original session awaits a verified zero hold")
        self.native.require_known_session(native)
        present = s.book.owned_present(native.physical)
        row = j.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        require_lifecycle_idle(g, continuing_local=continuing_local)
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
            and j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or g.controls._operations
            or g.charger._pending
            or g.vendor._pending
            or j.get("uncertain_vendor_command")
        ):
            raise RecoveryRequiredError("shared control awaits fresh zero and actual history/cloud reconciliation")

    # Verify again after recovery acknowledgement; release raises only the ceiling and never sends RemoteStart.
    async def restore_owner_control(
        self, expected_intent: ControlIntent | None = None, *, continuing_local: int | None = None
    ) -> None:
        g, j = self.session.gateway, self.session.journal
        expected_intent = expected_intent or self.requests.intent()

        # This guard also runs before LocalControl claims and again inside each actual outgoing RPC gate.
        def guard() -> None:
            timer = j.get(KEY, {})
            if continuing_local is not None and (
                not current_source(g, timer)
                or self.schedule.needs_source(timer)
                or j.active() != continuing_local
                or timer.get("unidentified_local") != continuing_local
                or timer.get("other_local") != continuing_local
                or timer.get("unreported_native")
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
            await self.native.resume(continuing_local=continuing_local)
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
        release_guard = self.schedule.check_release if continuing_local is None else guard
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
                self.fault("owner_release_unconfirmed")
            raise
        finally:
            self.session.book.before_positive_release = None
        self.released_epoch = epoch
        j.put("managed_owner_epoch", epoch)

    # One accepted return receives one guarded attempt; uncertainty never changes persisted paused intent to app.
    async def return_requested(self, identifier: str, revision: int) -> None:
        expected = self.requests.intent()
        if expected.revision != revision:
            self.requests.finish(identifier, "failed", {"classification": "stale_revision"})
            return
        j = self.session.journal
        control = j.get("control", {})
        if control.get("start_pending") or (
            local_start_receipt(j).get("pause_revision") == expected.revision
            and j.db.execute(
                "SELECT 1 FROM managed_requests WHERE action='pause' AND state IN ('accepted','uncertain') LIMIT 1"
            ).fetchone()
        ):
            return  # The existing Pause must verify the delayed local lifecycle before this Return can run.
        j.db.execute("UPDATE managed_requests SET state='running' WHERE id=? AND state='accepted'", (identifier,))
        try:
            await self.restore_owner_control(expected)
        except ReleaseDeferredError:
            self.session.journal.db.execute(
                "UPDATE managed_requests SET state='accepted' WHERE id=? AND state='running'", (identifier,)
            )
            return
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError:
            self.requests.finish(identifier, "uncertain", {"classification": "return_not_confirmed"})
            return
        if self.requests.intent() != expected:
            self.requests.finish(identifier, "failed", {"classification": "superseded_by_pause"})
            return
        with self.session.journal.atomic():
            self.session.journal.put("managed_control_intent", {"mode": "app", "revision": revision + 1})
            self.requests.finish(identifier, "completed", {"app_control_enabled": True, "charging_started": False})

    # Wait for outgoing resource mutations rather than classifying an inventory sampled across a legitimate write.
    async def settled_native(self) -> HandoverEvidence:
        async with asyncio.timeout(15):
            while self.session._resource_mutation_inflight():
                await asyncio.sleep(0.02)
            native = await self.native.read()
            if self.session._resource_mutation_inflight():
                raise UnavailableError("managed native observation crossed a resource mutation")
            await self.schedule.await_closure_agreement(native)
            if (
                native.physical.transaction_open
                and native.physical.native_transaction_id != self.session.journal.active()
            ):
                lease = self.session.journal.get(KEY, {})
                if lease.get("state") in OBSERVABLE and lease.get("unreported_native"):
                    await self.schedule.protect_station(lease)
                    raise RecoveryRequiredError("timer holds zero awaiting the genuine transaction or native closure")
                raise SecurityError("managed native transaction has no exact accepted local counterpart")
            self.native.require_known_session(native)
            self.native.require_selected(native)
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
        await self.current.settle()
        await self.tests.settle()
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
            "desired_control": self.requests.intent().mode,
            "control_revision": self.requests.intent().revision,
            "local_hold_verified": False,
        }
        if self.stopping:
            return {**base, "state": "stopping", "owner_control_enabled": False}
        if j.get("managed_control_fault"):
            await self.hold_fault()
            return {**base, "state": "attention", "fault": j.get("managed_control_fault")["classification"]}
        timer = j.get(KEY, {})
        pending_timer = timer.get("state") in OBSERVABLE
        cutoff_hold = pending_timer and "hold_since" in timer
        if not base["charger_connected"] or not base["boot_verified"]:
            return {**base, "state": "starting", "owner_control_enabled": False}
        if self.requests.intent().mode == "paused" and (self.pause_task is None or self.pause_task.done()):
            await self.ensure_pause_zero()
        if (
            not g.controls.ready
            and self.released_epoch != g._charger_epoch
            and not cutoff_hold
            and not (pending_timer and j.active() is not None)
        ):
            if pending_timer:
                try:
                    await self.schedule.retry_recovery_zero()
                except DeniedError, UnavailableError, RecoveryRequiredError:
                    pass
            return {**base, "state": "starting", "owner_control_enabled": False}
        verified_pause = False
        try:
            self.prepare_offline_pause()
            if pending_timer:
                await self.schedule.protect_deadline(timer)
                cutoff_hold = "hold_since" in j.get(KEY, {})
            pending_return = self.requests.pending_return()
            if self.requests.intent().mode == "paused" and pending_return is not None:
                await self.return_requested(*pending_return)
            if self.requests.intent().mode == "paused":
                await self.maintain_pause()
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
                if self.released_epoch != g._charger_epoch:
                    await self.restore_owner_control()
            native = await self.settled_native()
            await self.schedule.observe(native)
            if self.requests.intent().mode == "app" and self.released_epoch != g._charger_epoch:
                raise RecoveryRequiredError("timer retirement awaits fresh app-control restoration")
            if self.requests.intent().mode == "paused":
                self.require_paused(native)
                verified_pause = True
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
            # Keep exact diagnostic details private, including transient failures which recover before a minute check.
            j.put(
                "managed_native_failure",
                {"type": type(error).__name__, "detail": str(error)[:1024], "at_epoch": time.time()},
            )
            self.failures += 1
            if self.failures >= 3:
                lease = j.get(KEY, {})
                if lease.get("state") in OBSERVABLE and isinstance(
                    error, (NativeTransportUnavailableError, StaleEvidenceError)
                ):
                    # Telemetry can fail before the deadline. Hold at zero while waiting for a fresh read.
                    if not lease.get("retire_reason"):
                        lease = {**lease, "retire_reason": "native_observation_unavailable"}
                        j.put(KEY, lease)
                    await self.schedule.protect_station(lease)
                    return {
                        **base,
                        "state": "degraded",
                        "owner_control_enabled": False,
                        "fault": "native_observation_unavailable",
                    }
                self.fault("native_authority_or_preservation_failed")
            return {**base, "state": "degraded", "fault": "native_authority_or_preservation_failed"}
        self.failures = 0
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
        shared = control.get("owner") == "shared" and not control.get("inhibit") and not control.get("uncertain")
        paused = self.requests.intent().mode == "paused"
        if not (shared or verified_pause) or s.runtime.status.delivery_blocked or j.get("uncertain_vendor_command"):
            if not self.stopping:
                self.fault("owner_control_or_delivery_uncertain")
        shared = shared and not j.get("managed_control_fault") and not self.stopping
        verified_pause = verified_pause and not j.get("managed_control_fault") and not self.stopping
        cloud = (
            s.runtime.status.vendor_connected and g.vendor.sender is not None and g.vendor_registration == "Accepted"
        )
        local_healthy = (shared or verified_pause) and j.get("trial_phase") == "active" and not control.get("uncertain")
        healthy = local_healthy and cloud
        if healthy and not paused:
            pending_return = self.requests.pending_return()
            if pending_return is not None:
                self.requests.finish(
                    pending_return[0], "completed", {"app_control_enabled": True, "charging_started": False}
                )
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
            "local_hold_verified": verified_pause,
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
            "charger_sessions": charger_history(native.history),
            "temporary_schedule": self.schedule.summary(),
            "preservation_failed": bool(j.get("trial_preservation_fault") or j.get("trial_inventory_fault")),
        }
