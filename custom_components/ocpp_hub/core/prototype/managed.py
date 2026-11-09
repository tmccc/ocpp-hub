"""Persistent app/schedule supervision without assistant-initiated Start or fabricated recovery outcomes."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .policy import DeniedError
from .rpc import UnavailableError
from .security import SecurityError
from .store import RecoveryRequiredError, fingerprint
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
    def require_zero(self, native: HandoverEvidence) -> None:
        s, g, j = self.session, self.session.gateway, self.session.journal
        self.native.spec.require(native, j.identity, "trial", closed=True)
        self.native.require_known_session(native)
        present = s.book.owned_present(native.physical)
        row = j.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        require_lifecycle_idle(g)
        if (
            j.get("trial_phase") not in ("active", "recovering")
            or j.get("native_bounded_handover", {}).get("stage") != "admitted"
            or j.get("managed_control_fault")
            or not g.current_boot_seen(g._charger_epoch)
            or self.native.introduced_epoch != g._charger_epoch
            or self.native.introduced_revision != g.controls._revision
            or g.charger.sender is None
            or g.vendor.sender is None
            or g.vendor_registration != "Accepted"
            or not s.runtime.status.vendor_connected
            or not g.controls.ready
            or g.controls.desired() != 0
            or not j.get("control", {}).get("inhibit")
            or row is None
            or row["state"] != "confirmed"
            or not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present)
            or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or g.controls._operations
            or g.charger._pending
            or g.vendor._pending
            or j.get("uncertain_vendor_command")
        ):
            raise RecoveryRequiredError("shared control awaits fresh zero and actual history/cloud reconciliation")

    # Verify again after recovery acknowledgement; release raises only the ceiling and never sends RemoteStart.
    async def restore_owner_control(self) -> None:
        g, j = self.session.gateway, self.session.journal
        if self.stopping:
            raise UnavailableError("managed shutdown retains its hold")
        epoch, revision = g._charger_epoch, g.controls._revision
        native = await self.native.read(closed=True)
        self.require_zero(native)
        if j.get("trial_phase") == "recovering":
            await self.native.resume()
        ticket = j.get("control", {}).get("ticket")
        native = await self.native.read(closed=True)
        self.require_zero(native)
        if (
            self.stopping
            or epoch != g._charger_epoch
            or revision != g.controls._revision
            or ticket != j.get("control", {}).get("ticket")
            or j.get("trial_phase") != "active"
            or j.get("control", {}).get("uncertain")
        ):
            raise UnavailableError("managed release authority changed during its native check")
        try:
            await g.release()
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError:
            if not self.stopping:
                self.fault("owner_release_unconfirmed")
            raise
        self.released_epoch = epoch
        j.put("managed_owner_epoch", epoch)

    # Wait for outgoing resource mutations rather than classifying an inventory sampled across a legitimate write.
    async def settled_native(self) -> HandoverEvidence:
        async with asyncio.timeout(15):
            while self.session._resource_mutation_inflight():
                await asyncio.sleep(0.02)
            native = await self.native.read()
            if self.session._resource_mutation_inflight():
                raise UnavailableError("managed native observation crossed a resource mutation")
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
            return native

    # Health is measured from a fresh native view and known session; a legitimate owner charge remains healthy.
    async def tick(self) -> dict[str, Any]:
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
        }
        if self.stopping:
            return {**base, "state": "stopping", "owner_control_enabled": False}
        if j.get("managed_control_fault"):
            await self.hold_fault()
            return {**base, "state": "attention", "fault": j.get("managed_control_fault")["classification"]}
        if (
            not base["charger_connected"]
            or not base["boot_verified"]
            or not g.controls.ready
            and self.released_epoch != g._charger_epoch
        ):
            return {**base, "state": "starting", "owner_control_enabled": False}
        try:
            if self.released_epoch != g._charger_epoch:
                if not base["cloud_connected"]:
                    return {**base, "state": "waiting_for_cloud", "owner_control_enabled": False}
                await self.restore_owner_control()
            native = await self.settled_native()
        except UnavailableError, TimeoutError:
            return {
                **base,
                "state": "stopping" if self.stopping else "degraded",
                "owner_control_enabled": False,
                "fault": "native_observation_unavailable",
            }
        except RecoveryRequiredError:
            return {**base, "state": "reconciling", "owner_control_enabled": False}
        except SecurityError, DeniedError:
            if self.stopping:
                return {**base, "state": "stopping", "owner_control_enabled": False}
            self.failures += 1
            if self.failures >= 3:
                self.fault("native_authority_or_preservation_failed")
            return {**base, "state": "degraded", "fault": "native_authority_or_preservation_failed"}
        self.failures = 0
        control = j.get("control", {})
        shared = control.get("owner") == "shared" and not control.get("inhibit") and not control.get("uncertain")
        if not shared or s.runtime.status.delivery_blocked or j.get("uncertain_vendor_command"):
            if not self.stopping:
                self.fault("owner_control_or_delivery_uncertain")
        shared = shared and not j.get("managed_control_fault") and not self.stopping
        cloud = (
            s.runtime.status.vendor_connected and g.vendor.sender is not None and g.vendor_registration == "Accepted"
        )
        healthy = shared and cloud
        return {
            **base,
            "checked_at": time.time(),
            "phase": j.get("trial_phase"),
            "cloud_connected": cloud,
            "state": "stopping" if self.stopping else "healthy" if healthy else "degraded",
            "owner_control_enabled": shared,
            "native_observed_at": native.physical.observed_at,
            "native_limit_a": native.runtime_user_a,
            "station_ceiling_a": g.controls.desired(),
            "current_a": native.physical.current_a,
            "power_w": native.physical.power_w,
            "charging_session_open": native.physical.transaction_open,
            "history_records": len(native.history),
            "preservation_failed": bool(j.get("trial_preservation_fault") or j.get("trial_inventory_fault")),
        }
