"""Explicit connected commissioning under an independently verified persistent native six-ampere limit."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from .gateway import Gateway
from .ics import is_safe_transaction_query
from .managed_current import managed_current_ceiling
from .managed_recovery import local_recovery_binding
from .managed_schedule import TimerVehicleDisconnectedError, installed_variant
from .native_bounded import NativeBoundedSpec
from .native_history import audit_history
from .rpc import UnavailableError
from .security import SecurityError
from .station import StationChoice
from .store import RecoveryRequiredError, fingerprint
from .trial_allocation import initialize_allocation, verify_initial_history
from .trial_evidence import profile_projection
from .trial_handover import ObserveHandover
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook
from .trial_rollback import require_lifecycle_idle

NativePatch = Callable[[dict[str, Any]], Awaitable[None]]


class NativeBoundedHandover:
    """This mode permits bounded charging; it never infers vehicle pause or manufactures transaction closure."""

    # The explicit immutable specification and ordinary ledger bind this mode independently of vehicle permission.
    def __init__(
        self,
        gateway: Gateway,
        book: ResourceBook,
        observe: ObserveHandover,
        spec: NativeBoundedSpec,
        choice: StationChoice,
        *,
        allow_unplugged: bool = False,
    ) -> None:
        if type(allow_unplugged) is not bool or allow_unplugged and gateway.policy.allow_local_start:
            raise SecurityError("unplugged native monitoring requires disabled local Start")
        self.gateway, self.book, self.observe, self.spec, self.choice = gateway, book, observe, spec, choice
        self.allow_unplugged = allow_unplugged
        self.lock = asyncio.Lock()
        self.last_verified_at = 0.0
        self.introduced_epoch: int | None = None
        self.introduced_revision: int | None = None
        if fingerprint(spec.original_configuration) != book.baseline.configuration_digest:
            raise SecurityError("native bounded mode requires the exact original full configuration")
        saved = gateway.journal.get("native_bounded_handover")
        if saved and saved.get("spec") != spec.value():
            raise SecurityError("native bounded specification changed across process replacement")
        if not saved and gateway.journal.get("trial_phase") != "prepared":
            raise RecoveryRequiredError("an existing ordinary journal cannot acquire connected admission")

    # Report an observation's age honestly rather than treating a persisted desired limit as a live guarantee.
    def status(self) -> dict[str, Any]:
        state = self.gateway.journal.get("native_bounded_handover", {})
        return {
            "configured": True,
            "stage": state.get("stage", "not_admitted"),
            "target_native_limit_a": managed_current_ceiling(self.gateway.journal, self.spec),
            "native_limit_freshly_verified": 0 <= time.time() - self.last_verified_at <= 5,
            "vehicle_pause_asserted": False,
            "restoration_pending": state.get("stage") != "complete",
            "complete": state.get("stage") == "complete",
            "fault": state.get("fault"),
        }

    # Save each stage before its external effect; restart never turns an old observation into fresh authority.
    def save(self, stage: str, **values: Any) -> None:
        journal = self.gateway.journal
        state = journal.get("native_bounded_handover", {})
        journal.put(
            "native_bounded_handover",
            {
                **state,
                **values,
                "stage": stage,
                "spec": self.spec.value(),
                "generation": state.get("generation", 0) + 1,
            },
        )

    # Local changes invalidate a native read even when the charger returned a complete, individually valid body.
    def authority(self) -> str:
        gateway, journal = self.gateway, self.gateway.journal
        return fingerprint(
            {
                "epoch": gateway._charger_epoch,
                "revision": gateway.controls._revision,
                "phase": journal.get("trial_phase"),
                "control": journal.get("control"),
                "active": journal.active(),
                "boot": journal.get("boot"),
                "binding": journal.get("trial_binding"),
                "grant": journal.get("vendor_start_grant"),
                "state": journal.get("native_bounded_handover"),
                "current_capability": journal.get("managed_current_capability"),
                "temporary_schedule": journal.get("managed_temporary_schedule"),
                "resources": [tuple(row) for row in journal.db.execute("SELECT * FROM trial_resources ORDER BY key")],
            }
        )

    # Read current rich native evidence and preserve every original history body without adopting later sessions.
    async def read(self, route: str = "trial", closed: bool = False) -> HandoverEvidence:
        before = self.authority()
        try:
            native = await self.observe()
            if before != self.authority():
                raise UnavailableError("native bounded audit was superseded by local authority")
            self.require(native, route=route, closed=closed)
            audit_history(
                self.gateway.journal, self.spec.digest, list(native.history), self.gateway.policy.allow_local_start
            )
            self.book.check_originals(native.physical)
        except SecurityError, UnavailableError, TimeoutError:
            self.last_verified_at = 0
            raise
        self.last_verified_at = native.physical.observed_at
        return native

    # The explicit managed capability broadens only its native ceiling; selected-current enforcement stays independent.
    def require(self, native: HandoverEvidence, *, route: str = "trial", closed: bool = False) -> None:
        j = self.gateway.journal
        if managed_current_ceiling(j, self.spec) == 32:
            if route != "trial":
                raise SecurityError("managed current capability must be retired before backend restoration")
            self.spec.require_managed(
                native,
                j.identity,
                closed=closed,
                allow_unplugged=self.allow_unplugged,
                temporary_schedule_digest=installed_variant(
                    j, self.spec.saved_schedule_digest, native.saved_schedule_digest
                ),
            )
        else:
            self.spec.require(native, j.identity, route, closed=closed, allow_unplugged=self.allow_unplugged)

    # Measured selected-limit enforcement gates health and positive work, while absolute-bound reads still permit Stop.
    def require_selected(self, native: HandoverEvidence) -> None:
        j = self.gateway.journal
        if managed_current_ceiling(j, self.spec) == 32:
            selected = self.gateway.controls.policy.max_current_a
            selected = j.get("control", {}).get("resume_limit", selected)
            if type(selected) not in (int, float) or not 6 <= selected <= 32:
                raise SecurityError("managed selected current is malformed")
            if max(native.currents_a) > selected + 0.5 or max(native.powers_w) > min(8500, selected * 270 + 100):
                raise SecurityError("actual native power exceeds the selected managed current")

    # Only a copy of the already validated USER scalar is normalized; original admission bytes are never rewritten.
    def binding_digest(self, native: HandoverEvidence) -> str:
        if managed_current_ceiling(self.gateway.journal, self.spec) == 32:
            return fingerprint(native.configuration | {"AmpLmt": "6"})
        return native.physical.configuration_digest

    # Initial connected admission has its own guard; allocation is reserved before adding any mode metadata.
    async def admit(self) -> None:
        journal = self.gateway.journal
        if journal.get("trial_phase") != "prepared":
            raise RecoveryRequiredError("native bounded admission requires a fresh prepared journal")
        native = await self.observe()
        self.spec.require_admission(native, journal.identity, route="original")
        self.book.require_profile_free_baseline()
        if native.physical.history is None:
            raise SecurityError("native bounded admission requires complete actual history")
        initialize_allocation(journal, native.physical.history, journal.identity, self.spec.hardware_identity)
        with journal.atomic():
            self.book.activate_bounded(native, self.spec.hardware_identity)
            self.save("admitted", original_history=list(native.history))
            journal.put("control", {"owner": "local", "inhibit": True, "uncertain": False})
            journal.put("trial_starting", True)

    # Bind only an actual fresh Boot after routing under six amps; reconnects return to a zero hold.
    async def introduce(self) -> HandoverEvidence:
        gateway, journal = self.gateway, self.gateway.journal
        epoch = gateway._charger_epoch
        if journal.get("trial_phase") not in ("active", "recovering", "blocked"):
            raise RecoveryRequiredError("native bounded introduction cannot rearm frozen cleanup")
        if not gateway.current_boot_seen(epoch) or fingerprint(journal.get("boot")) != self.spec.boot_digest:
            raise SecurityError("native bounded introduction requires the commissioned actual Boot")
        binding = journal.get("trial_binding")
        native = await self.read(closed=binding is None)
        gateway._require_epoch("charger", epoch)
        if binding is None:
            if (
                journal.get("trial_phase") != "active"
                or journal.db.execute("SELECT 1 FROM trial_resources LIMIT 1").fetchone()
            ):
                raise RecoveryRequiredError("initial native bounded introduction cannot adopt existing resources")
            if native.physical.history is None:
                raise SecurityError("actual Boot introduction requires complete current history")
            verify_initial_history(journal, native.physical.history, journal.identity, self.spec.hardware_identity)
            journal.put(
                "trial_binding",
                {
                    "configuration_digest": self.binding_digest(native),
                    "station": self.choice.value(),
                    "boot_digest": self.spec.boot_digest,
                },
            )
        elif binding != {
            "configuration_digest": self.binding_digest(native),
            "station": self.choice.value(),
            "boot_digest": self.spec.boot_digest,
        }:
            raise SecurityError("native bounded introduction binding changed")
        if binding is None:
            self.require_known_session(native)
        else:
            self.require_recovery_session(native)
        self.book.owned_present(native.physical)
        if binding is not None or journal.get("control", {}).get("uncertain"):
            control = journal.get("control", {})
            journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
            if binding is None:
                # A first zero needs its ordinary write-ahead receipt before existing-resource recovery can own it.
                await gateway.controls.ensure_ceiling()
                gateway._require_epoch("charger", epoch)
            journal.put("trial_phase", "recovering")
        self.introduced_epoch = epoch
        journal.put("trial_starting", False)
        if binding is not None and native.physical.transaction_open:
            await gateway.stop()
        self.introduced_revision = gateway.controls._revision
        return native

    # An unknown native transaction is external activity, never a proxy-created Start or inferred Stop.
    def require_known_session(self, native: HandoverEvidence) -> None:
        known = self.gateway.journal.active()
        if native.physical.transaction_open:
            if known is None or native.physical.native_transaction_id != known:
                raise RecoveryRequiredError("bounded external native activity requires actual lifecycle reconciliation")
        elif native.physical.native_transaction_id is not None or known is not None:
            raise RecoveryRequiredError("native and local transaction closure disagree")

    # A genuine Stop may have occurred while the process was absent; retain the ledger debt until its real replay.
    def require_recovery_session(self, native: HandoverEvidence) -> None:
        if native.physical.transaction_open:
            self.require_known_session(native)
        elif native.physical.native_transaction_id is not None:
            raise RecoveryRequiredError("restricted recovery cannot adopt contradictory native session evidence")

    # Default recovery requires an empty queue; managed local recovery retains queued observations and the zero hold.
    async def resume(self, *, local_only: bool = False) -> dict[str, Any]:
        if type(local_only) is not bool:
            raise SecurityError("local recovery permission must be explicit")
        async with self.lock:
            gateway, journal = self.gateway, self.gateway.journal
            local_binding = local_recovery_binding(gateway, self.spec.digest) if local_only else None
            epoch, revision = gateway._charger_epoch, gateway.controls._revision
            ticket = journal.get("control", {}).get("ticket")
            if (
                journal.get("trial_phase") != "recovering"
                or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
                or gateway.charger.sender is None
                or not gateway.current_boot_seen(epoch)
                or self.introduced_epoch != epoch
                or self.introduced_revision != revision
            ):
                raise RecoveryRequiredError("bounded resume requires the current restricted recovery introduction")
            native = await self.read(closed=True)
            gateway._require_epoch("charger", epoch)
            self.spec.require_boot(journal.get("boot"))
            if journal.get("trial_binding") != {
                "configuration_digest": self.binding_digest(native),
                "station": self.choice.value(),
                "boot_digest": self.spec.boot_digest,
            }:
                raise SecurityError("bounded recovery binding changed before acknowledgement")
            present = self.book.owned_present(native.physical)
            receipt = journal.db.execute(
                "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
            ).fetchone()
            station = [r for r in present if r.identifier == HUB_PROFILE_ID]
            control = journal.get("control", {})
            require_lifecycle_idle(gateway)
            if (
                journal.get("trial_phase") != "recovering"
                or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
                or revision != gateway.controls._revision
                or ticket != control.get("ticket")
                or not gateway.current_boot_seen(epoch)
                or not control.get("inhibit")
                or gateway.controls.desired() != 0
                or not gateway.controls.ready
                or receipt is None
                or receipt["state"] != "confirmed"
                or len(station) != 1
                or station[0].canonical.get("limit") != 0
                or receipt["confirmed"] is None
                or json.loads(receipt["confirmed"]) != station[0].canonical
                or not local_only
                and journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
                or gateway.controls._operations
                or gateway.charger._pending
                or gateway.vendor._pending
                or journal.get("uncertain_vendor_command")
            ):
                raise RecoveryRequiredError("bounded recovery needs confirmed zero and actual history reconciliation")
            if local_only and local_recovery_binding(gateway, self.spec.digest) != local_binding:
                raise RecoveryRequiredError("local recovery intent changed during native verification")
            journal.require_allocation()
            with journal.atomic():
                journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": False})
                journal.put("trial_phase", "active")
            return {"recovery_acknowledged": True, "zero_hold_retained": True, "charging_resumed": False}

    # This check runs inside the actual outgoing RPC gate, after waits and immediately before the send guard.
    async def validate_send(self, action: str, payload: dict[str, Any]) -> None:
        if action == "DataTransfer" and is_safe_transaction_query(payload):
            return
        if action in ("GetConfiguration", "GetCompositeSchedule", "TriggerMessage", "RemoteStopTransaction"):
            return
        zero = action == "SetChargingProfile" and profile_projection(payload)["limit"] == 0
        if zero:
            return  # Existing ledger guards still forbid recreating a station profile after verified absence.
        selecting = self.gateway.journal.get("managed_current_change", {}).get("state") == "pending"
        timer = (
            action == "DataTransfer"
            and payload.get("vendorId") == "IoCharger"
            and payload.get("messageId") in ("setPlan", "clearPlans")
        )
        native = await self.read(
            closed=timer or action == "ClearChargingProfile" or selecting and action == "SetChargingProfile"
        )
        self.require_selected(native)
        if timer:
            if payload["messageId"] == "setPlan" and native.physical.connection != "Plug":
                raise TimerVehicleDisconnectedError("temporary timer installation requires the connected vehicle")
            lease = self.gateway.journal.get("managed_temporary_schedule", {})
            expected = (
                lease.get("original_digest") if payload["messageId"] == "setPlan" else lease.get("augmented_digest")
            )
            if native.saved_schedule_digest != expected:
                raise SecurityError("temporary timer write requires its exact native preimage")
            self.book.owned_present(native.physical)
        if action == "ClearChargingProfile":
            require_lifecycle_idle(self.gateway)
            present = self.book.owned_present(native.physical)
            if not present or set(payload) != {"id"} or present[0].identifier != payload["id"]:
                raise SecurityError("native bounded cleanup requires the exact next owned resource")
        else:
            self.require_known_session(native)
            if (
                self.gateway.journal.get("trial_phase") != "active"
                or self.gateway.journal.get("trial_starting")
                or self.gateway.journal.get("native_bounded_handover", {}).get("stage") != "admitted"
            ):
                raise RecoveryRequiredError("native bounded positive controls remain frozen")

    # Stop actual known work, drain real lifecycle delivery, then clear exact owned profiles with station last.
    async def cleanup(self, quiesce: Callable[[], Awaitable[None]]) -> dict[str, Any]:
        if managed_current_ceiling(self.gateway.journal, self.spec) != 6:
            raise SecurityError("retire the managed current capability before legacy cleanup")
        async with self.lock:
            gateway, journal = self.gateway, self.gateway.journal
            if journal.get("trial_phase") == "backend_restore_pending":
                await self.read(closed=True)
                return {"profiles_restored": True, "native_limit_a": 6, "complete": False}
            if journal.get("trial_phase") not in ("active", "recovering", "blocked", "rollback"):
                raise RecoveryRequiredError("native bounded cleanup is not available in this phase")
            if journal.get("trial_phase") != "rollback":
                self.save("closing")
                journal.put("trial_phase", "blocked")
                initial = await self.read()
                present = self.book.owned_present(initial.physical)
                self.require_known_session(initial)
                if present or initial.physical.transaction_open or journal.get("control", {}).get("start_pending"):
                    await gateway.stop()
                async with asyncio.timeout(30):
                    while True:
                        native = await self.read()
                        self.require_known_session(native)
                        if not native.physical.transaction_open:
                            try:
                                require_lifecycle_idle(gateway)
                                break
                            except RecoveryRequiredError:
                                pass
                        await asyncio.sleep(0.1)
            native = await self.read(closed=True)
            require_lifecycle_idle(gateway)
            self.book.owned_present(native.physical)
            journal.put("trial_phase", "rollback")
            self.save("clearing")
            gateway.controls.invalidate()
            await quiesce()
            epoch = gateway._charger_epoch
            async with gateway.charger.sequence(0, lambda: gateway._require_epoch("charger", epoch)) as call:
                while True:
                    native = await self.read(closed=True)
                    require_lifecycle_idle(gateway)
                    present = self.book.owned_present(native.physical)
                    if not present:
                        break
                    resource = present[0]
                    if resource.kind != "profile":
                        raise SecurityError("native bounded cleanup cannot automatically delete native plans")
                    self.save("clearing", clear_intent={"key": resource.key, "digest": resource.digest})
                    try:
                        answer = await call(
                            "ClearChargingProfile", commands.clear_charging_profile(resource.identifier)
                        )
                        if answer[0] != 3 or answer[2].get("status") not in ("Accepted", "Unknown"):
                            raise UnavailableError("native bounded cleanup was not acknowledged")
                    except UnavailableError:
                        # Only independently verified absence resolves a lost reply; never infer a command retry.
                        pass
                    verification = await self.read(closed=True)
                    if any(row.key == resource.key for row in self.book.owned_present(verification.physical)):
                        raise UnavailableError("native bounded cleanup has not established physical profile absence")
            self.save("awaiting_backend", clear_intent={})
            journal.put("trial_phase", "backend_restore_pending")
            return {"profiles_restored": True, "native_limit_a": 6, "complete": False}

    # A route intent is durable and narrowly scoped; a native adapter performs only this exact four-field patch.
    async def restore_route(self, patch: NativePatch, retire: Callable[[], Awaitable[None]]) -> None:
        journal = self.gateway.journal
        if journal.get("trial_phase") != "backend_restore_pending":
            raise RecoveryRequiredError("native bounded resources are not yet restored")
        state = journal.get("native_bounded_handover", {})
        current = await self.observe()
        if state.get("stage") == "route_intent" and current.configuration == self.spec.configuration("original"):
            await self.read(route="original", closed=True)
            self.save("awaiting_backend", route_restored=True)
            return
        native = await self.read(closed=True)
        require_lifecycle_idle(self.gateway)
        if self.book.owned_present(native.physical):
            raise SecurityError("native bounded resources remain installed")
        self.save("route_intent")
        await retire()
        disconnected = await self.read(closed=True)
        if disconnected.physical.upstream_connected:
            raise UnavailableError("retired proxy's native connection must be observed offline before routing")
        self.save("route_intent", backend_outage_at=disconnected.physical.observed_at)
        await patch({key: self.spec.original_configuration[key] for key in self.spec.trial_route})
        await self.read(route="original", closed=True)
        self.save("awaiting_backend", route_restored=True)

    # Reconnection requires an actual fresh offline view followed by online, after the original route was restored.
    async def observe_backend(self) -> dict[str, Any]:
        journal = self.gateway.journal
        state = journal.get("native_bounded_handover", {})
        if journal.get("trial_phase") != "backend_restore_pending" or not state.get("route_restored"):
            raise RecoveryRequiredError("native bounded original route has not been independently verified")
        if self.gateway.charger.sender is not None:
            raise SecurityError("proxy transport remains attached during original-backend verification")
        native = await self.read(route="original")
        if not native.physical.upstream_connected:
            self.save("awaiting_backend", backend_outage_at=native.physical.observed_at)
            return {"original_backend_connected": False, "native_limit_a": 6, "complete": False}
        if state.get("backend_outage_at") is None or native.physical.observed_at <= state["backend_outage_at"]:
            raise UnavailableError("original backend needs a fresh observed offline-to-online transition")
        self.save("backend_verified", backend_verified_at=native.physical.observed_at)
        return {"original_backend_connected": True, "native_limit_a": 6, "complete": False}

    # Restore the normal current limit only after independent native closure and original-backend verification.
    async def restore_settings(self, patch: NativePatch) -> dict[str, Any]:
        journal = self.gateway.journal
        state = journal.get("native_bounded_handover", {})
        if state.get("stage") not in ("backend_verified", "settings_intent"):
            raise RecoveryRequiredError("native bounded original backend has not been verified")
        if self.gateway.charger.sender is not None:
            raise SecurityError("proxy is still attached during current-limit restoration")
        native = await self.observe()
        # A lost write reply is reconciled from actual full native state before any possible resend.
        if native.configuration == self.spec.original_configuration and state.get("stage") != "settings_intent":
            raise RecoveryRequiredError("normal current settings changed without the recorded restoration intent")
        if native.configuration != self.spec.original_configuration:
            idle = await self.read(route="original", closed=True)
            self.require_restore_idle(idle)
            self.save("settings_intent")
            idle = await self.read(route="original", closed=True)
            self.require_restore_idle(idle)
            await patch({"AmpLmt": self.spec.original_configuration["AmpLmt"]})
            native = await self.observe()
        self.require_normal_restoration(native)
        require_lifecycle_idle(self.gateway)
        with journal.atomic():
            self.save("complete", clear_intent={}, fault=None)
            journal.put("trial_phase", "complete")
        self.last_verified_at = 0
        return {"original_configuration_restored": True, "native_backend_connected": True, "complete": True}

    # No unknown native profile or newly active session may release the independent limit during restoration.
    def require_restore_idle(self, native: HandoverEvidence) -> None:
        self.spec.require(native, self.gateway.journal.identity, "original", closed=True)
        require_lifecycle_idle(self.gateway)
        if not native.physical.upstream_connected:
            raise UnavailableError("original backend disconnected before native current restoration")
        originals = {resource.key for resource in self.book.baseline.resources}
        if any(resource.kind == "profile" or resource.key not in originals for resource in native.physical.resources):
            raise SecurityError("original backend has unreconciled resources before native current restoration")

    # Ordinary manufacturer charging may begin after the authorized restore; retain its real session and history.
    def require_normal_restoration(self, native: HandoverEvidence) -> None:
        journal = self.gateway.journal
        self.spec.require_normal(native, journal.identity)
        if not native.physical.upstream_connected:
            raise SecurityError("normal manufacturer settings and native connection remain unverified")
        self.book.check_originals(native.physical)
        original = journal.get("native_bounded_handover", {}).get("original_history", [])
        audit_history(journal, self.spec.digest, list(native.history), self.gateway.policy.allow_local_start)
        owned = {row[0] for row in journal.db.execute("SELECT key FROM trial_resources")}
        original_keys = {resource.key for resource in self.book.baseline.resources}
        original_ids = {row["transactionId"] for row in original}
        local_ids = {row[0] for row in journal.db.execute("SELECT local FROM transactions")}
        ordinary_ids = {native.physical.native_transaction_id} if native.physical.transaction_open else set()
        ordinary_ids.update(
            row["transactionId"]
            for row in native.history
            if row["transactionId"] not in original_ids | local_ids
            and type(row.get("startTime")) is int
            and type(row.get("stopTime")) is int
            and row["stopTime"] > row["startTime"] > 0
            and row.get("connectorId") == 1
        )
        for resource in native.physical.resources:
            if resource.key in owned:
                raise SecurityError("an owned trial profile reappeared after restoration")
            if resource.key in original_keys:
                continue
            if resource.kind != "profile" or (
                resource.canonical.get("purpose") != "TxProfile"
                or resource.canonical.get("transaction") not in ordinary_ids
                or resource.canonical.get("connector") != 1
                or resource.canonical.get("kind") != "Relative"
                or resource.canonical.get("stack") != 1
                or resource.canonical.get("unit") != "A"
                or resource.canonical.get("limit", 100) > 32
                or resource.canonical.get("phases") != 1
            ):
                raise SecurityError("normal restoration contains an unexplained native profile")
