"""Separate connected cleanup: verified car pause remains in force until original settings are restored."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from .gateway import Gateway
from .policy import number
from .rpc import UnavailableError
from .security import SecurityError
from .store import RecoveryRequiredError, fingerprint
from .trial_evidence import Resource
from .trial_handover_evidence import (
    HandoverEvidence,
    PauseObservation,
    PauseProvider,
    PauseRequest,
    PermissionObservation,
    require_digest,
)
from .trial_resources import ResourceBook
from .trial_rollback import Rollback, require_lifecycle_idle

ObserveHandover = Callable[[], Awaitable[HandoverEvidence]]


class ConnectedHandover:
    """No configured live provider means no connected cleanup, even if an old physical test passed."""

    # Providers are explicit integration dependencies; there is no JSON-proof import or allow-connected switch.
    def __init__(
        self,
        gateway: Gateway,
        book: ResourceBook,
        observe: ObserveHandover,
        hardware: str,
        provider: PauseProvider | None = None,
        vehicle_digest: str | None = None,
        original_configuration: dict[str, Any] | None = None,
    ) -> None:
        self.gateway, self.book, self.observe, self.hardware = gateway, book, observe, hardware
        self.provider, self.vehicle_digest, self.original = provider, vehicle_digest, original_configuration
        self.lock = asyncio.Lock()
        if provider is not None:
            require_digest(provider.digest)
            require_digest(vehicle_digest or "")
            if (
                original_configuration is None
                or fingerprint(original_configuration) != book.baseline.configuration_digest
            ):
                raise SecurityError("connected handover requires the unchanged original full configuration")
            if (
                original_configuration.get("Id") != gateway.journal.identity
                or original_configuration.get("bPSN") != hardware
            ):
                raise SecurityError("handover configuration belongs to another charger")
            if original_configuration.get("AmpLmt") != "32" or original_configuration.get("maxOutCurrent0") != "32":
                raise SecurityError("this handover capability is restricted to the verified original32A station")

    # Status deliberately omits identities, revisions, nonce values, configuration and history bodies.
    def status(self) -> dict[str, Any]:
        state = self.gateway.journal.get("connected_handover", {})
        return {
            "provider_configured": self.provider is not None,
            "stage": state.get("stage", "not_started"),
            "complete": state.get("stage") == "complete",
            "automatic_backend_restore": False,
            "vehicle_permission_restore_confirmed": bool(state.get("vehicle_restoration")),
        }

    # Never mutate the journal or freeze an ordinary trial merely because an unconfigured command was attempted.
    def require_provider(self) -> PauseProvider:
        if self.provider is None or self.original is None or self.vehicle_digest is None:
            raise UnavailableError("connected handover requires an explicitly configured live vehicle-pause provider")
        pinned = self.gateway.journal.get("connected_handover", {}).get("provider_digest")
        if pinned is not None and self.provider.digest != pinned:
            raise SecurityError("handover provider identity changed")
        return self.provider

    # All state transitions participate in the native-observation authority fence and survive process replacement.
    def save(self, state: dict[str, Any]) -> None:
        state["generation"] = state.get("generation", 0) + 1
        self.gateway.journal.put("connected_handover", state)

    # Bind asynchronous native/provider reads to the current transport, Boot, ownership and durable handover stage.
    def authority(self) -> str:
        gateway, journal = self.gateway, self.gateway.journal
        return fingerprint(
            {
                "epoch": gateway._charger_epoch,
                "revision": gateway.controls._revision,
                "phase": journal.get("trial_phase"),
                "control": journal.get("control"),
                "active": journal.active(),
                "grant": journal.get("vendor_start_grant"),
                "binding": journal.get("trial_binding"),
                "boot": journal.get("boot"),
                "handover": journal.get("connected_handover"),
                "resources": [tuple(r) for r in journal.db.execute("SELECT * FROM trial_resources")],
            }
        )

    # Create a private nonce once; an existing handover cannot be rebound to another vehicle or provider.
    def begin(self) -> dict[str, Any]:
        provider = self.require_provider()
        journal = self.gateway.journal
        state = journal.get("connected_handover")
        if state is None:
            binding = journal.get("trial_binding")
            if not isinstance(binding, dict) or journal.get("trial_phase") not in ("active", "recovering", "blocked"):
                raise RecoveryRequiredError("connected cleanup requires an existing bound trial")
            state = {
                "stage": "checking_pause",
                "nonce": uuid.uuid4().hex,
                "baseline_digest": fingerprint(self.book.baseline.value()),
                "provider_digest": provider.digest,
                "vehicle_digest": self.vehicle_digest,
                "trial_configuration_digest": binding["configuration_digest"],
                "boot_digest": binding["boot_digest"],
                "history": None,
            }
            self.save(state)
        if (
            state["provider_digest"] != provider.digest
            or state["vehicle_digest"] != self.vehicle_digest
            or state["baseline_digest"] != fingerprint(self.book.baseline.value())
        ):
            raise SecurityError("connected handover cannot change its vehicle, provider or original baseline")
        return dict(state)

    # Apply only the recorded USER delta; every other config/history byte stays pinned to its stage's baseline.
    def check_native(self, native: HandoverEvidence, state: dict[str, Any], configuration: str, user_a: int) -> None:
        native.require_closed(self.gateway.journal.identity, self.hardware, user_a, 32)
        expected = (
            state["trial_configuration_digest"] if configuration == "trial" else self.book.baseline.configuration_digest
        )
        if fingerprint(native.configuration | {"AmpLmt": "32"}) != expected:
            raise SecurityError("unowned configuration changes block connected handover")
        if state["history"] is not None and (
            len(state["history"]) != len(native.history) or any(row not in native.history for row in state["history"])
        ):
            raise SecurityError("retained native history changed during connected handover")

    # Two actual native zero readings are separated by time, rather than duplicating one cached snapshot.
    async def stable_native(self, state: dict[str, Any], configuration: str, user_a: int) -> HandoverEvidence:
        first = await self.observe()
        self.check_native(first, state, configuration, user_a)
        await asyncio.sleep(2)
        second = await self.observe()
        self.check_native(second, state, configuration, user_a)
        if (
            first.content_digest != second.content_digest
            or second.physical.observed_at - first.physical.observed_at < 2
        ):
            raise UnavailableError("connected native evidence changed between zero readings")
        return second

    # Every irreversible step obtains a new provider read tied to the complete current native snapshot.
    async def protected(self, stage: str, configuration: str = "trial", user_a: int = 6) -> HandoverEvidence:
        provider = self.require_provider()
        state = self.begin()
        before = self.authority()
        async with asyncio.timeout(15):
            native = await self.stable_native(state, configuration, user_a)
            if before != self.authority():
                raise UnavailableError("handover native read belongs to retired authority")
            request = PauseRequest(
                fingerprint({k: state[k] for k in ("nonce", "baseline_digest", "vehicle_digest")}),
                uuid.uuid4().hex,
                stage,
                fingerprint(self.gateway.journal.identity),
                fingerprint(self.hardware),
                state["vehicle_digest"],
                state["baseline_digest"],
                native.digest,
                fingerprint(self.gateway.journal.get("boot")),
                self.gateway._charger_epoch,
            )
            proof = await provider.observe(request)
            verification = await self.observe()
            if before != self.authority():
                raise UnavailableError("handover pause read belongs to retired authority")
            self.check_native(verification, state, configuration, user_a)
            if native.content_digest != verification.content_digest:
                raise UnavailableError("native protection changed during the vehicle-pause read")
            native = verification
            if provider.digest != state["provider_digest"]:
                raise SecurityError("handover provider identity changed during observation")
            proof.require_current(request, state["provider_digest"])
            if request.boot_digest != state["boot_digest"]:
                raise SecurityError("charger introduction changed during the protected handover")
            self.pin_pause(state, proof)
            require_lifecycle_idle(self.gateway)
            if state["history"] is None:
                state["history"] = list(native.history)
            state["last_request"] = asdict(request)
            state["pause_expires_at"] = proof.expires_at
            state["pause_valid_until"] = proof.pause_valid_until
            state["pause_release_only"] = proof.release_only
            self.save(state)
            return native

    # A provider's live permission revision must remain the same owned pause throughout all restoration steps.
    def pin_pause(self, state: dict[str, Any], proof: PauseObservation) -> None:
        receipt = {
            "revision": proof.revision,
            "permission": proof.permission_digest,
            "original_revision": proof.original_revision,
            "original_permission": proof.original_permission_digest,
            "capability": proof.capability.digest,
        }
        if "pause" in state and state["pause"] != receipt:
            raise SecurityError("vehicle permission or capability changed; owner changes cannot be overwritten")
        state["pause"] = receipt

    # A pending earlier Release cannot change the zero station hold while cleanup waits for the RPC gate.
    def require_zero_station(self, present: list[Resource]) -> None:
        if present and not any(
            resource.identifier == HUB_PROFILE_ID and resource.canonical.get("limit") == 0 for resource in present
        ):
            raise SecurityError("connected cleanup requires the independently observed owned zero station hold")

    # Inspect without weakening the legacy unplugged plan or opening a new transport.
    async def plan(self) -> dict[str, Any]:
        async with self.lock:
            native = await self.protected("plan")
            present = self.book.owned_present(native.physical)
            self.require_zero_station(present)
            self.book.require_profile_free_baseline()
            return {
                "connected_handover_checked": True,
                "owned_profiles_to_clear": len(present),
                "independent_user_ceiling_a": 6,
                "fresh_pause_required_for_each_step": True,
                "automatic_backend_restore": False,
                "complete": False,
            }

    # Freeze first; retain USER6 and the vehicle pause while selectively removing only journal-owned resources.
    async def run(self, quiesce: Callable[[], Awaitable[None]]) -> dict[str, Any]:
        async with self.lock:
            self.require_provider()
            state = self.begin()
            if state["stage"] == "complete":
                raise RecoveryRequiredError("connected handover is already complete")
            if state["stage"] in ("backend_verified", "settings_verified", "vehicle_restore_intent"):
                raise RecoveryRequiredError("resource cleanup already finished; continue restoration checks")
            journal = self.gateway.journal
            if journal.get("trial_phase") not in ("rollback", "backend_restore_pending"):
                journal.put("trial_phase", "blocked")
            self.gateway.controls.invalidate()
            native = await self.protected("freeze")
            present = self.book.owned_present(native.physical)
            self.book.require_profile_free_baseline()
            self.require_zero_station(present)
            if present and (
                self.gateway.charger.sender is None or not self.gateway.current_boot_seen(self.gateway._charger_epoch)
            ):
                raise SecurityError("connected clears require the current authenticated charger Boot")
            journal.put("trial_phase", "rollback")
            await quiesce()
            native = await self.protected("quiesced")
            epoch = self.gateway._charger_epoch
            async with self.gateway.charger.sequence(0, lambda: self.gateway._require_epoch("charger", epoch)) as call:
                while True:
                    native = await self.protected("clear")
                    present = self.book.owned_present(native.physical)
                    if not present:
                        break
                    self.require_zero_station(present)
                    resource = present[0]
                    if resource.kind != "profile":
                        raise SecurityError("connected handover cannot remove native plans")
                    state = journal.get("connected_handover")
                    state.update(stage="clearing", pending={"key": resource.key, "digest": resource.digest})
                    self.save(state)
                    authority = self.authority()

                    # Recheck lease, native age and ownership generation after waiting for the actual RPC send gate.
                    def check() -> None:
                        self.gateway._require_epoch("charger", epoch)
                        if self.authority() != authority or time.time() + 1 >= state["pause_expires_at"]:
                            raise UnavailableError("connected clear lost its current pause or local authority")
                        native.require_closed(journal.identity, self.hardware, 6, 32)

                    check()
                    answer = await call("ClearChargingProfile", commands.clear_charging_profile(resource.identifier))
                    if answer[0] != 3 or answer[2].get("status") not in ("Accepted", "Unknown"):
                        raise UnavailableError("connected scoped clear was not confirmed")
                    verification = await self.protected("verify_clear")
                    if any(r.key == resource.key for r in self.book.owned_present(verification.physical)):
                        raise UnavailableError("scoped clear acknowledgement did not establish actual absence")
                    state = journal.get("connected_handover")
                    state["pending"] = None
                    self.save(state)
            state = journal.get("connected_handover")
            state.update(stage="awaiting_backend", pending=None)
            self.save(state)
            journal.put("trial_phase", "backend_restore_pending")
            return {
                "profiles_restored": True,
                "vehicle_pause_retained": True,
                "native_user_ceiling_a": 6,
                "original_backend_restore_required": True,
                "complete": False,
            }

    # The existing explicit connection-restoration step remains manual and must leave USER6 and the car hold intact.
    async def confirm_backend(self) -> dict[str, Any]:
        async with self.lock:
            state = self.begin()
            if state["stage"] not in ("awaiting_backend", "backend_verified"):
                raise RecoveryRequiredError("connected resource cleanup has not finished")
            if self.gateway.charger.sender is not None:
                raise SecurityError("proxy transport must be retired before native backend confirmation")
            native = await self.protected("confirm_backend", "original")
            if self.gateway.charger.sender is not None or not native.physical.upstream_connected:
                raise SecurityError("original backend is not independently connected")
            if self.book.owned_present(native.physical):
                raise SecurityError("owned trial profiles remain installed")
            state = self.gateway.journal.get("connected_handover")
            state["stage"] = "backend_verified"
            self.save(state)
            return {
                "original_backend_verified": True,
                "vehicle_pause_retained": True,
                "original_user_setting_restore_required": True,
                "complete": False,
            }

    # Original USER32 is confirmed only after the backend proof; the independent vehicle pause is still required.
    async def confirm_settings(self) -> dict[str, Any]:
        async with self.lock:
            state = self.begin()
            if state["stage"] not in ("backend_verified", "settings_verified", "vehicle_restore_intent"):
                raise RecoveryRequiredError("original backend must be verified before original USER settings")
            if self.gateway.charger.sender is not None:
                raise SecurityError("proxy transport is still attached")
            native = await self.protected("confirm_settings", "original", 32)
            if self.gateway.charger.sender is not None or not native.physical.upstream_connected:
                raise SecurityError("original backend verification was lost")
            if native.physical.configuration_digest != self.book.baseline.configuration_digest:
                raise SecurityError("full original configuration has not been restored")
            if self.book.owned_present(native.physical):
                raise SecurityError("trial profile absence is not verified")
            state = self.gateway.journal.get("connected_handover")
            if state["stage"] != "vehicle_restore_intent":
                state["stage"] = "settings_verified"
                self.save(state)
            return {"original_configuration_verified": True, "vehicle_pause_retained": True, "complete": False}

    # Restored permission may start normal charging; observe actual native facts without adopting its transactions.
    async def completion_native(self, state: dict[str, Any]) -> HandoverEvidence:
        before = self.authority()
        async with asyncio.timeout(15):
            native = await self.observe()
        if before != self.authority() or self.gateway.charger.sender is not None:
            raise UnavailableError("original-backend verification belongs to retired authority")
        physical = native.physical
        physical.require_fresh(self.gateway.journal.identity, 5)
        for current in native.currents_a:
            number(current, 32.5)
        for power in native.powers_w:
            number(power, 9600)
        number(native.output_limit_a, 32)
        if (
            native.hardware_digest != fingerprint(self.hardware)
            or physical.configuration_digest != self.book.baseline.configuration_digest
            or native.saved_user_a != 32
            or native.runtime_user_a != 32
            or native.runtime_hardware_a != 32
            or {"A": "Unplug", "B": "Plug", "C": "Charging"}.get(physical.cp) != physical.connection
            or len(native.currents_a) != 3
            or len(native.powers_w) != 4
            or any(value > 32.5 for value in native.currents_a)
            or any(value != 0 for value in native.currents_a[1:])
            or any(value > 9600 for value in native.powers_w)
            or native.output_limit_a > 32
            or physical.cp == "C"
            and not physical.transaction_open
            or physical.transaction_open != (physical.native_transaction_id is not None)
            or physical.cp != "C"
            and any(v != 0 for v in native.currents_a + native.powers_w)
            or physical.cp == "A"
            and physical.transaction_open
        ):
            raise SecurityError("normal native operation does not match the restored original station")
        if any(row not in native.history for row in state["history"]):
            raise SecurityError("original native history changed after vehicle permission restoration")
        self.book.check_originals(physical)
        owned = {row[0] for row in self.gateway.journal.db.execute("SELECT key FROM trial_resources")}
        if any(resource.key in owned for resource in physical.resources) or self.gateway.journal.get(
            "trial_inventory_fault"
        ):
            raise SecurityError("an owned trial resource reappeared after restoration")
        originals = {resource.key for resource in self.book.baseline.resources}
        old_history = {row["transactionId"] for row in state["history"]}
        normal_transactions = {physical.native_transaction_id} if physical.native_transaction_id is not None else set()
        for row in native.history:
            if (
                row["transactionId"] not in old_history
                and type(row.get("startTime")) is int
                and type(row.get("stopTime")) is int
                and row["stopTime"] >= row["startTime"] >= state["vehicle_operation_at"] - 2
            ):
                normal_transactions.add(row["transactionId"])
        for resource in physical.resources:
            if resource.key in originals:
                continue
            profile = resource.canonical
            if (
                resource.kind != "profile"
                or profile.get("purpose") != "TxProfile"
                or profile.get("connector") != 1
                or profile.get("stack") != 1
                or profile.get("kind") != "Relative"
                or profile.get("unit") != "A"
                or profile.get("phases") != 1
                or type(profile.get("transaction")) is not int
                or profile["transaction"] not in normal_transactions
            ):
                raise SecurityError("unrelated native resource appeared during final restoration")
            number(profile.get("limit"), 32)
        require_lifecycle_idle(self.gateway)
        if not physical.upstream_connected:
            raise SecurityError("native backend or trial-profile restoration is incomplete")
        return native

    # Reconcile a lost permission response through provider readback; never replay an unconfirmed vehicle Start.
    async def restore_vehicle(self) -> dict[str, Any]:
        async with self.lock:
            provider = self.require_provider()
            state = self.begin()
            if state["stage"] not in ("settings_verified", "vehicle_restore_intent"):
                raise RecoveryRequiredError("full original charger settings must precede vehicle restoration")
            result: PermissionObservation | None
            if state["stage"] == "settings_verified":
                native = await self.protected("before_vehicle_restore", "original", 32)
                if self.gateway.charger.sender is not None or not native.physical.upstream_connected:
                    raise SecurityError("original backend must remain connected before restoring the car")
                if self.book.owned_present(native.physical):
                    raise SecurityError("trial profiles remain installed")
                state = self.gateway.journal.get("connected_handover")
                request = PauseRequest(**state["last_request"])
                state.update(
                    stage="vehicle_restore_intent", vehicle_operation=uuid.uuid4().hex, vehicle_operation_at=time.time()
                )
                self.save(state)
                before = self.authority()
                async with asyncio.timeout(5):
                    result = await provider.restore(
                        request,
                        state["vehicle_operation"],
                        state["pause"]["revision"],
                        state["pause"]["original_permission"],
                    )
            else:
                # Outcome readback remains available even if normal charging started or native completion is blocked.
                before = self.authority()
                async with asyncio.timeout(15):
                    native = await self.observe()
                if before != self.authority():
                    raise UnavailableError("vehicle outcome read belongs to retired authority")
                request = PauseRequest(
                    fingerprint({k: state[k] for k in ("nonce", "baseline_digest", "vehicle_digest")}),
                    uuid.uuid4().hex,
                    "reconcile_vehicle_restore",
                    fingerprint(self.gateway.journal.identity),
                    fingerprint(self.hardware),
                    state["vehicle_digest"],
                    state["baseline_digest"],
                    native.digest,
                    fingerprint(self.gateway.journal.get("boot")),
                    self.gateway._charger_epoch,
                )
                before = self.authority()
                async with asyncio.timeout(5):
                    result = await provider.restoration_status(request, state["vehicle_operation"])
            if before != self.authority():
                raise UnavailableError("vehicle restoration response belongs to retired authority")
            if result is None:
                raise UnavailableError("vehicle restoration outcome remains unknown; the command was not repeated")
            if (
                result.request != request
                or result.provider_digest != state["provider_digest"]
                or provider.digest != state["provider_digest"]
                or result.operation != state["vehicle_operation"]
                or result.permission_digest != state["pause"]["original_permission"]
                or result.previous_revision != state["pause"]["revision"]
                or type(result.observed_at) not in (int, float)
                or not 0 <= time.time() - result.observed_at <= 5
                or not isinstance(result.revision, str)
                or not 1 <= len(result.revision) <= 128
            ):
                raise SecurityError("actual vehicle permission restoration was not independently confirmed")
            state["vehicle_restoration"] = asdict(result)
            self.save(state)
            completed = await self.completion_native(state)
            state["normal_native_transaction"] = completed.physical.native_transaction_id
            state["normal_native_resource_count"] = len(completed.physical.resources)
            state["normal_native_history_count"] = len(completed.history)
            with self.gateway.journal.atomic():
                state["stage"] = "complete"
                self.save(state)
                self.gateway.journal.put("trial_phase", "complete")
            return {
                "original_configuration_restored": True,
                "vehicle_permission_restored": True,
                "lead_may_remain_connected": True,
                "complete": True,
            }

    # Actual unplugged evidence remains a safe resource-cleanup fallback without silently releasing the car pause.
    async def fallback_native(self) -> HandoverEvidence:
        before = self.authority()
        async with asyncio.timeout(15):
            native = await self.observe()
        native.physical.require_unplugged(self.gateway.journal.identity, 5)
        if (
            before != self.authority()
            or self.gateway.charger.sender is not None
            or native.hardware_digest != fingerprint(self.hardware)
            or native.physical.configuration_digest != self.book.baseline.configuration_digest
            or native.saved_user_a != 32
            or native.runtime_user_a != 32
            or native.runtime_hardware_a != 32
            or native.output_limit_a != 0
            or not native.physical.upstream_connected
        ):
            raise SecurityError("unplugged fallback original configuration is not verified")
        require_lifecycle_idle(self.gateway)
        history = self.gateway.journal.get("connected_handover", {}).get("history")
        if history is not None and any(row not in native.history for row in history):
            raise SecurityError("retained native history changed during unplugged fallback")
        if self.book.owned_present(native.physical):
            raise SecurityError("unplugged fallback still has owned trial profiles")
        return native

    # Preserve a known pause obligation even when ordinary unplugged cleanup has safely restored the charger.
    async def confirm_unplugged_backend(self, rollback: Rollback) -> dict[str, Any]:
        async with self.lock:
            await rollback.verify_original_backend()
            await self.fallback_native()
            state = self.gateway.journal.get("connected_handover")
            pending = bool(state.get("pause") and not state.get("vehicle_restoration"))
            with self.gateway.journal.atomic():
                state["stage"] = "unplugged_vehicle_permission_pending" if pending else "complete"
                state["unplugged_fallback"] = True
                self.save(state)
                self.gateway.journal.put("trial_phase", "backend_restore_pending" if pending else "complete")
            return {
                "original_configuration_restored": True,
                "vehicle_permission_restore_required": pending,
                "complete": not pending,
            }

    # Read-only verification can discharge the obligation after an owner restores permission, without sending Start.
    async def confirm_unplugged_vehicle(self) -> dict[str, Any]:
        async with self.lock:
            provider = self.require_provider()
            state = self.gateway.journal.get("connected_handover", {})
            if state.get("stage") != "unplugged_vehicle_permission_pending":
                raise RecoveryRequiredError("no unplugged vehicle-permission obligation is pending")
            native = await self.fallback_native()
            request = PauseRequest(
                fingerprint({k: state[k] for k in ("nonce", "baseline_digest", "vehicle_digest")}),
                uuid.uuid4().hex,
                "unplugged_vehicle_permission",
                fingerprint(self.gateway.journal.identity),
                fingerprint(self.hardware),
                state["vehicle_digest"],
                state["baseline_digest"],
                native.digest,
                fingerprint(self.gateway.journal.get("boot")),
                self.gateway._charger_epoch,
            )
            before = self.authority()
            async with asyncio.timeout(5):
                result = await provider.permission_status(request)
            if (
                before != self.authority()
                or provider.digest != state["provider_digest"]
                or result.request != request
                or result.provider_digest != state["provider_digest"]
                or result.permission_digest != state["pause"]["original_permission"]
                or type(result.observed_at) not in (int, float)
                or not 0 <= time.time() - result.observed_at <= 5
            ):
                raise SecurityError("original vehicle permission still needs independent reconciliation")
            await self.fallback_native()
            with self.gateway.journal.atomic():
                state["stage"] = "complete"
                state["vehicle_restoration"] = {"read_only_owner_reconciliation": asdict(result)}
                self.save(state)
                self.gateway.journal.put("trial_phase", "complete")
            return {
                "original_configuration_restored": True,
                "vehicle_permission_verified": True,
                "vehicle_command_sent": False,
                "complete": True,
            }
