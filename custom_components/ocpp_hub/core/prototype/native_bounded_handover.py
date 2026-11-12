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
from .local_control import local_start_receipt
from .managed_current import managed_current_ceiling, record_current_source, require_current_session
from .managed_recovery import local_recovery_binding
from .managed_schedule import (
    TimerVehicleDisconnectedError,
    installed_variant,
    known_timer_closure_gap,
    record_session_source,
    recorded_station_profile,
    recorded_timer_hold,
    remember_disconnect,
    request_timer_stop,
    require_continuing_session,
    supervised_timer,
)
from .native_bounded import NativeBoundedSpec
from .native_history import audit_history
from .native_http import NativeTransportUnavailableError
from .policy import DeniedError
from .rpc import IntroductionDeferredError, UnavailableError
from .security import SecurityError, StaleEvidenceError
from .station import StationChoice
from .store import RecoveryRequiredError, ReleaseDeferredError, fingerprint
from .trial_allocation import initialize_allocation, verify_initial_history
from .trial_evidence import profile_projection
from .trial_handover import ObserveHandover
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook
from .trial_rollback import require_lifecycle_idle

NativePatch = Callable[[dict[str, Any]], Awaitable[None]]


class AuditSupersededError(UnavailableError):
    """The independently read snapshot straddled a change to the journal's control or lifecycle authority."""


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
        self.command_source_observer: Callable[[HandoverEvidence], None] | None = None
        self.ordinary_source_observer: Callable[[HandoverEvidence], None] | None = None
        self.allow_unplugged = allow_unplugged
        self.lock = asyncio.Lock()
        self.last_verified_at = 0.0
        self.read_sequence = 0
        self.last_resolved_read = 0
        self.observation_listener: Callable[[HandoverEvidence], None] | None = None
        self.continuation_guard: Callable[[HandoverEvidence, int], None] | None = None
        self.recovery_zero_only: Callable[[], bool] | None = None
        self.positive_guard: Callable[[HandoverEvidence], None] | None = None
        self.introduced_epoch: int | None = None
        self.introduced_revision: int | None = None
        if fingerprint(spec.original_configuration) != book.baseline.configuration_digest:
            raise SecurityError("native bounded mode requires the exact original full configuration")
        saved = gateway.journal.get("native_bounded_handover")
        if saved and saved.get("spec") != spec.value():
            raise SecurityError("native bounded specification changed across process replacement")
        if not saved and gateway.journal.get("trial_phase") != "prepared":
            raise RecoveryRequiredError("an existing ordinary journal cannot acquire connected admission")

    # The last verified native snapshot expires on the same clock and within the same limit as its admission audit.
    def observation_age(self) -> float:
        return time.time() - self.last_verified_at

    # Report an observation's age honestly rather than treating a persisted desired limit as a live guarantee.
    def status(self) -> dict[str, Any]:
        state = self.gateway.journal.get("native_bounded_handover", {})
        return {
            "configured": True,
            "stage": state.get("stage", "not_admitted"),
            "target_native_limit_a": managed_current_ceiling(self.gateway.journal, self.spec),
            "native_limit_freshly_verified": 0 <= self.observation_age() <= 5,
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
                "boot_observation": gateway.boot_observation,
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
                "ordinary_recovery": journal.get("managed_ordinary_recovery"),
                "resources": [tuple(row) for row in journal.db.execute("SELECT * FROM trial_resources ORDER BY key")],
            }
        )

    # Read current rich native evidence and preserve every original history body without adopting later sessions.
    async def read(self, route: str = "trial", closed: bool = False) -> HandoverEvidence:
        self.read_sequence += 1
        sequence = self.read_sequence
        before = self.authority()

        # A newer observation may finish first while this read waits on the independent HTTP channel.
        def resolve() -> None:
            if sequence < self.last_resolved_read:
                raise AuditSupersededError("a newer native observation superseded this read")
            self.last_resolved_read = sequence

        try:
            native = await self.observe()
            resolve()
            if before != self.authority():
                raise AuditSupersededError("native bounded audit was superseded by local authority")
            self.require(native, route=route)
            audit_history(
                self.gateway.journal, self.spec.digest, list(native.history), self.gateway.policy.allow_local_start
            )
            self.book.check_originals(native.physical)
            if native.physical.connection == "Unplug":
                remember_disconnect(self.gateway.journal)
            if self.command_source_observer is not None:
                self.command_source_observer(native)
            record_session_source(self.gateway.journal, native, self.gateway.boot_observation)
            record_current_source(self.gateway.journal, native, self.gateway.boot_observation)
            if self.ordinary_source_observer is not None:
                self.ordinary_source_observer(native)
        except (NativeTransportUnavailableError, StaleEvidenceError) as error:
            resolve()
            self.last_verified_at = 0
            timer = self.gateway.journal.get("managed_temporary_schedule", {})
            if supervised_timer(timer) and (
                "hold_since" in timer or self.gateway.journal.get("managed_control_intent", {}).get("mode") == "paused"
            ):
                raise UnavailableError("timer hold awaits fresh native evidence") from error
            if self.gateway.journal.get("managed_current_change", {}).get("state") == "recovering":
                raise UnavailableError("current selection hold awaits fresh native evidence") from error
            ordinary = self.gateway.journal.get("managed_ordinary_recovery", {})
            intent = self.gateway.journal.get("managed_control_intent", {})
            if ordinary.get("state") == "holding" and intent == {"mode": "app", "revision": ordinary.get("revision")}:
                raise UnavailableError("ordinary recovery awaits fresh native evidence") from error
            raise
        except SecurityError, UnavailableError, TimeoutError:
            resolve()
            self.last_verified_at = 0
            raise
        self.last_verified_at = native.physical.observed_at
        if self.observation_listener is not None:
            self.observation_listener(native)
        # A verified open source stays fresh even if this particular operation needs the station idle.
        if closed:
            self.require(native, route=route, closed=True)
        return native

    # The explicit managed capability broadens only its native ceiling; selected-current enforcement stays independent.
    def require(self, native: HandoverEvidence, *, route: str = "trial", closed: bool = False) -> None:
        j = self.gateway.journal
        if managed_current_ceiling(j, self.spec) == 32:
            if route != "trial":
                raise SecurityError("managed current capability must be retired before backend restoration")
            lease = j.get("managed_temporary_schedule", {})
            closing_local = j.active() if supervised_timer(lease) else None
            if (
                closing_local is None
                and native.physical.transaction_open
                and (
                    known_timer_closure_gap(j, native)
                    or recorded_timer_hold(j, self.spec.digest, self.spec.boot_digest)
                )
            ):
                # A zero-output unplugged observation can maintain the restrictive hold without adopting the native ID.
                closing_local = native.physical.native_transaction_id
            self.spec.require_managed(
                native,
                j.identity,
                closed=closed,
                allow_unplugged=self.allow_unplugged,
                closing_local=closing_local,
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
        try:
            native = await self.read(closed=binding is None)
        except (NativeTransportUnavailableError, StaleEvidenceError, UnavailableError, TimeoutError) as error:
            # Retrying this initial observation cannot replay any Stop, ceiling change or lifecycle event.
            raise IntroductionDeferredError("introduction awaits fresh native evidence") from error
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
        verified_revision = gateway.controls._revision
        journal.put("trial_starting", False)
        if binding is not None and native.physical.transaction_open:
            lease = journal.get("managed_temporary_schedule", {})
            if journal.get("managed_current_change", {}).get("state") == "recovering":
                pass  # This operation owns only zero; fresh supervision must identify the independent session.
            elif not supervised_timer(lease):
                if (
                    journal.get("managed_mode")
                    and journal.get("managed_control_intent", {}).get("mode") == "app"
                    or self.recovery_zero_only is not None
                    and self.recovery_zero_only()
                ):
                    await gateway.controls.ensure_ceiling()
                    if lease and journal.active() is not None:
                        journal.put(
                            "managed_temporary_schedule",
                            {
                                **lease,
                                "recovery_session": True,
                                "unidentified_local": journal.active(),
                                "hold_since": time.time(),
                            },
                        )
                elif journal.active() is not None:
                    await gateway.stop()
            elif (
                lease.get("native_plan_confirmed")
                and native.physical.native_transaction_id == journal.active() == lease.get("bound_local")
                and native.session_plan_id == lease["body"]["planId"]
            ):
                # Introduction remains verified if this restrictive RPC fails. Its durable hold supervises retries.
                def same_boot() -> bool:
                    return (
                        gateway.current_boot_seen(epoch)
                        and verified_revision == gateway.controls._revision
                        and fingerprint(journal.get("boot")) == self.spec.boot_digest
                    )

                try:
                    await request_timer_stop(gateway, lease, same_boot)
                except UnavailableError, RecoveryRequiredError, DeniedError:
                    pass
            elif journal.active() is not None:
                # Preserve any other open transaction under the recovery zero until its provenance is verified.
                journal.put(
                    "managed_temporary_schedule",
                    {
                        **lease,
                        "unidentified_local": journal.active(),
                        "hold_since": lease.get("hold_since", time.time()),
                    },
                )
        gateway._require_epoch("charger", epoch)
        if verified_revision != gateway.controls._revision:
            raise UnavailableError("the verified charger introduction was superseded")
        self.introduced_revision = verified_revision
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
            j = self.gateway.journal
            control = j.get("control", {})
            if (
                j.get("managed_current_change", {}).get("state") == "recovering"
                and recorded_station_profile(j, self.spec.digest, self.spec.boot_digest)
                and control.get("inhibit")
            ):
                return  # Introduce only the restricted listener; no native ID is adopted or given Stop authority.
            if (
                j.active() is None
                and j.get("managed_control_intent", {}).get("mode") == "paused"
                and control.get("start_pending")
                and control.get("inhibit")
                and control.get("owner") == "local"
                and local_start_receipt(j).get("stage") in ("issued", "accepted")
                and recorded_station_profile(j, self.spec.digest, self.spec.boot_digest)
                and all(v == 0 for v in native.currents_a + native.powers_w)
                and any(
                    r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0
                    for r in self.book.owned_present(native.physical)
                )
            ):
                return  # The verified zero listener can receive the real lifecycle without adopting this native ID.
            if j.get("managed_temporary_schedule", {}).get("unreported_native"):
                if recorded_timer_hold(j, self.spec.digest, self.spec.boot_digest):
                    return  # Only the restricted listener/Boot can proceed. No local transaction is adopted.
            if not known_timer_closure_gap(j, native):
                self.require_known_session(native)
        elif native.physical.native_transaction_id is not None:
            raise RecoveryRequiredError("restricted recovery cannot adopt contradictory native session evidence")

    # Default recovery requires an empty queue; managed local recovery retains queued observations and the zero hold.
    async def resume(
        self,
        *,
        local_only: bool = False,
        continuing_local: int | None = None,
        preserve_expired_start: Callable[[HandoverEvidence, int | None], bool] | None = None,
    ) -> dict[str, Any]:
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
            native = await self.read(closed=continuing_local is None)
            if continuing_local is not None:
                if self.continuation_guard is None:
                    require_continuing_session(journal, native, continuing_local)
                else:
                    self.continuation_guard(native, continuing_local)
                if local_only or any(v != 0 for v in native.currents_a + native.powers_w):
                    raise RecoveryRequiredError("original session continuation requires online verified zero")
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
            # Evaluate managed debt against this read, after any waits; an earlier source cannot permit recovery.
            require_lifecycle_idle(
                gateway,
                continuing_local=continuing_local,
                preserve_expired_start=not local_only
                and preserve_expired_start is not None
                and preserve_expired_start(native, continuing_local),
            )
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
        change = self.gateway.journal.get("managed_current_change", {})
        if action == "SetChargingProfile" and change.get("state") == "recovering":
            local = self.gateway.journal.active()
            if local is not None:
                require_current_session(self.gateway, native, local)
            elif native.physical.transaction_open:
                raise ReleaseDeferredError("current recovery awaits actual lifecycle reconciliation")
        if self.positive_guard is not None:
            self.positive_guard(native)
        lease = self.gateway.journal.get("managed_temporary_schedule", {})
        continuing = lease.get("unidentified_local")
        if action == "SetChargingProfile" and supervised_timer(lease) and continuing is not None:
            require_continuing_session(self.gateway.journal, native, continuing)
        if timer:
            if payload["messageId"] == "setPlan" and native.physical.connection != "Plug":
                raise TimerVehicleDisconnectedError("temporary timer installation requires the connected vehicle")
            lease = self.gateway.journal.get("managed_temporary_schedule", {})
            expected = (
                lease.get("original_digest") if payload["messageId"] == "setPlan" else lease.get("augmented_digest")
            )
            if native.saved_schedule_digest != expected:
                raise SecurityError("temporary timer write requires its exact native preimage")
            present = self.book.owned_present(native.physical)
            if payload["messageId"] == "clearPlans" and self.gateway.journal.get("trial_phase") == "recovering":
                g = self.gateway
                self.require_known_session(native)
                if (
                    not recorded_timer_hold(g.journal, self.spec.digest, self.spec.boot_digest)
                    or "hold_since" not in lease
                    or not g.current_boot_seen(g._charger_epoch)
                    or self.introduced_epoch != g._charger_epoch
                    or self.introduced_revision != g.controls._revision
                    or not g.controls.ready
                    or g.controls.desired() != 0
                    or any(v != 0 for v in native.currents_a + native.powers_w)
                    or not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present)
                ):
                    raise RecoveryRequiredError("timer removal during recovery awaits a verified physical hold")
                return
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
                        try:
                            native = await self.read()
                        except AuditSupersededError:
                            await asyncio.sleep(0.1)
                            continue  # Retry a snapshot crossed by Stop within the same recovery deadline.
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
