"""Dormant preflight and an explicitly started, standalone commissioning session; no Home Assistant imports."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import stat
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..control import HUB_PROFILE_ID
from .command_processor import CommandPriority
from .connection_scope import control_scope_matches, current_control_scope
from .gateway import Gateway
from .local_control import StopReplyUnconfirmedError, join_retirement, recover_unreported_local_start
from .managed_current import managed_current_ceiling
from .managed_faults import ManagedRefusal, latch_managed_fault
from .native_bounded import NativeBoundedSpec, NativeReadinessError, readiness_decision
from .native_bounded_handover import NativeBoundedHandover
from .native_faults import (
    MEASURED_FAULT,
    STATION_CEILING_LOST,
    MeasuredOutputLimitError,
    _NativeMeasurementError,
    diagnostic_type,
)
from .native_history import ORIGINAL_HISTORY_OMISSION, STARTUP_HISTORY_OMISSION, MissingOriginalHistoryError
from .native_history_recovery import history_recovery_idle
from .native_http import NativeTransportUnavailableError
from .policy import Policy
from .recovery_contracts import require_allowed
from .rpc import IntroductionDeferredError, UnavailableError
from .runtime import ConnectionRuntime, RuntimeSettings
from .security import SecurityError, StaleEvidenceError
from .station import StationChoice, StationDiscovery
from .store import Journal, RecoveryRequiredError, encoded, fingerprint
from .trial_config import PreparedTLS, TrialConfig, prepare_tls, private_json
from .trial_evidence import NativeObserver
from .trial_handover_evidence import HandoverEvidence, ObserveHandover
from .trial_resources import Baseline, ResourceBook
from .trial_transport import TrialTransport


# State must be protected before SQLite creates any side files; preflight itself creates nothing.
def validate_state_path(path: Path) -> None:
    try:
        parent = path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 0o077:
            raise ValueError
        for candidate in (path, Path(str(path) + ".lock"), Path(str(path) + "-wal"), Path(str(path) + "-shm")):
            if candidate.exists() or candidate.is_symlink():
                info = candidate.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                    raise ValueError
    except OSError, ValueError:
        raise SecurityError(
            "trial journal requires an owned private directory and private regular state files"
        ) from None


# Replace a private data file atomically without following an existing destination symlink.
def save_private(path: Path, value: Any) -> None:
    validate_state_path(path)
    temporary = path.parent / (".trial-" + uuid.uuid4().hex)
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(repr=False)
class PreparedNative:
    """Private configuration and the native observer used without an OCPP transport."""

    config: TrialConfig
    baseline: Baseline
    observer: NativeObserver


@dataclass(repr=False)
class PreparedTrial(PreparedNative):
    """Full service preparation includes the TLS identities of both OCPP peers."""

    tls: PreparedTLS

    # Return only readiness classifications, never endpoints, identities, tag values or file contents.
    def summary(self) -> dict[str, Any]:
        return {
            "offline_preflight_passed": True,
            "network_contacted": False,
            "hardware_admission_verified": False,
            "hardware_ceiling_verified": False,
            "original_resources": len(self.baseline.resources),
            "original_profiles": sum(resource.kind == "profile" for resource in self.baseline.resources),
            "new_profile_writes_blocked": any(resource.kind == "profile" for resource in self.baseline.resources),
            "ceiling_a": 6,
            "station_profile": StationChoice(
                self.config.station_profile_kind, self.config.station_stack_level, self.config.station_profile_start
            ).value(),
            "station_capability_discovery_required": True,
            "fresh_native_check_required_at_start": True,
        }


# Validate the files used by native operations without requiring an unused OCPP certificate.
def preflight_native(path: Path) -> PreparedNative:
    config = TrialConfig.load(path)
    validate_state_path(config.journal)
    baseline = Baseline.parse(private_json(config.baseline))
    if baseline.identity_digest != fingerprint(config.identity):
        raise SecurityError("trial baseline identity does not match configuration")
    return PreparedNative(config, baseline, NativeObserver(config))


# Fully validate both OCPP endpoints before constructing a listener or journal.
def preflight(path: Path) -> PreparedTrial:
    native = preflight_native(path)
    return PreparedTrial(native.config, native.baseline, native.observer, prepare_tls(native.config))


# Refuse retired installations under the journal lock before normalizing interrupted operations.
def require_supported_installation(db: sqlite3.Connection, spec: NativeBoundedSpec) -> None:
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone():
        return
    metadata = {
        key: json.loads(value)
        for key, value in db.execute(
            "SELECT key,value FROM meta WHERE key IN ('connected_handover','native_bounded_handover','trial_phase')"
        )
    }
    retired = metadata.get("connected_handover")
    if retired is not None and (not isinstance(retired, dict) or retired.get("stage") != "complete"):
        raise SecurityError("retired vehicle-pause handover requires its original recovery implementation")
    native = metadata.get("native_bounded_handover")
    if not native and metadata.get("trial_phase") not in (None, "prepared"):
        raise RecoveryRequiredError("an existing ordinary journal cannot acquire connected admission")
    if native and (not isinstance(native, dict) or native.get("spec") != spec.value()):
        raise SecurityError("native bounded specification changed across process replacement")


# Check the exact installation under the journal lock before any normalisation, including in offline tools.
def open_native_journal(path: Path, identity: str, spec: NativeBoundedSpec) -> Journal:
    return Journal(path, identity, check_existing=lambda db: require_supported_installation(db, spec))


class TrialSession:
    """Only explicit start opens sockets; expiry/shutdown leaves a hold rather than silently rolling back a live car."""

    # Require the native specification explicitly; observation overrides remain confined to isolated test peers.
    def __init__(
        self,
        prepared: PreparedTrial,
        settings: RuntimeSettings | None = None,
        *,
        native_bounded: NativeBoundedSpec,
        handover_observe: ObserveHandover | None = None,
        allow_local_start: bool = True,
        allow_unplugged_native: bool = False,
    ) -> None:
        if not isinstance(native_bounded, NativeBoundedSpec):
            raise SecurityError("session requires an explicit native bounded specification")
        if type(allow_unplugged_native) is not bool or allow_unplugged_native and allow_local_start:
            raise SecurityError("unplugged native monitoring requires bounded mode and disabled local Start")
        self.prepared = prepared
        config = prepared.config
        self.journal = open_native_journal(config.journal, config.identity, native_bounded)
        try:
            managed = self.journal.get("managed_mode")
            if managed is not None and (
                managed.get("mode") != "app_schedule" or managed.get("local_start") is not False or allow_local_start
            ):
                raise SecurityError("managed journal requires its saved local Start restriction")
            self.book = ResourceBook(self.journal, prepared.baseline)
            policy = Policy(
                max_current_a=managed_current_ceiling(self.journal, native_bounded),
                allowed_tags=config.local_tags,
                trust_vendor_start_tags=True,
                strict_cloud_authorization=True,
                allow_local_start=allow_local_start,
                local_stack_level=config.station_stack_level,
                extensions={
                    ("IoCharger", name): mode
                    for name, mode in (
                        ("getTransaction", "read"),
                        ("setTransaction", "write"),
                        ("delayStop", "write"),
                    )
                },
            )
            self.gateway = Gateway(self.journal, policy, timeout=5)
            processor = self.gateway.use_command_processor()
            self._station_inventory = (sum(r.kind == "profile" for r in prepared.baseline.resources), False)
            self.station = StationDiscovery(
                self.journal,
                StationChoice(config.station_profile_kind, config.station_stack_level, config.station_profile_start),
                lambda: self._station_inventory,
                commands=processor,
            )
            self.gateway.controls.prepare_station = self.station.prepare_result
            self.gateway.controls.station_missing = self.book.station_ceiling_lost
            self.gateway.charger.before_send = self.book.before_send
            self.gateway.charger.after_result = self.book.after_result
            actual_settings = settings or RuntimeSettings(boot_probe_after=10)
            transport = TrialTransport(config, prepared.tls, actual_settings)
            self.runtime = ConnectionRuntime(self.gateway, transport, actual_settings, self._before_ceiling)
            self.runtime.vendor_enabled = lambda: (
                self.journal.phase.read().active
                or self.journal.phase.read().held
                and (
                    self.gateway.controls.ready
                    or self.book.station_ceiling_lost()
                    and self.journal.get("managed_mode")
                    and fingerprint(self.journal.get("boot")) == native_bounded.boot_digest
                )
            )
            # Reconfirming zero can briefly invalidate readiness without revoking an established lifecycle channel.
            self.runtime.vendor_session_enabled = lambda: self.journal.phase.read().supervised
            self.native_bounded = NativeBoundedHandover(
                self.gateway,
                self.book,
                handover_observe or prepared.observer.observe_handover,
                native_bounded,
                StationChoice(config.station_profile_kind, config.station_stack_level, config.station_profile_start),
                observe_profiles=prepared.observer.observe_profiles,
                allow_unplugged=allow_unplugged_native,
            )
            self.native_bounded.introduction_service_pending = lambda: self.runtime.introduction_pending
            self.gateway.controls.require_retained_station = self.native_bounded.require_retained_station
            self.gateway.charger.validate_send = self.native_bounded.validate_send
            self.connected_cleanup_only = False
            self._introduction_refusal: ManagedRefusal | None = None
            self._introduction_refused_epoch: int | None = None
            self.runtime.ceiling_enabled = lambda: not self.connected_cleanup_only
        except BaseException:
            self.journal.close()
            raise
        self.started = False
        self.closed = False
        self.closing = False
        self._closing_task: asyncio.Task[None] | None = None
        self.final_status: dict[str, Any] | None = None

    # Never classify native profile bytes while their actual outgoing Set/Clear response is still unresolved.
    def _resource_mutation_inflight(self) -> bool:
        return bool(self.gateway.charger._pending) and self.gateway.charger.diagnostics.action in (
            "SetChargingProfile",
            "ClearChargingProfile",
        )

    # Reaudit the explicit native-bound mode on every real charger introduction, retaining cleanup-only startup.
    async def _before_ceiling(self) -> None:
        if self._introduction_refusal is not None:
            self._persist_introduction_refusal()
        if self._introduction_refused_epoch == self.gateway._charger_epoch and self.journal.fault.latched():
            raise UnavailableError("native bounded introduction remains terminally refused for this connection")
        try:
            if self.connected_cleanup_only:
                native = await self.native_bounded.read(closed=True)

                # Confirm cleanup introduction against the actual native inventory and current Boot.
                def confirm_cleanup() -> None:
                    present = self.book.owned_present(native.physical)
                    self.book.require_zero_station(present)
                    if not self.gateway.current_boot_seen(self.gateway._charger_epoch):
                        raise SecurityError("bounded cleanup introduction needs actual Boot")

                self.gateway.decide(confirm_cleanup, priority=CommandPriority.RESTRICT)
                return
            try:
                selected = self._commissioned_selection()
            except sqlite3.Error as storage:
                raise IntroductionDeferredError("selected current awaits required journal storage") from storage
            try:
                native = await self.native_bounded.introduce(selected_current=selected)
            except _NativeMeasurementError as error:
                try:
                    refusal = self._measured_refusal(error, selected)
                except sqlite3.Error as storage:
                    raise IntroductionDeferredError("measurement proof awaits required journal storage") from storage
                self._introduction_refused_epoch = self.gateway._charger_epoch
                self._introduction_refusal = refusal
                self._persist_introduction_refusal()
                return
            self._station_inventory = (
                sum(resource.kind == "profile" for resource in native.physical.resources),
                any(resource.key == "profile:7001" for resource in native.physical.resources),
            )
        except (SecurityError, RecoveryRequiredError) as error:
            if self.journal.get("managed_mode"):
                self._introduction_refused_epoch = self.gateway._charger_epoch
                self._introduction_refusal = self._native_refusal(error)
                self._persist_introduction_refusal()
            self.journal.phase.block_unless_cleanup()
            raise UnavailableError("native bounded introduction failed; positive controls remain blocked") from None

    # Startup and charger introduction use the same persisted diagnostic for a strict native refusal.
    @staticmethod
    def _native_refusal(error: Exception) -> ManagedRefusal:
        diagnostic = {"type": diagnostic_type(error), "detail": str(error)[:1024], "at_epoch": time.time()}
        if isinstance(error, MissingOriginalHistoryError):
            diagnostic["source"] = ORIGINAL_HISTORY_OMISSION
        return ManagedRefusal(
            "native_authority_or_preservation_failed",
            diagnostic,
        )

    # Retry only the failed atomic publication, retaining the proved refusal even if the device changes meanwhile.
    def _persist_introduction_refusal(self) -> None:
        refusal = self._introduction_refusal
        if refusal is None:
            return
        self.gateway.controls.invalidate()
        try:
            latch_managed_fault(self.gateway, refusal.classification, diagnostic=refusal.diagnostic)
        except sqlite3.Error as error:
            raise IntroductionDeferredError("strict introduction refusal awaits durable diagnostic") from error
        self._introduction_refusal = None
        raise UnavailableError("native bounded introduction failed; positive controls remain blocked")

    # Use the last settled current before ordinary recovery can hide an excessive measurement behind zero.
    def _commissioned_selection(self) -> int | None:
        j, spec = self.journal, self.native_bounded.spec
        if not j.get("managed_mode") or not j.get("trial_binding") or j.phase.starting:
            return None
        change = j.current_change.read()
        selected = (
            change.value.get("previous_a", 6) if change.selecting else j.get("control", {}).get("resume_limit", 6)
        )
        if (
            type(selected) not in (int, float)
            or not 6 <= selected <= managed_current_ceiling(j, spec)
            or int(selected) != selected
        ):
            raise SecurityError("persisted selected current is malformed")
        return int(selected)

    # A measured excess on this exact installation can open only the path to its existing protective zero.
    def _measured_refusal(self, error: _NativeMeasurementError, selected: int | None) -> ManagedRefusal:
        j, native = self.journal, self.native_bounded
        sample, spec = error.native, native.spec
        if selected != self._commissioned_selection():
            raise IntroductionDeferredError("current selection changed during measurement proof")
        if (
            selected is None
            or not native.commissioned_binding_matches(sample)
            or error.authority != native.authority()
            or error.sequence != native.last_resolved_read
            or not sample.physical.transaction_open
        ):
            raise error
        require_allowed(readiness_decision(sample, False, native.allow_unplugged))
        native.require_known_session(sample)
        present = self.book.owned_present(sample.physical)
        if self.book.station_ceiling_lost():
            # The listener can receive actual Boot and Stop evidence without recreating the missing profile.
            return ManagedRefusal(STATION_CEILING_LOST, {"type": "StationCeilingLost", "at_epoch": time.time()})
        station = j.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", (f"profile:{HUB_PROFILE_ID}",)
        ).fetchone()
        if (
            station is None
            or station["state"] != "confirmed"
            or not any(
                resource.identifier == HUB_PROFILE_ID and encoded(resource.canonical) == station["confirmed"]
                for resource in present
            )
        ):
            raise error
        measured = error.measured.selected(selected) if managed_current_ceiling(j, spec) == 32 else error.measured
        failure = MeasuredOutputLimitError(sample, measured, spec.digest, selected)
        return ManagedRefusal(
            MEASURED_FAULT,
            {
                "type": diagnostic_type(failure),
                "detail": str(failure),
                "at_epoch": time.time(),
                "cause": failure.cause,
            },
        )

    # Verify native admission or recovery before the listener opens; actual Boot still establishes readiness.
    async def start(self) -> None:
        phase = self.journal.phase.read()
        if not self.started and self.journal.get("managed_mode"):
            recover_unreported_local_start(self.journal)
        if phase.value == "prepared":
            await self.native_bounded.admit()
        elif phase.supervised:
            selected = self._commissioned_selection()
            known_local = self.journal.active() if self.journal.get("trial_binding") is not None else None
            try:
                try:
                    native = await self.native_bounded.read(selected_current=selected, closing_local=known_local)
                except NativeTransportUnavailableError, StaleEvidenceError, UnavailableError:
                    if not self.journal.get("managed_mode") or not self.journal.get("trial_binding"):
                        raise
                    # Retry a brief outage for an installed proxy while still requiring a complete startup audit.
                    await asyncio.sleep(0.25)
                    native = await self.native_bounded.read(selected_current=selected, closing_local=known_local)
            except _NativeMeasurementError as error:
                refusal = self._measured_refusal(error, selected)
                latch_managed_fault(self.gateway, refusal.classification, diagnostic=refusal.diagnostic)
            except MissingOriginalHistoryError as error:
                if (
                    self.journal.get("managed_mode")
                    and self.journal.get("trial_binding")
                    and not self.journal.fault.latched()
                    and history_recovery_idle(self.journal, retain_queued_status=True)
                ):
                    refusal = self._native_refusal(error)
                    diagnostic = {**refusal.diagnostic, "source": STARTUP_HISTORY_OMISSION}
                    latch_managed_fault(self.gateway, refusal.classification, diagnostic=diagnostic)
                raise
            else:
                self.gateway.decide(lambda: self._accept_startup_observation(native), priority=CommandPriority.RESTRICT)
        elif phase.value == "rollback":
            native = await self.native_bounded.read(closed=True)
            if not self.book.owned_present(native.physical):
                return
            self.connected_cleanup_only = True
        else:
            raise RecoveryRequiredError("native bounded restoration requires no new listener")
        await self.runtime.start()
        self.started = True

    # Establish startup's actual session and resources before publishing the restricted introduction.
    def _accept_startup_observation(self, native: HandoverEvidence) -> None:
        if self.journal.get("trial_binding") is None:
            self.native_bounded.require_known_session(native)
        else:
            self.native_bounded.require_recovery_session(native)
        self.book.owned_present(native.physical)
        self.journal.control.hold(self.journal.get("control", {}))
        if self.journal.get("trial_binding") is None:
            self.native_bounded.spec.require_admission(native, self.journal.identity)
            if self.journal.db.execute("SELECT 1 FROM trial_resources LIMIT 1").fetchone():
                raise RecoveryRequiredError("unintroduced connected recovery cannot adopt existing resources")
            self.journal.require_allocation()
            self.journal.phase.activate()
            self.journal.phase.begin_introduction()
        else:
            self.journal.phase.recover()

    # A small explicit operator interface keeps all controls inside the original gateway ownership rules.
    async def command(self, value: dict[str, Any]) -> dict[str, Any]:
        if set(value) != {"command"} or not isinstance(value["command"], str):
            raise SecurityError("trial command fields are invalid")
        command = value["command"]
        if command == "status":
            return {
                **self.book.status(),
                "charger_connected": self.runtime.status.charger_connected,
                "vendor_connected": self.runtime.status.vendor_connected,
                "effective_ceiling_confirmed": self.gateway.controls.ready,
                "native_bounded_handover": self.native_bounded.status(),
                "station_capabilities": self.station.snapshot(),
                "boot_observed": self.gateway.current_boot_seen(self.gateway._charger_epoch),
                "vendor_registered": self.gateway.vendor_registration == "Accepted",
                "delivery_blocked": self.runtime.status.delivery_blocked,
                "failure_classification": self.runtime.status.last_failure,
                "protocol_diagnostics": {
                    "vendor_attempts": self.runtime.status.vendor_attempts,
                    "vendor_sessions_opened": self.runtime.status.reconnects,
                    "vendor_rpc": self.gateway.vendor.diagnostics.snapshot(),
                    "charger_rpc": self.gateway.charger.diagnostics.snapshot(),
                    "vendor_fault": self.runtime.status.vendor_fault.snapshot(),
                    "charger_fault": self.runtime.status.charger_fault.snapshot(),
                },
                "local_hold": bool(self.journal.get("control", {}).get("inhibit")),
                "local_outcome_uncertain": bool(self.journal.get("control", {}).get("uncertain")),
                "pending_lifecycle_events": self.journal.db.execute(
                    "SELECT COUNT(*) FROM outbox WHERE action IN ('StartTransaction','StopTransaction')"
                ).fetchone()[0],
                "retained_observations": self.journal.db.execute(
                    "SELECT COUNT(*) FROM outbox WHERE action NOT IN ('StartTransaction','StopTransaction')"
                ).fetchone()[0],
            }
        if command == "stop":
            await self.gateway.stop()
            return {"stop_requested": True, "transaction_still_open": self.journal.active() is not None}
        if command == "native-bounded-handover":
            return await self.native_bounded.cleanup(self.runtime.stop_vendor)
        if command == "native-bounded-resume":
            return await self.native_bounded.resume()
        if command == "start":
            self.journal.require_allocation()
            if self.journal.phase.starting or not self.gateway.controls.ready:
                raise RecoveryRequiredError("local Start requires the current verified trial ceiling")
            tags = self.prepared.config.local_tags
            if len(tags) != 1:
                raise SecurityError("local trial start requires exactly one explicitly approved owner tag")
            await self.gateway.start(next(iter(tags)))
            return {"start_requested": True, "ceiling_a": 6}
        if command == "release":
            if self.journal.phase.starting or not self.gateway.controls.ready:
                raise RecoveryRequiredError("release requires the current verified trial ceiling")
            await self.gateway.release()
            return {"local_owner_released": True, "ceiling_a": 6}
        raise SecurityError("trial command is not supported")

    # Shutdown never removes an unverified hold; even a journal failure must retire both transport paths.
    async def close(self) -> None:
        if self._closing_task is None:
            self.closing = True
            self._closing_task = asyncio.create_task(self._close_owned())
        await join_retirement(self._closing_task)

    # A single owned close retires every journal user before closing SQLite, including under repeated cancellation.
    async def _close_owned(self) -> None:
        if self.closed:
            return
        self.closing = True
        uncertain = False
        verified = False
        attempt = uuid.uuid4().hex
        try:
            if self.started and self.journal.phase.read().supervised:
                try:
                    if self.gateway.charger.sender is None:
                        control = self.journal.get("control", {})
                        self.journal.control.hold(control)
                        raise UnavailableError("charger disconnected before the final physical hold")
                    local = self.journal.active()
                    try:
                        await self._request_shutdown_stop(attempt, local)
                    finally:
                        await self._wait_for_shutdown_closure(attempt, stop_late_start=local is None)
                    self._record_shutdown_outcome(attempt, "verified")
                    verified = True
                except Exception:
                    # This outer cleanup boundary records only uncertainty, never protocol bodies or exception text.
                    uncertain = True
                    try:
                        self._record_shutdown_outcome(attempt, "uncertain")
                        self.journal.put("trial_shutdown_uncertain", True)
                    except Exception:
                        # Failed state storage cannot skip socket retirement or falsely establish a successful Stop.
                        uncertain = True
        finally:
            try:
                await self.runtime.close()
            finally:
                try:
                    self.final_status = await self.command({"command": "status"})
                    self.final_status.update(
                        running=False,
                        shutdown_attempt_uncertain=not verified,
                        shutdown_uncertain=uncertain or bool(self.journal.get("trial_shutdown_uncertain")),
                    )
                except Exception:
                    # Status must remain printable even if the private database became unavailable during shutdown.
                    self.final_status = {
                        "running": False,
                        "phase": "unavailable",
                        "shutdown_uncertain": True,
                        "shutdown_attempt_uncertain": True,
                    }
                self.journal.close()
                self.closed = True

    # Give final shutdown its own restrictive turn while the charger receiver is still open.
    async def _request_shutdown_stop(self, attempt: str, local: int | None) -> None:
        g, j, native = self.gateway, self.journal, self.native_bounded
        if not j.get("managed_mode"):
            await g.stop()
            return
        connection = current_control_scope(g)

        # A shutdown never borrows another owner's Stop claim, including on the same socket.
        def bind(ticket: str) -> None:
            j.put(
                "trial_shutdown_attempt",
                {
                    "id": attempt,
                    "local": local,
                    "ticket": ticket,
                    "state": "pending",
                    "boot": connection[0].boot if connection else None,
                },
            )

        # Give the zero write its own authority. A Stop must still belong to the original live Boot.
        async def prove(local: int, ceiling_failed: bool) -> Callable[[], None]:
            # Bind shutdown Stop to the commissioned Boot and the same active transaction.
            def require_scope() -> None:
                if (
                    not control_scope_matches(g, connection)
                    or j.active() != local
                    or fingerprint(j.get("boot")) != native.spec.boot_digest
                ):
                    raise RecoveryRequiredError("shutdown Stop lost its original connection or transaction")

            require_scope()
            if not ceiling_failed:
                return require_scope

            # When zero cannot be confirmed, independently match the existing physical session.
            try:
                async with asyncio.timeout(5):
                    sample = await native.read(closing_local=local, stop_only=True)
            except _NativeMeasurementError as error:
                if error.authority != native.authority() or error.sequence != native.last_resolved_read:
                    raise
                sample = error.native
                require_allowed(readiness_decision(sample, False, native.allow_unplugged, local))
            native.require_known_session(sample)
            sequence = native.last_resolved_read

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

            return guard

        try:
            await g.controls.stop(
                expected_local=local,
                prove_stop=prove,
                on_claim=bind,
                owner=("shutdown", attempt, local, connection),
                priority=CommandPriority.SHUTDOWN,
            )
        except StopReplyUnconfirmedError:
            # Closure and an owned physical zero can still be confirmed by the independent final checks.
            pass

    # Keep the receiver alive to journal the real Stop, even if confirming zero failed.
    async def _wait_for_shutdown_closure(self, attempt: str, *, stop_late_start: bool) -> None:
        managed = bool(self.journal.get("managed_mode"))
        late = await self._wait_for_shutdown_reports(accept_late_start=managed and stop_late_start)
        if late is not None:
            try:
                await self._request_shutdown_stop(attempt, late)
            finally:
                await self._wait_for_shutdown_reports(accept_late_start=False)

    # The final result records independent closure, never a guessed acknowledgement of the Stop RPC.
    def _record_shutdown_outcome(self, attempt: str, state: str) -> None:
        current = self.journal.get("trial_shutdown_attempt", {})
        if current.get("id") == attempt:
            self.journal.put("trial_shutdown_attempt", {**current, "state": state})

    # Wait for closure and verified zero, or return a late Start so shutdown can Stop it explicitly.
    async def _wait_for_shutdown_reports(self, *, accept_late_start: bool) -> int | None:
        async with asyncio.timeout(5):
            while True:
                local = self.journal.active()
                if accept_late_start and local is not None:
                    return local
                if (
                    local is None
                    and not self.journal.get("control", {}).get("start_pending")
                    and not self.gateway.authorisation.pending()
                ):
                    break
                await asyncio.sleep(0.05)
            if self.journal.get("managed_mode"):
                await self._verify_shutdown_hold()
        return None

    # The charger can report Stop before its independent native session has closed.
    async def _verify_shutdown_hold(self) -> None:
        while True:
            try:
                native = await self.native_bounded.read(closed=True, owned=True)
            except NativeReadinessError, NativeTransportUnavailableError, StaleEvidenceError, _NativeMeasurementError:
                await asyncio.sleep(0.05)
                continue
            self.native_bounded.require_known_session(native)
            present = self.book.owned_present(native.physical)
            station = [resource for resource in present if resource.identifier == HUB_PROFILE_ID]
            receipt = self.journal.db.execute(
                "SELECT state,confirmed FROM trial_resources WHERE key=?", (f"profile:{HUB_PROFILE_ID}",)
            ).fetchone()
            if (
                not self.gateway.current_boot_seen(self.gateway._charger_epoch)
                or not self.gateway.controls.ready
                or self.gateway.controls.desired() != 0
                or not self.book.confirmed_station_zero(station, receipt)
            ):
                raise RecoveryRequiredError("shutdown awaits the independently verified station zero")
            return
