"""Dormant preflight and an explicitly started, standalone commissioning session; no Home Assistant imports."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import stat
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .gateway import Gateway
from .local_control import join_retirement, recover_unreported_local_start
from .managed_current import managed_current_ceiling
from .managed_faults import ManagedRefusal, latch_managed_fault
from .native_bounded import NativeBoundedSpec
from .native_bounded_handover import NativeBoundedHandover
from .native_faults import diagnostic_type
from .policy import Policy
from .rpc import IntroductionDeferredError, UnavailableError
from .runtime import ConnectionRuntime, RuntimeSettings
from .security import SecurityError
from .station import StationChoice, StationDiscovery
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_config import PreparedTLS, TrialConfig, prepare_tls, private_json
from .trial_evidence import NativeObserver
from .trial_handover_evidence import ObserveHandover
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
class PreparedTrial:
    """Prepared objects remain dormant: certificate parsing and private file reads do not establish live admission."""

    config: TrialConfig
    tls: PreparedTLS
    baseline: Baseline
    observer: NativeObserver

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


# Fully validate files, TLS and baseline before constructing a listener, journal or network connector.
def preflight(path: Path) -> PreparedTrial:
    config = TrialConfig.load(path)
    validate_state_path(config.journal)
    baseline = Baseline.parse(private_json(config.baseline))
    if baseline.identity_digest != fingerprint(config.identity):
        raise SecurityError("trial baseline identity does not match configuration")
    tls = prepare_tls(config)
    observer = NativeObserver(config)
    return PreparedTrial(config, tls, baseline, observer)


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
        self.journal = Journal(config.journal, config.identity)
        try:
            retired = self.journal.get("connected_handover")
            if retired is not None and (not isinstance(retired, dict) or retired.get("stage") != "complete"):
                raise SecurityError("retired vehicle-pause handover requires its original recovery implementation")
            if not self.journal.get("native_bounded_handover") and self.journal.phase.read().value not in (
                None,
                "prepared",
            ):
                raise RecoveryRequiredError("an existing ordinary journal cannot acquire connected admission")
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
            self._station_inventory = (sum(r.kind == "profile" for r in prepared.baseline.resources), False)
            self.station = StationDiscovery(
                self.journal,
                StationChoice(config.station_profile_kind, config.station_stack_level, config.station_profile_start),
                lambda: self._station_inventory,
            )
            self.gateway.controls.prepare_station = self.station.prepare_result
            self.gateway.charger.before_send = self.book.before_send
            self.gateway.charger.after_result = self.book.after_result
            actual_settings = settings or RuntimeSettings(boot_probe_after=10)
            transport = TrialTransport(config, prepared.tls, actual_settings)
            self.runtime = ConnectionRuntime(self.gateway, transport, actual_settings, self._before_ceiling)
            self.runtime.vendor_enabled = lambda: (
                self.journal.phase.read().active or (self.journal.phase.read().held and self.gateway.controls.ready)
            )
            # Reconfirming zero can briefly invalidate readiness without revoking an established lifecycle channel.
            self.runtime.vendor_session_enabled = lambda: self.journal.phase.read().supervised
            self.native_bounded = NativeBoundedHandover(
                self.gateway,
                self.book,
                handover_observe or prepared.observer.observe_handover,
                native_bounded,
                StationChoice(config.station_profile_kind, config.station_stack_level, config.station_profile_start),
                allow_unplugged=allow_unplugged_native,
            )
            self.native_bounded.introduction_service_pending = lambda: self.runtime.introduction_pending
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
                present = self.book.owned_present(native.physical)
                self.book.require_zero_station(present)
                if not self.gateway.current_boot_seen(self.gateway._charger_epoch):
                    raise SecurityError("bounded cleanup introduction needs actual Boot")
                return
            native = await self.native_bounded.introduce()
            self._station_inventory = (
                sum(resource.kind == "profile" for resource in native.physical.resources),
                any(resource.key == "profile:7001" for resource in native.physical.resources),
            )
        except (SecurityError, RecoveryRequiredError) as error:
            if self.journal.get("managed_mode"):
                self._introduction_refused_epoch = self.gateway._charger_epoch
                self._introduction_refusal = ManagedRefusal(
                    "native_authority_or_preservation_failed",
                    {
                        "type": diagnostic_type(error),
                        "detail": str(error)[:1024],
                        "at_epoch": time.time(),
                    },
                )
                self._persist_introduction_refusal()
            self.journal.phase.block_unless_cleanup()
            raise UnavailableError("native bounded introduction failed; positive controls remain blocked") from None

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

    # Verify native admission or recovery before the listener opens; actual Boot still establishes readiness.
    async def start(self) -> None:
        phase = self.journal.phase.read()
        if not self.started and self.journal.get("managed_mode"):
            recover_unreported_local_start(self.journal)
        if phase.value == "prepared":
            await self.native_bounded.admit()
        elif phase.supervised:
            native = await self.native_bounded.read()
            if self.journal.get("trial_binding") is None:
                self.native_bounded.require_known_session(native)
            else:
                self.native_bounded.require_recovery_session(native)
            self.book.owned_present(native.physical)
            control = self.journal.get("control", {})
            self.journal.control.hold(control)
            if self.journal.get("trial_binding") is None:
                self.native_bounded.spec.require_admission(native, self.journal.identity)
                if self.journal.db.execute("SELECT 1 FROM trial_resources LIMIT 1").fetchone():
                    raise RecoveryRequiredError("unintroduced connected recovery cannot adopt existing resources")
                self.journal.require_allocation()
                self.journal.phase.activate()
                self.journal.phase.begin_introduction()
            else:
                self.journal.phase.recover()
        elif phase.value == "rollback":
            native = await self.native_bounded.read(closed=True)
            if not self.book.owned_present(native.physical):
                return
            self.connected_cleanup_only = True
        else:
            raise RecoveryRequiredError("native bounded restoration requires no new listener")
        await self.runtime.start()
        self.started = True

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
                "boot_observed": self.runtime.status.boot_observed,
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
        try:
            if self.started and self.journal.phase.read().supervised:
                try:
                    if self.gateway.charger.sender is None:
                        control = self.journal.get("control", {})
                        self.journal.control.hold(control)
                        raise UnavailableError("charger disconnected before the final physical hold")
                    await self.gateway.stop()
                    async with asyncio.timeout(5):
                        while self.journal.active() is not None or self.journal.get("control", {}).get("start_pending"):
                            await asyncio.sleep(0.05)
                except Exception:
                    # This outer cleanup boundary records only uncertainty, never protocol bodies or exception text.
                    uncertain = True
                    try:
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
                        shutdown_uncertain=uncertain or bool(self.journal.get("trial_shutdown_uncertain")),
                    )
                except Exception:
                    # Status must remain printable even if the private database became unavailable during shutdown.
                    self.final_status = {"running": False, "phase": "unavailable", "shutdown_uncertain": True}
                self.journal.close()
                self.closed = True
