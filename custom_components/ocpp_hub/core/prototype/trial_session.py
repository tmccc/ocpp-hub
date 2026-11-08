"""Dormant preflight and an explicitly started, standalone commissioning session; no Home Assistant imports."""

from __future__ import annotations

import asyncio
import json
import os
import stat
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .gateway import Gateway
from .policy import Policy
from .rpc import UnavailableError
from .runtime import ConnectionRuntime, RuntimeSettings
from .security import SecurityError
from .station import StationChoice, StationDiscovery
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_config import PreparedTLS, TrialConfig, prepare_tls, private_json
from .trial_evidence import Evidence, NativeObserver
from .trial_resources import Baseline, ResourceBook
from .trial_rollback import Rollback
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

    # The observer override is solely dependency injection for the synthetic, network-guarded trial bench.
    def __init__(
        self,
        prepared: PreparedTrial,
        settings: RuntimeSettings | None = None,
        observe: Callable[[], Awaitable[Evidence]] | None = None,
    ) -> None:
        self.prepared = prepared
        self.observe = observe or prepared.observer.observe
        config = prepared.config
        self.journal = Journal(config.journal, config.identity)
        try:
            self.book = ResourceBook(self.journal, prepared.baseline)
            policy = Policy(
                max_current_a=6,
                allowed_tags=config.local_tags,
                trust_vendor_start_tags=True,
                strict_cloud_authorization=True,
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
            self.gateway.controls.prepare_station = self.station.prepare
            self.gateway.charger.before_send = self.book.before_send
            self.gateway.charger.after_result = self.book.after_result
            transport = TrialTransport(config, prepared.tls, settings)
            self.runtime = ConnectionRuntime(self.gateway, transport, settings, self._before_ceiling)
            self.runtime.vendor_enabled = lambda: (
                self.journal.get("trial_phase") == "active"
                or (self.journal.get("trial_phase") in ("recovering", "blocked") and self.gateway.controls.ready)
            )
            self.rollback = Rollback(self.gateway, self.book, self.observe)
        except BaseException:
            self.journal.close()
            raise
        self.started = False
        self.closed = False
        self.final_status: dict[str, Any] | None = None

    # Initial admission is unplugged only; an already active supervised trial can reapply its hold after reconnect.
    async def _before_ceiling(self) -> None:
        phase = self.journal.get("trial_phase")
        if phase not in ("prepared", "active", "recovering", "blocked"):
            raise UnavailableError("trial ceiling reapplication remains frozen during rollback")
        try:
            evidence = await self.observe()
            self._station_inventory = (
                sum(resource.kind == "profile" for resource in evidence.resources),
                any(resource.key == "profile:7001" for resource in evidence.resources),
            )
            if phase == "prepared":
                self.book.activate(evidence)
            else:
                evidence.require_fresh(self.journal.identity)
                self.book.owned_present(evidence)
                if phase in ("recovering", "blocked"):
                    await self.gateway.stop()
        except SecurityError, RecoveryRequiredError:
            self.journal.put("trial_phase", "blocked")
            raise UnavailableError("native trial audit failed; controls remain blocked") from None

    # Take a final native idle check immediately before binding; the charger then introduces itself through real Boot.
    async def start(self) -> None:
        evidence = await self.observe()
        phase = self.journal.get("trial_phase")
        if phase == "prepared":
            self.book.require_profile_free_baseline()
            self.book.activate(evidence)
        elif phase == "active":
            evidence.require_disconnected(self.journal.identity)
            self.book.owned_present(evidence)
            control = self.journal.get("control", {})
            self.journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
            self.journal.put("trial_phase", "recovering")
        elif phase in ("recovering", "blocked"):
            evidence.require_disconnected(self.journal.identity)
            control = self.journal.get("control", {})
            self.journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
            self.journal.put("trial_phase", "recovering")
        elif phase in ("rollback", "backend_restore_pending"):
            self.rollback.require_idle(evidence)
        else:
            raise RecoveryRequiredError("completed or unrecognised trial cannot start")
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
        if command == "start":
            tags = self.prepared.config.local_tags
            if len(tags) != 1:
                raise SecurityError("local trial start requires exactly one explicitly approved owner tag")
            await self.gateway.start(next(iter(tags)))
            return {"start_requested": True, "ceiling_a": 6}
        if command == "release":
            await self.gateway.release()
            return {"local_owner_released": True, "ceiling_a": 6}
        if command == "rollback-plan":
            return await self.rollback.plan()
        if command == "rollback":
            return await self.rollback.run(self.runtime.stop_vendor)
        if command == "confirm-original-backend":
            return await self.rollback.confirm_original_backend()
        raise SecurityError("trial command is not supported")

    # Shutdown never removes an unverified hold; even a journal failure must retire both transport paths.
    async def close(self) -> None:
        if self.closed:
            return
        uncertain = False
        try:
            if self.started and self.journal.get("trial_phase") in ("active", "recovering", "blocked"):
                try:
                    if self.gateway.charger.sender is None:
                        control = self.journal.get("control", {})
                        self.journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
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
