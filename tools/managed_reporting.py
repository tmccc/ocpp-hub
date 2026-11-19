"""Bounded optional service reporting, kept separate from required journal and charger operations."""

from __future__ import annotations

import hashlib
import json
import platform
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ocpp_hub_core.prototype.managed_diagnostics import FAULTS
from ocpp_hub_core.prototype.managed_view import ReportDetails
from ocpp_hub_core.prototype.security import SecurityError


# Fingerprint the installed public source rather than guessing a deployed commit from an unrelated Git checkout.
def software(root: Path) -> dict[str, Any]:
    try:
        names = sorted((root / "custom_components/ocpp_hub/core").rglob("*.py"))
        names += [
            root / "tools" / name
            for name in ("managed_proxy.py", "managed_reporting.py", "managed_incident.py", "trial_proxy.py")
        ]
        digest = hashlib.sha256()
        for path in names:
            digest.update(str(path.relative_to(root)).encode() + b"\0" + path.read_bytes() + b"\0")
        source = digest.hexdigest()
    except OSError:
        source = None
    return {"proxy_python": platform.python_version(), "proxy_source_sha256": source}


class Reporting:
    """A small process-local timeline complements the existing rotated transition file across replacements."""

    # No journal or controller is retained; reporting can neither acknowledge work nor acquire charger authority.
    def __init__(self, root: Path) -> None:
        self.software = software(root)
        self.timeline: deque[dict[str, Any]] = deque(maxlen=20)
        self.previous: str | None = None
        self.persisted: str | None = None
        self.output_errors = 0

    # Ages and ordinary protocol counters do not create a repeated transition every five-second poll.
    def observe(self, status: dict[str, Any]) -> ReportDetails:
        diagnostic = status["diagnostics"]
        state, fault, phase = status.get("state"), status.get("fault"), status.get("phase")
        transition = {
            "state": state
            if isinstance(state, str)
            and state
            in {
                "starting",
                "healthy",
                "local_only",
                "degraded",
                "attention",
                "reconciling",
                "pausing",
                "stopping",
                "stopped",
                "waiting_for_cloud",
            }
            else "other",
            "fault": fault
            if isinstance(fault, str) and fault in FAULTS
            else "other_fault"
            if fault is not None
            else None,
            "phase": phase
            if isinstance(phase, str)
            and phase
            in {
                "active",
                "recovering",
                "blocked",
                "complete",
                "ready",
                "prepared",
                "admitted",
                "rollback",
                "backend_restore_pending",
            }
            else None,
            "desired_control": status.get("desired_control"),
            "control_revision": status.get("control_revision"),
            "blockers": diagnostic.get("blockers", []),
            "recovery": diagnostic.get("recovery"),
            "commands": [
                {k: row.get(k) for k in ("action", "state", "confirmation")}
                for row in diagnostic.get("pending_commands", [])
            ],
            "diagnostics_available": diagnostic.get("available") is True,
        }
        signature = json.dumps(transition, sort_keys=True)
        if signature != self.previous:
            self.timeline.append({"at_epoch": status.get("checked_at"), **transition})
            self.previous = signature
        return ReportDetails(dict(self.software), list(self.timeline), self.output_errors)

    # A failed optional file/stdout write cannot escape into required supervision or settle a command as successful.
    def write(
        self,
        status: dict[str, Any],
        status_path: Path,
        events_path: Path,
        save: Callable[[Path, dict[str, Any]], None],
        event: Callable[[Path, dict[str, Any]], None],
    ) -> bool:
        try:
            save(status_path, status)
        except OSError, SecurityError:
            self.output_errors += 1
        changed = self.previous != self.persisted
        if changed and self.timeline:
            try:
                event(events_path, self.timeline[-1])
            except OSError:
                self.output_errors += 1
            else:
                self.persisted = self.previous
                try:
                    print(json.dumps(self.timeline[-1], sort_keys=True), flush=True)
                except OSError:
                    self.output_errors += 1
        status["diagnostics"]["reporting_errors"] = self.output_errors
        return changed
