"""Persist strict managed refusals before introduction or ordinary control can proceed."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .gateway import Gateway
from .store import fingerprint


@dataclass(frozen=True)
class ManagedRefusal:
    """A proved introduction refusal retained until its diagnostic and restrictive latch commit together."""

    classification: str
    diagnostic: dict[str, Any]


# Keep the first proved refusal and its diagnostic atomic, including when introduction precedes the first poll.
def latch_managed_fault(gateway: Gateway, reason: str, *, diagnostic: dict[str, Any] | None = None) -> None:
    journal = gateway.journal
    if not journal.get("managed_control_fault"):
        with journal.atomic():
            fault: dict[str, Any] = {"classification": reason, "at_epoch": time.time()}
            if diagnostic is not None:
                journal.put("managed_native_failure", diagnostic)
                fault["native_failure_digest"] = fingerprint(diagnostic)
            journal.put("managed_control_fault", fault)
            control = journal.get("control", {})
            journal.control.hold(control)
            if journal.get("trial_phase") in ("active", "recovering", "blocked"):
                journal.put("trial_phase", "blocked")
        gateway.controls.invalidate()
