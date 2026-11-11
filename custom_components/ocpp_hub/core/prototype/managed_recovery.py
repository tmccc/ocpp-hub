"""Managed-only recovery may retain queued observations, while every unresolved electrical/lifecycle fact blocks it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .managed_schedule import KEY, OBSERVABLE
from .store import RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .gateway import Gateway


# Bind an intentional zero-only recovery before and after native I/O without consuming or acknowledging its queue.
def local_recovery_binding(gateway: Gateway, spec_digest: str) -> str:
    journal = gateway.journal
    intent = journal.get("managed_control_intent", {})
    timer = journal.get(KEY, {})
    timer_id = timer.get("id") if timer.get("state") in OBSERVABLE else None
    if (
        gateway.policy.allow_local_start
        or journal.get("managed_mode") != {"mode": "app_schedule", "spec_digest": spec_digest, "local_start": False}
        or not isinstance(intent, dict)
        or set(intent) != {"mode", "revision"}
        or intent.get("mode") != "paused"
        or type(intent.get("revision")) is not int
        or not 0 <= intent["revision"] < 2**53
        or journal.get("managed_control_fault")
        or journal.get("trial_preservation_fault")
        or journal.get("trial_inventory_fault")
        or journal.get("managed_current_change", {}).get("state") in ("pending", "uncertain", "interrupted")
        or journal.get("managed_test_charge", {}).get("state")
        in ("armed", "starting", "awaiting_power", "charging", "running", "stopping")
        or journal.db.execute(
            "SELECT 1 FROM managed_requests WHERE state IN ('accepted','running') AND action!='pause' "
            "AND NOT (action='temporary_schedule' AND id IS ? AND revision IS ? AND state='accepted') LIMIT 1",
            (timer_id, timer.get("revision")),
        ).fetchone()
        or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or journal.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
        or journal.db.execute(
            "SELECT 1 FROM outbox WHERE state!='queued' OR action NOT IN "
            "('StatusNotification','MeterValues','FirmwareStatusNotification','DiagnosticsStatusNotification') LIMIT 1"
        ).fetchone()
    ):
        raise RecoveryRequiredError("local recovery requires paused managed authority and reconciled lifecycle facts")
    return fingerprint(
        {
            "intent": intent,
            "mode": journal.get("managed_mode"),
            "current_capability": journal.get("managed_current_capability"),
            "current_change": journal.get("managed_current_change"),
            "test": journal.get("managed_test_charge"),
            "temporary_schedule": timer,
        }
    )
