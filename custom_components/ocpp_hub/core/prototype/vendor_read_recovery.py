"""Retire only a legacy read-only RPC marker without acknowledging any physical command."""

from __future__ import annotations

import time
from typing import Any

from .managed_cutoffs import supervised_timer
from .policy import READS
from .store import Journal


# Legacy markers contain no payload, so only fixed protocol read action names establish harmlessness.
def recover_legacy_vendor_read(journal: Journal) -> bool:
    with journal.atomic():
        marker = journal.get("uncertain_vendor_command")
        if (
            not isinstance(marker, dict)
            or set(marker) != {"action", "digest"}
            or not isinstance(marker.get("action"), str)
            or marker["action"] not in READS
            or not isinstance(marker.get("digest"), str)
            or len(marker["digest"]) != 64
            or any(character not in "0123456789abcdef" for character in marker["digest"])
        ):
            return False
        control = journal.get("control", {})
        intent = journal.get("managed_control_intent", {})
        fault = journal.get("managed_control_fault")
        mode = journal.get("managed_mode", {})
        local_start = journal.get("local_start_receipt", {})
        charge = journal.get("managed_test_charge", {})
        current = journal.get("managed_current_change", {})
        timer = journal.get("managed_temporary_schedule", {})
        timers = journal.get("managed_temporary_occurrences", {})
        if (
            not all(
                isinstance(value, dict)
                for value in (control, intent, mode, local_start, charge, current, timer, timers)
            )
            or mode.get("mode") != "app_schedule"
            or mode.get("local_start") is not False
            or set(intent) != {"mode", "revision"}
            or intent.get("mode") not in ("app", "paused")
            or type(intent.get("revision")) is not int
            or not 0 <= intent["revision"] < 2**53 - 1
            or journal.get("trial_phase") not in ("active", "recovering", "blocked")
            or fault is not None
            and not isinstance(fault, dict)
            or fault is not None
            and fault.get("classification") != "owner_control_or_delivery_uncertain"
            or journal.get("trial_preservation_fault")
            or journal.get("trial_inventory_fault")
            or journal.get("native_bounded_handover", {}).get("fault")
            or journal.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
            or control.get("start_pending")
            or journal.get("vendor_start_grant")
            or local_start.get("local") is None
            and local_start.get("stage") in ("prepared", "issued", "accepted")
            or charge.get("start_rpc_accepted") is True
            and charge.get("local_transaction") is None
            or charge.get("state") in ("armed", "starting", "awaiting_power", "charging", "running", "stopping")
            or current.get("state") not in (None, "completed", "cancelled")
            or timer.get("state") == "requested"
            or supervised_timer(timer)
            or any(
                not isinstance(value, dict) or value.get("state") == "requested" or supervised_timer(value)
                for value in timers.values()
            )
            or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            or journal.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
            or journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
        ):
            return False
        managed_requests = journal.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='managed_requests'"
        ).fetchone()
        if (
            managed_requests
            and journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE action!='pause' "
                "AND state IN ('accepted','running','uncertain') LIMIT 1"
            ).fetchone()
        ):
            return False
        migration: dict[str, Any] = {"marker": marker, "prior_fault": fault, "at_epoch": time.time()}
        journal.put("vendor_read_failure_recovery", migration)
        journal.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
        if fault is not None and fault.get("classification") == "owner_control_or_delivery_uncertain":
            journal.db.execute("DELETE FROM meta WHERE key='managed_control_fault'")
        journal.put(
            "managed_control_intent",
            {"mode": "paused", "revision": intent["revision"] + int(intent["mode"] != "paused")},
        )
        journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
        journal.put("trial_phase", "recovering")
        return True
