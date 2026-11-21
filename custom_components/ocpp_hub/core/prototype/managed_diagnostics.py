"""Optional read-only explanations of retained facts; these summaries grant no control or recovery permission."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from typing import TYPE_CHECKING, Any

from .managed_intent import unfinished_owner_requests
from .managed_native_repair import AUDITS, REVOCATIONS, repair_pending, repair_revoked, repair_sources
from .security import SecurityError
from .store import RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController

FAULTS = frozenset(
    {
        "owner_release_unconfirmed",
        "native_authority_or_preservation_failed",
        "measured_output_limit_exceeded",
        "station_composite_current_exceeded",
        "station_ceiling_lost",
        "owner_control_or_delivery_uncertain",
        "operator_debt_reconciled_at_zero",
        "native_observation_unavailable",
        "status_unavailable",
        "current_update_pending",
        "start_operation_pending",
        "listener_certificate_expiring",
        "temporary_schedule_cleanup_unconfirmed",
        "temporary_schedule_awaiting_start_report",
        "temporary_schedule_stop_unconfirmed",
    }
)
ACTIONS = frozenset(
    {"pause", "app_control", "start", "start_test", "arm_cloud_test", "set_current", "temporary_schedule"}
)


# Reject implausible/overflowing times without trying to reconstruct missing historical receipt timestamps.
def timestamp(value: Any) -> float | None:
    return float(value) if type(value) in (int, float) and 0 < value < 2**53 and math.isfinite(value) else None


# A worker disappearing is distinct from shielded I/O still owned by the gateway.
def worker_state(worker: Any) -> str:
    return "absent" if worker is None else "cancelled" if worker.cancelled() else "done" if worker.done() else "running"


# Protocol snapshots already contain bounded labels, counts and close codes, rather than frames or identifiers.
def protocol_snapshot(c: ManagedController) -> dict[str, Any]:
    runtime, gateway = c.session.runtime.status, c.session.gateway
    return {
        "charger": {"rpc": gateway.charger.diagnostics.snapshot(), "fault": runtime.charger_fault.snapshot()},
        "cloud": {"rpc": gateway.vendor.diagnostics.snapshot(), "fault": runtime.vendor_fault.snapshot()},
        "reconnects": runtime.reconnects,
        "connection_attempts": runtime.vendor_attempts,
        "recent_handler_failures": len(gateway.failures),
        "delivery_blocked": runtime.delivery_blocked,
        "delivery_cause": runtime.delivery_blocked_cause
        if runtime.delivery_blocked_cause in ("storage", "reconciliation")
        else None,
    }


# Exact current and verified ancestor markers matter; unrelated old audit rows cannot explain today's fault.
def fault_summary(c: ManagedController) -> tuple[dict[str, Any] | None, list[str], str]:
    journal = c.session.journal
    fault = journal.fault.read()
    if not fault:
        return None, [], "not_assessed"
    sources = repair_sources(journal, fault)
    for name in (AUDITS, REVOCATIONS):
        markers = journal.get(name, {})
        if not isinstance(markers, dict):
            raise ValueError("diagnostic audit metadata is unavailable")
        for source in sources:
            key = fingerprint(source)
            if key not in markers:
                continue
            marker = markers[key]
            if (
                not isinstance(marker, dict)
                or type(marker.get("version")) is not int
                or marker.get("version") != 1
                or marker.get("fault") != source
                or not isinstance(marker.get("attempt"), str)
                or len(marker["attempt"]) != 32
                or any(character not in "0123456789abcdef" for character in marker["attempt"])
            ):
                raise ValueError("diagnostic audit identity is unavailable")
            if name == REVOCATIONS:
                refusal = marker.get("typed_refusal")
                if not isinstance(refusal, dict) or refusal.get("stage") not in (
                    "identity",
                    "content",
                    "configuration",
                    "history",
                    "resources",
                ):
                    raise ValueError("diagnostic refusal evidence is unavailable")
    first = sources[-1]
    code = first.get("classification")
    summary = {"code": code if code in FAULTS else "other_fault", "first_at": timestamp(first.get("at_epoch"))}
    blockers, recovery = ["fault_latched"], "not_assessed"
    if repair_revoked(journal, fault):
        blockers.append("strict_native_refusal")
        recovery = "strict_refusal"
    if repair_pending(journal, fault):
        blockers.append("native_audit_outcome_unknown")
        if recovery != "strict_refusal":
            recovery = "audit_outcome_unknown"
    return summary, blockers, recovery


# Receipt states remain durable facts; neither a completed worker nor an old accepted time proves physical success.
def pending_commands(c: ManagedController, now: float) -> tuple[list[dict[str, Any]], int]:
    intent = c.requests.intent()
    # Once a newer app intent won, old uncertain Returns are receipt history rather than pending work.
    rows = [
        row
        for row in unfinished_owner_requests(c.session.journal, include_pause=True)
        if not (
            intent.mode == "app"
            and row["action"] == "app_control"
            and row["state"] == "uncertain"
            and row["revision"] < intent.revision
        )
    ]
    commands = []
    for row in rows[:5]:
        result = json.loads(row["result"])
        if not isinstance(result, dict):
            raise ValueError("diagnostic receipt metadata is unavailable")
        accepted = timestamp(result.get("accepted_at"))
        age = now - accepted if accepted is not None and accepted <= now else None
        commands.append(
            {
                "action": row["action"] if row["action"] in ACTIONS else "other",
                "state": row["state"],
                "accepted_at": accepted,
                "age_seconds": age,
                "confirmation": "unknown" if row["state"] == "uncertain" else "pending",
            }
        )
    return commands, len(rows)


# This synchronous projection reads retained state only: it never calls tick, audit, reconciliation or charger I/O.
def snapshot(c: ManagedController, status: dict[str, Any], now: float | None = None) -> dict[str, Any]:
    current = time.time() if now is None else now
    try:
        if "pending_lifecycle_events" in status and status["pending_lifecycle_events"] is None:
            raise ValueError("diagnostic queue counts are unavailable")
        fault, blockers, recovery = fault_summary(c)
        commands, count = pending_commands(c, current)
        gateway, journal = c.session.gateway, c.session.journal
        for present, reason in (
            (status.get("fault") == "native_observation_unavailable", "native_observation_unavailable"),
            (status.get("fault") == "current_update_pending", "current_update_pending"),
            (status.get("fault") == "start_operation_pending", "start_operation_pending"),
            (gateway.authorisation.pending(), "cloud_start_pending"),
            (status.get("charger_connected") is False, "charger_disconnected"),
            (status.get("cloud_connected") is False, "cloud_disconnected"),
            (status.get("boot_verified") is False, "boot_unverified"),
            (
                status.get("desired_control") == "paused" and status.get("local_hold_verified") is not True,
                "pause_verification_pending",
            ),
            (bool(status.get("pending_lifecycle_events")), "lifecycle_acknowledgement_pending"),
            (bool(status.get("operator_reconciliation_required")), "command_outcome_unknown"),
            (bool(status.get("vendor_reply_storage_pending")), "vendor_reply_storage_pending"),
            (c.session.runtime.status.delivery_blocked_cause == "storage", "delivery_storage_pending"),
            (
                bool(journal.get("trial_preservation_fault") or journal.get("trial_inventory_fault")),
                "preservation_review_required",
            ),
        ):
            if present:
                blockers.append(reason)
        return {
            "schema_version": 1,
            "available": True,
            "sampled_at": current,
            "original_fault": fault,
            "current_fault": status.get("fault") if status.get("fault") in FAULTS else None,
            "blockers": blockers,
            "recovery": recovery,
            "pending_commands": commands,
            "pending_command_count": count,
            "commands_truncated": count > len(commands),
            "workers": {
                "current": worker_state(c.current.worker),
                "start": worker_state(c.charging.worker),
                "owned_io_pending": bool(gateway.controls._operations or gateway.charger._pending),
            },
            "protocol": protocol_snapshot(c),
        }
    except (
        sqlite3.Error,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        RecursionError,
        SecurityError,
        RecoveryRequiredError,
    ):
        # Optional diagnostic reads cannot suppress required settlement, restrictive work or future supervision.
        return {
            "schema_version": 1,
            "available": False,
            "sampled_at": current,
            "unavailable_reason": "evidence_unavailable",
        }
