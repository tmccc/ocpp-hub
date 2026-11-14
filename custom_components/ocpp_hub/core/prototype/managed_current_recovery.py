"""Recognize the historical current/tick collision without treating arbitrary owner faults as recoverable."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .managed_current import current_failure, managed_current_ceiling
from .native_bounded import NativeBoundedSpec
from .store import Journal

if TYPE_CHECKING:
    from .managed import ManagedController

FAULT = "owner_control_or_delivery_uncertain"
INTERRUPTED = "managed_current_interrupted_fault"


# Preserve a real accepted app update before startup replaces its receipt and pending-operation metadata.
def capture_interrupted_collision(j: Journal, spec: NativeBoundedSpec) -> None:
    fault, change = j.get("managed_control_fault", {}), j.get("managed_current_change", {})
    if (
        fault.get("classification") != FAULT
        or j.get(INTERRUPTED) is not None
        or change.get("state") != "pending"
        or change.get("mode") != "app"
        or managed_current_ceiling(j, spec) != 32
        or type(change.get("previous_a")) is not int
        or not 6 <= change["previous_a"] <= 32
    ):
        return
    row = j.db.execute(
        "SELECT r.rowid,r.action,r.revision,r.state,r.result,c.current_a FROM managed_requests r "
        "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
        (change.get("id"),),
    ).fetchone()
    intent, control, pause = j.get("managed_control_intent", {}), j.get("control", {}), j.get("managed_owner_pause", {})
    if (
        row is None
        or row["action"] != "set_current"
        or row["state"] not in ("accepted", "running")
        or row["current_a"] != change.get("current_a")
        or not (
            intent == {"mode": "app", "revision": row["revision"] + 1}
            or intent.get("mode") == "paused"
            and intent.get("revision", -1) > row["revision"] + 1
            and pause.get("revision") == intent["revision"]
            and bool(pause.get("ticket"))
            and control.get("owner") == "local"
            and control.get("inhibit") is True
        )
    ):
        return
    j.put(INTERRUPTED, {"fault": fault, "change": change, "request": dict(row), "previous_a": change["previous_a"]})


# Startup may normalize an interrupted request but cannot turn a different operation into collision evidence.
def interrupted_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    j = c.session.journal
    saved, change = j.get(INTERRUPTED, {}), j.get("managed_current_change", {})
    original, request = saved.get("change", {}), saved.get("request", {})
    if (
        not saved
        or saved.get("fault") != fault
        or fault.get("classification") != FAULT
        or change.get("id") != original.get("id")
        or change.get("current_a") != original.get("current_a")
        or change.get("mode") != "app"
        or change.get("previous_a") != saved.get("previous_a")
        or change.get("state") not in ("recovering", "interrupted", "cancelled")
        or change.get("state") != "cancelled"
        and change.get("recovery_reason") != "process_replaced"
    ):
        return None
    row = j.db.execute(
        "SELECT r.rowid,r.action,r.revision,r.state,r.result,c.current_a FROM managed_requests r "
        "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
        (change.get("id"),),
    ).fetchone()
    if (
        row is None
        or any(row[key] != request.get(key) for key in ("rowid", "action", "revision", "current_a"))
        or row["state"] not in ("uncertain", "failed")
        or json.loads(row["result"]).get("classification") != "process_replaced"
    ):
        return None
    return {
        "change": change,
        "failure": current_failure(j, change),
        "request": dict(row),
        "previous_a": saved["previous_a"],
        "interrupted": saved,
    }


# Match the exact durable failed request; independent closed-zero reconciliation still has to settle every debt.
def legacy_current_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    j = c.session.journal
    if managed_current_ceiling(j, c.native.spec) == 32:
        if saved := interrupted_evidence(c, fault):
            return saved
    change = j.get("managed_current_change", {})
    failure = current_failure(j, change)
    if (
        fault.get("classification") != FAULT
        or failure is None
        or managed_current_ceiling(j, c.native.spec) != 32
        or change.get("state") != "uncertain"
        or type(change.get("current_a")) is not int
        or not 6 <= change["current_a"] <= 32
        or (failure.get("type"), failure.get("detail"))
        not in (
            ("UnavailableError", "current update was superseded by another control operation"),
            ("AuditSupersededError", "a newer native observation superseded this read"),
            ("AuditSupersededError", "native bounded audit was superseded by local authority"),
        )
    ):
        return None
    row = j.db.execute(
        "SELECT r.rowid,r.action,r.state,r.result,c.current_a FROM managed_requests r "
        "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
        (change.get("id"),),
    ).fetchone()
    if (
        row is None
        or row["action"] != "set_current"
        or row["state"] not in ("failed", "uncertain")
        or row["current_a"] != change["current_a"]
        or json.loads(row["result"]).get("classification") != "current_unconfirmed"
    ):
        return None
    # Old uncertain records discarded previous_a. Only an earlier completed selection can replace commissioned six.
    previous = change.get("previous_a")
    if previous is None:
        completed = j.db.execute(
            "SELECT c.current_a FROM managed_current_requests c JOIN managed_requests r ON r.id=c.id "
            "WHERE r.action='set_current' AND r.state='completed' AND r.rowid<? ORDER BY r.rowid DESC LIMIT 1",
            (row["rowid"],),
        ).fetchone()
        previous = completed["current_a"] if completed is not None else 6
    if type(previous) is not int or not 6 <= previous <= 32:
        return None
    return {"change": change, "failure": failure, "request": dict(row), "previous_a": previous}
