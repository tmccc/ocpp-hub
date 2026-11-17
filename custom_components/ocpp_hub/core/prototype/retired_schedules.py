"""Read old one-off evidence without reviving it or modifying its journal."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from .store import RecoveryRequiredError

_MESSAGE = "unfinished retired one-off schedule requires its original recovery implementation"
_IDENTITY = (
    "id",
    "revision",
    "accepted_epoch",
    "start_epoch",
    "end_epoch",
    "floor",
    "body",
    "install",
    "original_digest",
    "augmented_digest",
)


# Refuse old physical/source obligations before normal journal initialization can normalize their evidence.
def schedule_history(db: sqlite3.Connection) -> frozenset[int]:
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    values: dict[str, Any] = {}
    if "meta" in tables:
        try:
            values = {
                key: json.loads(value)
                for key, value in db.execute(
                    "SELECT key,value FROM meta "
                    "WHERE key IN ('managed_temporary_schedule','managed_temporary_occurrences')"
                )
            }
        except ValueError, TypeError:
            raise RecoveryRequiredError(_MESSAGE) from None
    current = values.get("managed_temporary_schedule", {})
    archive = values.get("managed_temporary_occurrences", {})
    if not isinstance(current, dict) or not isinstance(archive, dict):
        raise RecoveryRequiredError(_MESSAGE)
    records = dict(archive)
    for key, lease in records.items():
        if not isinstance(key, str) or not key or not isinstance(lease, dict) or lease.get("id") != key:
            raise RecoveryRequiredError(_MESSAGE)
    if current:
        key = current.get("id")
        if not isinstance(key, str) or not key:
            raise RecoveryRequiredError(_MESSAGE)
        previous = records.get(key, {})
        if any(previous[name] != current[name] for name in _IDENTITY if name in previous and name in current):
            raise RecoveryRequiredError(_MESSAGE)
        # The working occurrence is newer than its archived snapshot; never revive the stale copy.
        records[key] = current
    identifiers: set[int] = set()
    for lease in records.values():
        if not isinstance(lease, dict) or lease.get("state") != "complete":
            raise RecoveryRequiredError(_MESSAGE)
        discharged = (
            lease.get("result") in ("cancelled_before_install", "session_closed")
            or lease.get("retire_reason") == "session_closed"
            or lease.get("idle_cutoff_released") in ("owner_return", "session_closed", "observed_closure")
        )
        if not discharged or any(
            lease.get(key)
            for key in (
                "idle_cutoff_hold",
                "recovery_session",
                "closure_gap",
                "unreported_native",
                "pending_native_reports",
                "temporary_attempts",
                "unidentified_local",
            )
        ):
            raise RecoveryRequiredError(_MESSAGE)
        body = lease.get("body")
        identifier = body.get("planId") if isinstance(body, dict) else None
        if type(identifier) is not int or identifier < 1:
            raise RecoveryRequiredError(_MESSAGE)
        identifiers.add(identifier)
    if (
        "managed_requests" in tables
        and db.execute(
            "SELECT 1 FROM managed_requests WHERE action='temporary_schedule' "
            "AND state IS NOT 'completed' AND state IS NOT 'failed' LIMIT 1"
        ).fetchone()
    ):
        raise RecoveryRequiredError(_MESSAGE)
    if (
        "managed_pending_native_reports" in tables
        and db.execute("SELECT 1 FROM managed_pending_native_reports LIMIT 1").fetchone()
    ):
        raise RecoveryRequiredError(_MESSAGE)
    if "managed_unnumbered_reports" in tables:
        if "transactions" not in tables:
            pending = db.execute("SELECT 1 FROM managed_unnumbered_reports LIMIT 1").fetchone()
        else:
            pending = db.execute(
                "SELECT 1 FROM managed_unnumbered_reports u LEFT JOIN transactions t ON t.local=u.local "
                "WHERE t.local IS NULL OR t.stop IS NULL LIMIT 1"
            ).fetchone()
        if pending:
            raise RecoveryRequiredError(_MESSAGE)
    if (
        "trial_resources" in tables
        and db.execute(
            "SELECT 1 FROM trial_resources WHERE key LIKE 'plan:%' AND state IS NOT 'absent_verified' LIMIT 1"
        ).fetchone()
    ):
        raise RecoveryRequiredError(_MESSAGE)
    return frozenset(identifiers)
