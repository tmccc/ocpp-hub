"""Durable operator intent and bounded idempotent management receipts, independent of physical command delivery."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .security import SecurityError
from .store import Journal, RecoveryRequiredError


@dataclass(frozen=True)
class ControlIntent:
    """A monotonic choice survives runtime replacement and invalidates older positive operations."""

    mode: str
    revision: int


class ManagedRequests:
    """A paused intent is recorded before its effect; a return remains paused until physically confirmed."""

    # Create only after native admission, without resetting existing records or accepting malformed mode metadata.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_requests("
            "id TEXT PRIMARY KEY,action TEXT NOT NULL,revision INTEGER NOT NULL,"
            "state TEXT NOT NULL,result TEXT NOT NULL)"
        )
        journal.db.execute("CREATE INDEX IF NOT EXISTS managed_requests_state ON managed_requests(state)")
        if journal.get("managed_control_intent") is None:
            journal.put("managed_control_intent", {"mode": "app", "revision": 0})
        self.intent()
        journal.db.execute(
            "UPDATE managed_requests SET state='uncertain',result=? "
            "WHERE action IN ('app_control','start','start_test','arm_cloud_test','set_current') "
            "AND state IN ('accepted','running')",
            (json.dumps({"classification": "process_replaced"}),),
        )

    # Corrupted intent never becomes an implicit permission to release the zero hold.
    def intent(self) -> ControlIntent:
        value = self.journal.get("managed_control_intent")
        if (
            not isinstance(value, dict)
            or set(value) != {"mode", "revision"}
            or value["mode"] not in ("app", "paused")
            or type(value["revision"]) is not int
            or not 0 <= value["revision"] < 2**53
        ):
            raise SecurityError("managed control intent is malformed")
        return ControlIntent(value["mode"], value["revision"])

    # Historical UUID results remain immutable identities; only unresolved work consumes the bounded ordinary pool.
    def has_capacity(self) -> bool:
        return bool(
            self.journal.db.execute(
                "SELECT COUNT(*) FROM managed_requests WHERE state IN ('accepted','running','uncertain')"
            ).fetchone()[0]
            < 4095
        )

    # Duplicate request identifiers bind both the action and caller's observed revision; no retry repeats a write.
    def existing(self, identifier: str, action: str, revision: int) -> dict[str, Any] | None:
        if re.fullmatch(r"[a-f0-9]{32}", identifier) is None or action not in (
            "pause",
            "app_control",
            "start",
            "start_test",
            "arm_cloud_test",
            "set_current",
            "temporary_schedule",
        ):
            raise SecurityError("management request shape is invalid")
        if type(revision) is not int or not 0 <= revision < 2**53:
            raise SecurityError("management revision is invalid")
        row = self.journal.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()
        if row is not None:
            if row["action"] != action or row["revision"] != revision:
                raise SecurityError("request identifier already binds another operation")
            return self.receipt(identifier)
        return None

    # Store acceptance and the desired mode together; return requests cannot replace or overtake another return.
    def accept(
        self, identifier: str, action: str, revision: int, on_pause: Callable[[], None] | None = None
    ) -> dict[str, Any]:
        existing = self.existing(identifier, action, revision)
        if existing is not None:
            return existing
        current = self.intent()
        if action != "pause" and not self.has_capacity():
            raise RecoveryRequiredError("management receipt capacity requires operator maintenance")
        if action == "app_control" and (
            current.revision != revision
            or self.journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE action='app_control' AND state IN ('accepted','running')"
            ).fetchone()
        ):
            raise RecoveryRequiredError("return requires the current revision and no pending return")
        with self.journal.atomic():
            if action == "pause":
                # One newest restrictive receipt survives even a legacy full ordinary pool; all prior UUID rows remain.
                self.journal.db.execute(
                    "UPDATE managed_requests SET state='failed',result=? "
                    "WHERE action='pause' AND state IN ('accepted','running','uncertain')",
                    (json.dumps({"classification": "superseded_by_pause", "superseded_by": identifier}),),
                )
                self.journal.put("managed_control_intent", {"mode": "paused", "revision": current.revision + 1})
                if on_pause is not None:
                    on_pause()
                self.journal.db.execute(
                    "UPDATE managed_requests SET state='failed',result=? "
                    "WHERE action='app_control' AND state IN ('accepted','running')",
                    (json.dumps({"classification": "superseded_by_pause"}),),
                )
            self.journal.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'accepted',?)",
                (identifier, action, revision, json.dumps({"accepted_at": time.time()})),
            )
        return self.receipt(identifier)

    # Expose a redacted receipt rather than any protocol payload, identity or database row.
    def receipt(self, identifier: str) -> dict[str, Any]:
        row = self.journal.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()
        if row is None:
            return {"id": identifier, "state": "not_found"}
        return {"id": identifier, "action": row["action"], "state": row["state"], "result": json.loads(row["result"])}

    # Select one accepted return; a recovered running/uncertain request is never executed a second time.
    def pending_return(self) -> tuple[str, int] | None:
        row = self.journal.db.execute(
            "SELECT id,revision FROM managed_requests WHERE action='app_control' AND state='accepted' "
            "ORDER BY rowid LIMIT 1"
        ).fetchone()
        return (row["id"], row["revision"]) if row is not None else None

    # Admission remains exclusive after the selected Return starts awaiting real charger evidence.
    def return_in_progress(self) -> bool:
        return (
            self.journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE action='app_control' AND state IN ('accepted','running') LIMIT 1"
            ).fetchone()
            is not None
        )

    # A separately authorized normal Start retires earlier Pause requests without replaying their physical effect.
    def supersede_pauses(self, identifier: str) -> None:
        self.journal.db.execute(
            "UPDATE managed_requests SET state='failed',result=? "
            "WHERE action='pause' AND state IN ('accepted','uncertain')",
            (json.dumps({"classification": "superseded_by_start", "superseded_by": identifier}),),
        )

    # Make a single operation's final outcome durable without overwriting a newer superseding receipt.
    def finish(self, identifier: str, state: str, result: dict[str, Any]) -> None:
        self.journal.db.execute(
            "UPDATE managed_requests SET state=?,result=? WHERE id=? AND state IN ('accepted','running')",
            (state, json.dumps(result, sort_keys=True), identifier),
        )

    # A fresh physical zero proof may complete every accepted Pause without reissuing its Stop effect.
    def confirm_pauses(self) -> None:
        self.journal.db.execute(
            "UPDATE managed_requests SET state='completed',result=? "
            "WHERE action='pause' AND state IN ('accepted','uncertain')",
            (json.dumps({"pause_verified": True, "completed_at": time.time()}),),
        )
