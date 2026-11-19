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

OWNER_PAUSE = "managed_owner_pause"
OWNER_STOP = "managed_owner_stop"


@dataclass(frozen=True)
class ControlIntent:
    """A monotonic choice survives runtime replacement and invalidates older positive operations."""

    mode: str
    revision: int

    # Domain changes advance revision; legacy read recovery may retain a mode it already requested.
    def next(self, mode: str, *, coalesce: bool = False) -> ControlIntent:
        return ControlIntent(mode, self.revision + int(not coalesce or self.mode != mode))


# Apply the shared revision transition inside the caller's original transaction, without creating request work.
def write_intent(journal: Journal, current: ControlIntent, mode: str, *, coalesce: bool = False) -> ControlIntent:
    intent = current.next(mode, coalesce=coalesce)
    journal.put("managed_control_intent", {"mode": intent.mode, "revision": intent.revision})
    return intent


@dataclass(frozen=True)
class PauseClaim:
    """An owner's persisted generation and, when complete, the exact ticket it claimed."""

    revision: int | None
    ticket: str | None
    complete: bool

    # A historical generation alone can denote precedence but cannot prove ownership of a physical hold.
    def owns(self, intent: ControlIntent, ticket: object) -> bool:
        return self.complete and intent.mode == "paused" and self.revision == intent.revision and self.ticket == ticket


# Keep the same journal format and distinguish a partial historical marker from an exact ticket.
def read_pause_claim(journal: Journal, *, stopping: bool = False) -> PauseClaim:
    record = journal.get(OWNER_STOP if stopping else OWNER_PAUSE, {})
    if not isinstance(record, dict):
        return PauseClaim(None, None, False)
    revision, ticket = record.get("revision"), record.get("ticket")
    generation = revision if type(revision) is int else None
    claim_ticket = ticket if isinstance(ticket, str) and ticket else None
    return PauseClaim(generation, claim_ticket, set(record) == {"revision", "ticket"} and claim_ticket is not None)


@dataclass(frozen=True)
class PauseOwnership:
    """The current desired mode and the owner's saved Pause."""

    intent: ControlIntent
    pause: PauseClaim

    # A completed owner Pause still takes precedence over work admitted under an older generation.
    @property
    def requested(self) -> bool:
        return self.intent.mode == "paused" and self.pause.revision == self.intent.revision

    # A Start can be admitted under an existing Pause, so equal revisions do not establish a newer request.
    def supersedes(self, operation_revision: object) -> bool:
        return self.requested and type(operation_revision) is int and 0 <= operation_revision < self.intent.revision

    # A physical Pause hold must still belong to the current intent and exact control ticket.
    def owns_ticket(self, ticket: object) -> bool:
        return self.pause.owns(self.intent, ticket)


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
        if action == "app_control" and (current.revision != revision or self.return_in_progress()):
            raise RecoveryRequiredError("return requires the current revision and no pending return")
        result = {"accepted_at": time.time()}
        with self.journal.atomic():
            if action == "pause":
                # One newest restrictive receipt survives even a legacy full ordinary pool; all prior UUID rows remain.
                self.journal.db.execute(
                    "UPDATE managed_requests SET state='failed',result=? "
                    "WHERE action='pause' AND state IN ('accepted','running','uncertain')",
                    (json.dumps({"classification": "superseded_by_pause", "superseded_by": identifier}),),
                )
                write_intent(self.journal, current, "paused")
                if on_pause is not None:
                    on_pause()
                # A new restrictive intent revokes an old Return, including one whose reply was lost at replacement.
                self.journal.db.execute(
                    "UPDATE managed_requests SET state='failed',result=? "
                    "WHERE action='app_control' AND (state IN ('accepted','running') "
                    "OR state='uncertain' AND revision<=?)",
                    (json.dumps({"classification": "superseded_by_pause"}), current.revision),
                )
            self.journal.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'accepted',?)",
                (identifier, action, revision, json.dumps(result)),
            )
        return {"id": identifier, "action": action, "state": "accepted", "result": result}

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

    # Read precedence from the current journal rather than reviving a ticket remembered before an await.
    def pause_ownership(self) -> PauseOwnership:
        return PauseOwnership(self.intent(), read_pause_claim(self.journal))

    # A protective Stop may hold a separate ticket while the explicit Pause still owns the desired intent.
    def stop_owns_ticket(self, ticket: object) -> bool:
        return read_pause_claim(self.journal, stopping=True).owns(self.intent(), ticket)

    # Bind Pause or Stop to the transaction that took its physical claim.
    def record_pause_claim(self, intent: ControlIntent, ticket: str, *, stopping: bool = False) -> None:
        if not self.journal.db.in_transaction or self.intent() != intent or intent.mode != "paused":
            raise RecoveryRequiredError("Pause claim requires the current paused intent in its owning transaction")
        self.journal.put(OWNER_STOP if stopping else OWNER_PAUSE, {"ticket": ticket, "revision": intent.revision})

    # Retry Stop only for the still-current explicit Pause, including its replacement-uncertain receipt.
    def pending_pause(self) -> bool:
        return (
            self.pause_ownership().requested
            and self.journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE action='pause' AND state IN ('accepted','uncertain') LIMIT 1"
            ).fetchone()
            is not None
        )

    # Admission remains exclusive after the selected Return starts awaiting real charger evidence.
    def return_in_progress(self, revision: int | None = None) -> bool:
        query = "SELECT 1 FROM managed_requests WHERE action='app_control' AND state IN ('accepted','running')"
        if revision is not None:
            query += " AND revision=?"
        return (
            self.journal.db.execute(query + " LIMIT 1", () if revision is None else (revision,)).fetchone() is not None
        )

    # The accepted Start joins app intent and old Pause supersession to its wire child's publication transaction.
    def start_published(self, identifier: str, expected: ControlIntent, epoch: int) -> None:
        self._publish_app(expected)
        self.journal.put("managed_owner_epoch", epoch)
        self.journal.db.execute(
            "UPDATE managed_requests SET state='failed',result=? "
            "WHERE action='pause' AND state IN ('accepted','uncertain')",
            (json.dumps({"classification": "superseded_by_start", "superseded_by": identifier}),),
        )

    # Test-stop revocation follows its caller's induced Pause and claim, without consuming replacement uncertainty.
    def charge_stopped(self) -> None:
        self.journal.db.execute(
            "UPDATE managed_requests SET state='failed',result=? WHERE action='app_control' "
            "AND state IN ('accepted','running')",
            (json.dumps({"classification": "superseded_by_test_stop"}),),
        )

    # A paused release belongs to its exact current Return; automatic app release has no Return request.
    def return_current(self, identifier: str | None, expected: ControlIntent) -> bool:
        if self.intent() != expected:
            return False
        if expected.mode != "paused":
            return identifier is None
        row = self.journal.db.execute(
            "SELECT action,revision,state FROM managed_requests WHERE id=?", (identifier,)
        ).fetchone()
        return row is not None and tuple(row) in (
            ("app_control", expected.revision, "accepted"),
            ("app_control", expected.revision, "running"),
        )

    # The owner claims Return immediately before launch; busy or stopping polls leave it accepted.
    def return_started(self, identifier: str) -> None:
        self.journal.db.execute(
            "UPDATE managed_requests SET state='running' WHERE id=? AND state='accepted'", (identifier,)
        )

    # Waiting never replays a claimed positive attempt or changes a newer superseding receipt.
    def return_waiting(self, identifier: str, expected: ControlIntent, *, unconfirmed: bool = False) -> None:
        if unconfirmed:
            self.finish(identifier, "uncertain", {"classification": "return_not_confirmed"})
        elif self.intent() == expected:
            self.journal.db.execute(
                "UPDATE managed_requests SET state='accepted' WHERE id=? AND state='running'", (identifier,)
            )

    # Actual release publishes app intent in its child transaction; legacy healthy completion changes no intent.
    def returned(
        self, identifier: str, expected: ControlIntent | None = None, before_finish: Callable[[], None] | None = None
    ) -> None:
        if expected is not None:
            self._publish_app(expected)
        if before_finish is not None:
            before_finish()
        self.finish(identifier, "completed", {"app_control_enabled": True, "charging_started": False})

    # Positive intent publication joins the exact current owner's transaction and grants nothing across an await.
    def _publish_app(self, expected: ControlIntent) -> None:
        if not self.journal.db.in_transaction or self.intent() != expected:
            raise RecoveryRequiredError("control intent publication requires its current owning transaction")
        write_intent(self.journal, expected, "app")

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
