"""Durable operator intent and bounded idempotent management receipts, independent of physical command delivery."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .security import SecurityError
from .store import Journal, RecoveryRequiredError

if TYPE_CHECKING:
    from .recovery_contracts import RestoreProgress

OWNER_PAUSE = "managed_owner_pause"
OWNER_STOP = "managed_owner_stop"
OFFLINE_PAUSE = "managed_offline_pause"
RELEASE_PAUSE_ACK = "managed_release_pause_acknowledgement"
PAUSE_RELEASE_PROOF = "_release_ack"
RETURN_OUTCOME = (
    "CASE WHEN action='app_control' AND json_valid(result) THEN CASE "
    "WHEN json_type(result,'$._return')='object' "
    "THEN json_set(:outcome,'$._return',json_extract(result,'$._return')) ELSE :outcome END "
    "ELSE :outcome END"
)

type ReturnFailure = Literal["return_not_sent", "return_not_confirmed"]


# Use the same durable receipt format for each command; the domain owner supplies its acceptance evidence.
def insert_request(
    journal: Journal, identifier: str, action: str, revision: int, result: dict[str, Any], *, state: str = "accepted"
) -> None:
    journal.db.execute(
        "INSERT INTO managed_requests VALUES(?,?,?,?,?)", (identifier, action, revision, state, json.dumps(result))
    )


# A worker may act only for the original outstanding command and caller revision.
def request_pending(journal: Journal, identifier: str, action: str, revision: int) -> bool:
    return (
        journal.db.execute(
            "SELECT 1 FROM managed_requests WHERE id=? AND action=? AND revision=? AND state IN ('accepted','running')",
            (identifier, action, revision),
        ).fetchone()
        is not None
    )


@dataclass(frozen=True)
class ControlIntent:
    """A monotonic choice survives runtime replacement and invalidates older positive operations."""

    mode: str
    revision: int

    # Domain changes advance revision; legacy read recovery may retain a mode it already requested.
    def next(self, mode: str, *, coalesce: bool = False) -> ControlIntent:
        return ControlIntent(mode, self.revision + int(not coalesce or self.mode != mode))


# Recognize the exact saved intent without opening a request owner or guessing a historical value.
def decode_intent(value: object) -> ControlIntent:
    if (
        not isinstance(value, dict)
        or set(value) != {"mode", "revision"}
        or value["mode"] not in ("app", "paused")
        or type(value["revision"]) is not int
        or not 0 <= value["revision"] < 2**53
    ):
        raise SecurityError("managed control intent is malformed")
    return ControlIntent(value["mode"], value["revision"])


# Use the current Pause receipt when available and keep reading acknowledgements saved by older versions.
def saved_pause_ack(journal: Journal) -> dict[str, Any] | None:
    intent = journal.get("managed_control_intent")
    if intent is None:
        legacy = journal.get(RELEASE_PAUSE_ACK)
        return legacy if isinstance(legacy, dict) else None
    try:
        current = decode_intent(intent)
    except SecurityError:
        return None
    if current.mode != "paused":
        return None
    row = journal.db.execute(
        "SELECT id,result FROM managed_requests WHERE action='pause' "
        "AND state IN ('accepted','uncertain','completed') ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    try:
        result = json.loads(row["result"])
    except json.JSONDecodeError:
        return None
    if not isinstance(result, dict):
        return None
    if PAUSE_RELEASE_PROOF in result:
        ack, pause = result[PAUSE_RELEASE_PROOF], result.get("_pause")
        if (
            not isinstance(pause, dict)
            or type(pause.get("revision")) is not int
            or pause["revision"] != current.revision
            or not isinstance(pause.get("hold"), str)
            or not pause["hold"]
        ):
            return None
    else:
        ack = journal.get(RELEASE_PAUSE_ACK)
    if (
        isinstance(ack, dict)
        and ack.get("pause_request") == row["id"]
        and type(ack.get("revision")) is int
        and ack["revision"] == current.revision
    ):
        return ack
    return None


# Consume only the matching Pause when recovery settles or a strict refusal invalidates its proof.
def retire_pause_ack(journal: Journal, fault: dict[str, Any]) -> None:
    ack = saved_pause_ack(journal)
    if ack is not None and ack.get("fault") == fault and journal.get("managed_control_intent") is not None:
        journal.db.execute(
            "UPDATE managed_requests SET result=json_remove(result,?) WHERE id=? AND json_type(result,?)='object'",
            (f"$.{PAUSE_RELEASE_PROOF}", ack["pause_request"], f"$.{PAUSE_RELEASE_PROOF}"),
        )
    legacy = journal.get(RELEASE_PAUSE_ACK)
    if isinstance(legacy, dict) and legacy.get("fault") == fault:
        journal.db.execute("DELETE FROM meta WHERE key=?", (RELEASE_PAUSE_ACK,))


# Apply the shared revision transition inside the caller's original transaction, without creating request work.
def write_intent(journal: Journal, current: ControlIntent, mode: str, *, coalesce: bool = False) -> ControlIntent:
    intent = current.next(mode, coalesce=coalesce)
    journal.put("managed_control_intent", {"mode": intent.mode, "revision": intent.revision})
    if current.mode == "paused" and mode == "app":
        journal.db.execute("DELETE FROM meta WHERE key=?", (OFFLINE_PAUSE,))
    return intent


# Finish only the original live request and keep an already settled receipt intact.
def finish_request(journal: Journal, identifier: str, state: str, result: dict[str, Any]) -> None:
    journal.db.execute(
        f"UPDATE managed_requests SET state=:state,result={RETURN_OUTCOME} "
        "WHERE id=:identifier AND state IN ('accepted','running')",
        {"state": state, "outcome": json.dumps(result, sort_keys=True), "identifier": identifier},
    )


# Use the owning Pause revision for new receipts and the accepted legacy check for older unbound ones.
def verified_pause_after(
    result: object, start_revision: int, pause_revision: int, prior_revision: int, fault_at: float
) -> bool:
    if not isinstance(result, dict) or result.get("pause_verified") is not True:
        return False
    completed = result.get("completed_at")
    if not isinstance(completed, (int, float)) or isinstance(completed, bool) or not 0 < completed <= fault_at:
        return False
    if "_pause" not in result:
        return 0 <= start_revision < pause_revision < prior_revision
    binding = result["_pause"]
    if not isinstance(binding, dict) or set(binding) != {"revision", "local", "hold"}:
        return False
    revision, local, hold = binding["revision"], binding["local"], binding["hold"]
    return (
        type(revision) is int
        and 0 <= start_revision < revision <= prior_revision
        and (local is None or type(local) is int and local > 0)
        and isinstance(hold, str)
        and bool(hold)
    )


# Recognise an old timed Start only when the saved history repair proves its exact later, verified Pause.
def resolved_timed_starts(
    db: sqlite3.Connection, proof: object, revision: int, active_test_id: object
) -> frozenset[str]:
    if not isinstance(proof, dict):
        return frozenset()
    prior = decode_intent(proof.get("prior_intent"))
    fault, entries = proof.get("prior_fault"), proof.get("superseded_test_proofs")
    if not isinstance(fault, dict) or not isinstance(entries, list):
        return frozenset()
    fault_at, repaired_at = fault.get("at_epoch"), proof.get("at_epoch")
    if (
        fault.get("classification") != "native_authority_or_preservation_failed"
        or not isinstance(fault_at, (int, float))
        or not isinstance(repaired_at, (int, float))
        or isinstance(fault_at, bool)
        or isinstance(repaired_at, bool)
        or not 0 < fault_at <= repaired_at <= time.time() + 2
        or prior.revision >= revision
    ):
        return frozenset()
    resolved: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            return frozenset()
        request, pause = entry.get("request_id"), entry.get("verified_pause_id")
        if (
            not isinstance(request, str)
            or not isinstance(pause, str)
            or request == active_test_id
            or request in resolved
        ):
            return frozenset()
        row = db.execute(
            "SELECT start.result, pause.result, start.revision, pause.revision "
            "FROM managed_requests start JOIN managed_requests pause ON pause.id=? "
            "WHERE start.id=? AND start.action='start_test' AND start.state='uncertain' "
            "AND pause.action='pause' AND pause.state='completed' AND start.rowid<pause.rowid",
            (pause, request),
        ).fetchone()
        if row is None or json.loads(row[0]) != {"classification": "start_not_confirmed"}:
            return frozenset()
        if not verified_pause_after(json.loads(row[1]), row[2], row[3], prior.revision, fault_at):
            return frozenset()
        resolved.add(request)
    return frozenset(resolved)


# Keep live owner debt separate from a completed historical proof, without rewriting either receipt.
def unfinished_owner_requests(journal: Journal, *, include_pause: bool = False) -> list[sqlite3.Row]:
    rows = journal.db.execute(
        "SELECT * FROM managed_requests WHERE (action!='pause' OR ?) "
        "AND state IN ('accepted','running','uncertain') ORDER BY rowid DESC",
        (include_pause,),
    ).fetchall()
    proof = journal.get("native_start_attempt_recovery")
    if not rows or proof is None:
        return rows
    resolved = resolved_timed_starts(
        journal.db,
        proof,
        decode_intent(journal.get("managed_control_intent")).revision,
        journal.get("managed_test_charge", {}).get("id"),
    )
    return [row for row in rows if row["id"] not in resolved]


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
    return decode_pause_claim(journal.get(OWNER_STOP if stopping else OWNER_PAUSE, {}))


# Read an existing marker without needing an active journal owner.
def decode_pause_claim(record: object) -> PauseClaim:
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


@dataclass(frozen=True)
class PauseRequest:
    """The exact operator request behind a persistent Pause."""

    identifier: str
    revision: int
    local: int | None
    hold: str


@dataclass(frozen=True)
class ReturnSettlement:
    """The exact Return's receipt decision can be retried without repeating its physical work."""

    identifier: str
    intent: ControlIntent
    failure: ReturnFailure | None = None
    await_cloud: bool = False

    # An exception can be final failure only when the positive command definitely did not go out.
    @classmethod
    def failed(cls, identifier: str, intent: ControlIntent, *, possibly_sent: bool) -> ReturnSettlement:
        return cls(identifier, intent, failure="return_not_confirmed" if possibly_sent else "return_not_sent")

    # Interpret all release progress for the original Return; shared publication already completed the request.
    @classmethod
    def observed(
        cls, identifier: str, intent: ControlIntent, progress: RestoreProgress
    ) -> tuple[ReturnSettlement | None, RestoreProgress]:
        from .recovery_contracts import Prerequisite, RestoreProgress, RestoreState
        from .recovery_policy import PreflightDisposition, preflight_disposition

        if progress.shared:
            return None, progress
        operation, pending = progress.operation, progress.pending
        if progress.state is RestoreState.REQUEST_NOT_SENT:
            return cls.failed(identifier, intent, possibly_sent=False), progress
        await_cloud = False
        if (operation is None or not operation.claimed) and pending is not None:
            if preflight_disposition("explicit", pending) is PreflightDisposition.NOT_SENT:
                return cls.failed(identifier, intent, possibly_sent=False), RestoreProgress(
                    RestoreState.REQUEST_NOT_SENT, pending, operation
                )
            await_cloud = pending.reason is Prerequisite.CLOUD_ACK
        return cls(identifier, intent, await_cloud=await_cloud), progress


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
        # Keep the original Return-to-wire link while withdrawing its unfinished authority on replacement.
        journal.db.execute(
            f"UPDATE managed_requests SET state='uncertain',result={RETURN_OUTCOME} "
            "WHERE action IN ('app_control','start','start_test','arm_cloud_test','set_current') "
            "AND state IN ('accepted','running')",
            {"outcome": json.dumps({"classification": "process_replaced"})},
        )

    # Corrupted intent never becomes an implicit permission to release the zero hold.
    def intent(self) -> ControlIntent:
        return decode_intent(self.journal.get("managed_control_intent"))

    # Historical UUID results remain immutable identities; only unresolved work consumes the bounded ordinary pool.
    def has_capacity(self) -> bool:
        count = self.journal.db.execute(
            "SELECT COUNT(*) FROM managed_requests WHERE state IN ('accepted','running','uncertain')"
        ).fetchone()[0]
        return count < 4095 or len(unfinished_owner_requests(self.journal, include_pause=True)) < 4095

    # Duplicate request identifiers bind both the action and caller's observed revision; no retry repeats a write.
    def existing(self, identifier: str, action: str, revision: int) -> dict[str, Any] | None:
        if re.fullmatch(r"[a-f0-9]{32}", identifier) is None or action not in (
            "pause",
            "app_control",
            "start",
            "start_test",
            "arm_cloud_test",
            "set_current",
            "stop_session",
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
        self,
        identifier: str,
        action: str,
        revision: int,
        on_pause: Callable[[], None] | None = None,
        *,
        capture_release_proof: Callable[[], dict[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        existing = self.existing(identifier, action, revision)
        if existing is not None:
            return existing
        current = self.intent()
        if action != "pause" and not self.has_capacity():
            raise RecoveryRequiredError("management receipt capacity requires operator maintenance")
        if action == "app_control" and (current.revision != revision or self.return_in_progress()):
            raise RecoveryRequiredError("return requires the current revision and no pending return")
        accepted_at = time.time()
        result: dict[str, Any] = {"accepted_at": accepted_at}
        with self.journal.atomic():
            if action == "pause":
                # One newest restrictive receipt survives even a legacy full ordinary pool; all prior UUID rows remain.
                self.journal.db.execute(
                    "UPDATE managed_requests SET state='failed',result=? "
                    "WHERE action='pause' AND state IN ('accepted','running','uncertain')",
                    (json.dumps({"classification": "superseded_by_pause", "superseded_by": identifier}),),
                )
                write_intent(self.journal, current, "paused")
                self.journal.db.execute("DELETE FROM meta WHERE key=?", (OFFLINE_PAUSE,))
                if on_pause is not None:
                    on_pause()
                    hold = read_pause_claim(self.journal)
                    if hold.complete and hold.revision == current.revision + 1:
                        result["_pause"] = {
                            "revision": hold.revision,
                            "local": self.journal.active(),
                            "hold": hold.ticket,
                        }
                # A new restrictive intent revokes an old Return, including one whose reply was lost at replacement.
                self.journal.db.execute(
                    f"UPDATE managed_requests SET state='failed',result={RETURN_OUTCOME} "
                    "WHERE action='app_control' AND (state IN ('accepted','running') "
                    "OR state='uncertain' AND revision<=:revision)",
                    {"outcome": json.dumps({"classification": "superseded_by_pause"}), "revision": current.revision},
                )
                if "_pause" in result and capture_release_proof is not None:
                    proof = capture_release_proof()
                    if proof is not None:
                        result[PAUSE_RELEASE_PROOF] = proof
            insert_request(self.journal, identifier, action, revision, result)
        # Acceptance is already durable. An unrelated read must not prevent the command worker from launching.
        return {"id": identifier, "action": action, "state": "accepted", "result": {"accepted_at": accepted_at}}

    # Expose a redacted receipt rather than any protocol payload, identity or database row.
    def receipt(self, identifier: str, *, include_binding: bool = False) -> dict[str, Any]:
        command = self.journal.outcomes.owner(identifier)
        if command is None:
            return {"id": identifier, "state": "not_found"}
        result = command.result
        if command.action == "pause":
            result.pop("_pause", None)
            result.pop("_stop", None)
            result.pop(PAUSE_RELEASE_PROOF, None)
        if command.action == "app_control":
            result.pop("_return", None)
            result.pop("_await_cloud", None)
        target = None
        if command.action == "stop_session":
            target = result.pop("target", None)
            result.pop("_session_stop", None)
        receipt: dict[str, Any] = {"id": identifier, "action": command.action, "state": command.state, "result": result}
        if include_binding:
            receipt["revision"] = command.revision
            if command.action == "stop_session":
                receipt["target"] = target
            if command.action == "set_current":
                bound = self.journal.db.execute(
                    "SELECT current_a FROM managed_current_requests WHERE id=?", (identifier,)
                ).fetchone()
                receipt["current_a"] = bound["current_a"] if bound is not None else None
        return receipt

    # Read the current request independently of the latest physical Stop it owns.
    def pause_request(self) -> PauseRequest | None:
        row = self.journal.db.execute(
            "SELECT id,result FROM managed_requests WHERE action='pause' AND state IN ('accepted','uncertain') "
            "ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        if row is None or (saved := json.loads(row["result"]).get("_pause")) is None:
            return None  # Compatibility receipts predate the request-to-hold binding.
        revision, local, hold = saved.get("revision"), saved.get("local"), saved.get("hold")
        if type(revision) is not int or (local is not None and type(local) is not int) or not isinstance(hold, str):
            raise SecurityError("saved Pause command binding is malformed")
        return PauseRequest(row["id"], revision, local, hold)

    # The wire owner records the exact known session under the current Pause in its hold transaction.
    def bind_pause_stop(
        self, intent: ControlIntent, ticket: str, request: PauseRequest | None, local: int | None, boot: str
    ) -> None:
        if self.pause_request() != request:
            raise RecoveryRequiredError("the Pause request changed before claiming Stop")
        self.record_pause_claim(intent, ticket, stopping=True)
        if request is None:
            return
        row = self.journal.db.execute(
            "SELECT result FROM managed_requests WHERE id=?", (request.identifier,)
        ).fetchone()
        result = json.loads(row["result"])
        result["_stop"] = {"ticket": ticket, "local": local, "boot": boot}
        self.journal.db.execute(
            "UPDATE managed_requests SET result=? WHERE id=?", (json.dumps(result), request.identifier)
        )

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

    # Reject a stale accepted Return before the caller checks or dispatches release work.
    def check_return_revision(self, identifier: str, revision: int) -> ControlIntent | None:
        intent = self.intent()
        if intent.revision == revision:
            return intent
        self._finish_return(identifier, "failed", {"classification": "stale_revision"}, revision=revision)
        return None

    # A paused release belongs to its exact current Return; automatic app release has no Return request.
    def return_current(self, identifier: str | None, expected: ControlIntent) -> bool:
        if self.intent() != expected:
            return False
        if expected.mode != "paused":
            return identifier is None
        return identifier is not None and request_pending(self.journal, identifier, "app_control", expected.revision)

    # Only a revision-bound offline Pause can return automatically; older Boolean markers confer no authority.
    def offline_pause_current(self, expected: ControlIntent) -> bool:
        marker = self.journal.get(OFFLINE_PAUSE)
        return (
            type(marker) is int
            and marker == expected.revision
            and expected.mode == "paused"
            and self.intent() == expected
        )

    # The owner claims Return immediately before launch; busy or stopping polls leave it accepted.
    def return_started(self, identifier: str, expected: ControlIntent) -> bool:
        return (
            self.intent() == expected
            and self.journal.db.execute(
                "UPDATE managed_requests SET state='running' WHERE id=? AND action='app_control' "
                "AND revision=? AND state='accepted'",
                (identifier, expected.revision),
            ).rowcount
            == 1
        )

    # A release is bound to the exact verified idle hold before its wire worker can replace it.
    def bind_return(self, identifier: str, expected: ControlIntent, ticket: str, boot: str) -> None:
        if not self.return_current(identifier, expected):
            raise RecoveryRequiredError("Return no longer owns its verified idle hold")
        row = self.journal.db.execute("SELECT result FROM managed_requests WHERE id=?", (identifier,)).fetchone()
        result = json.loads(row["result"])
        result["_return"] = {"ticket": ticket, "boot": boot, "local": None}
        self.journal.db.execute("UPDATE managed_requests SET result=? WHERE id=?", (json.dumps(result), identifier))

    # A pre-send cloud wait belongs to this exact receipt across later supervision passes.
    def return_awaiting_cloud(self, identifier: str, expected: ControlIntent) -> bool:
        return (
            self.journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE id=? AND action='app_control' AND revision=? "
                "AND state IN ('accepted','running') AND json_extract(result,'$._await_cloud')=1",
                (identifier, expected.revision),
            ).fetchone()
            is not None
        )

    # The accepted Return stays queued while its Pause still owns unresolved Start or Stop evidence.
    def return_waits_for_pause(self, expected: ControlIntent) -> bool:
        j = self.journal
        control, receipt = j.get("control", {}), j.starts.receipt()
        if control.get("start_pending"):
            return True
        if receipt.get("pause_revision") == expected.revision:
            session = j.outcomes.session(receipt.get("local"))
            if (session is not None and not session.closed) or receipt.get(
                "verified_pause_revision"
            ) != expected.revision:
                return True
        ticket = control.get("ticket")
        return (self.pause_ownership().owns_ticket(ticket) or self.stop_owns_ticket(ticket)) and self.pending_pause()

    # Settle or park only the original Return. Neither outcome replays a device command.
    def settle_return(
        self,
        identifier: str,
        expected: ControlIntent,
        *,
        failure: ReturnFailure | None = None,
        await_cloud: bool = False,
    ) -> None:
        if failure is not None:
            self._finish_return(
                identifier,
                "failed" if failure == "return_not_sent" else "uncertain",
                {"classification": failure},
                revision=expected.revision,
            )
        elif self.intent() == expected:
            self.journal.db.execute(
                "UPDATE managed_requests SET state='accepted', "
                "result=CASE WHEN ? THEN json_set(result,'$._await_cloud',1) ELSE result END "
                "WHERE id=? AND action='app_control' AND revision=? AND state='running'",
                (await_cloud, identifier, expected.revision),
            )

    # Healthy app observation can close the oldest accepted legacy receipt with no authority change.
    def complete_legacy_return(self) -> None:
        try:
            pending = self.pending_return()
            if pending is not None:
                self.returned(pending[0])
        except sqlite3.Error:
            if self.journal.db.in_transaction:
                raise
            # A later healthy poll can finish this old receipt without affecting existing app authority.

    # Actual release publishes app intent in its child transaction; legacy healthy completion changes no intent.
    def returned(
        self, identifier: str, expected: ControlIntent | None = None, before_finish: Callable[[], None] | None = None
    ) -> None:
        if expected is not None:
            if not self.return_current(identifier, expected):
                raise RecoveryRequiredError("Return publication no longer owns its accepted request")
            self._publish_app(expected)
        if before_finish is not None:
            before_finish()
        result: dict[str, Any] = {"app_control_enabled": True, "charging_started": False}
        changed = self._finish_return(identifier, "completed", result, revision=expected.revision if expected else None)
        if expected is not None and changed != 1:
            raise RecoveryRequiredError("Return receipt changed before atomic publication")

    # All successful and unsuccessful Return completions preserve the original wire binding.
    def _finish_return(self, identifier: str, state: str, result: dict[str, Any], *, revision: int | None) -> int:
        return self.journal.db.execute(
            f"UPDATE managed_requests SET state=:state,result={RETURN_OUTCOME} "
            "WHERE id=:identifier AND action='app_control' AND (:revision IS NULL OR revision=:revision) "
            "AND state IN ('accepted','running')",
            {
                "identifier": identifier,
                "state": state,
                "outcome": json.dumps(result, sort_keys=True),
                "revision": revision,
            },
        ).rowcount

    # Positive intent publication joins the exact current owner's transaction and grants nothing across an await.
    def _publish_app(self, expected: ControlIntent) -> None:
        if not self.journal.db.in_transaction or self.intent() != expected:
            raise RecoveryRequiredError("control intent publication requires its current owning transaction")
        write_intent(self.journal, expected, "app")

    # Make a single operation's final outcome durable without overwriting a newer superseding receipt.
    def finish(self, identifier: str, state: str, result: dict[str, Any]) -> None:
        finish_request(self.journal, identifier, state, result)

    # After verified closed zero, save the matching hold and Start evidence before publishing Pause success.
    def complete_verified_pause(self) -> None:
        j = self.journal
        try:
            with j.atomic():
                control, receipt = j.get("control", {}), j.starts.receipt()
                ownership = self.pause_ownership()
                ticket = control.get("ticket")
                if control.get("uncertain") and (
                    ownership.owns_ticket(ticket)
                    or self.stop_owns_ticket(ticket)
                    or receipt.get("stage") in ("unsent", "rejected")
                    and receipt.get("settled_ticket") == ticket
                    or receipt.get("bound_ticket") == ticket
                    and j.db.execute(
                        "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (receipt.get("local"),)
                    ).fetchone()
                ):
                    j.control.complete(control["ticket"], uncertain=False)
                pause_revision = self.intent().revision
                if (
                    receipt.get("pause_revision") == pause_revision
                    and receipt.get("verified_pause_revision") != pause_revision
                ):
                    j.starts.pause_verified(receipt, pause_revision)
                self.confirm_pauses()
        except sqlite3.Error as error:
            if j.db.in_transaction:
                raise
            # The saved hold lets the next poll verify zero and retry the receipt without another Stop.
            raise RecoveryRequiredError("verified Pause awaits receipt publication") from error

    # A fresh physical zero proof completes the current Pause. Old unbound receipts keep their historical behavior.
    def confirm_pauses(self) -> None:
        request = self.pause_request()
        result = {"pause_verified": True, "completed_at": time.time()}
        if request is not None:
            intent, claim = self.intent(), read_pause_claim(self.journal)
            if (
                intent.mode != "paused"
                or not claim.complete
                or claim.ticket != request.hold
                or claim.revision != request.revision
                or request.revision > intent.revision
            ):
                raise RecoveryRequiredError("verified zero no longer belongs to the accepted Pause request")
            row = self.journal.db.execute(
                "SELECT result FROM managed_requests WHERE id=?", (request.identifier,)
            ).fetchone()
            prior = json.loads(row["result"])
            result.update({key: prior[key] for key in ("_pause", "_stop", PAUSE_RELEASE_PROOF) if key in prior})
            self.journal.db.execute(
                "UPDATE managed_requests SET state='completed',result=? "
                "WHERE id=? AND state IN ('accepted','uncertain')",
                (json.dumps(result), request.identifier),
            )
            return
        self.journal.db.execute(
            "UPDATE managed_requests SET state='completed',result=? "
            "WHERE action='pause' AND state IN ('accepted','uncertain') AND json_extract(result,'$._pause') IS NULL",
            (json.dumps(result),),
        )
