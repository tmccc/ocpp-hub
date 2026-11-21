"""Bounded optional Start observations and a read-only projection of the private journal."""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import sqlite3
import stat
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

from ocpp_hub_core.prototype.incident_observation import IncidentObservation
from ocpp_hub_core.prototype.managed_intent import OWNER_PAUSE, decode_intent, decode_pause_claim
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.trial_config import private_json, read_file

MAX_EVENT_BYTES = 64 * 1024
MAX_REPORT_EVENTS = 24
UNKNOWN = "unknown"
STAGES = ("prepared", "issued", "accepted", "rejected", "unsent", "reconciled")


# Accept only the engine's exact random UUID/ticket shape.
def identifier(value: object) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(r"[a-f0-9]{32}", value) else None


# Correlate local records without publishing the original request or control ticket.
def reference(value: object) -> str | None:
    original = identifier(value)
    return hashlib.sha256(original.encode("ascii")).hexdigest()[:32] if original else None


# Invalid or unavailable intent stays unknown rather than turning into a new Pause.
def intent_fields(value: object) -> dict[str, Any] | None:
    try:
        intent = decode_intent(value)
    except SecurityError:
        return None
    return {"mode": intent.mode, "revision": intent.revision}


class IncidentRecorder:
    """The controller can enqueue facts without sharing a database or waiting for optional disk writes."""

    # Use a dedicated bounded writer so a stalled destination cannot delay the owner or service shutdown.
    def __init__(self, path: Path, append: Callable[[Path, dict[str, Any]], None]) -> None:
        self.path, self.append = path, append
        self.run = uuid.uuid4().hex
        self.sequence = 0
        self.pending: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=32)
        self.closed = threading.Event()
        threading.Thread(target=self.write, name="ics-incident-observations", daemon=True).start()
        self.enqueue({"event": "process_started"})

    # Only typed, allowlisted fields enter the queue. A gap after a dropped record is visible in the sequence.
    def observe(self, observation: IncidentObservation) -> None:
        request, ticket = reference(observation.request), reference(observation.ticket)
        if request is None or ticket is None or self.closed.is_set():
            return
        intent = observation.intent
        self.enqueue(
            {
                "event": observation.kind,
                "request": request,
                "ticket": ticket,
                "intent": {"mode": intent.mode, "revision": intent.revision} if intent is not None else None,
                "pause_ticket": reference(observation.pause_ticket),
                "reply": observation.reply,
            }
        )

    # Number observations when they happen, even when the writer cannot retain one.
    def enqueue(self, record: dict[str, Any]) -> None:
        self.sequence += 1
        try:
            self.pending.put_nowait({**record, "run": self.run, "sequence": self.sequence, "at": time.time()})
        except queue.Full:
            pass

    # Drain in observation order without coupling optional file errors to the charger task.
    def write(self) -> None:
        while not self.closed.is_set() or not self.pending.empty():
            try:
                record = self.pending.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                self.append(self.path, record)
            except OSError, SecurityError:
                continue

    # Pending optional observations can be lost on termination; the journal remains the committed source.
    def close(self) -> None:
        self.closed.set()


# A malformed or truncated optional record cannot inject arbitrary fields into an export.
def event_fields(raw: bytes) -> dict[str, Any] | None:
    if len(raw) > 1024:
        return None
    try:
        row = json.loads(raw)
    except ValueError, UnicodeError, RecursionError:
        return None
    if not isinstance(row, dict):
        return None
    run = identifier(row.get("run"))
    sequence, at, event = (row.get(key) for key in ("sequence", "at", "event"))
    if not run or type(sequence) is not int or not 0 < sequence < 2**53:
        return None
    if type(at) not in (int, float) or not 0 < at < 2**53:
        return None
    common = {"run": run, "sequence": sequence, "at": at, "event": event}
    if event == "process_started":
        return common
    request, ticket = (identifier(row.get(key)) for key in ("request", "ticket"))
    reply = row.get("reply")
    if (
        not request
        or not ticket
        or event not in ("start_sent", "start_reply", "pause_committed")
        or (event == "start_reply" and reply not in ("accepted", "rejected", "call_error", "other"))
        or (event != "start_reply" and reply is not None)
    ):
        return None
    return {
        **common,
        "request": request,
        "ticket": ticket,
        "intent": intent_fields(row.get("intent")),
        "pause_ticket": identifier(row.get("pause_ticket")),
        "reply": reply,
    }


# Read at most two bounded generations and ignore incomplete appends and repeated records during rotation.
def observations(path: Path) -> list[dict[str, Any]]:
    events: deque[dict[str, Any]] = deque(maxlen=256)
    seen: set[tuple[str, int]] = set()
    for candidate in (path.with_suffix(".jsonl.1"), path):
        try:
            body = read_file(candidate, maximum=MAX_EVENT_BYTES + 1024)
        except SecurityError:
            continue
        for line in body.splitlines(keepends=True):
            parsed = event_fields(line) if line.endswith(b"\n") else None
            if parsed is not None:
                key = parsed["run"], parsed["sequence"]
                if key not in seen:
                    seen.add(key)
                    events.append(parsed)
    return list(events)


# Read small named metadata values without returning opaque payloads or entire journal rows.
def metadata(db: sqlite3.Connection, key: str) -> dict[str, Any]:
    row = db.execute("SELECT CASE WHEN length(value)<=8192 THEN value END FROM meta WHERE key=?", (key,)).fetchone()
    try:
        value = json.loads(row[0]) if row is not None and row[0] is not None else None
    except ValueError, UnicodeError, RecursionError:
        return {}
    return value if isinstance(value, dict) else {}


# Select a retained local Start by its exact management UUID, preserving unrelated historical receipts.
def retained_start(db: sqlite3.Connection) -> tuple[str | None, dict[str, Any] | None]:
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='managed_requests'").fetchone() is None:
        return None, None
    row = db.execute(
        "SELECT id,action,revision,state FROM managed_requests "
        "WHERE action IN ('start','start_test') ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    if row is None or (raw := identifier(row[0])) is None or type(row[2]) is not int or not 0 <= row[2] < 2**53:
        return None, None
    state = row[3] if row[3] in ("accepted", "running", "uncertain", "completed", "failed") else UNKNOWN
    return raw, {"request": reference(raw), "action": row[1], "revision": row[2], "state": state}


# Only the transaction named by this receipt can establish lifecycle or cloud delivery evidence.
def lifecycle_fields(db: sqlite3.Connection, local: object) -> tuple[str, str, list[dict[str, Any]]]:
    if local is None:
        return "unbound", UNKNOWN, []
    if type(local) is not int or not 0 < local < 2**63:
        return UNKNOWN, UNKNOWN, []
    row = db.execute("SELECT stop IS NOT NULL,vendor IS NOT NULL FROM transactions WHERE local=?", (local,)).fetchone()
    if row is None:
        return UNKNOWN, UNKNOWN, []
    delivery = [
        {
            "event": action,
            "state": state if state in ("queued", "inflight", "ambiguous", "rejected") else UNKNOWN,
            "reply_saved": bool(reply_saved),
        }
        for action, state, reply_saved in db.execute(
            "SELECT o.action,o.state,EXISTS(SELECT 1 FROM delivery_replies r WHERE r.token=o.token "
            "AND r.action=o.action AND r.payload=o.payload AND r.local=o.local) "
            "FROM outbox o WHERE o.local=? AND o.action IN ('StartTransaction','StopTransaction') "
            "ORDER BY o.seq LIMIT 4",
            (local,),
        )
    ]
    return "closed" if row[0] else "open", "recorded" if row[1] else "unmapped", delivery


# Use a single read-only WAL snapshot with no service lock, normalization, socket or recovery call.
def journal_facts(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"available": False, "intent": None, "request": None, "start": None}
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            return result
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.1)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            intent = intent_fields(metadata(db, "managed_control_intent"))
            claim = decode_pause_claim(metadata(db, OWNER_PAUSE))
            if intent is not None:
                intent["pause_ticket"] = (
                    reference(claim.ticket)
                    if intent["mode"] == "paused" and claim.complete and claim.revision == intent["revision"]
                    else None
                )
            wire = metadata(db, "local_start_receipt")
            owner = metadata(db, "managed_test_charge")
            original, request = retained_start(db)
            if request is not None and original == owner.get("id"):
                current = owner.get("current_a")
                if type(current) in (int, float) and 6 <= current <= 32:
                    request["selected_current_a"] = current
            start = None
            ticket = reference(wire.get("ticket"))
            stage = wire.get("stage")
            if original is not None and wire.get("request_id") == original and ticket and stage in STAGES:
                lifecycle, cloud_mapping, delivery = (
                    lifecycle_fields(db, wire["local"]) if "local" in wire else (UNKNOWN, UNKNOWN, [])
                )
                published = owner.get("start_rpc_accepted") if owner.get("id") == original else None
                start = {
                    "ticket": ticket,
                    "stage": stage,
                    "saved_reply": stage if stage in ("accepted", "rejected") else UNKNOWN,
                    "authority_published": published if type(published) is bool else None,
                    "lifecycle": lifecycle,
                    "cloud_mapping": cloud_mapping,
                    "pending_delivery": delivery,
                }
            return {"available": True, "intent": intent, "request": request, "start": start}
    except OSError, ValueError, sqlite3.Error:
        return result


# Display the previous service sample separately from the current journal and exact wire observations.
def sampled_status(path: Path) -> dict[str, Any] | None:
    try:
        sample = private_json(path)
    except SecurityError:
        return None
    when, state = sample.get("checked_at"), sample.get("state")
    if type(when) not in (int, float) or not 0 < when < 2**53:
        return None
    return {
        "checked_at": when,
        "state": state
        if state
        in (
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
        )
        else UNKNOWN,
    }


# Correlate only one request/ticket pair and the exact Pause claims associated with it.
def incident_report(journal: Path, event_path: Path) -> dict[str, Any]:
    # Sample observations first so the journal view cannot predate a printed reply.
    records = observations(event_path)
    saved = journal_facts(journal)
    starts = [row for row in records if row["event"] in ("start_sent", "start_reply")]
    request = (saved["request"] or {}).get("request")
    ticket = (saved["start"] or {}).get("ticket")
    if ticket is None:
        observed = next(
            (row for row in reversed(starts) if request is None or row["request"] == request),
            None,
        )
        if observed is not None:
            request, ticket = observed["request"], observed["ticket"]
    wire = [row for row in starts if row["request"] == request and row["ticket"] == ticket]
    pause_tickets = {(saved["intent"] or {}).get("pause_ticket"), *(row["pause_ticket"] for row in wire)}
    processes = [row for row in records if row["event"] == "process_started"]
    relevant_runs = {row["run"] for row in wire}
    relevant_runs.update(row["run"] for row in processes[-2:])
    first = wire[0] if wire else None
    timeline = [
        row
        for row in records
        if row in wire
        or row["event"] == "pause_committed"
        and (
            row["ticket"] in pause_tickets
            or first is not None
            and row["run"] == first["run"]
            and row["sequence"] > first["sequence"]
        )
        or row["event"] == "process_started"
        and row["run"] in relevant_runs
    ]
    if saved["intent"] is not None:
        saved["intent"]["pause_request"] = next(
            (
                row["request"]
                for row in reversed(timeline)
                if row["event"] == "pause_committed"
                and row["ticket"] == saved["intent"]["pause_ticket"]
                and row["intent"] == {key: saved["intent"][key] for key in ("mode", "revision")}
            ),
            None,
        )
    reply = next((row["reply"] for row in reversed(wire) if row["event"] == "start_reply"), UNKNOWN)
    start = saved["start"] or {}
    transport = UNKNOWN
    if any(row["event"] == "start_sent" for row in wire):
        transport = "send_returned"
    elif reply != UNKNOWN or start.get("saved_reply", UNKNOWN) != UNKNOWN:
        transport = "matching_reply"
    waiting = []
    if start.get("lifecycle") == "unbound":
        if start.get("stage") == "issued":
            waiting.append("start_outcome_or_lifecycle")
        elif start.get("stage") == "accepted":
            waiting.append("start_lifecycle")
    if start.get("lifecycle") == "open" and (saved["intent"] or {}).get("mode") == "paused":
        waiting.append("session_closure")
    pending = start.get("pending_delivery", ())
    if any(row["reply_saved"] for row in pending):
        waiting.append("saved_cloud_reply_settlement")
    if any(not row["reply_saved"] for row in pending):
        waiting.append("cloud_delivery")
    return {
        "schema_version": 1,
        "journal": saved,
        "wire": {"request": request, "ticket": ticket, "send": transport, "observed_reply": reply},
        "waiting_for_evidence": waiting if start.get("lifecycle", UNKNOWN) != UNKNOWN else None,
        "observations": timeline[-MAX_REPORT_EVENTS:],
        "sampled_status": sampled_status(journal.with_suffix(".status.json")),
    }
