"""Authoritative command outcomes and their independently observed effects."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal

from ..frames import Frame
from .delivery_reply import DeliveryReply
from .plans import PlanBook
from .store import Delivery, RecoveryRequiredError, encoded
from .trial_evidence import profile_projection

if TYPE_CHECKING:
    from .store import Journal

TRANSACTION_COMMANDS = frozenset({"RemoteStartTransaction", "RemoteStopTransaction"})
VENDOR_RESULTS = ("Accepted", "Rejected", "NotSupported", "Unknown", "reply")


@dataclass(frozen=True)
class SessionEffect:
    """Only actual retained charger lifecycle reports prove a session opened or closed."""

    local: int
    closed: bool


@dataclass(frozen=True)
class VendorCommand:
    """One issued or prepared cloud command, including its retained charger evidence."""

    token: str
    action: str
    state: str
    wire: str | None
    wire_result: str | None
    status: str | None
    session: SessionEffect | None
    saving_reply: bool

    # A recorded result can finish an outcome while the associated lifecycle remains open.
    @property
    def settled(self) -> bool:
        return self.state == "confirmed" and not self.saving_reply


@dataclass(frozen=True)
class VendorWriteEvidence:
    """The common decision about outstanding vendor work and the reason it is still outstanding."""

    received: int
    uncertain: tuple[str, ...]
    legacy_marker: bool
    unfinished: bool

    # Exact missing outcomes and historical unbound markers can call for a protective hold.
    @property
    def unresolved(self) -> bool:
        return bool(self.uncertain) or self.legacy_marker


@dataclass(frozen=True)
class ForwardedReply:
    """A definite charger answer belongs to this exact issued mutation."""

    mutation: str
    action: str
    wire: str
    status: Literal["Accepted", "Rejected"]


@dataclass(frozen=True)
class VendorProfileWrite:
    """The original translated profile and ownership reservation."""

    vendor: str
    local: int
    transaction: int | None
    allocated: bool


@dataclass
class VendorProfileReply:
    """A known acceptance and the exact resource change it acknowledged."""

    mutation: str
    wire: str
    action: str
    payload: dict[str, Any]
    answer: Frame
    profile: VendorProfileWrite | None
    tracked: bool
    status: Literal["Accepted"] = "Accepted"
    saved: bool = False


@dataclass(frozen=True)
class VendorWriteReply:
    """A classified answer to another forwarded write, without retaining its proprietary body."""

    mutation: str
    action: str
    wire: str
    status: str
    plans: tuple[int, ...] = ()
    clearing: bool = False


@dataclass(frozen=True)
class UnsentVendor:
    """A definite no-send that may need to retire a local reservation."""

    mutation: str
    grant: str | None
    profile: VendorProfileWrite | None


@dataclass(frozen=True)
class UnansweredVendor:
    """An issued command whose outcome is still absent."""

    mutation: str
    action: str
    digest: str


type ReceivedVendor = ForwardedReply | VendorProfileReply | VendorWriteReply
type PendingVendor = ReceivedVendor | UnsentVendor | UnansweredVendor


@dataclass(frozen=True)
class OwnerWire:
    """One actual physical write within an owner command. A rollback keeps a separate wire identity."""

    wire: str
    request: str | None
    ticket: str
    boot: str | None
    action: str
    local: int | None
    limit_a: float | None
    reply: str | None = None


@dataclass(frozen=True)
class OwnerCommand:
    """The API receipt and the actual wire evidence belonging to that exact request."""

    identifier: str
    action: str
    revision: int
    state: str
    result: dict[str, Any]
    wires: tuple[OwnerWire, ...]
    sessions: tuple[SessionEffect, ...]

    # A real reply is reporting evidence even when local completion or physical closure still awaits proof.
    @property
    def reply_known(self) -> bool:
        if self.action in ("start", "start_test"):
            relevant = [wire for wire in self.wires if wire.action == "RemoteStartTransaction"]
        elif self.action == "pause":
            stops = [wire for wire in self.wires if wire.action == "RemoteStopTransaction"]
            relevant = stops or [wire for wire in self.wires if wire.action == "SetChargingProfile"]
        else:
            relevant = [wire for wire in self.wires if wire.action == "SetChargingProfile" and wire.limit_a]
        return bool(relevant) and relevant[-1].reply in ("Accepted", "Rejected", "NotSupported", "Unknown")


@dataclass(frozen=True)
class OwnerWireScope:
    """Capture only the hardware work of an already claimed owner operation."""

    outcomes: CommandOutcomes
    request: str | None
    ticket: str
    boot: str | None
    wires: dict[str, OwnerWire] = field(default_factory=dict)

    # Report possible dispatch after the final control-scope and resource checks.
    def sent(self, wire: str, action: str, payload: dict[str, Any]) -> OwnerWire | None:
        if action in ("RemoteStartTransaction", "RemoteStopTransaction", "SetChargingProfile", "ClearChargingProfile"):
            local = payload.get("transactionId") if action == "RemoteStopTransaction" else None
            limit = profile_projection(payload).get("limit") if action == "SetChargingProfile" else None
            sent = OwnerWire(wire, self.request, self.ticket, self.boot, action, local, limit)
            self.wires[wire] = sent
            self.outcomes.record_owner_wire(sent)
            return sent
        return None

    # Match a real reply to its earlier exact wire identity; no response can supply a lifecycle by itself.
    def received(self, wire: OwnerWire, frame: Frame) -> None:
        self.wires[wire.wire] = self.outcomes.record_owner_reply(wire, frame)


@dataclass(frozen=True)
class DeliveryOutcome:
    """A report's cloud acknowledgement is separate from the charger session it describes."""

    item: Delivery
    reply: DeliveryReply | None
    session: SessionEffect | None
    uncertain: bool


class CommandOutcomes:
    """Own saved and received command evidence independently of present charging permission."""

    # The journal's mutation capacity also bounds replies waiting for a local write.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        self._pending: dict[str, PendingVendor] = {}
        self._owner_writes: dict[str, OwnerWire] = {}
        self._owner_sessions: dict[str, int] = {}
        self._read_binding_pending = False
        self._shutdown_ticket: str | None = None
        self._shutdown_stop_dispatched = False
        self.delivery_reply: DeliveryReply | None = None

    # Sample delivery certainty from the original report and actual reply, independently of the pump's last error.
    def delivery(self, item: Delivery | None = None) -> DeliveryOutcome | None:
        j = self.journal
        head = j.next_delivery() if item is None else item
        if head is None:
            return None
        reply = self.delivery_reply or DeliveryReply.restore(j, head)
        if reply is not None:
            reply.require(head)
            uncertain = not reply.storage_only(j, head)
        else:
            uncertain = head.action in ("StartTransaction", "StopTransaction") and head.state in (
                "inflight",
                "ambiguous",
                "rejected",
            )
        return DeliveryOutcome(head, reply, self.session(head.transaction), uncertain)

    # Checkpoint a real reply even if the transport is still finishing its outgoing send.
    def capture_delivery(self, item: Delivery, reply: DeliveryReply) -> None:
        self.retain_delivery(reply)
        try:
            reply.save(self.journal, self.journal.next_delivery() or item)
        except sqlite3.Error:
            if self.journal.db.in_transaction:
                raise

    # A known reply is immediately available to readers and retained until the exact head settles.
    def retain_delivery(self, reply: DeliveryReply | None) -> None:
        self.delivery_reply = reply

    # The journal can add these per-wire facts without changing the shape of old management receipts.
    def initialize(self) -> None:
        self.journal.db.executescript("""
            CREATE TABLE IF NOT EXISTS owner_command_wires(
                wire TEXT PRIMARY KEY, request TEXT, ticket TEXT NOT NULL, boot TEXT,
                action TEXT NOT NULL, local INTEGER, limit_a REAL, reply TEXT);
            CREATE INDEX IF NOT EXISTS owner_command_wires_request ON owner_command_wires(request);
            CREATE INDEX IF NOT EXISTS owner_command_wires_ticket ON owner_command_wires(ticket);
            CREATE INDEX IF NOT EXISTS owner_command_wires_accepted_stop ON owner_command_wires(local)
                WHERE action='RemoteStopTransaction' AND reply='Accepted';
            CREATE INDEX IF NOT EXISTS vendor_command_scope_stop ON vendor_command_scope(local)
                WHERE wire_result IN ('Accepted','dispatched');
            CREATE TABLE IF NOT EXISTS owner_start_sessions(
                ticket TEXT PRIMARY KEY, local INTEGER NOT NULL REFERENCES transactions(local));
            CREATE TABLE IF NOT EXISTS vendor_profile_effects(
                mutation TEXT PRIMARY KEY REFERENCES vendor_command_scope(mutation) ON DELETE CASCADE,
                projected TEXT, vendor_alias TEXT, local_session INTEGER, tracked INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS vendor_plan_effects(
                mutation TEXT PRIMARY KEY REFERENCES vendor_command_scope(mutation) ON DELETE CASCADE,
                plans TEXT NOT NULL, clearing INTEGER NOT NULL);
        """)

    # Exact command claims retain their own wire correlation through every separately checked call in the sequence.
    def owner_scope(self, request: str | None, ticket: str, boot: str | None) -> OwnerWireScope:
        return OwnerWireScope(self, request, ticket, boot)

    # Historical receipts without this supplementary evidence remain readable and prove only their stored facts.
    def owner(
        self, identifier: str, *, include_wires: bool = False, row: sqlite3.Row | None = None
    ) -> OwnerCommand | None:
        if row is None:
            row = self.journal.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()
        if row is None:
            return None
        result = json.loads(row["result"])
        if not isinstance(result, dict):
            raise ValueError("owner command metadata is unavailable")
        wires = self.owner_wires("request", identifier) if include_wires else ()
        locals_seen = dict.fromkeys(wire.local for wire in wires if wire.local is not None)
        sessions = tuple(session for local in locals_seen if (session := self.session(local)) is not None)
        return OwnerCommand(identifier, row["action"], row["revision"], row["state"], result, wires, sessions)

    # Merge only buffered evidence for the exact saved call or logical owner ID, including a failed first write.
    def owner_wires(self, key: Literal["request", "ticket"], identifier: str) -> tuple[OwnerWire, ...]:
        rows = self.journal.db.execute(
            "SELECT w.wire,w.request,w.ticket,w.boot,w.action,COALESCE(s.local,w.local),w.limit_a,w.reply "
            "FROM owner_command_wires w LEFT JOIN owner_start_sessions s "
            "ON w.action='RemoteStartTransaction' AND s.ticket=w.ticket "
            f"WHERE w.{key}=? ORDER BY w.rowid",
            (identifier,),
        )
        wires = {row["wire"]: OwnerWire(*row) for row in rows}
        for retained in self._retained_owner_stops():
            if (retained.request if key == "request" else retained.ticket) == identifier:
                wires[retained.wire] = retained
        for wire in self._owner_writes.values():
            if (wire.request if key == "request" else wire.ticket) == identifier:
                wires[wire.wire] = wire
        return tuple(self._with_bound_session(wire) for wire in wires.values())

    # A reply waiting for storage must still show the actual committed session bound to that exact Start.
    def _with_bound_session(self, wire: OwnerWire) -> OwnerWire:
        if wire.action != "RemoteStartTransaction" or wire.local is not None:
            return wire
        local = self._owner_sessions.get(wire.ticket)
        if local is None:
            row = self.journal.db.execute(
                "SELECT local FROM owner_start_sessions WHERE ticket=?", (wire.ticket,)
            ).fetchone()
            local = row["local"] if row is not None else None
        return wire if local is None else replace(wire, local=local)

    # After the actual Start commits, retain its exact association independently of the wire receipt write.
    def retain_bound_owner_start(self) -> None:
        try:
            saved = self.journal.starts.receipt()
            ticket, local = saved.get("ticket"), saved.get("local")
            if isinstance(ticket, str) and type(local) is int and self.session(local) is not None:
                self._owner_sessions[ticket] = local
            self._read_binding_pending = False
        except sqlite3.Error:
            self._read_binding_pending = True
        self._save_owner_sessions()

    # A link already committed by the real Start can be saved again after temporary storage refusal.
    def _save_owner_sessions(self) -> None:
        for ticket, local in tuple(self._owner_sessions.items()):
            try:
                self.journal.db.execute(
                    "INSERT INTO owner_start_sessions VALUES(?,?) ON CONFLICT(ticket) DO NOTHING", (ticket, local)
                )
            except sqlite3.Error:
                break
            self._owner_sessions.pop(ticket, None)

    # Keep the exact evidence in memory before trying the independent storage write.
    def record_owner_wire(self, wire: OwnerWire) -> None:
        if wire.action == "RemoteStopTransaction" and wire.ticket == self._shutdown_ticket:
            self._shutdown_stop_dispatched = True
        self._owner_writes[wire.wire] = wire
        if len(self._owner_writes) > self.journal.capacity:
            self._owner_writes.pop(next(iter(self._owner_writes)))
        self._save_owner_wire(wire)

    # Capture an exact received status before the action-specific result owner can fail while saving it.
    def record_owner_reply(self, wire: OwnerWire, frame: Frame) -> OwnerWire:
        if frame[0] == 4:
            status = "error"
        else:
            status = next(
                (
                    value
                    for value in ("Accepted", "Rejected", "NotSupported", "Unknown")
                    if frame[2] == {"status": value}
                ),
                "other",
            )
        received = replace(wire, reply=status)
        self.record_owner_wire(received)
        if wire.action == "RemoteStopTransaction" and status == "Accepted" and wire.wire in self._owner_writes:
            # Preserve the actual reply if SQLite refused its normal receipt write.
            self._retain_stop_reply("owner", received)
        return received

    # An independent checkpoint can succeed when the normal receipt write was refused.
    def _retain_stop_reply(self, source: Literal["owner", "vendor"], reply: OwnerWire | ForwardedReply) -> None:
        try:
            self.journal.put(f"retained_stop_{source}:{reply.wire}", asdict(reply))
        except sqlite3.Error:
            if self.journal.db.in_transaction:
                raise

    # Read fallback checkpoints for the exact wires that received a real Stop acknowledgment.
    def _retained_owner_stops(self) -> tuple[OwnerWire, ...]:
        rows = self.journal.db.execute("SELECT value FROM meta WHERE key GLOB 'retained_stop_owner:*'")
        return tuple(OwnerWire(**json.loads(row[0])) for row in rows)

    # Retire a fallback only after the original outcome is durable.
    def _clear_retained_stop(self, source: Literal["owner", "vendor"], wire: str) -> None:
        in_transaction = self.journal.db.in_transaction
        try:
            self.journal.db.execute("DELETE FROM meta WHERE key=?", (f"retained_stop_{source}:{wire}",))
        except sqlite3.Error:
            # A refused cleanup is repeatable. An implicit rollback must end the original transaction.
            if in_transaction and not self.journal.db.in_transaction:
                raise

    # The optional evidence write cannot delay a protective send already authorized by the original journal.
    def _save_owner_wire(self, wire: OwnerWire) -> None:
        try:
            self.journal.db.execute(
                "INSERT INTO owner_command_wires VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(wire) DO UPDATE SET reply=excluded.reply",
                (wire.wire, wire.request, wire.ticket, wire.boot, wire.action, wire.local, wire.limit_a, wire.reply),
            )
        except sqlite3.Error:
            if self.journal.db.in_transaction:
                raise
            return
        self._owner_writes.pop(wire.wire, None)

    # A failed optional evidence write can recover after its caller finishes.
    def retry_owner_writes(self) -> None:
        retained = self._retained_owner_stops()
        for reply in retained:
            self._owner_writes.setdefault(reply.wire, reply)
        for wire in tuple(self._owner_writes.values()):
            self._save_owner_wire(wire)
        for reply in retained:
            if reply.wire not in self._owner_writes:
                self._clear_retained_stop("owner", reply.wire)
        if self._read_binding_pending:
            self.retain_bound_owner_start()
        else:
            self._save_owner_sessions()

    # Recover an older Start stage only when a real reply names that exact issued Start.
    def recover_owner_start(self) -> None:
        self.retain_bound_owner_start()
        receipt = self.journal.starts.receipt()
        ticket = receipt.get("ticket")
        if not isinstance(ticket, str) or receipt.get("stage") != "issued":
            return
        for wire in self.owner_wires("ticket", ticket):
            if wire.action == "RemoteStartTransaction" and wire.reply in ("Accepted", "Rejected"):
                try:
                    self.journal.starts.record_reply_status(ticket, wire.reply)
                except sqlite3.Error:
                    if self.journal.db.in_transaction:
                        raise
                break

    # Only a real received answer takes the place of the existing timeout or unsent bookkeeping.
    def _received(self, mutation: str) -> ReceivedVendor | None:
        pending = self._pending.get(mutation)
        return pending if isinstance(pending, (ForwardedReply, VendorProfileReply, VendorWriteReply)) else None

    # Read the real journal lifecycle without granting authority to a matching number by itself.
    def session(self, local: int | None) -> SessionEffect | None:
        if type(local) is not int:
            return None
        row = self.journal.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()
        return SessionEffect(local, row["stop"] is not None) if row is not None else None

    # Idle requires ledger closure, including the original target if the operation already had one.
    def stop_report_closed(self, local: int | None = None) -> bool:
        if self.journal.active() is not None:
            return False
        session = self.session(local)
        return local is None or (session is not None and session.closed)

    # A matched reply stays with its exact allocated session across Boot and process replacement.
    def accepted_stop(self, local: int | None, *, ticket: str | None = None) -> bool:
        if local is None:
            return False
        if any(
            reply.local == local and (ticket is None or reply.ticket == ticket)
            for reply in self._retained_owner_stops()
        ):
            return True
        if (
            any(
                wire.action == "RemoteStopTransaction"
                and wire.local == local
                and wire.reply == "Accepted"
                and (ticket is None or wire.ticket == ticket)
                for wire in self._owner_writes.values()
            )
            or self.journal.db.execute(
                "SELECT 1 FROM owner_command_wires WHERE local=:local AND action='RemoteStopTransaction' "
                "AND reply='Accepted' AND (:ticket IS NULL OR ticket=:ticket) LIMIT 1",
                {"local": local, "ticket": ticket},
            ).fetchone()
            is not None
        ):
            return True
        return ticket is None and self._accepted_forwarded_stop(local)

    # A forwarded Stop has the same session effect while its exact reply is being saved or already durable.
    def _accepted_forwarded_stop(self, local: int) -> bool:
        rows = self.journal.db.execute(
            "SELECT token,wire_request,wire_result FROM vendor_command_scope JOIN mutations ON mutation=token "
            "WHERE local=? AND origin='vendor' AND action='RemoteStopTransaction' "
            "AND wire_result IN ('Accepted','dispatched')",
            (local,),
        )
        for row in rows:
            if row["wire_result"] == "Accepted":
                return True
            reply = self._received(row["token"])
            if isinstance(reply, ForwardedReply) and reply.wire == row["wire_request"] and reply.status == "Accepted":
                return True
        return False

    # Pin the target before any wire work and retain an older unanswered Stop for that same session.
    def claim_shutdown_stop(self, attempt: str, local: int | None, ticket: str, boot: str | None) -> None:
        self.journal.put(
            "trial_shutdown_attempt",
            {
                "id": attempt,
                "local": local,
                "ticket": ticket,
                "boot": boot,
                "state": "pending",
                "prior_pending_stop": self.awaits_stop_report(local),
            },
        )
        self._shutdown_ticket, self._shutdown_stop_dispatched = ticket, False

    # Only the original live executor can attest that it retired without dispatching its tracked Stop.
    def record_shutdown_stop(self, attempt: str, state: str) -> None:
        latest = self.journal.get("trial_shutdown_attempt", {})
        if latest.get("id") == attempt:
            unsent = latest.get("ticket") == self._shutdown_ticket and not self._shutdown_stop_dispatched
            self.journal.put("trial_shutdown_attempt", {**latest, "state": state, "unsent": unsent})

    # A possibly sent shutdown Stop also retains zero until the exact charger report resolves it.
    def awaits_stop_report(self, local: int | None) -> bool:
        if local is None or self.journal.active() != local:
            return False
        return self.accepted_stop(local) or self.unanswered_shutdown_stop(local)

    # This is evidence to wait, without an Accepted reply that can be sent to another caller.
    def unanswered_shutdown_stop(self, local: int) -> bool:
        shutdown = self.journal.get("trial_shutdown_attempt", {})
        ticket = shutdown.get("ticket")
        if (
            shutdown.get("local") != local
            or not isinstance(ticket, str)
            or shutdown.get("state") not in ("pending", "uncertain")
        ):
            return False
        if shutdown.get("prior_pending_stop"):
            return True
        if shutdown.get("unsent") or ticket == self._shutdown_ticket and not self._shutdown_stop_dispatched:
            return False
        stops = [
            wire
            for wire in self.owner_wires("ticket", ticket)
            if wire.local == local and wire.action == "RemoteStopTransaction"
        ]
        # A temporary write failure can leave the durable claim without a saved dispatch row.
        return not stops or any(wire.reply != "Rejected" for wire in stops)

    # Read one original mutation. Historical unrelated rows cannot lend it a reply or local session.
    def vendor(self, mutation: object) -> VendorCommand | None:
        if not isinstance(mutation, str):
            return None
        row = self.journal.db.execute(
            "SELECT token,action,state,wire_request,wire_result,local FROM mutations "
            "LEFT JOIN vendor_command_scope ON mutation=token WHERE token=? AND origin='vendor'",
            (mutation,),
        ).fetchone()
        if row is None:
            return None
        local = row["local"]
        if local is None and row["action"] == "RemoteStartTransaction":
            # Old builds kept the binding only in the exact unfinished grant.
            grant = self.journal.get("vendor_start_grant", {})
            if grant.get("mutation") == mutation:
                local = grant.get("local")
        pending = self._received(mutation)
        result = row["wire_result"]
        if pending is not None:
            status = pending.status
        elif result in (*VENDOR_RESULTS, "call_error"):
            status = result
        else:
            status = None
        return VendorCommand(
            mutation,
            row["action"],
            row["state"],
            row["wire_request"],
            result,
            status,
            self.session(local),
            pending is not None,
        )

    # The Start's real session link is retained after its single-use authorization grant expires or closes.
    def bind_vendor_start(self, mutation: object, local: int) -> None:
        if not isinstance(mutation, str):
            return
        row = self.journal.db.execute(
            "SELECT action,origin,local FROM mutations JOIN vendor_command_scope ON mutation=token WHERE token=?",
            (mutation,),
        ).fetchone()
        if row is None or (row["action"], row["origin"]) != ("RemoteStartTransaction", "vendor"):
            return  # An older grant without a scoped command can still retain its historical behavior.
        if row["local"] not in (None, local):
            raise RecoveryRequiredError("vendor Start already belongs to a different real session")
        self.journal.db.execute("UPDATE vendor_command_scope SET local=? WHERE mutation=?", (local, mutation))

    # Distinguish physical uncertainty from normal waiting and local settlement, including old journal markers.
    def vendor_evidence(
        self,
        has_pending_call: Callable[[str | None], bool] | None = None,
    ) -> VendorWriteEvidence:
        rows = self.journal.db.execute(
            "SELECT token,state,wire_request,wire_result FROM mutations "
            "LEFT JOIN vendor_command_scope ON mutation=token WHERE origin='vendor' AND state!='confirmed'"
        ).fetchall()
        received: set[str] = set()
        uncertain = []
        for row in rows:
            token = row["token"]
            pending = self._received(token)
            status = pending.status if pending is not None else row["wire_result"]
            if status == "call_error":
                uncertain.append(token)
            elif status in VENDOR_RESULTS:
                received.add(token)
            elif row["state"] == "uncertain" or (
                row["wire_result"] not in ("prepared", "prepared_new_profile")
                and (has_pending_call is None or not has_pending_call(row["wire_request"]))
            ):
                uncertain.append(token)
        marker = self.journal.get("uncertain_vendor_command")
        reference = marker.get("mutation") if isinstance(marker, dict) else None
        legacy = bool(marker) and (
            not isinstance(reference, str) or reference not in uncertain and reference not in received
        )
        return VendorWriteEvidence(len(received), tuple(uncertain), legacy, bool(rows) or legacy)

    # Only one exact unanswered physical command can own a recoverable reply hold.
    def sole_forwarded_timeout(self, evidence: VendorWriteEvidence) -> str | None:
        if evidence.legacy_marker or len(evidence.uncertain) != 1:
            return None
        command = self.vendor(evidence.uncertain[0])
        if (
            command is not None
            and command.action in TRANSACTION_COMMANDS
            and command.wire_result == "dispatched"
            and command.status is None
        ):
            return command.token
        return None

    # Keep the fact before any write, even when cancellation ends the original request handler.
    def capture(self, mutation: str, action: str, wire: str, frame: Frame) -> ForwardedReply | None:
        if (
            action not in TRANSACTION_COMMANDS
            or frame[0] != 3
            or frame[1] != wire
            or frame[2] not in ({"status": "Accepted"}, {"status": "Rejected"})
        ):
            return None
        status: Literal["Accepted", "Rejected"] = "Accepted" if frame[2] == {"status": "Accepted"} else "Rejected"
        reply = ForwardedReply(mutation, action, wire, status)
        self._pending[mutation] = reply
        return reply

    # A received outcome is available to lifecycle admission even while its write is pending.
    def status(self, mutation: object) -> Literal["Accepted", "Rejected"] | None:
        result = self.vendor(mutation)
        if result is not None and result.status == "Accepted":
            return "Accepted"
        if result is not None and result.status == "Rejected":
            return "Rejected"
        return None

    # Status can distinguish a known reply awaiting storage from an unanswered command.
    def awaits_storage(self, mutation: str) -> bool:
        return self._received(mutation) is not None

    # Persist the classified answer before finishing its independent local bookkeeping.
    def publish(self, reply: ForwardedReply) -> bool:
        if not self.awaits_storage(reply.mutation):
            return True
        j = self.journal
        checkpoint = False
        try:
            with j.atomic():
                row = j.db.execute(
                    "SELECT origin,action,wire_request,state,wire_result FROM mutations "
                    "JOIN vendor_command_scope ON mutation=token WHERE token=?",
                    (reply.mutation,),
                ).fetchone()
                if (
                    row is None
                    or tuple(row)[:3] != ("vendor", reply.action, reply.wire)
                    or (row["state"], row["wire_result"])
                    not in (
                        ("pending", "dispatched"),
                        ("uncertain", "dispatched"),
                        ("pending", reply.status),
                        ("confirmed", reply.status),
                    )
                    and not (
                        row["state"] == "confirmed"
                        and row["wire_result"] == "dispatched"
                        and reply.action == "RemoteStopTransaction"
                        and reply.status == "Accepted"
                    )
                ):
                    raise RecoveryRequiredError("charger reply no longer has its original forwarded command")
                if row["wire_result"] == "dispatched":
                    checkpoint = reply.action == "RemoteStopTransaction" and reply.status == "Accepted"
                    j.record_vendor_result(reply.mutation, reply.status)
                    if row["state"] != "confirmed":
                        j.db.execute("UPDATE mutations SET state='pending' WHERE token=?", (reply.mutation,))
            checkpoint = False
            if row["state"] != "confirmed":
                with j.atomic():
                    self._settle(reply.mutation, reply.action, reply.status)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            if checkpoint:
                self._retain_stop_reply("vendor", reply)
            return False
        self._pending.pop(reply.mutation, None)
        if reply.action == "RemoteStopTransaction" and reply.status == "Accepted":
            self._clear_retained_stop("vendor", reply.wire)
        return True

    # Save the exact reply before applying any plan changes or final outcome.
    def save_vendor_write(self, reply: VendorWriteReply) -> bool:
        self._pending[reply.mutation] = reply
        j = self.journal
        try:
            with j.atomic():
                row = j.db.execute(
                    "SELECT action,wire_request,wire_result,state FROM mutations "
                    "JOIN vendor_command_scope ON mutation=token WHERE token=? AND origin='vendor'",
                    (reply.mutation,),
                ).fetchone()
                if row is None or (row["action"], row["wire_request"]) != (reply.action, reply.wire):
                    raise RecoveryRequiredError("charger reply no longer owns its original vendor write")
                state = row["state"], row["wire_result"]
                late_error = (
                    state == ("uncertain", "dispatched")
                    and reply.action in TRANSACTION_COMMANDS
                    and reply.status == "call_error"
                )
                if not late_error and state not in (("pending", "dispatched"), ("pending", reply.status)):
                    raise RecoveryRequiredError("vendor write is no longer awaiting this reply")
                if row["wire_result"] == "dispatched":
                    if reply.plans:
                        j.db.execute(
                            "INSERT INTO vendor_plan_effects VALUES(?,?,?)",
                            (reply.mutation, encoded(reply.plans), reply.clearing),
                        )
                    j.record_vendor_result(reply.mutation, reply.status)
                    if late_error:
                        j.db.execute("UPDATE mutations SET state='pending' WHERE token=?", (reply.mutation,))
            with j.atomic():
                self._settle_vendor_write(reply.mutation, reply.action, reply.status)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            return False
        self._pending.pop(reply.mutation, None)
        return True

    # Finish a saved answer without either peer, preserving uncertainty from an actual error reply.
    def _settle_vendor_write(self, mutation: str, action: str, status: str) -> None:
        j = self.journal
        saved = j.db.execute("SELECT plans,clearing FROM vendor_plan_effects WHERE mutation=?", (mutation,)).fetchone()
        if saved is not None:
            PlanBook(j).accepted(tuple(json.loads(saved["plans"])), bool(saved["clearing"]))
            j.db.execute("DELETE FROM vendor_plan_effects WHERE mutation=?", (mutation,))
        confirmed = status != "call_error"
        j.mutation_finished(mutation, confirmed)
        if not confirmed:
            digest = j.db.execute("SELECT digest FROM mutations WHERE token=?", (mutation,)).fetchone()[0]
            j.put("uncertain_vendor_command", {"mutation": mutation, "action": action, "digest": digest})

    # Checkpoint the matched answer before making any change to the profile's resource record.
    def save_profile_reply(self, reply: VendorProfileReply) -> bool:
        self._pending[reply.mutation] = reply
        if reply.saved:
            return True
        j = self.journal
        try:
            with j.atomic():
                row = j.db.execute(
                    "SELECT action,wire_request,wire_result,profile FROM mutations "
                    "JOIN vendor_command_scope ON mutation=token WHERE token=? AND origin='vendor' AND state='pending'",
                    (reply.mutation,),
                ).fetchone()
                if (
                    row is None
                    or (row["action"], row["wire_request"]) != (reply.action, reply.wire)
                    or row["wire_result"] not in ("dispatched", "Accepted")
                ):
                    raise RecoveryRequiredError("profile reply no longer owns its original command")
                if row["wire_result"] == "dispatched":
                    projected = (
                        encoded(profile_projection(reply.payload)) if reply.action == "SetChargingProfile" else None
                    )
                    profile = reply.profile
                    j.db.execute(
                        "INSERT INTO vendor_profile_effects VALUES(?,?,?,?,?)",
                        (
                            reply.mutation,
                            projected,
                            profile.vendor if profile is not None else None,
                            profile.transaction if profile is not None else None,
                            reply.tracked,
                        ),
                    )
                    j.record_vendor_result(reply.mutation, reply.status)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            return False
        reply.saved = True
        return True

    # Settle once the actual response is durable. A failed write retains this exact resource for retry.
    def publish_profile(self, reply: VendorProfileReply) -> bool:
        if not self.awaits_storage(reply.mutation):
            return True
        if not self.save_profile_reply(reply):
            return False
        j = self.journal
        try:
            with j.atomic():
                self._settle_profile(reply.mutation)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            return False
        self._pending.pop(reply.mutation, None)
        return True

    # Finish the same saved resource on retry or restart. A verified absence wins over an earlier acknowledgement.
    def _settle_profile(self, mutation: str) -> None:
        j = self.journal
        saved = j.db.execute(
            "SELECT action,profile,projected,vendor_alias,local_session,tracked FROM vendor_profile_effects "
            "JOIN vendor_command_scope USING(mutation) JOIN mutations ON token=mutation "
            "WHERE mutation=? AND origin='vendor' AND state='pending' AND wire_result='Accepted'",
            (mutation,),
        ).fetchone()
        if saved is None:
            raise RecoveryRequiredError("saved profile reply is missing its original resource")
        identifier = saved["profile"]
        current = True
        if saved["tracked"]:
            key = "profile:" + str(identifier)
            row = j.db.execute("SELECT state,desired FROM trial_resources WHERE key=?", (key,)).fetchone()
            current = (
                row is not None
                and row["state"] != "absent_verified"
                and (saved["action"] == "ClearChargingProfile" or row["desired"] == saved["projected"])
            )
            if current:
                if saved["action"] == "SetChargingProfile":
                    j.db.execute(
                        "UPDATE trial_resources SET confirmed=desired,state='confirmed',observed_digest=NULL "
                        "WHERE key=? AND state!='confirmed'",
                        (key,),
                    )
                else:
                    j.db.execute(
                        "UPDATE trial_resources SET state='clear_acknowledged' "
                        "WHERE key=? AND state!='clear_acknowledged'",
                        (key,),
                    )
        if current and saved["vendor_alias"] is not None:
            self._accept_profile(VendorProfileWrite(saved["vendor_alias"], identifier, saved["local_session"], False))
        j.mutation_finished(mutation, True)

    # Commit the last accepted definition even if its old transaction has since closed.
    def _accept_profile(self, profile: VendorProfileWrite) -> None:
        j = self.journal
        profiles, transactions = j.get("vendor_profiles", {}), j.get("vendor_profile_transactions", {})
        if profile.transaction is None:
            profiles[profile.vendor] = profile.local
            transactions.pop(profile.vendor, None)
        elif j.active() == profile.transaction:
            profiles[profile.vendor] = profile.local
            transactions[profile.vendor] = profile.transaction
        elif profiles.get(profile.vendor) == profile.local:
            profiles.pop(profile.vendor, None)
            transactions.pop(profile.vendor, None)
        j.put("vendor_profiles", profiles)
        j.put("vendor_profile_transactions", transactions)

    # Retire only this exact unsent reservation, without waiting on or sending any device command.
    def retire_unsent(self, mutation: str | None, grant: str | None, profile: VendorProfileWrite | None) -> bool:
        j = self.journal
        try:
            with j.atomic():
                if mutation is not None:
                    j.db.execute("DELETE FROM mutations WHERE token=?", (mutation,))
                if grant is not None and j.get("vendor_start_grant", {}).get("token") == grant:
                    j.put("vendor_start_grant", {})
                if profile is not None and profile.allocated:
                    profiles = j.get("vendor_profiles", {})
                    if profiles.get(profile.vendor) == profile.local:
                        profiles.pop(profile.vendor)
                        j.put("vendor_profiles", profiles)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            if mutation is not None:
                self._pending[mutation] = UnsentVendor(mutation, grant, profile)
            return False
        if mutation is not None:
            self._pending.pop(mutation, None)
        return True

    # Retry the saved timeout without replacing a real answer that may have arrived meanwhile.
    def record_unanswered(self, mutation: str, action: str, digest: str) -> bool:
        if self.awaits_storage(mutation):
            return True
        j = self.journal
        try:
            with j.atomic():
                row = j.db.execute("SELECT state FROM mutations WHERE token=?", (mutation,)).fetchone()
                if row is not None and row[0] == "pending":
                    j.mutation_finished(mutation, False)
                    j.put("uncertain_vendor_command", {"mutation": mutation, "action": action, "digest": digest})
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            self._pending[mutation] = UnansweredVendor(mutation, action, digest)
            return False
        self._pending.pop(mutation, None)
        return True

    # One command has at most one retained settlement, independent of either connection's lifetime.
    def retry(self) -> None:
        for pending in tuple(self._pending.values()):
            if isinstance(pending, ForwardedReply):
                self.publish(pending)
            elif isinstance(pending, VendorProfileReply):
                self.publish_profile(pending)
            elif isinstance(pending, VendorWriteReply):
                self.save_vendor_write(pending)
            elif isinstance(pending, UnsentVendor):
                self.retire_unsent(pending.mutation, pending.grant, pending.profile)
            else:
                self.record_unanswered(pending.mutation, pending.action, pending.digest)

    # Startup already owns the journal transaction. A saved answer can settle without either connection.
    def recover_saved(self) -> None:
        rows = self.journal.db.execute("SELECT value FROM meta WHERE key GLOB 'retained_stop_vendor:*'").fetchall()
        for row in rows:
            reply = ForwardedReply(**json.loads(row[0]))
            original = self.journal.db.execute(
                "SELECT wire_request,wire_result FROM vendor_command_scope "
                "JOIN mutations ON mutation=token WHERE token=? AND origin='vendor' AND action=?",
                (reply.mutation, reply.action),
            ).fetchone()
            if original is not None and tuple(original) == (reply.wire, "dispatched"):
                try:
                    self.journal.record_vendor_result(reply.mutation, reply.status)
                except sqlite3.Error:
                    if not self.journal.db.in_transaction:
                        raise
                    self._pending[reply.mutation] = reply
                    continue
            self._clear_retained_stop("vendor", reply.wire)
        for row in self.journal.db.execute(
            "SELECT token,action,wire_result FROM mutations JOIN vendor_command_scope ON mutation=token "
            "WHERE origin='vendor' AND state IN ('pending','uncertain') "
            "AND action IN ('RemoteStartTransaction','RemoteStopTransaction') "
            "AND wire_result IN ('Accepted','Rejected')"
        ).fetchall():
            self._settle(row["token"], row["action"], row["wire_result"])
        for row in self.journal.db.execute(
            "SELECT mutation FROM vendor_profile_effects JOIN vendor_command_scope USING(mutation) "
            "JOIN mutations ON token=mutation WHERE state='pending' AND wire_result='Accepted'"
        ).fetchall():
            self._settle_profile(row["mutation"])
        for row in self.journal.db.execute(
            "SELECT token,action,wire_result FROM mutations JOIN vendor_command_scope ON mutation=token "
            "WHERE origin='vendor' AND state='pending' "
            "AND wire_result IN ('Accepted','Rejected','NotSupported','Unknown','reply','call_error')"
        ).fetchall():
            self._settle_vendor_write(row["token"], row["action"], row["wire_result"])

    # A refusal revokes only its own Start grant. A Stop answer never closes the physical session.
    def _settle(self, mutation: str, action: str, status: str) -> None:
        j = self.journal
        if action == "RemoteStartTransaction" and status == "Rejected":
            grant = j.get("vendor_start_grant", {})
            if grant.get("mutation") == mutation:
                j.put("vendor_start_grant", {})
        self.clear_vendor_marker(mutation)
        j.mutation_finished(mutation, True)
        j.retire_reply_fault("forwarded_timeout", mutation)

    # Exact reply or operator evidence can retire only the matching command's old hold marker.
    def clear_vendor_marker(self, mutation: str) -> None:
        j = self.journal
        marker = j.get("uncertain_vendor_command", {})
        if isinstance(marker, dict) and marker.get("mutation") == mutation:
            j.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
