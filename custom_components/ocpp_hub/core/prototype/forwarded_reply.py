"""Retain definite replies to cloud Start and Stop commands sent to the charger."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from ..frames import Frame
from .store import RecoveryRequiredError

if TYPE_CHECKING:
    from .store import Journal

TRANSACTION_COMMANDS = frozenset({"RemoteStartTransaction", "RemoteStopTransaction"})


@dataclass(frozen=True)
class ForwardedReply:
    """A definite charger answer belongs to this exact issued mutation."""

    mutation: str
    action: str
    wire: str
    status: Literal["Accepted", "Rejected"]


class ForwardedReplies:
    """Own capture and local publication independently of current charging permission."""

    # The journal's mutation capacity also bounds replies waiting for a local write.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        self._pending: dict[str, ForwardedReply] = {}

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
        reply = self._pending.get(mutation) if isinstance(mutation, str) else None
        return reply.status if reply is not None else None

    # Status can distinguish a known reply awaiting storage from an unanswered command.
    def awaits_storage(self, mutation: str) -> bool:
        return mutation in self._pending

    # Persist the classified answer before finishing its independent local bookkeeping.
    def publish(self, reply: ForwardedReply) -> bool:
        if not self.awaits_storage(reply.mutation):
            return True
        j = self.journal
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
                ):
                    raise RecoveryRequiredError("charger reply no longer has its original forwarded command")
                if row["wire_result"] == "dispatched":
                    j.record_vendor_result(reply.mutation, reply.status)
                    j.db.execute("UPDATE mutations SET state='pending' WHERE token=?", (reply.mutation,))
            if row["state"] != "confirmed":
                with j.atomic():
                    self._settle(reply.mutation, reply.action, reply.status)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            return False
        self._pending.pop(reply.mutation, None)
        return True

    # Retry only retained replies. Reconnection and local storage failure never resend a charger command.
    def retry(self) -> None:
        for reply in tuple(self._pending.values()):
            self.publish(reply)

    # Startup already owns the journal transaction. A saved answer can settle without either connection.
    def recover_saved(self) -> None:
        for row in self.journal.db.execute(
            "SELECT token,action,wire_result FROM mutations JOIN vendor_command_scope ON mutation=token "
            "WHERE origin='vendor' AND state='pending' "
            "AND action IN ('RemoteStartTransaction','RemoteStopTransaction') "
            "AND wire_result IN ('Accepted','Rejected')"
        ).fetchall():
            self._settle(row["token"], row["action"], row["wire_result"])

    # A refusal revokes only its own Start grant. A Stop answer never closes the physical session.
    def _settle(self, mutation: str, action: str, status: str) -> None:
        j = self.journal
        if action == "RemoteStartTransaction" and status == "Rejected":
            grant = j.get("vendor_start_grant", {})
            if grant.get("mutation") == mutation:
                j.put("vendor_start_grant", {})
        marker = j.get("uncertain_vendor_command", {})
        if isinstance(marker, dict) and marker.get("mutation") == mutation:
            j.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
        j.mutation_finished(mutation, True)
