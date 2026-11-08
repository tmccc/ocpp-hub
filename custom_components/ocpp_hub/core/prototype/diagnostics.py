"""Bounded protocol classifications: no endpoint, identifier, frame body or remote error text is retained."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

ACTIONS = frozenset(
    {
        "BootNotification",
        "Heartbeat",
        "StatusNotification",
        "MeterValues",
        "StartTransaction",
        "StopTransaction",
        "Authorize",
        "DataTransfer",
        "GetConfiguration",
        "SetChargingProfile",
        "ClearChargingProfile",
        "GetCompositeSchedule",
        "RemoteStartTransaction",
        "RemoteStopTransaction",
        "TriggerMessage",
    }
)
STAGES = frozenset(
    {
        "opening",
        "registration",
        "history",
        "heartbeat",
        "authorization",
        "session",
        "charger_receive",
        "boot_or_ceiling",
    }
)


# Peer-supplied action names cannot turn a diagnostic label into an arbitrary secret-bearing string.
def action_label(action: str) -> str:
    return action if action in ACTIONS else "Other"


@dataclass
class RpcDiagnostics:
    """Sticky registration and the last bounded exchange survive transport retirement."""

    action: str = "None"
    phase: str = "idle"
    failed_phase: str | None = None
    send_attempts: int = 0
    matched_replies: int = 0
    unmatched_replies: int = 0
    call_id_length: int | None = None
    reply_id_length: int | None = None
    reply_matched: bool | None = None
    last_registration: str | None = None
    accepted_registrations: int = 0

    # Record only the outbound vocabulary and identifier length before the actual send can become uncertain.
    def sending(self, action: str, length: int) -> None:
        self.action, self.phase = action_label(action), "sending"
        self.call_id_length, self.reply_id_length, self.reply_matched = length, None, None
        self.send_attempts += 1

    # Unknown replies stay ignored by the engine but become visible without exposing their correlation values.
    def reply(self, length: int, matched: bool) -> None:
        self.reply_id_length, self.reply_matched = length, matched
        if matched:
            self.matched_replies += 1
            self.phase = "reply_received"
        else:
            self.unmatched_replies += 1

    # Keep actual registration acceptance visible even after detach resets the gateway's current registration state.
    def registration(self, status: str) -> None:
        if status not in ("Accepted", "Pending", "Rejected"):
            raise ValueError("registration classification is invalid")
        self.last_registration = status
        self.accepted_registrations += int(status == "Accepted")

    # A later disconnect changes present state without erasing the phase in which an exchange became uncertain.
    def interrupted(self) -> None:
        if self.phase in ("sending", "awaiting_reply", "reply_received"):
            self.failed_phase = self.phase
            self.phase = "unconfirmed"

    # All fields have fixed vocabularies or numeric/boolean values; no opaque protocol data enters this shape.
    def snapshot(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FaultDiagnostics:
    """Separate charger and vendor failures prevent cleanup on one leg from hiding the other leg's cause."""

    count: int = 0
    classification: str | None = None
    stage: str | None = None
    action: str | None = None
    received_close_code: int | None = None
    sent_close_code: int | None = None

    # Capture numeric close details before another child task or our own cleanup can replace the transport state.
    def record(self, error: BaseException, stage: str, action: str) -> None:
        self.count += 1
        self.classification = type(error).__name__
        self.stage = stage if stage in STAGES else "session"
        self.action = action_label(action)
        for source, destination in (("rcvd", "received_close_code"), ("sent", "sent_close_code")):
            frame = getattr(error, source, None)
            code = getattr(frame, "code", None)
            setattr(self, destination, int(code) if isinstance(code, int) and 1000 <= code <= 4999 else None)

    # A clean remote close ends iteration normally; keep that event without ever retaining its arbitrary reason.
    def record_close(self, stage: str, action: str, received: int, sent: int | None) -> None:
        self.count += 1
        self.classification = "ConnectionClosedOK" if received in (1000, 1001) else "ConnectionClosedError"
        self.stage = stage if stage in STAGES else "session"
        self.action = action_label(action)
        self.received_close_code = received if 1000 <= received <= 4999 else None
        self.sent_close_code = sent if sent is not None and 1000 <= sent <= 4999 else None

    # Deliberately omit arbitrary websocket reasons and exception messages from printable status.
    def snapshot(self) -> dict[str, Any]:
        return asdict(self)
