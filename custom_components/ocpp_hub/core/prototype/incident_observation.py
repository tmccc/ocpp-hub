"""Small optional observations of a managed Start and the Pause that may supersede it."""

from dataclasses import dataclass
from typing import Literal

from .managed_intent import ControlIntent

type ObservationKind = Literal["start_sent", "start_reply", "pause_committed"]
type ObservedReply = Literal["accepted", "rejected", "call_error", "other"]


@dataclass(frozen=True)
class IncidentObservation:
    """An exact local request and claim at a known boundary, with the intent sampled there."""

    kind: ObservationKind
    request: str
    ticket: str
    intent: ControlIntent | None
    pause_ticket: str | None
    reply: ObservedReply | None = None
