"""Validate lifecycle observations before acknowledging or storing them."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

from .policy import DeniedError, integer
from .store import ConflictError, Journal

STATUSES = frozenset(
    {
        "Available",
        "Preparing",
        "Charging",
        "SuspendedEV",
        "SuspendedEVSE",
        "Finishing",
        "Reserved",
        "Unavailable",
        "Faulted",
    }
)


# Reject naive or malformed timestamps; old event time is kept separate from receive time.
def event_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise DeniedError("timestamp must be a timezone-aware string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DeniedError("invalid event timestamp") from None
    if result.tzinfo is None:
        raise DeniedError("event timestamp requires a timezone")
    try:
        return result.astimezone(UTC)
    except OverflowError, ValueError:
        raise DeniedError("event timestamp is outside the supported UTC range") from None


# Prevent a malformed but acknowledged record from poisoning all subsequent cloud delivery.
def validate_observation(action: str, payload: dict[str, Any], journal: Journal) -> None:
    if action == "StatusNotification":
        integer(payload.get("connectorId"), maximum=1)
        if (
            not isinstance(payload.get("status"), str)
            or payload["status"] not in STATUSES
            or not isinstance(payload.get("errorCode"), str)
        ):
            raise DeniedError("invalid connector status")
        if "timestamp" in payload:
            event_time(payload["timestamp"])
    elif action == "MeterValues":
        integer(payload.get("connectorId"), maximum=1)
        if "transactionId" in payload:
            local = integer(payload["transactionId"])
            if journal.db.execute("SELECT local FROM transactions WHERE local=?", (local,)).fetchone() is None:
                raise ConflictError("meter names an unknown local transaction")
        rows = payload.get("meterValue")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 128:
            raise DeniedError("invalid meter rows")
        for row in rows:
            if not isinstance(row, dict):
                raise DeniedError("invalid meter row")
            event_time(row.get("timestamp"))
            samples = row.get("sampledValue")
            if not isinstance(samples, list) or not 1 <= len(samples) <= 128:
                raise DeniedError("invalid meter samples")
            for sample in samples:
                if not isinstance(sample, dict) or not isinstance(sample.get("value"), str):
                    raise DeniedError("invalid meter sample")
                try:
                    value = float(sample["value"])
                except ValueError:
                    raise DeniedError("invalid numeric sample") from None
                if not math.isfinite(value) or abs(value) > 1e15:
                    raise DeniedError("meter sample is not finite or bounded")
    elif not isinstance(payload.get("status"), str):
        raise DeniedError("invalid firmware/diagnostic status")
