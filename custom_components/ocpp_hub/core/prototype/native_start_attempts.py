"""Recognise iCS start-only history records whose OCPP transaction number was never assigned."""

from __future__ import annotations

from typing import Any

from .policy import integer
from .security import SecurityError

# The firmware reports offered capacity separately from the three measured phase currents.
# A new field on an unnumbered record needs interpretation before the record can establish zero draw.
_ATTEMPT_FIELDS = frozenset(
    "syncStart connectorId idTagStart idTypeStart meterStart startQ4 reservationId startTime startTimeStr startMs "
    "transactionId syncStop idTagStop idTypeStop meterStop stopQ4 stopTime stopTimeStr stopMs reason reasonStr "
    "MeterValues filename".split()
)
_SAMPLE_FIELDS = frozenset(
    "syncMeter timestamp timestampStr timeMs V A energy Q4dot Temp Offer PowerOffer context".split()
)


# Separate unallocated records from numbered sessions while retaining the full native body and its opaque filename.
def split_attempts(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    numbered = []
    attempts: dict[str, dict[str, Any]] = {}
    try:
        if not isinstance(records, list):
            raise ValueError
        for row in records:
            if not isinstance(row, dict):
                raise ValueError
            if row["transactionId"] != -1:
                numbered.append(row)
                continue
            filename = row["filename"]
            start = integer(row["startTime"], 1)
            integer(row["startMs"], 0, 999)
            integer(row["meterStart"], 0, 10**12)
            samples = row["MeterValues"]
            if (
                type(row["transactionId"]) is not int
                or row.keys() - _ATTEMPT_FIELDS
                or not isinstance(filename, str)
                or not 1 <= len(filename) <= 255
                or filename in attempts
                or not isinstance(row["idTagStart"], str)
                or not 1 <= len(row["idTagStart"]) <= 20
                or not isinstance(samples, list)
                or len(samples) != 1
            ):
                raise ValueError
            for field, expected in (
                ("connectorId", 1),
                ("syncStart", 1),
                ("syncStop", 0),
                ("idTypeStop", 0),
                ("reason", 0),
                ("meterStop", 0),
                ("stopTime", 0),
                ("stopMs", 0),
                ("stopQ4", 0),
            ):
                integer(row[field], expected, expected)
            if any(row[key] != "" for key in ("idTagStop", "stopTimeStr", "reasonStr")):
                raise ValueError
            sample = samples[0]
            if not isinstance(sample, dict) or sample.keys() - _SAMPLE_FIELDS:
                raise ValueError
            integer(sample["syncMeter"], 1, 1)
            integer(sample["context"], 5, 5)
            integer(sample["timestamp"], start)
            integer(sample["energy"], row["meterStart"], row["meterStart"])
            q4 = integer(row["startQ4"], 0)
            integer(sample["Q4dot"], q4, q4)
            if not isinstance(sample["A"], list) or len(sample["A"]) != 3:
                raise ValueError
            for current in sample["A"]:
                integer(current, 0, 0)
            attempts[filename] = row
        if any(isinstance(row.get("filename"), str) and row["filename"] in attempts for row in numbered):
            raise ValueError
    except ValueError, TypeError, KeyError, IndexError:
        raise SecurityError("unassigned native history has incomplete or contradictory start evidence") from None
    return numbered, attempts
