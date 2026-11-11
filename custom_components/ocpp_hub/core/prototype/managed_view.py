"""Redacted cached management status and bounded history; reading never initiates charger operations."""

from __future__ import annotations

import json
import math
import time
from datetime import UTC, datetime
from typing import Any

from .store import Journal


# Reject booleans and non-finite values before calculating status ages or certificate coverage.
def finite_number(value: object) -> float | None:
    return (
        float(value)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        else None
    )


# A fresh file or API response must still prove the applicable app or intentional zero-hold authority.
def health_snapshot(value: dict[str, Any], now: float | None = None) -> dict[str, Any]:
    current = time.time() if now is None else now
    checked, native = finite_number(value.get("checked_at")), finite_number(value.get("native_observed_at"))
    age = current - checked if checked is not None else math.inf
    native_age = current - native if native is not None else math.inf
    remaining = finite_number(value.get("certificate_remaining_seconds"))
    paused = value.get("desired_control") == "paused"
    selected = finite_number(value.get("requested_current_a", 6))
    native_ceiling = 32 if value.get("current_capability") is True else 6
    authority = (
        value.get("local_hold_verified") is True
        and value.get("station_ceiling_a") == 0
        and value.get("current_a") == 0
        and value.get("power_w") == 0
        and value.get("charging_session_open") is False
        if paused
        else value.get("owner_control_enabled") is True and value.get("station_ceiling_a") == selected
    )
    local_okay = (
        0 <= age <= 45
        and 0 <= native_age <= 15
        and value.get("state") in ("healthy", "local_only")
        and value.get("running") is True
        and value.get("local_start_disabled") is True
        and value.get("desired_control", "app") in ("app", "paused")
        and authority
        and value.get("charger_connected") is True
        and value.get("boot_verified") is True
        and value.get("phase") == "active"
        and value.get("preservation_failed") is False
        and selected is not None
        and 6 <= selected <= native_ceiling
        and value.get("native_limit_a") == native_ceiling
        and remaining is not None
        and remaining > 1800
    )
    return {
        **value,
        "healthy": local_okay and value.get("cloud_connected") is True,
        "local_healthy": local_okay,
        "status_age_seconds": age if math.isfinite(age) else None,
        "native_age_seconds": native_age if math.isfinite(native_age) else None,
    }


# Only timestamps, energy and completion facts leave the journal; user tags and protocol identifiers stay private.
def session_history(journal: Journal) -> list[dict[str, Any]]:
    records = []
    for row in journal.db.execute("SELECT local,start,stop,vendor FROM transactions ORDER BY local DESC LIMIT 20"):
        start = json.loads(row["start"])
        stop = json.loads(row["stop"]) if row["stop"] else None
        records.append(
            {
                "started_at": start.get("timestamp"),
                "stopped_at": stop.get("timestamp") if stop else None,
                "energy_kwh": (stop["meterStop"] - start["meterStart"]) / 1000 if stop else None,
                "cloud_acknowledged": row["vendor"] is not None
                and not journal.db.execute(
                    "SELECT 1 FROM outbox WHERE local=? AND action IN ('StartTransaction','StopTransaction') LIMIT 1",
                    (row["local"],),
                ).fetchone(),
            }
        )
    return records


# Native history is independently refreshed with each successful audit; strip identities and reject incomplete records.
def charger_history(records: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for record in records:
        if record.get("transactionId") == -1:
            continue
        start, stop = record.get("startTime"), record.get("stopTime", 0)
        first, last = record.get("meterStart"), record.get("meterStop", 0)
        if (
            type(start) is not int
            or type(stop) is not int
            or type(first) is not int
            or type(last) is not int
            or not 0 < start <= time.time() + 30
            or not 0 <= stop <= time.time() + 30
            or not 0 <= first <= 10**12
            or not 0 <= last <= 10**12
            or stop != 0
            and (stop < start or last < first)
        ):
            continue
        summaries.append(
            {
                "started_at": datetime.fromtimestamp(start, UTC).isoformat(),
                "stopped_at": datetime.fromtimestamp(stop, UTC).isoformat() if stop else None,
                "energy_kwh": (last - first) / 1000 if stop else None,
            }
        )
    return sorted(summaries, key=lambda r: r["started_at"], reverse=True)[:20]
