"""Redacted cached management status and bounded history; reading never initiates charger operations."""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from .store import Journal
from .supervision import HealthVerdict, PollStatus

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence


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
        and value.get("cloud_start_pending") is not True
        and selected is not None
        and 6 <= selected <= native_ceiling
        and value.get("native_limit_a") == native_ceiling
        and remaining is not None
        and remaining > 1800
    )
    blockers = [
        reason
        for passed, reason in (
            (0 <= age <= 45, "status_stale"),
            (0 <= native_age <= 15, "native_stale"),
            (value.get("running") is True, "service_not_running"),
            (value.get("charger_connected") is not False, "charger_disconnected"),
            (value.get("boot_verified") is not False, "boot_unverified"),
            (value.get("cloud_start_pending") is not True, "cloud_start_pending"),
            (bool(authority), "control_verification_pending"),
            (remaining is not None and remaining > 1800, "certificate_validity_insufficient"),
            (value.get("cloud_connected") is not False, "cloud_disconnected"),
        )
        if not passed
    ]
    if not local_okay and not any(reason != "cloud_disconnected" for reason in blockers):
        blockers.append("local_health_unverified")
    return {
        **value,
        "healthy": local_okay and value.get("cloud_connected") is True,
        "local_healthy": local_okay,
        "status_age_seconds": age if math.isfinite(age) else None,
        "native_age_seconds": native_age if math.isfinite(native_age) else None,
        "health_blockers": blockers,
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


@dataclass(frozen=True)
class ChargerHistorySummary:
    """Redacted valid records and whether any real record could not be represented honestly."""

    sessions: list[dict[str, Any]]
    complete: bool


# A rejected timestamp cannot establish which session was latest; retain this fact alongside the valid summaries.
def summarize_charger_history(records: tuple[dict[str, Any], ...]) -> ChargerHistorySummary:
    summaries: list[dict[str, Any]] = []
    complete = True
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
            complete = False
            continue
        summaries.append(
            {
                "started_at": datetime.fromtimestamp(start, UTC).isoformat(),
                "stopped_at": datetime.fromtimestamp(stop, UTC).isoformat() if stop else None,
                "energy_kwh": (last - first) / 1000 if stop else None,
            }
        )
    return ChargerHistorySummary(sorted(summaries, key=lambda r: r["started_at"], reverse=True)[:20], complete)


# Callers needing just the bounded archive retain the same redacted list contract.
def charger_history(records: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    return summarize_charger_history(records).sessions


# Both poll shapes share connection and intent fields; pending work deliberately omits durable-history fields.
def poll_base(c: ManagedController, *, pending: bool = False) -> dict[str, Any]:
    intent = c.requests.intent()
    g, runtime = c.session.gateway, c.session.runtime.status
    value: dict[str, Any] = {
        "checked_at": time.time(),
        "mode": "app_schedule",
        "desired_control": intent.mode,
        "control_revision": intent.revision,
        "local_start_disabled": not g.policy.allow_local_start,
        "charger_connected": runtime.charger_connected,
        "cloud_connected": runtime.vendor_connected and g.vendor_registration == "Accepted",
        "boot_verified": g.current_boot_seen(g._charger_epoch),
        "cloud_start_pending": g.authorisation.pending(),
        "owner_control_enabled": False,
        "local_hold_verified": False,
    }
    if pending:
        value.update(
            {
                key: None
                for key in (
                    "native_observed_at",
                    "native_limit_a",
                    "current_a",
                    "power_w",
                    "station_ceiling_a",
                    "charging_session_open",
                    "vehicle_drawing_power",
                )
            }
        )
    else:
        j = c.session.journal
        value.update(
            phase=j.phase.read().value,
            pending_lifecycle_events=j.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE action IN ('StartTransaction','StopTransaction')"
            ).fetchone()[0],
            pending_observations=j.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE action NOT IN ('StartTransaction','StopTransaction')"
            ).fetchone()[0],
            automatic_charge_restart=False,
            vendor_write_uncertain=j.db.execute(
                "SELECT COUNT(*) FROM mutations WHERE origin='vendor' AND state!='confirmed'"
            ).fetchone()[0],
            operator_reconciliation_required=bool(j.get("uncertain_vendor_command"))
            or j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed'").fetchone()
            is not None,
        )
    return value


# A stopped pass publishes its named state and no verified owner authority.
def poll_status(
    c: ManagedController, result: PollStatus, *, base: dict[str, Any] | None = None, pending: bool = False
) -> dict[str, Any]:
    value = poll_base(c, pending=pending) if base is None else dict(base)
    value["state"] = result.state
    if result.fault is not None:
        value["fault"] = result.fault
    if result.intent is not None:
        value.update(desired_control=result.intent.mode, control_revision=result.intent.revision)
    return value


# Render the complete fresh observation using the single health verdict; readiness retains its domain guards.
def observed_status(
    c: ManagedController, base: dict[str, Any], native: HandoverEvidence, verdict: HealthVerdict
) -> dict[str, Any]:
    from .managed_current import managed_current_ceiling

    j, g = c.session.journal, c.session.gateway
    history = summarize_charger_history(native.history)
    return {
        **base,
        "start_authorization": c.charging.authorization_status(),
        "local_healthy": verdict.local_healthy,
        "checked_at": time.time(),
        "phase": j.phase.read().value,
        "cloud_connected": verdict.cloud_ready,
        "state": verdict.state,
        "owner_control_enabled": verdict.shared,
        "desired_control": c.requests.intent().mode,
        "control_revision": c.requests.intent().revision,
        "local_hold_verified": verdict.hold_verified,
        "native_observed_at": native.physical.observed_at,
        "native_limit_a": native.runtime_user_a,
        "requested_current_a": c.current.selected(),
        "current_capability": managed_current_ceiling(j, c.native.spec) == 32,
        "can_set_current": c.current.available(native),
        "station_ceiling_a": g.controls.desired(),
        "current_a": native.physical.current_a,
        "power_w": native.physical.power_w,
        "charging_session_open": native.physical.transaction_open,
        "vehicle_connected": native.physical.connection != "Unplug",
        "history_records": len(native.history),
        "test_charge": c.charging.summary(),
        "test_start_ready": c.charging.available(native),
        "local_charge": c.charging.normal_summary(),
        "normal_start_ready": c.charging.available(native, normal=True),
        "history_archive": j.get("native_history_summary"),
        "history_warning": "records_absent_from_charger"
        if j.get("native_history_summary", {}).get("missing_records", 0)
        else None,
        "charger_sessions": history.sessions,
        "charger_history_complete": history.complete,
        "preservation_failed": bool(j.get("trial_preservation_fault") or j.get("trial_inventory_fault")),
    }
