"""Redacted cached management status and bounded history; reading never initiates charger operations."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from .managed_diagnostics import snapshot as diagnostic_snapshot
from .managed_facts import ManagedFacts
from .managed_intent import OFFLINE_PAUSE
from .managed_release_wait import awaiting_known_stops, awaiting_observations
from .native_faults import STATION_CEILING_LOST
from .native_history import latest_completion_available
from .store import Journal
from .supervision import ControlProof, HealthVerdict, PollStatus

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence


NATIVE_SAMPLE_FIELDS = (
    "native_observed_at",
    "native_limit_a",
    "current_a",
    "power_w",
    "charging_session_open",
    "vehicle_drawing_power",
)


# The control poll has already finished. A failed projection withdraws displayed authority only.
def render_poll(
    c: ManagedController, outcome: PollStatus | tuple[HandoverEvidence, HealthVerdict], *, pending: bool = False
) -> dict[str, Any]:
    try:
        if isinstance(outcome, PollStatus):
            return poll_status(c, outcome, pending=pending)
        native, verdict = outcome
        return observed_status(c, poll_base(c), native, verdict)
    except sqlite3.Error:
        c.status_authority = None
        c.status_latest_closed = None
        c.status_vendor_epoch = None
        return {
            "state": "degraded",
            "fault": "status_unavailable",
            "checked_at": time.time(),
            "owner_control_enabled": False,
            "local_hold_verified": False,
        }


# An unavailable native sample leaves the independently saved proxy history intact.
def clear_native_sample(status: dict[str, Any]) -> None:
    status.update(dict.fromkeys((*NATIVE_SAMPLE_FIELDS, "vehicle_connected", "history_records")))
    status.update(charger_sessions=[], charger_history_complete=False, charger_latest_completion_known=False)
    status["last_session_energy_kwh"] = None


# Reject booleans and non-finite values before calculating status ages or certificate coverage.
def finite_number(value: object) -> float | None:
    return (
        float(value)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        else None
    )


# A fresh file or API response must still prove the applicable app or intentional zero-hold authority.
def health_snapshot(
    value: dict[str, Any], now: float | None = None, *, delivery_blocked: bool = False
) -> dict[str, Any]:
    current = time.time() if now is None else now
    checked, native = finite_number(value.get("checked_at")), finite_number(value.get("native_observed_at"))
    age = current - checked if checked is not None else math.inf
    native_age = current - native if native is not None else math.inf
    remaining = finite_number(value.get("certificate_remaining_seconds"))
    certificate_valid = remaining is not None and remaining - max(age, 0) > 1800
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
        and not delivery_blocked
        and value.get("cloud_start_pending") is not True
        and value.get("operator_reconciliation_required") is not True
        and selected is not None
        and 6 <= selected <= native_ceiling
        and value.get("native_limit_a") == native_ceiling
        and certificate_valid
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
            (value.get("operator_reconciliation_required") is not True, "command_outcome_unknown"),
            (bool(authority), "control_verification_pending"),
            (certificate_valid, "certificate_validity_insufficient"),
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
    """Redacted history and the valid completed sessions used to identify the latest one."""

    sessions: list[dict[str, Any]]
    complete: bool
    completed: dict[int, int]
    energy: dict[int, float]
    latest_completion_known: bool


# Use the known transaction order for managed charges, even when the charger clock moves backwards.
def summarize_charger_history(
    records: tuple[dict[str, Any], ...], latest_closed: int | None = None
) -> ChargerHistorySummary:
    summaries: list[dict[str, Any]] = []
    completed: dict[int, int] = {}
    energy: dict[int, float] = {}
    newest_unreadable_start = 0.0
    unreadable_recent = False
    latest_time = time.time() + 30
    for record in records:
        identifier = record.get("transactionId")
        if identifier == -1:
            continue
        raw_start, stop = record.get("startTime"), record.get("stopTime", 0)
        start = raw_start if type(raw_start) is int else 0
        first, last = record.get("meterStart"), record.get("meterStop", 0)
        start_valid = 0 < start <= latest_time
        if (
            not start_valid
            or type(identifier) is not int
            or type(stop) is not int
            or type(first) is not int
            or type(last) is not int
            or not 0 <= stop <= latest_time
            or not 0 <= first <= 10**12
            or not 0 <= last <= 10**12
            or stop != 0
            and (stop < start or last < first)
        ):
            newest_unreadable_start = max(newest_unreadable_start, start if start_valid else math.inf)
            if latest_closed is not None and (type(identifier) is not int or identifier >= latest_closed):
                unreadable_recent = True
            continue
        if stop:
            completed[identifier] = start
            energy[identifier] = (last - first) / 1000
        summaries.append(
            {
                "started_at": datetime.fromtimestamp(start, UTC).isoformat(),
                "stopped_at": datetime.fromtimestamp(stop, UTC).isoformat() if stop else None,
                "energy_kwh": energy[identifier] if stop else None,
            }
        )
    complete = newest_unreadable_start == 0
    if latest_closed is None:
        latest_known = complete or max(completed.values(), default=0) > newest_unreadable_start
    else:
        latest_known = not unreadable_recent and max(completed, default=None) == latest_closed
    return ChargerHistorySummary(
        sorted(summaries, key=lambda r: r["started_at"], reverse=True)[:20],
        complete,
        completed,
        energy,
        latest_known,
    )


# Callers needing just the bounded archive retain the same redacted list contract.
def charger_history(records: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    return summarize_charger_history(records).sessions


# Show evidence that has actually arrived, including replies which are still being saved.
def vendor_command_status(c: ManagedController) -> dict[str, Any]:
    try:
        evidence = c.session.gateway.vendor_write_evidence()
        return {
            "vendor_write_uncertain": len(evidence.uncertain),
            "vendor_reply_storage_pending": evidence.received,
            "operator_reconciliation_required": evidence.unresolved,
        }
    except sqlite3.Error:
        return dict.fromkeys(
            ("vendor_write_uncertain", "vendor_reply_storage_pending", "operator_reconciliation_required")
        )


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
        value.update(dict.fromkeys((*NATIVE_SAMPLE_FIELDS, "station_ceiling_a")))
    else:
        j = c.session.journal
        value.update(phase=j.phase.read().value, automatic_charge_restart=False)
        value.update(vendor_command_status(c))
        value.update(delivery_counts(j))
    return value


# An early return reports current vendor registration and no verified owner authority.
def poll_status(
    c: ManagedController, result: PollStatus, *, base: dict[str, Any] | None = None, pending: bool = False
) -> dict[str, Any]:
    value = poll_base(c, pending=pending) if base is None else dict(base)
    gateway = c.session.gateway
    c.status_vendor_epoch = gateway._vendor_epoch
    value["cloud_connected"] = (
        c.session.runtime.status.vendor_connected
        and gateway.vendor.sender is not None
        and gateway.vendor_registration == "Accepted"
    )
    value["state"] = result.state
    if result.fault is not None:
        value["fault"] = result.fault
    if result.intent is not None:
        value.update(desired_control=result.intent.mode, control_revision=result.intent.revision)
    return value


# The newest accepted closure survives charger clock changes between sessions.
def latest_closed_transaction(journal: Journal) -> int | None:
    row = journal.db.execute("SELECT local FROM transactions WHERE stop IS NOT NULL ORDER BY local DESC LIMIT 1")
    closed = row.fetchone()
    return int(closed[0]) if closed is not None else None


# A cache read failure leaves this status hint unknown without interrupting supervision.
def _reported_start_authorization(c: ManagedController) -> str | None:
    try:
        return c.charging.authorization_status()
    except sqlite3.Error:
        return None


# Render the complete fresh observation using the single health verdict; readiness retains its domain guards.
def observed_status(
    c: ManagedController, base: dict[str, Any], native: HandoverEvidence, verdict: HealthVerdict
) -> dict[str, Any]:
    from .managed_current import managed_current_ceiling

    j, g = c.session.journal, c.session.gateway
    try:
        closed = latest_closed_transaction(j)
        archive = j.get("native_history_summary")
        if archive is not None and not isinstance(archive, dict):
            archive = None
            history_read = False
        else:
            history_read = True
    except sqlite3.Error:
        closed, archive, history_read = None, None, False
    history = summarize_charger_history(native.history, closed)
    records_missing = bool(archive and (archive.get("missing_records") or archive.get("missing_attempts")))
    # A previously completed charge can come back damaged or open under the same transaction ID.
    check_archive = archive and (
        records_missing
        or not history.complete
        or any(row.get("transactionId") != -1 and row.get("stopTime", 0) == 0 for row in native.history)
    )
    latest_known = (
        history_read
        and history.latest_completion_known
        and (not check_archive or latest_completion_available(j, history.completed, closed))
    )
    # A delivered Stop can reach the journal before the charger includes that same assigned ID in its history.
    if closed is None:
        last_energy = next((row["energy_kwh"] for row in history.sessions if row["stopped_at"] is not None), None)
    else:
        last_energy = history.energy.get(closed)
    try:
        can_set_current = c.current.available(native)
        test_start_ready = c.charging.available(native)
        normal_start_ready = c.charging.available(native, normal=True)
    except sqlite3.Error:
        # Availability hints are optional. A command still checks its own prerequisites at admission.
        can_set_current = test_start_ready = normal_start_ready = False
    c.status_latest_closed = closed
    c.status_authority = c.authority()
    c.status_vendor_epoch = g._vendor_epoch
    return {
        **base,
        "start_authorization": _reported_start_authorization(c),
        "local_healthy": verdict.local_healthy,
        "checked_at": time.time(),
        "phase": j.phase.read().value,
        "cloud_connected": verdict.cloud_ready,
        "state": verdict.state,
        "owner_control_enabled": verdict.proof.shared,
        "desired_control": c.requests.intent().mode,
        "control_revision": c.requests.intent().revision,
        "local_hold_verified": verdict.proof.hold_verified,
        "native_observed_at": native.physical.observed_at,
        "native_limit_a": native.runtime_user_a,
        "requested_current_a": c.current.selected(),
        "current_capability": managed_current_ceiling(j, c.native.spec) == 32,
        "can_set_current": can_set_current,
        "station_ceiling_a": g.controls.desired(),
        "current_a": native.physical.current_a,
        "power_w": native.physical.power_w,
        "charging_session_open": native.physical.transaction_open,
        "vehicle_connected": native.physical.connection != "Unplug",
        "history_records": len(native.history),
        "test_charge": c.charging.summary(),
        "test_start_ready": test_start_ready,
        "local_charge": c.charging.normal_summary(native),
        "normal_start_ready": normal_start_ready,
        "history_archive": archive,
        "history_warning": "records_absent_from_charger" if records_missing else None,
        "charger_sessions": history.sessions,
        "charger_history_complete": history.complete and latest_known,
        "charger_latest_completion_known": latest_known,
        "last_session_energy_kwh": last_energy if latest_known else None,
        "preservation_failed": bool(j.get("trial_preservation_fault") or j.get("trial_inventory_fault")),
    }


@dataclass(frozen=True)
class ReportDetails:
    """Process-local software identity, transition history and optional output failures."""

    software: dict[str, Any]
    timeline: list[dict[str, Any]]
    output_errors: int


# Add the reporting fields in one place for the file and its cached socket projection.
def with_reporting(status: dict[str, Any], details: ReportDetails) -> dict[str, Any]:
    return {
        **status,
        "software": details.software,
        "diagnostics": {
            **status["diagnostics"],
            "timeline": details.timeline,
            "timeline_scope": "current_process",
            "reporting_errors": details.output_errors,
        },
    }


# Finish a poll with process metadata and optional evidence, then record the resulting transition.
def publish_poll(
    c: ManagedController,
    poll: dict[str, Any],
    *,
    pid: int,
    started: float,
    expires: float,
    observe: Callable[[dict[str, Any]], ReportDetails],
) -> dict[str, Any]:
    remaining = expires - time.time()
    result = dict(poll, pid=pid, started_at=started, certificate_remaining_seconds=remaining, running=True)
    if remaining <= 1800:
        result.update(state="degraded", fault="listener_certificate_expiring")
    result["diagnostics"] = diagnostic_snapshot(c, result)
    return with_reporting(result, observe(result))


# This status hint uses published evidence. Return execution rechecks its own authority and fresh zero.
def _return_refusal(c: ManagedController, mode: str, status: dict[str, Any]) -> str | None:
    if mode != "paused":
        return "pause_not_requested"
    if not status["healthy"]:
        return "service_unhealthy"
    if status.get("local_hold_verified") is not True:
        return "local_hold_unverified"
    if c.requests.return_in_progress():
        return "return_pending"
    if c.current.pending():
        return "current_update_pending"
    if c.charging.return_blocked():
        return "managed_start_active"
    if not c.requests.has_capacity():
        return "request_capacity_exhausted"
    journal = c.session.journal
    if not ManagedFacts(c).cloud_queue_empty() and not (
        awaiting_known_stops(journal) or awaiting_observations(journal)
    ):
        return "delivery_needs_reconciliation"
    return None


# Decide the owner-facing mode once from live work and the retained physical proof.
def control_mode(status: dict[str, Any], pending_fault: str | None, *, fresh: bool) -> str:
    paused = status["desired_control"] == "paused"
    if paused and status["pause_pending"]:
        return "pause_requested"
    if paused and status["return_pending"]:
        return "return_requested"
    if pending_fault == "current_update_pending":
        return "updating_current"
    if pending_fault == "start_operation_pending":
        return "start_pending"
    local = status.get("local_charge")
    if isinstance(local, dict) and local.get("state") in ("starting", "awaiting_session", "running", "stopping"):
        if not fresh or not status["local_healthy"]:
            return "waiting"
        if local["state"] in ("starting", "awaiting_session"):
            return "starting_locally"
        if local["state"] == "stopping":
            return "stopping_locally"
        return "local_charging" if local.get("charging") is True else "local_session"
    test = status.get("test_charge")
    if (
        isinstance(test, dict)
        and test.get("source") in ("local", "cloud")
        and test.get("state") in ("armed", "starting", "awaiting_power", "charging", "stopping")
    ):
        if not fresh or not status["healthy"]:
            return "waiting"
        return "local_test" if test["source"] == "local" else "app_test"
    if paused:
        if fresh and status["local_hold_verified"]:
            return "paused"
    elif fresh and status["local_healthy"] and status["owner_control_enabled"]:
        return "app_offline" if status.get("cloud_connected") is False else "app"
    return "attention" if status["state"] in ("attention", "degraded") else "waiting"


# Read the lifecycle and observation backlogs from the same journal snapshot.
def delivery_counts(journal: Journal) -> dict[str, int | None]:
    try:
        lifecycle, observations = journal.db.execute(
            "SELECT count(*) FILTER (WHERE action IN ('StartTransaction','StopTransaction')), "
            "count(*) FILTER (WHERE action NOT IN ('StartTransaction','StopTransaction')) FROM outbox"
        ).fetchone()
    except sqlite3.Error:
        lifecycle = observations = None
    return {"pending_lifecycle_events": lifecycle, "pending_observations": observations}


# Apply newer intent and refresh live availability without promoting stale poll evidence to current authority.
def refresh_for_read(c: ManagedController, published: dict[str, Any]) -> dict[str, Any]:
    result = dict(published)
    intent = c.requests.intent()
    g, runtime, j = c.session.gateway, c.session.runtime.status, c.session.journal
    control = j.control.read()
    phase = j.phase.read()
    result.update(
        phase=phase.value,
        cloud_start_pending=g.authorisation.pending(),
        requested_current_a=c.current.selected(),
    )
    result.update(vendor_command_status(c))
    if not runtime.charger_connected or g.charger.sender is None:
        result["charger_connected"] = False
    if not g.current_boot_seen(g._charger_epoch):
        result["boot_verified"] = False
    if (
        not runtime.vendor_connected
        or g.vendor.sender is None
        or g.vendor_registration != "Accepted"
        or c.status_vendor_epoch != g._vendor_epoch
    ):
        result["cloud_connected"] = False
    current, observed = c.authority(), c.status_authority
    current_worker, start_worker = c.current.worker, c.charging.worker
    pending_fault = None
    if c.current.pending() or (current_worker is not None and not current_worker.done()):
        pending_fault = "current_update_pending"
    elif start_worker is not None and not start_worker.done():
        pending_fault = "start_operation_pending"
    elif c.recovery.pending():
        pending_fault = "owner_release_pending"
    pause_pending = c.requests.pending_pause()
    station_lost = c.session.book.station_ceiling_lost()
    # A pending Pause may show the last sample for the same session, but a new lifecycle needs a new reading.
    native = c.latest_native
    sample_available = (
        result.get("charger_connected") is True
        and current is not None
        and observed is not None
        and current.scope == observed.scope
        and (current == observed or pause_pending and observed.intent.mode == "app")
        and native is not None
        and c.native.last_verified_at != 0
        and pending_fault is None
        and not station_lost
    )
    same_session = (
        sample_available
        and native is not None
        and c.current_native_sample() is native
        and native.physical.native_transaction_id == j.active()
    )
    if not sample_available:
        clear_native_sample(result)
    elif not same_session:
        result.update(
            current_a=None,
            power_w=None,
            charging_session_open=None,
            vehicle_drawing_power=None,
            vehicle_connected=None,
            local_charge=None,
            test_charge=None,
        )
    result.setdefault("last_session_energy_kwh", None)
    uncertain = result.get("operator_reconciliation_required") is True
    supervising = not c.stopping and not j.fault.latched() and not station_lost and not uncertain
    scope_current = same_session and current == observed and supervising
    intent_current = result.get("desired_control") == intent.mode and result.get("control_revision") == intent.revision
    if not (scope_current and intent_current):
        result.update(station_ceiling_a=None, local_charge=None, test_charge=None)
    published_fault = result.get("fault")
    published_pending = published_fault in (
        "current_update_pending",
        "start_operation_pending",
        "owner_release_pending",
    )
    # Only supervision can tell whether a cancelled Start still owns work that it must retire.
    cancelled_start = (
        published_fault == "start_operation_pending" and start_worker is not None and start_worker.cancelled()
    )
    # Retire a sampled command label once its live command has settled.
    if published_pending and "control_revision" in published and not cancelled_start:
        result.pop("fault")
    if station_lost and not c.stopping:
        result.update(
            state="attention",
            fault=j.fault.classification() if j.fault.latched() else STATION_CEILING_LOST,
            normal_start_ready=False,
            test_start_ready=False,
        )
    elif pending_fault is not None and supervising:
        result.update(state="reconciling", fault=pending_fault)
    elif not intent_current:
        result["state"] = "pausing" if intent.mode == "paused" and not published_pending else "reconciling"
    proof = ControlProof(
        shared=result.get("owner_control_enabled") is True,
        hold_verified=result.get("local_hold_verified") is True and result.get("charging_session_open") is False,
    ).retain(
        control,
        scope_current=scope_current and intent_current,
        released=phase.active and c.released_epoch == g._charger_epoch,
    )
    result.update(
        api_version=1,
        desired_control=intent.mode,
        control_revision=intent.revision,
        pause_pending=pause_pending,
        return_pending=intent.mode == "paused" and c.requests.return_in_progress(intent.revision),
        owner_control_enabled=proof.shared,
        local_hold_verified=proof.hold_verified,
    )
    if intent.mode == "paused" and (j.get(OFFLINE_PAUSE) is True or c.requests.offline_pause_current(intent)):
        result["pause_reason"] = "cloud_connection_failed"
    else:
        result.pop("pause_reason", None)
    result = health_snapshot(result, delivery_blocked=runtime.delivery_blocked)
    fresh = all(
        age is not None and 0 <= age <= 15 for age in (result["status_age_seconds"], result["native_age_seconds"])
    )
    if not fresh:
        result["last_session_energy_kwh"] = None
    elif result.get("charger_latest_completion_known") is True:
        try:
            same_closure = latest_closed_transaction(j) == c.status_latest_closed
        except sqlite3.Error:
            same_closure = False
        if not same_closure:
            result.update(
                last_session_energy_kwh=None, charger_latest_completion_known=False, charger_history_complete=False
            )
    if result["state"] in ("healthy", "local_only") and not result["healthy"]:
        result["state"] = "local_only" if result["local_healthy"] else "degraded"
    result["can_pause"] = not c.stopping
    result["stop_target"] = c.session_stop.target() if fresh and scope_current else None
    try:
        result.update(
            can_start_test=result["healthy"] and c.charging.available(native),
            can_start=result["local_healthy"] and c.charging.available(native, normal=True),
            can_set_current=result["local_healthy"] and c.current.available(native),
            can_return=_return_refusal(c, intent.mode, result) is None,
        )
    except sqlite3.Error:
        result.update(can_start_test=False, can_start=False, can_set_current=False, can_return=False)
    result["start_authorization"] = _reported_start_authorization(c)
    result["control_mode"] = control_mode(result, pending_fault if supervising else None, fresh=fresh)
    result.update(delivery_counts(j))
    result["sessions"] = session_history(j)
    # Refresh active diagnostics from the same live view and keep the poll's recorded reporting history.
    reported = published.get("diagnostics")
    if isinstance(reported, dict):
        result["diagnostics"] = diagnostic_snapshot(c, result) | {
            key: reported[key] for key in ("timeline", "timeline_scope", "reporting_errors") if key in reported
        }
    return result
