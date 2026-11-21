"""Bounded diagnostic vocabulary shared by the dashboard, notice and cached support download."""

from __future__ import annotations

from typing import Any

REASONS = {
    "fault_latched": "A recorded fault still requires verified reconciliation.",
    "strict_native_refusal": "A charger-data contradiction blocks ordinary recovery; operator review is required.",
    "native_audit_outcome_unknown": (
        "An interrupted verification has an unknown outcome; ordinary recovery remains blocked."
    ),
    "native_observation_unavailable": "Fresh independent charger readings are unavailable.",
    "current_update_pending": "A current update is awaiting its required operation or recorded outcome.",
    "start_operation_pending": "A Start operation is awaiting its required operation or recorded outcome.",
    "cloud_start_pending": (
        "A manufacturer cloud Start is awaiting its matching session; further controls remain blocked."
    ),
    "charger_disconnected": "The charger connection is unavailable.",
    "cloud_disconnected": "The manufacturer cloud connection is unavailable.",
    "boot_unverified": "The current charger connection has not completed registration verification.",
    "pause_verification_pending": "Pause is requested; physical zero output has not yet been verified.",
    "lifecycle_acknowledgement_pending": "A genuine session event is awaiting the manufacturer's acknowledgement.",
    "command_outcome_unknown": "A previously issued command has an unresolved outcome.",
    "vendor_reply_storage_pending": "The charger answered, and the proxy is saving the reply.",
    "delivery_storage_pending": "Delivery is waiting for required journal storage to recover.",
    "preservation_review_required": "Preserved configuration or charger resources require operator review.",
    "status_stale": "The service status is stale or its timestamp is unavailable.",
    "native_stale": "The independent charger reading is stale or its timestamp is unavailable.",
    "service_not_running": "The service is not reporting a running process.",
    "control_verification_pending": "The requested control state has not been verified.",
    "certificate_validity_insufficient": "The loaded listener certificate has insufficient remaining validity.",
    "local_health_unverified": "The local service has not passed every required health check.",
}
START_AUTHORIZATION = {
    "renewal_required": "Start will request fresh approval from the manufacturer cloud before charging.",
    "expired_cloud_unavailable": "Local Start needs fresh approval, but the manufacturer cloud is unavailable.",
    "denied": "The manufacturer has not approved a configured local Start tag.",
    "no_approved_tag": "No local Start tag is configured.",
}
FAULTS = {
    "owner_release_unconfirmed": "Returning app/schedule control was not confirmed.",
    "native_authority_or_preservation_failed": "Charger authority or preservation verification failed.",
    "measured_output_limit_exceeded": "Fully validated measurements exceeded the selected output bound.",
    "station_composite_current_exceeded": "The charger reported an effective current limit above the selected limit.",
    "station_ceiling_lost": "The charger lost its current-limit profile.",
    "owner_control_or_delivery_uncertain": "Control or command delivery has an uncertain outcome.",
    "operator_debt_reconciled_at_zero": "An operator reconciliation still requires its remaining verification.",
    "native_observation_unavailable": REASONS["native_observation_unavailable"],
    "native_repair_publication_pending": "The proxy is saving the result of its charger check.",
    "owner_release_pending": "The proxy is finishing the return to app and schedule control.",
    "status_unavailable": "The proxy could not read its latest status.",
    "current_update_pending": REASONS["current_update_pending"],
    "start_operation_pending": REASONS["start_operation_pending"],
    "listener_certificate_expiring": REASONS["certificate_validity_insufficient"],
    "temporary_schedule_cleanup_unconfirmed": "Temporary-schedule cleanup has not been confirmed.",
    "temporary_schedule_awaiting_start_report": "The temporary schedule is awaiting its actual session report.",
    "temporary_schedule_stop_unconfirmed": "The temporary schedule's Stop has not been confirmed.",
    "other_fault": "Another recorded fault requires review.",
}
STATES = frozenset(
    {
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
    }
)
PHASES = frozenset(
    {
        "active",
        "recovering",
        "blocked",
        "complete",
        "ready",
        "prepared",
        "admitted",
        "rollback",
        "backend_restore_pending",
    }
)
ACTIONS = frozenset(
    {"pause", "app_control", "start", "start_test", "arm_cloud_test", "set_current", "temporary_schedule", "other"}
)
RECOVERIES = frozenset({"not_assessed", "verification_pending", "strict_refusal", "audit_outcome_unknown"})
RPC_ACTIONS = frozenset(
    {
        "None",
        "Other",
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
RPC_PHASES = frozenset({"idle", "sending", "awaiting_reply", "reply_received", "unconfirmed", "complete"})


# Bound before arithmetic, accepting neither booleans nor enormous JSON integers as numeric observations.
def number(value: Any, maximum: float = 2**53) -> int | float | None:
    return value if type(value) in (int, float) and 0 <= value <= maximum else None


# Unknown or malformed peer labels never become arbitrary strings in entities, notices or diagnostic downloads.
def label(value: Any, choices: Any) -> str | None:
    return value if isinstance(value, str) and value in choices else None


# Pending acceptance and uncertain delivery remain distinct; terminal or arbitrary peer receipts are not projected.
def command(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    action, state = label(value.get("action"), ACTIONS), label(value.get("state"), {"accepted", "running", "uncertain"})
    if action is None or state is None:
        return None
    expected = "unknown" if state == "uncertain" else "pending"
    confirmation = label(value.get("confirmation"), {expected, "reply_received"})
    if (
        confirmation is None
        or "accepted_at" not in value
        or "age_seconds" not in value
        or any(value[key] is not None and number(value[key]) is None for key in ("accepted_at", "age_seconds"))
        or value["accepted_at"] is None
        and value["age_seconds"] is not None
    ):
        return None
    return {
        "action": action,
        "state": state,
        "accepted_at": number(value.get("accepted_at")),
        "age_seconds": number(value.get("age_seconds")),
        "confirmation": confirmation,
    }


# Copy only fixed protocol labels/counters, excluding arbitrary exception types, reasons, frame bodies and IDs.
def protocol(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {
        "reconnects": number(value.get("reconnects")),
        "connection_attempts": number(value.get("connection_attempts")),
        "recent_handler_failures": number(value.get("recent_handler_failures")),
        "delivery_blocked": value.get("delivery_blocked") if type(value.get("delivery_blocked")) is bool else None,
        "delivery_cause": label(value.get("delivery_cause"), {"storage", "reconciliation"}),
    }
    for name in ("charger", "cloud"):
        leg = value.get(name)
        if not isinstance(leg, dict):
            continue
        rpc, fault = leg.get("rpc"), leg.get("fault")
        rpc = rpc if isinstance(rpc, dict) else {}
        fault = fault if isinstance(fault, dict) else {}
        result[name] = {
            "rpc": {
                "action": label(rpc.get("action"), RPC_ACTIONS),
                "phase": label(rpc.get("phase"), RPC_PHASES),
                "failed_phase": label(rpc.get("failed_phase"), RPC_PHASES),
                **{
                    key: number(rpc.get(key))
                    for key in ("send_attempts", "matched_replies", "unmatched_replies", "accepted_registrations")
                },
                "last_registration": label(rpc.get("last_registration"), {"Accepted", "Pending", "Rejected"}),
            },
            "fault": {
                "count": number(fault.get("count")),
                "action": label(fault.get("action"), RPC_ACTIONS),
                "stage": label(
                    fault.get("stage"),
                    {
                        "opening",
                        "registration",
                        "history",
                        "heartbeat",
                        "authorization",
                        "session",
                        "charger_receive",
                        "charger_retirement",
                        "boot_or_ceiling",
                        "boot_probe",
                    },
                ),
                **{
                    key: number(fault.get(key), 4999)
                    if number(fault.get(key), 4999) is not None and fault[key] >= 1000
                    else None
                    for key in ("received_close_code", "sent_close_code")
                },
            },
        }
    return result


# Timeline entries have their own reconstruction, so nested extra fields cannot bypass the report allowlist.
def transition(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or label(value.get("state"), STATES) is None:
        return None
    blockers = value.get("blockers")
    return {
        "at_epoch": number(value.get("at_epoch")),
        "state": label(value.get("state"), STATES),
        "fault": label(value.get("fault"), FAULTS),
        "phase": label(value.get("phase"), PHASES),
        "desired_control": label(value.get("desired_control"), {"app", "paused"}),
        "control_revision": number(value.get("control_revision")),
        "blockers": [item for item in blockers[:20] if label(item, REASONS)] if isinstance(blockers, list) else [],
        "recovery": label(value.get("recovery"), RECOVERIES),
        "diagnostics_available": value.get("diagnostics_available") is True,
    }


# Older proxies remain usable; missing/new optional schemas yield unavailable detail without changing base health.
def diagnostics(value: Any, *, timeline: bool = False) -> dict[str, Any]:
    if not isinstance(value, dict) or type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        return {"available": False, "reason": "unsupported_or_missing"}
    if value.get("available") is not True:
        return {"available": False, "reason": "evidence_unavailable"}
    fault, workers, rows, blockers = (
        value.get("original_fault"),
        value.get("workers"),
        value.get("pending_commands"),
        value.get("blockers"),
    )
    if (
        number(value.get("sampled_at")) is None
        or label(value.get("recovery"), RECOVERIES) is None
        or not isinstance(blockers, list)
        or any(label(item, REASONS) is None for item in blockers)
        or not isinstance(rows, list)
        or any(command(row) is None for row in rows)
        or number(value.get("pending_command_count")) is None
        or type(value.get("pending_command_count")) is not int
        or len(rows) > 5
        or value["pending_command_count"] < len(rows)
        or type(value.get("commands_truncated")) is not bool
        or value["commands_truncated"] != (value["pending_command_count"] > len(rows))
        or (value.get("current_fault") is not None and label(value["current_fault"], FAULTS) is None)
        or (fault is not None and (not isinstance(fault, dict) or label(fault.get("code"), FAULTS) is None))
    ):
        return {"available": False, "reason": "evidence_unavailable"}
    fault = fault if isinstance(fault, dict) else {}
    workers = workers if isinstance(workers, dict) else {}
    result = {
        "available": True,
        "sampled_at": number(value.get("sampled_at")),
        "original_fault": {"code": label(fault.get("code"), FAULTS), "first_at": number(fault.get("first_at"))},
        "current_fault": label(value.get("current_fault"), FAULTS),
        "blockers": list(dict.fromkeys(item for item in blockers[:20] if label(item, REASONS)))
        if isinstance(blockers, list)
        else [],
        "recovery": label(value.get("recovery"), RECOVERIES) or "not_assessed",
        "pending_commands": [item for row in rows[:5] if (item := command(row)) is not None]
        if isinstance(rows, list)
        else [],
        "pending_command_count": number(value.get("pending_command_count")),
        "commands_truncated": value.get("commands_truncated") is True,
        "workers": {
            **{
                key: label(workers.get(key), {"absent", "running", "done", "cancelled"}) for key in ("current", "start")
            },
            "owned_io_pending": workers.get("owned_io_pending")
            if type(workers.get("owned_io_pending")) is bool
            else None,
        },
        "protocol": protocol(value.get("protocol")),
        "reporting_errors": number(value.get("reporting_errors")),
    }
    if timeline:
        events = value.get("timeline")
        result["timeline"] = (
            [item for row in events[-20:] if (item := transition(row)) is not None] if isinstance(events, list) else []
        )
        result["timeline_scope"] = "current_process" if value.get("timeline_scope") == "current_process" else "unknown"
    return result


# English text is chosen locally, rather than rendering unchecked peer exception strings as advice.
def explanation(data: dict[str, Any], *, fresh: bool, native_fresh: bool) -> str:
    if not fresh:
        return "Proxy status is unavailable or stale; retained diagnostic details are last known."
    detail = diagnostics(data.get("diagnostics"))
    if not detail["available"]:
        return "Detailed diagnostic evidence is unavailable."
    if detail["recovery"] == "strict_refusal":
        return REASONS["strict_native_refusal"]
    if detail["current_fault"] == "native_repair_publication_pending":
        return FAULTS["native_repair_publication_pending"]
    if detail["recovery"] == "audit_outcome_unknown":
        return REASONS["native_audit_outcome_unknown"]
    health = data.get("health_blockers")
    blockers = detail["blockers"] + (
        [item for item in health[:20] if label(item, REASONS)] if isinstance(health, list) else []
    )
    if "delivery_storage_pending" in blockers:
        return REASONS["delivery_storage_pending"]
    fault = detail["original_fault"]["code"]
    if fault:
        return FAULTS[fault]
    if detail["current_fault"]:
        return FAULTS[detail["current_fault"]]
    authorization = label(data.get("start_authorization"), START_AUTHORIZATION)
    if authorization == "expired_cloud_unavailable" and native_fresh and set(blockers) == {"cloud_disconnected"}:
        return START_AUTHORIZATION[authorization]
    if blockers:
        return REASONS[blockers[0]]
    if not native_fresh:
        return REASONS["native_stale"]
    if detail["pending_command_count"]:
        rows = detail["pending_commands"]
        if any(row["confirmation"] == "pending" for row in rows):
            return "A recorded command is still awaiting its confirmed outcome."
        if any(row["confirmation"] == "reply_received" for row in rows):
            return "The charger replied. The proxy is still checking the outcome."
        if (
            data.get("desired_control") == "paused"
            and rows
            and not detail["commands_truncated"]
            and all(row["action"] == "app_control" for row in rows)
        ):
            if data.get("can_return") is True:
                return "The previous Return ended without confirmation. Press Return again to try again."
            return "The previous Return ended without confirmation."
        return REASONS["command_outcome_unknown"]
    if authorization is not None:
        return START_AUTHORIZATION[authorization]
    return (
        "No problem reported in the latest verified status."
        if data.get("healthy") is True
        else REASONS["local_health_unverified"]
    )
