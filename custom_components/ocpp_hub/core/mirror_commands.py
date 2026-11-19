"""Answers to the commands the vendor backend sends to its virtual charge point. None of them reaches the charger."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .frames import ERROR_FORMATION_VIOLATION, ERROR_NOT_SUPPORTED, build_call_error, build_call_result
from .mirror_queue import ACTION_BOOT, ACTION_STATUS, is_integer
from .readings import ScheduleWindow

# A fixed refusal per command. Every status below is a value OCPP 1.6 allows for that command, so a strict vendor
# backend still accepts the answer. ClearChargingProfile has no "Rejected": "Unknown" is its way to say nothing was
# cleared. UpdateFirmware and GetDiagnostics have an empty answer, so they are acknowledged and then never happen.
REFUSALS: dict[str, dict[str, Any]] = {
    "SetChargingProfile": {"status": "Rejected"},
    "ClearChargingProfile": {"status": "Unknown"},
    "RemoteStartTransaction": {"status": "Rejected"},
    "RemoteStopTransaction": {"status": "Rejected"},
    "ChangeConfiguration": {"status": "Rejected"},
    "Reset": {"status": "Rejected"},
    "ChangeAvailability": {"status": "Rejected"},
    "UnlockConnector": {"status": "UnlockFailed"},
    "DataTransfer": {"status": "Rejected"},
    "UpdateFirmware": {},
    "GetDiagnostics": {},
}

# The messages the mirror can send again from what it remembers.
TRIGGERABLE_ACTIONS = frozenset({ACTION_BOOT, ACTION_STATUS})


@dataclass(frozen=True, slots=True)
class TriggerRequest:
    """A TriggerMessage the mirror accepted: which message to send again, and for which connector."""

    message: str
    connector_id: int | None


@dataclass(frozen=True, slots=True)
class VendorAnswer:
    """The reply frame to send at once, and the follow-up to do after the reply has gone out."""

    frame: str
    trigger: TriggerRequest | None = None


# Answer one command from the vendor backend.
# Every command gets an answer, because a backend that gets none stops talking. can_trigger tells whether the mirror
# has the frame a TriggerMessage asks for.
def answer_vendor_call(
    unique_id: str,
    action: str,
    payload: dict[str, Any],
    configuration: Mapping[str, str],
    can_trigger: Callable[[TriggerRequest], bool],
    limit_a: float | None = None,
    schedule_for: Callable[[int], ScheduleWindow | None] | None = None,
) -> VendorAnswer:
    match action:
        case "GetConfiguration":
            return VendorAnswer(build_call_result(unique_id, _configuration_answer(payload, configuration)))
        case "TriggerMessage":
            return _trigger_answer(unique_id, payload, can_trigger)
        case "GetCompositeSchedule":
            return VendorAnswer(build_call_result(unique_id, _composite_answer(payload, limit_a, schedule_for)))
        case _ if action in REFUSALS:
            return VendorAnswer(build_call_result(unique_id, dict(REFUSALS[action])))
        case _:
            return VendorAnswer(build_call_error(unique_id, ERROR_NOT_SUPPORTED, f"{action} is not supported"))


# Answer GetCompositeSchedule from the limit the hub knows is in effect, so the vendor sees the limit that Home
# Assistant or its own app set. Without a known limit the vendor is told there is none to report (Rejected).
def _composite_answer(
    payload: dict[str, Any],
    limit_a: float | None,
    schedule_for: Callable[[int], ScheduleWindow | None] | None,
) -> dict[str, Any]:
    connector = payload.get("connectorId")
    duration = payload.get("duration")
    if not is_integer(connector) or not is_integer(duration) or duration <= 0:
        return {"status": "Rejected"}
    # A question about more than the present moment gets the charger's own schedule when the hub has it.
    real = schedule_for(duration) if schedule_for is not None and duration > 3600 else None
    if real is None:
        if limit_a is None:
            return {"status": "Rejected"}
        real = ScheduleWindow(duration, ((0, limit_a),))
    periods = [{"startPeriod": start, "limit": limit} for start, limit in real.periods]
    schedule = {"duration": real.duration_s, "chargingRateUnit": "A", "chargingSchedulePeriod": periods}
    return {"status": "Accepted", "connectorId": connector, "chargingSchedule": schedule}


# Describe the configuration the hub read from the charger, as read-only keys.
# The vendor gets the real values, so its view of the charger matches, but it can never change them.
def _configuration_answer(payload: dict[str, Any], configuration: Mapping[str, str]) -> dict[str, Any]:
    requested = payload.get("key")
    if isinstance(requested, list) and requested and all(isinstance(key, str) for key in requested):
        known = [key for key in requested if key in configuration]
        unknown = [key for key in requested if key not in configuration]
    else:
        known, unknown = list(configuration), []
    answer: dict[str, Any] = {
        "configurationKey": [{"key": key, "readonly": True, "value": configuration[key]} for key in known]
    }
    if unknown:
        answer["unknownKey"] = unknown
    return answer


# Accept a TriggerMessage for Boot or Status when the mirror still has that frame, refuse the rest politely.
# The triggered frame is sent after this reply, which is the order the protocol requires.
def _trigger_answer(
    unique_id: str, payload: dict[str, Any], can_trigger: Callable[[TriggerRequest], bool]
) -> VendorAnswer:
    message = payload.get("requestedMessage")
    connector = payload.get("connectorId")
    if not isinstance(message, str) or (connector is not None and not is_integer(connector)):
        return VendorAnswer(build_call_error(unique_id, ERROR_FORMATION_VIOLATION, "invalid TriggerMessage"))
    if message not in TRIGGERABLE_ACTIONS:
        return VendorAnswer(build_call_result(unique_id, {"status": "NotImplemented"}))
    request = TriggerRequest(message, connector)
    if not can_trigger(request):
        return VendorAnswer(build_call_result(unique_id, {"status": "Rejected"}))
    return VendorAnswer(build_call_result(unique_id, {"status": "Accepted"}), request)
