"""Observed iCS mode and plan writes; exact schemas and real peer outcomes only."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..frames import Frame
from .ics import unique_object
from .plans import PlanBook
from .policy import DeniedError, Policy, integer, number
from .rpc import UnavailableError
from .store import Journal, encoded

WRITES = frozenset({"setTransaction", "setPlan", "clearPlans", "delayStop"})
MODES = frozenset({"Fast", "Eco", "EcoPlus"})
PLAN_KEYS = {
    "planId",
    "type",
    "stopModel",
    "limitCurrent",
    "stopValue",
    "validTime",
    "startTime",
    "connectorId",
    "kindType",
    "recurring",
    "idTag",
    "eco",
}


# No other vendor or message inherits the observed command's codec or classification.
def known_write(payload: dict[str, Any]) -> bool:
    return (
        payload.get("vendorId") == "IoCharger"
        and isinstance(payload.get("messageId"), str)
        and payload["messageId"] in WRITES
    )


# Parse a bounded string, retaining wire types and rejecting ambiguous JSON objects everywhere.
def data_value(value: Any) -> Any:
    if not isinstance(value, str):
        raise DeniedError("iCS data must remain a JSON string")
    try:
        if len(value.encode()) > 8192:
            raise DeniedError("iCS command data is too large")
        data = json.loads(value, object_pairs_hook=unique_object)
        encoded(data)
        return data
    except ValueError, RecursionError:
        raise DeniedError("invalid iCS command JSON") from None


# Preserve the exact observed UTC wire representation; parsing alone would allow compact and week dates.
def utc_time(value: Any) -> None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z", value) is None:
        raise DeniedError("time must use the observed UTC wire format")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DeniedError("invalid UTC instant") from None


# Validate the complete evidenced command before allocating ids or contacting the charger.
def validate_write(payload: dict[str, Any], policy: Policy) -> Any:
    if not known_write(payload) or set(payload) != {"vendorId", "messageId", "data"}:
        raise DeniedError("unobserved iCS write envelope")
    data = data_value(payload["data"])
    message = payload["messageId"]
    if message == "clearPlans":
        if not isinstance(data, list) or not 1 <= len(data) <= 128:
            raise DeniedError("clearPlans requires explicit plan ids")
        ids = [integer(value, 1) for value in data]
        if len(set(ids)) != len(ids):
            raise DeniedError("duplicate plan ids")
        return ids
    if message == "delayStop":
        if not isinstance(data, dict) or set(data) not in ({"transactionId"}, {"transactionId", "stopTime"}):
            raise DeniedError("unobserved delayed-stop fields")
        integer(data["transactionId"], 1)
        if "stopTime" in data:
            utc_time(data["stopTime"])
        return data
    keys = {"transactionId", "eco"} if message == "setTransaction" else PLAN_KEYS
    if not isinstance(data, dict) or set(data) != keys:
        raise DeniedError("unobserved iCS command fields")
    if message == "setTransaction":
        integer(data["transactionId"], 1)
        if not isinstance(data["eco"], str) or data["eco"] not in MODES:
            raise DeniedError("unobserved transaction eco mode")
        return data
    integer(data["planId"], 1)
    integer(data["connectorId"], 1, 1)
    integer(data["startTime"], 0, 1439)
    if data["type"] != "Plan" or data["eco"] != "Fast":
        raise DeniedError("unobserved plan type or eco mode")
    if not isinstance(data["idTag"], str) or data["idTag"] not in policy.allowed_tags:
        raise DeniedError("plan tag is not explicitly approved")
    if number(data["limitCurrent"], policy.max_current_a) < 6:
        raise DeniedError("unobserved plan current below six amps")
    if data["kindType"] == "Recurring":
        integer(data["recurring"], 1, 127)
        if data["stopModel"] != "Duration":
            raise DeniedError("unobserved recurring stop model")
    elif data["kindType"] == "Relative":
        integer(data["recurring"], 0, 0)
    else:
        raise DeniedError("unobserved plan recurrence kind")
    duration = number(data["stopValue"], 1440)
    if data["stopModel"] == "Duration":
        if duration < 1 or not duration.is_integer():
            raise DeniedError("duration requires whole positive minutes")
    elif data["stopModel"] != "Full" or data["kindType"] != "Relative" or duration != 0:
        raise DeniedError("unobserved stop condition")
    utc_time(data["validTime"])
    return data


@dataclass(frozen=True)
class IcsWrite:
    """One translated command and its pending plan effect; no body is persisted."""

    payload: dict[str, Any]
    plans: tuple[int, ...] = ()
    clearing: bool = False

    # Called inside the outgoing CALL gate and the journal transaction after current authority checks.
    @classmethod
    def prepare(cls, payload: dict[str, Any], policy: Policy, journal: Journal, book: PlanBook) -> IcsWrite:
        data = validate_write(payload, policy)
        message = payload["messageId"]
        plans: tuple[int, ...] = ()
        if message in ("setTransaction", "delayStop"):
            local = journal.local_id(data["transactionId"])
            if local is None:
                raise DeniedError("mode write names no active mapped transaction")
            data = {**data, "transactionId": local}
        elif message == "setPlan":
            vendor = data["planId"]
            plans = (vendor,)
            data = {**data, "planId": book.reserve(vendor)}
        else:
            plans = tuple(data)
            data = book.clear_targets(data)
        return cls({**payload, "data": encoded(data)}, plans, message == "clearPlans")

    # Preserve actual refusals/errors; an unknown success body cannot establish a confirmed effect.
    def accepted(self, frame: Frame) -> bool:
        if frame[0] == 4:
            return False
        result = frame[2]
        status = result.get("status")
        if set(result) - {"status", "data"} or result.get("data", "") != "":
            raise UnavailableError("unobserved iCS write response")
        if status == "Accepted" and set(result) == {"status", "data"}:
            return True
        if status in ("Rejected", "UnknownMessageId", "UnknownVendorId"):
            return False
        raise UnavailableError("iCS write outcome was not confirmed")
