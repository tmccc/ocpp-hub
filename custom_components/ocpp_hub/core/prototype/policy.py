"""Explicit single-phase limits and command ownership for the isolated gateway."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any

from ..control import HUB_PROFILE_ID

READS = frozenset({"GetConfiguration", "TriggerMessage", "GetCompositeSchedule"})
WRITES = frozenset(
    {
        "RemoteStartTransaction",
        "RemoteStopTransaction",
        "SetChargingProfile",
        "ClearChargingProfile",
        "ChangeConfiguration",
        "ChangeAvailability",
        "UnlockConnector",
    }
)


class DeniedError(ValueError):
    """A request does not fit the explicitly approved prototype policy."""


# Accept bounded finite numbers without bool conversion, coercion or huge-integer overflow.
def number(value: Any, maximum: float) -> float:
    if type(value) not in (int, float) or not 0 <= value <= maximum:
        raise DeniedError("number outside the configured range")
    result = float(value)
    if not math.isfinite(result):
        raise DeniedError("number must be finite")
    return result


# Protocol connector, profile and transaction ids must be actual bounded integers.
def integer(value: Any, minimum: int = 0, maximum: int = 2**31 - 1) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise DeniedError("invalid integer identifier")
    return value


@dataclass(frozen=True)
class Policy:
    """A synthetic test policy; no actual household permissions or credentials are embedded."""

    max_current_a: float = 16.0
    phases: int = 1
    local_stack_level: int = 16
    trust_vendor_start_tags: bool = False
    vendor_tag_ttl_s: int = 120
    allowed_tags: frozenset[str] = frozenset({"LOCAL_TEST"})
    extensions: dict[tuple[str, str], str] = field(default_factory=dict)
    configuration_keys: frozenset[str] = frozenset()
    extra_vendor_actions: frozenset[str] = frozenset()

    # Reject a malformed policy at construction rather than weakening validation later.
    def __post_init__(self) -> None:
        if number(self.max_current_a, 32) < 6 or self.phases != 1:
            raise DeniedError("prototype requires a 6–32 A single-phase ceiling")
        integer(self.local_stack_level, 1, 255)
        integer(self.vendor_tag_ttl_s, 1, 300)
        if type(self.trust_vendor_start_tags) is not bool:
            raise DeniedError("vendor tag trust must be explicit")
        if any(mode not in ("read", "write") for mode in self.extensions.values()):
            raise DeniedError("extensions must be explicitly classified as read or write")

    # Classify known requests; opaque vendor bodies require an explicit message-specific rule.
    def kind(self, action: str, payload: dict[str, Any]) -> str:
        if action == "DataTransfer":
            vendor, message = payload.get("vendorId"), payload.get("messageId")
            if not isinstance(vendor, str) or not isinstance(message, str):
                raise DeniedError("vendor extension identifiers must be strings")
            rule = self.extensions.get((vendor, message))
            intrinsic = {
                "getTransaction": "read",
                "setTransaction": "write",
                "setPlan": "write",
                "clearPlans": "write",
                "delayStop": "write",
            }
            if vendor == "IoCharger" and message in intrinsic and rule != intrinsic[message]:
                raise DeniedError("known iCS commands require their intrinsic read/write classification")
            if rule is None:
                raise DeniedError("vendor extension is not explicitly allowed")
            return rule
        if action in READS:
            return "read"
        if action in WRITES or action in self.extra_vendor_actions:
            return "write"
        raise DeniedError("vendor action is not explicitly allowed")

    # Validate a cloud command on the single connector before its transaction ids are translated.
    def validate(self, action: str, payload: dict[str, Any]) -> None:
        if action == "DataTransfer" and payload.get("vendorId") == "IoCharger":
            from .ics_writes import known_write, validate_write

            if known_write(payload):
                validate_write(payload, self)
        elif action == "SetChargingProfile":
            self.profile(payload)
        elif action == "ClearChargingProfile":
            if set(payload) != {"id"} or integer(payload["id"]) == HUB_PROFILE_ID:
                raise DeniedError("only an explicitly owned cloud profile may be cleared")
        elif action == "ChangeConfiguration":
            if (
                not isinstance(payload.get("key"), str)
                or payload["key"] not in self.configuration_keys
                or not isinstance(payload.get("value"), str)
            ):
                raise DeniedError("configuration key is not explicitly allowed")
        elif action == "RemoteStartTransaction":
            tag = payload.get("idTag")
            dynamic = (
                self.trust_vendor_start_tags and isinstance(tag, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,20}", tag)
            )
            if not isinstance(tag, str) or (tag not in self.allowed_tags and not dynamic):
                raise DeniedError("start tag or connector is not allowed")
            integer(payload.get("connectorId", 1), 1, 1)
            if "chargingProfile" in payload:
                # Embedded profiles could bypass the ordinary SetChargingProfile checks.
                raise DeniedError("embedded remote-start profiles are not supported")
        elif action == "RemoteStopTransaction":
            integer(payload.get("transactionId"))
        elif action == "ChangeAvailability":
            integer(payload.get("connectorId"), maximum=1)
            if payload.get("type") not in ("Operative", "Inoperative"):
                raise DeniedError("invalid availability type")
        elif action == "UnlockConnector":
            integer(payload.get("connectorId"), 1, 1)

    # Validate every period; watts are converted conservatively for this one-phase unit.
    def profile(self, payload: dict[str, Any]) -> None:
        connector = integer(payload.get("connectorId"), maximum=1)
        profile = payload.get("csChargingProfiles")
        if not isinstance(profile, dict):
            raise DeniedError("charging profile must be an object")
        if integer(profile.get("chargingProfileId")) == HUB_PROFILE_ID:
            raise DeniedError("cloud cannot replace the local profile")
        stack = integer(profile.get("stackLevel"), maximum=255)
        purpose = profile.get("chargingProfilePurpose")
        if purpose not in ("ChargePointMaxProfile", "TxDefaultProfile", "TxProfile"):
            raise DeniedError("unsupported profile purpose")
        if purpose == "ChargePointMaxProfile" and (connector != 0 or stack >= self.local_stack_level):
            raise DeniedError("station profile cannot replace the reserved local ceiling stack")
        if purpose == "TxProfile":
            integer(profile.get("transactionId"))
            if connector != 1:
                raise DeniedError("transaction profile requires connector one")
        if profile.get("chargingProfileKind") not in ("Absolute", "Relative", "Recurring"):
            raise DeniedError("unsupported profile kind")
        schedule = profile.get("chargingSchedule")
        if not isinstance(schedule, dict) or schedule.get("chargingRateUnit") not in ("A", "W"):
            raise DeniedError("unsupported charging schedule")
        periods = schedule.get("chargingSchedulePeriod")
        if not isinstance(periods, list) or not 1 <= len(periods) <= 96:
            raise DeniedError("invalid schedule periods")
        previous = -1
        for period in periods:
            if not isinstance(period, dict):
                raise DeniedError("invalid schedule period")
            start = integer(period.get("startPeriod"))
            if (previous == -1 and start != 0) or start <= previous:
                raise DeniedError("periods must start at zero and increase")
            previous = start
            if type(period.get("numberPhases", 1)) is not int or period.get("numberPhases", 1) != 1:
                raise DeniedError("profile is not for a single-phase charger")
            # 200 V is a conservative conversion bound, not a measured voltage.
            maximum = self.max_current_a if schedule["chargingRateUnit"] == "A" else self.max_current_a * 200
            number(period.get("limit"), maximum)
