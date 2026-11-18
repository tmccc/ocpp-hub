"""Which commands of the vendor backend reach the charger, and what is changed or refused on the way. Pure rules."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .control import HUB_PROFILE_ID, ControlPolicy
from .mirror_queue import is_integer
from .txid_map import TransactionIdMap

# The commands that may reach the charger when the owner turned forwarding on. Reset, UpdateFirmware and GetDiagnostics
# are not in the list on purpose: they stay refused until the owner decides otherwise. DataTransfer is passed on as it
# is, on the owner's explicit decision; its content is vendor-specific, so it is logged in full, both ways.
# GetConfiguration and TriggerMessage are answered by the mirror from what it knows, never by the charger.
FORWARDED = frozenset(
    {
        "SetChargingProfile",
        "ClearChargingProfile",
        "RemoteStartTransaction",
        "RemoteStopTransaction",
        "ChangeConfiguration",
        "ChangeAvailability",
        "UnlockConnector",
        "DataTransfer",
    }
)

# The configuration keys the vendor may change: standard OCPP 1.6 keys that touch neither the connection, nor how the
# charger authenticates, nor what or how often it reports (so no heartbeat, aligned-data or status-duration keys),
# nor how a charging profile is read. Everything else is refused, because a key
# that is not on a list cannot be judged: a vendor-specific key may set a maximum current. Compared in lower case
# and exactly, so a key with a stray space does not pass.
_ALLOWED_KEYS = frozenset(
    key.lower()
    for key in (
        "ConnectionTimeOut",
        "LightIntensity",
        "BlinkRepeat",
        "ResetRetries",
        "StopTransactionOnEVSideDisconnect",
        "StopTransactionOnInvalidId",
        "UnlockConnectorOnEVSideDisconnect",
        "LocalAuthorizeOffline",
        "LocalPreAuthorize",
        "AuthorizeRemoteTxRequests",
        "AuthorizationCacheEnabled",
        "AllowOfflineTxForUnknownId",
        "TransactionMessageAttempts",
        "TransactionMessageRetryInterval",
    )
)
# The longest value of a configuration key that is passed on.
_MAX_VALUE = 500
# The most periods a profile may have, and the voltage a profile in watts is measured against.
_MAX_PERIODS = 24
# No charger takes a limit this large in watts; anything above is refused before it is converted or compared.
_MAX_LIMIT = 1.0e9
_NOMINAL_VOLTS = 230.0
_DEFAULT_PHASES = 3


@dataclass(frozen=True, slots=True)
class Forward:
    """Send this to the charger. The payload already carries the hub's transaction id."""

    action: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class Refuse:
    """Do not send it: answer the vendor with this payload, and say why in the log."""

    answer: dict[str, Any]
    reason: str


# Decide what happens to one vendor command. Anything that is not in FORWARDED is not decided here.
# The checks are on the way in, so a refused command never reaches the charger.
def plan_forward(
    action: str, payload: dict[str, Any], policy: ControlPolicy, transactions: TransactionIdMap
) -> Forward | Refuse:
    if action not in FORWARDED:
        return Refuse({"status": "Rejected"}, f"{action} is not one of the commands that are passed on")
    match action:
        case "SetChargingProfile":
            return _plan_profile(payload, policy, transactions)
        case "ClearChargingProfile":
            return _plan_clear(payload)
        case "RemoteStartTransaction":
            return _plan_start(payload, policy)
        case "RemoteStopTransaction":
            return _plan_stop(payload, transactions)
        case "ChangeConfiguration":
            return _plan_configuration(payload)
        case _:
            return Forward(action, payload)


# The refusal answer a command uses when the mirror says no: a status the protocol allows for that very command.
def refusal_for(action: str) -> dict[str, Any]:
    statuses = {"UnlockConnector": "UnlockFailed", "ClearChargingProfile": "Unknown"}
    return {"status": statuses.get(action, "Rejected")}


# Why a charging profile may not be passed on, or None when it may. It needs a schedule in exactly amperes or watts with
# a usable limit in every period; a profile in watts is converted with its number of phases (three when it names none),
# so a single-phase profile cannot hide a current above the ceiling; and it may not reuse the hub's own profile id.
def _profile_problem(profile: object, policy: ControlPolicy) -> str | None:
    schedule = profile.get("chargingSchedule") if isinstance(profile, dict) else None
    periods = schedule.get("chargingSchedulePeriod") if isinstance(schedule, dict) else None
    if not isinstance(profile, dict) or not isinstance(schedule, dict) or not isinstance(periods, list) or not periods:
        return "the profile has no schedule"
    if len(periods) > _MAX_PERIODS:
        return "the profile has too many periods"
    if not is_integer(profile.get("chargingProfileId")):
        return "the profile id is not a number"
    if profile.get("chargingProfileId") == HUB_PROFILE_ID:
        return "the profile id belongs to the hub"
    unit = schedule.get("chargingRateUnit")
    if unit not in ("A", "W"):
        return "the schedule names no unit of A or W"
    for period in periods:
        limit = period.get("limit") if isinstance(period, dict) else None
        if isinstance(limit, bool) or not isinstance(limit, (int, float)) or limit < 0 or limit > _MAX_LIMIT:
            return "a period has no usable limit"
        if not math.isfinite(limit):
            return "a period has no usable limit"
        phases = period.get("numberPhases", _DEFAULT_PHASES)
        if isinstance(phases, bool) or not isinstance(phases, int) or not 1 <= phases <= _DEFAULT_PHASES:
            return "a period names an impossible number of phases"
        amps = limit if unit == "A" else limit / (_NOMINAL_VOLTS * phases)
        if amps > policy.max_current_a:
            return f"a period asks for {amps:g} A, above the ceiling {policy.max_current_a:g} A"
    return None


# A charging profile is passed on only when it passes _profile_problem. A profile for a transaction is translated to
# the hub's transaction id, or refused when it is unknown.
def _plan_profile(payload: dict[str, Any], policy: ControlPolicy, transactions: TransactionIdMap) -> Forward | Refuse:
    profile = payload.get("csChargingProfiles")
    problem = _profile_problem(profile, policy)
    if problem is not None or not isinstance(profile, dict):
        return Refuse(refusal_for("SetChargingProfile"), problem or "the profile is missing")
    forwarded = dict(payload)
    vendor_transaction = profile.get("transactionId")
    if vendor_transaction is not None:
        hub_transaction = transactions.to_hub(vendor_transaction) if is_integer(vendor_transaction) else None
        if hub_transaction is None:
            return Refuse(refusal_for("SetChargingProfile"), "the profile names a transaction the hub does not know")
        forwarded["csChargingProfiles"] = {**profile, "transactionId": hub_transaction}
    return Forward("SetChargingProfile", forwarded)


# A remote start may carry a charging profile of its own; it gets the same checks as a profile of its own. A profile
# that names a transaction is refused, because the charge it would belong to does not exist yet.
def _plan_start(payload: dict[str, Any], policy: ControlPolicy) -> Forward | Refuse:
    if "chargingProfile" not in payload:
        return Forward("RemoteStartTransaction", payload)
    profile = payload["chargingProfile"]
    problem = _profile_problem(profile, policy)
    if problem is None and isinstance(profile, dict) and profile.get("transactionId") is not None:
        problem = "the profile names a transaction that has not started"
    if problem is not None:
        return Refuse(refusal_for("RemoteStartTransaction"), problem)
    return Forward("RemoteStartTransaction", payload)


# An accepted clear matches a specific ID or all of its supplied filters. Missing profile facts cannot prove a match.
def clears_profile(
    payload: dict[str, Any],
    profile_id: int | None,
    purpose: str | None,
    connector_id: int | None,
    stack_level: int | None,
) -> bool:
    if "id" in payload:
        return bool(payload["id"] == profile_id)
    scope = (("connectorId", connector_id), ("chargingProfilePurpose", purpose), ("stackLevel", stack_level))
    return all(key not in payload or value is not None and payload[key] == value for key, value in scope)


# Forward clears except a direct request for the hub's reserved profile ID.
def _plan_clear(payload: dict[str, Any]) -> Forward | Refuse:
    if "id" in payload and not is_integer(payload["id"]):
        return Refuse({"status": "Unknown"}, "the profile id is not a number")
    if payload.get("id") == HUB_PROFILE_ID:
        return Refuse({"status": "Unknown"}, "the profile id belongs to the hub")
    return Forward("ClearChargingProfile", payload)


# A stop names the vendor's transaction id; the charger knows the hub's. An id the hub does not know is refused,
# because passing it on could stop another charge.
def _plan_stop(payload: dict[str, Any], transactions: TransactionIdMap) -> Forward | Refuse:
    vendor_transaction = payload.get("transactionId")
    hub_transaction = transactions.to_hub(vendor_transaction) if is_integer(vendor_transaction) else None
    if hub_transaction is None:
        return Refuse(refusal_for("RemoteStopTransaction"), "the transaction is not one the hub knows")
    return Forward("RemoteStopTransaction", {**payload, "transactionId": hub_transaction})


# A configuration change is passed on only for a key on the allowlist, with a text value of reasonable length.
def _plan_configuration(payload: dict[str, Any]) -> Forward | Refuse:
    key, value = payload.get("key"), payload.get("value")
    if not isinstance(key, str) or key.lower() not in _ALLOWED_KEYS:
        return Refuse(refusal_for("ChangeConfiguration"), "the key is not one the hub lets the vendor change")
    if not isinstance(value, str) or len(value) > _MAX_VALUE:
        return Refuse(refusal_for("ChangeConfiguration"), "the value is not a short text")
    return Forward("ChangeConfiguration", payload)


# The lowest limit of a vendor profile in amperes: the number that is shown as the vendor's limit. A profile in watts is
# converted with its number of phases. None when the profile has no usable period; the profile was checked before it was
# passed on, so this only reads it.
def profile_limit_a(profile: object) -> float | None:
    schedule = profile.get("chargingSchedule") if isinstance(profile, dict) else None
    periods = schedule.get("chargingSchedulePeriod") if isinstance(schedule, dict) else None
    if not isinstance(schedule, dict) or not isinstance(periods, list):
        return None
    amps: list[float] = []
    for period in periods:
        limit = period.get("limit") if isinstance(period, dict) else None
        if isinstance(limit, bool) or not isinstance(limit, (int, float)) or not math.isfinite(limit):
            continue
        phases = period.get("numberPhases", _DEFAULT_PHASES)
        if schedule.get("chargingRateUnit") == "W":
            amps.append(limit / (_NOMINAL_VOLTS * (phases if isinstance(phases, int) and phases else _DEFAULT_PHASES)))
        else:
            amps.append(float(limit))
    return min(amps) if amps else None
