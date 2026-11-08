"""Builders for the payloads of requests the hub sends to a charger. Pure functions, no I/O."""

from __future__ import annotations

from typing import Any

PURPOSE_CHARGE_POINT_MAX = "ChargePointMaxProfile"
PURPOSE_TX_DEFAULT = "TxDefaultProfile"
PURPOSE_TX = "TxProfile"


# Build GetConfiguration; no keys asks for every key.
# Some chargers answer a long key list badly, so callers can ask for a few keys at a time.
def get_configuration(keys: list[str] | None = None) -> dict[str, Any]:
    return {"key": keys} if keys else {}


# Build ChangeConfiguration for one key.
# The protocol changes one key per request, and the value is always a string.
def change_configuration(key: str, value: str) -> dict[str, Any]:
    return {"key": key, "value": value}


# Build TriggerMessage to make a charger resend one of its own messages.
# A late joiner (the hub after a restart) learns the charger's state without waiting for the next change.
def trigger_message(requested: str, connector_id: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"requestedMessage": requested}
    if connector_id is not None:
        payload["connectorId"] = connector_id
    return payload


# Build SetChargingProfile limiting current in ampere with a single period starting at once.
# A flat ampere limit is all load balancing needs; the purpose decides whether it is a station ceiling or per session.
def set_charging_profile(
    connector_id: int,
    purpose: str,
    amps: float,
    profile_id: int,
    stack_level: int = 0,
    transaction_id: int | None = None,
    kind: str = "Relative",
    start_schedule: str | None = None,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "chargingProfileId": profile_id,
        "stackLevel": stack_level,
        "chargingProfilePurpose": purpose,
        "chargingProfileKind": kind,
        "chargingSchedule": {
            "chargingRateUnit": "A",
            "chargingSchedulePeriod": [{"startPeriod": 0, "limit": amps}],
        },
    }
    if purpose == PURPOSE_TX and transaction_id is not None:
        profile["transactionId"] = transaction_id
    if start_schedule is not None:
        profile["chargingSchedule"]["startSchedule"] = start_schedule
    return {"connectorId": connector_id, "csChargingProfiles": profile}


# Build ClearChargingProfile for one profile id.
# Removing the hub's own limit must not touch profiles somebody else installed.
def clear_charging_profile(profile_id: int) -> dict[str, Any]:
    return {"id": profile_id}


# Build RemoteStartTransaction for a connector.
# Starting a session from Home Assistant needs an id tag; chargers that skip authorisation accept any.
def remote_start_transaction(id_tag: str, connector_id: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"idTag": id_tag}
    if connector_id is not None:
        payload["connectorId"] = connector_id
    return payload


# Build RemoteStopTransaction for a running transaction.
# The charger identifies the session by the transaction id the hub handed out.
def remote_stop_transaction(transaction_id: int) -> dict[str, Any]:
    return {"transactionId": transaction_id}


# Build GetCompositeSchedule: ask what limit is in effect now on a connector, in amperes. It only reads.
# The charger combines all its profiles, so the answer is what really applies, whoever set it.
def get_composite_schedule(connector_id: int, duration_s: int = 3600) -> dict[str, Any]:
    return {"connectorId": connector_id, "duration": duration_s, "chargingRateUnit": "A"}


# Build SetChargingProfile with a recurring schedule, daily or weekly: a profile that starts at `anchor` and repeats
# every 24 hours (or every week), with one limit in amperes from each start offset (in seconds after the anchor) until
# the next one.
def set_charging_schedule(
    connector_id: int,
    purpose: str,
    periods: list[tuple[int, float]],
    profile_id: int,
    anchor_iso: str,
    stack_level: int = 0,
    recurrency: str = "Daily",
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "chargingProfileId": profile_id,
        "stackLevel": stack_level,
        "chargingProfilePurpose": purpose,
        "chargingProfileKind": "Recurring",
        "recurrencyKind": recurrency,
        "chargingSchedule": {
            "duration": 604800 if recurrency == "Weekly" else 86400,
            "startSchedule": anchor_iso,
            "chargingRateUnit": "A",
            "chargingSchedulePeriod": [{"startPeriod": start, "limit": limit} for start, limit in periods],
        },
    }
    return {"connectorId": connector_id, "csChargingProfiles": profile}
