"""The exact read-only IoCharger transaction metadata exchange observed in private device logs."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..frames import Frame
from .policy import DeniedError, integer
from .store import encoded


# Match one evidenced extension; no other IoCharger message inherits its permission or codec.
def is_transaction_query(payload: dict[str, Any]) -> bool:
    return payload.get("vendorId") == "IoCharger" and payload.get("messageId") == "getTransaction"


# Refuse duplicate keys instead of allowing JSON's last-key-wins behavior to change an identifier.
def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DeniedError("duplicate iCS data key")
        result[key] = value
    return result


# Preserve the observed JSON-string boundary and reject unknown shapes without forwarding a wrong id.
def data_object(value: Any, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, str):
        raise DeniedError("iCS data must be a JSON string")
    try:
        if len(value.encode()) > 4096:
            raise DeniedError("iCS data must be bounded")
        data = json.loads(value, object_pairs_hook=unique_object)
        encoded(data)
    except ValueError, RecursionError:
        raise DeniedError("invalid iCS data JSON") from None
    if not isinstance(data, dict) or set(data) != keys:
        raise DeniedError("unobserved iCS data shape")
    integer(data["transactionId"])
    return data


@dataclass(frozen=True)
class TransactionQuery:
    """One request's verified mapping, retained until its actual charger reply arrives."""

    vendor: int
    local: int

    # Historical lookup is explicit and must not weaken active-only mappings used by stop/profile commands.
    @classmethod
    def from_request(cls, payload: dict[str, Any], lookup: Callable[[int], int | None]) -> TransactionQuery:
        if not is_transaction_query(payload) or set(payload) != {"vendorId", "messageId", "data"}:
            raise DeniedError("unobserved iCS transaction query")
        vendor = data_object(payload["data"], {"transactionId"})["transactionId"]
        local = lookup(vendor)
        if local is None:
            raise DeniedError("unknown iCS cloud transaction")
        return cls(vendor, integer(local))

    # Rewrite only the known identifier inside the string, preserving the surrounding extension envelope.
    def request(self, original: dict[str, Any]) -> dict[str, Any]:
        return {**original, "data": encoded({"transactionId": self.local})}

    # Translate an actual Accepted metadata reply; refusals and protocol errors retain their real meaning.
    def response(self, frame: Frame) -> Frame:
        if frame[0] == 4:
            return frame
        payload = frame[2]
        if payload.get("status") in ("Rejected", "UnknownMessageId", "UnknownVendorId"):
            if set(payload) != {"status"}:
                raise DeniedError("unobserved iCS refusal data cannot be translated safely")
            return frame
        if payload.get("status") != "Accepted" or set(payload) != {"status", "data"}:
            raise DeniedError("unobserved iCS transaction response")
        data = data_object(payload["data"], {"transactionId", "eco"})
        if data["transactionId"] != self.local or data["eco"] != "Fast":
            raise DeniedError("iCS response differs from observed transaction metadata")
        return [3, frame[1], {**payload, "data": encoded({**data, "transactionId": self.vendor})}]
