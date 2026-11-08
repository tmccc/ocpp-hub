"""Borrow current production SEAT access read-only; never consume or persist a refresh token."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any

from .seat_pause import SeatAccess, SeatChargingSource, SeatPauseVerifier
from .security import SecurityError
from .trial_config import private_json
from .trial_handover_evidence import require_digest


# Require explicit timezone information instead of interpreting a local or guessed epoch timestamp.
def expiry(value: Any) -> float:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError
    return parsed.timestamp()


# Read one private production store atomically; a pending/rejected/expired grant causes no refresh request.
def read_access(path: Path, vehicle_digest: str) -> SeatAccess:
    require_digest(vehicle_digest)
    try:
        envelope = private_json(path)
        if (
            set(envelope) != {"version", "minor_version", "key", "data"}
            or type(envelope["version"]) is not int
            or envelope["version"] != 1
            or type(envelope["minor_version"]) is not int
            or envelope["minor_version"] != 1
            or envelope["key"] != "seat_readonly." + vehicle_digest
        ):
            raise ValueError
        data = envelope["data"]
        if (
            not isinstance(data, dict)
            or data.get("proof_rejected") is not False
            or data.get("pending_refresh") is not None
            or data.get("vehicle_sha256") != vehicle_digest
            or not isinstance(data.get("vin"), str)
            or hashlib.sha256(data["vin"].encode()).hexdigest() != vehicle_digest
        ):
            raise ValueError
        auth, proof = data["auth"], data["device_proof"]
        access = SeatAccess(
            data["vin"],
            auth["access_token"],
            auth["user_id"],
            proof["token"],
            expiry(auth["access_expires_at"]),
            expiry(proof["expires_at"]),
            proof["public_headers"],
        )
        access.require_valid(vehicle_digest)
        return access
    except KeyError, TypeError, ValueError, OverflowError:
        raise SecurityError("SEAT private production access is unavailable") from None


# Validate private configuration offline; only a later explicit check may contact the existing cloud endpoint.
def configured_verifier(path: Path) -> SeatPauseVerifier:
    config = private_json(path)
    if set(config) != {"session", "vehicle_digest"} or not isinstance(config["session"], str):
        raise SecurityError("SEAT verification configuration is invalid")
    session, vehicle = Path(config["session"]), config["vehicle_digest"]

    # Borrow the original production access on every read rather than maintaining a stale credential export.
    def access() -> SeatAccess:
        return read_access(session, vehicle)

    access()
    return SeatPauseVerifier(SeatChargingSource(vehicle, access))
