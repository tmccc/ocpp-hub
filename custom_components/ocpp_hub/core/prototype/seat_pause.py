"""Read the real SEAT charging endpoint without turning a cloud fetch into fresh vehicle permission."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import ssl
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .ics import unique_object
from .native_http import response_body
from .security import SecurityError
from .trial_handover_evidence import require_digest

HOST = "ola.prod.code.seat.cloud.vwgroup.com"
MODES = frozenset({"manual", "off", "stopped", "timer"})
OVERALL = frozenset(
    {
        "charging",
        "connected",
        "conservation",
        "discharging",
        "disconnected",
        "finished",
        "finishedAndConservation",
        "notReady",
        "off",
    }
)
STATES = frozenset(
    {
        "charging",
        "off",
        "readyForCharging",
        "notReadyForCharging",
        "error",
        "conservation",
        "discharging",
        "chargePurposeReachedAndConservation",
        "chargePurposeReachedAndNotConservationCharging",
    }
)
Connect = Callable[[], Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]]]


@dataclass(frozen=True, repr=False)
class SeatAccess:
    """Borrow one current access grant; refresh grants are neither retained nor redeemed here."""

    vin: str
    access_token: str
    user_id: str
    device_proof: str
    access_expires_at: float
    proof_expires_at: float
    public_headers: dict[str, str]

    # Validate every interpolated header and the exact vehicle before opening a socket.
    def require_valid(self, vehicle_digest: str) -> None:
        require_digest(vehicle_digest)
        if not isinstance(self.vin, str) or re.fullmatch(r"[A-HJ-NPR-Z0-9]{17}", self.vin) is None:
            raise SecurityError("SEAT vehicle identity is invalid")
        if hashlib.sha256(self.vin.encode()).hexdigest() != vehicle_digest:
            raise SecurityError("SEAT access belongs to another vehicle")
        if (
            any(
                type(value) not in (float, int) or not math.isfinite(value) or value < time.time() + 15
                for value in (self.access_expires_at, self.proof_expires_at)
            )
            or not isinstance(self.public_headers, dict)
            or set(self.public_headers) - {"app-market", "app-brand", "app-version", "origin", "User-Agent"}
        ):
            raise SecurityError("SEAT current account and device proof are required")
        for value in (self.access_token, self.user_id, self.device_proof):
            if (
                not isinstance(value, str)
                or not 1 <= len(value) <= 32768
                or any(ord(c) < 33 or ord(c) > 126 for c in value)
            ):
                raise SecurityError("SEAT access header is invalid")
        if any(
            not isinstance(value, str) or not 1 <= len(value) <= 1024 or any(ord(c) < 32 or ord(c) > 126 for c in value)
            for value in self.public_headers.values()
        ):
            raise SecurityError("SEAT public header is invalid")


@dataclass(frozen=True, repr=False)
class SeatReadback:
    """The fetch interval belongs to this one request; it is not a vehicle observation timestamp."""

    vehicle_digest: str
    nonce: str
    started_at: float
    fetched_at: float
    body: dict[str, Any]


@dataclass(frozen=True)
class SeatPauseReport:
    """A safe diagnostic result deliberately has none of the PauseProvider authority fields."""

    classification: str
    mode: str | None = None
    charging_state: str | None = None
    plug_connection: str | None = None
    power_kw: float | None = None
    fetched_at: float | None = None

    # Omit raw bodies, identities, nonces, tokens and server headers from operator output.
    def summary(self) -> dict[str, Any]:
        return {
            "car_pause_check": self.classification,
            "reported_mode": self.mode,
            "reported_charging_state": self.charging_state,
            "reported_plug_connection": self.plug_connection,
            "reported_power_kw": self.power_kw,
            "fetch_completed_at": self.fetched_at,
            "source_age_unknown": True,
            "vehicle_observed_at": None,
            "current_pause_verified": False,
            "durable_pause_verified": False,
            "handover_authority": False,
        }


# Parse only the charging response; HTTP Date, vehicle/status updatedAt and unknown extra fields prove no freshness.
def assess_pause(read: SeatReadback, vehicle_digest: str, nonce: str, started_at: float) -> SeatPauseReport:
    now = time.time()
    if (
        read.vehicle_digest != vehicle_digest
        or read.nonce != nonce
        or any(type(v) not in (int, float) or not math.isfinite(v) for v in (read.started_at, read.fetched_at))
        or not started_at <= read.started_at <= read.fetched_at <= now
        or now - read.fetched_at > 5
        or now - started_at > 15
        or not isinstance(read.body, dict)
    ):
        raise SecurityError("SEAT readback does not belong to this fresh verification request")
    charging, plug = read.body.get("charging"), read.body.get("plug")
    if not isinstance(charging, dict) or not isinstance(plug, dict):
        return SeatPauseReport("unsupported_response", fetched_at=read.fetched_at)
    overall = read.body.get("state")
    mode, state, connection, power = (
        charging.get("mode"),
        charging.get("state"),
        plug.get("connection"),
        charging.get("chargedPowerInKw"),
    )
    if (
        not isinstance(overall, str)
        or overall not in OVERALL
        or not isinstance(mode, str)
        or mode not in MODES
        or not isinstance(state, str)
        or state not in STATES
        or not isinstance(connection, str)
        or connection not in {"connected", "disconnected"}
        or (
            power is not None
            and (
                isinstance(power, bool)
                or not isinstance(power, (float, int))
                or not 0 <= power <= 500
                or not math.isfinite(power)
            )
        )
    ):
        return SeatPauseReport("unsupported_response", fetched_at=read.fetched_at)
    classification = "pause_not_reported"
    if mode == "stopped":
        classification = (
            "contradictory_response"
            if state in {"charging", "conservation", "discharging", "error"}
            or (power is not None and power != 0)
            or connection != "connected"
            or overall in {"charging", "discharging", "disconnected"}
            else "pause_reported_source_age_unknown"
        )
    return SeatPauseReport(
        classification, mode, state, connection, float(power) if power is not None else None, read.fetched_at
    )


class SeatChargingSource:
    """One pinned TLS GET; no redirects, proxy environment, refresh, wake, Start, Stop or fallback cache."""

    # Loading access for each read observes production rotation without becoming another refresh-token owner.
    def __init__(self, vehicle_digest: str, access: Callable[[], SeatAccess], connect: Connect | None = None) -> None:
        require_digest(vehicle_digest)
        self.vehicle_digest, self.access, self.connect = vehicle_digest, access, connect

    # Fetch only the inspected path with the existing device proof and a total bounded deadline.
    async def read(self, nonce: str) -> SeatReadback:
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(10):
                access = self.access()
                access.require_valid(self.vehicle_digest)
                public_headers = dict(access.public_headers)
                started = time.time()
                if self.connect is None:
                    reader, writer = await asyncio.open_connection(
                        HOST, 443, ssl=ssl.create_default_context(), server_hostname=HOST, limit=65536
                    )
                else:
                    reader, writer = await self.connect()
                headers = {
                    "Host": HOST,
                    "Accept": "application/json",
                    "Accept-Language": "en-GB",
                    "Authorization": "Bearer " + access.access_token,
                    "User-ID": access.user_id,
                    "VIN": access.vin,
                    "X-Firebase-AppCheck": access.device_proof,
                    **public_headers,
                    "Accept-Encoding": "identity",
                    "Connection": "close",
                }
                request = "GET /v1/vehicles/" + access.vin + "/charging/status HTTP/1.1\r\n"
                request += "".join(key + ": " + value + "\r\n" for key, value in headers.items()) + "\r\n"
                writer.write(request.encode("ascii"))
                await writer.drain()
                raw = await response_body(reader)
                body = json.loads(raw, object_pairs_hook=unique_object, parse_constant=reject_constant)
                if not isinstance(body, dict):
                    raise ValueError
                return SeatReadback(self.vehicle_digest, nonce, started, time.time(), body)
        except OSError, TimeoutError, ValueError, TypeError, KeyError, RecursionError, asyncio.IncompleteReadError:
            raise SecurityError("SEAT live charging read is unavailable") from None
        finally:
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(1):
                        await writer.wait_closed()
                except OSError, TimeoutError:
                    pass


# Reject non-JSON numbers at the boundary without retaining the source text in an exception.
def reject_constant(_value: str) -> Any:
    raise ValueError("non-finite JSON number")


class SeatPauseVerifier:
    """Automatic diagnostic verification remains separate from a release-authorising PauseProvider."""

    # A real source is mandatory; historical JSON and Home Assistant entity cache are not input options.
    def __init__(self, source: SeatChargingSource) -> None:
        self.source = source

    # Never reuse an in-flight read or last known success after transport, authentication or schema failure.
    async def check(self) -> SeatPauseReport:
        nonce, started = uuid.uuid4().hex, time.time()
        try:
            read = await self.source.read(nonce)
            return assess_pause(read, self.source.vehicle_digest, nonce, started)
        except SecurityError:
            return SeatPauseReport("live_read_unavailable")
