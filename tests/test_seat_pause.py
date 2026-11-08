"""Real TLS read-only vehicle checks and fail-closed separation from connected handover authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
import runpy
import ssl
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.seat_access import read_access
from ocpp_hub_core.prototype.seat_pause import (
    HOST,
    SeatAccess,
    SeatChargingSource,
    SeatPauseVerifier,
    SeatReadback,
    assess_pause,
)
from ocpp_hub_core.prototype.security import SecurityError

from tests.handover_helpers import ready_handover
from tests.test_prototype_tls import tls as tls

VIN = "VSSZZZ5FZJR000001"
VEHICLE = hashlib.sha256(VIN.encode()).hexdigest()
BODY = {
    "state": "connected",
    "charging": {"mode": "stopped", "state": "readyForCharging", "chargedPowerInKw": 0},
    "plug": {"connection": "connected", "externalPower": "ready"},
}


# Build synthetic owner access; real account storage and household identifiers never enter this test suite.
def access() -> SeatAccess:
    return SeatAccess(
        VIN,
        "synthetic-access",
        "synthetic-user",
        "synthetic-proof",
        time.time() + 600,
        time.time() + 600,
        {"User-Agent": "Synthetic test", "app-brand": "seat"},
    )


# Return distinct fresh read intervals without supplying an invented vehicle-source observation time.
def readback(body: dict[str, Any]) -> tuple[SeatReadback, float]:
    started = time.time() - 0.02
    return SeatReadback(VEHICLE, "nonce", started + 0.001, time.time() - 0.001, body), started


# Intent: even stopped mode on a freshly fetched body retains unknown source age and grants no release authority.
# Why: fetch time and other endpoint timestamps must never become a current vehicle pause proof.
@pytest.mark.parametrize("extra", [{}, {"updatedAt": "2099-01-01T00:00:00Z"}, {"observedAt": time.time()}])
def test_reported_stop_never_grants_handover(extra: dict[str, Any]) -> None:
    read, started = readback(BODY | extra)
    report = assess_pause(read, VEHICLE, "nonce", started).summary()
    assert report["car_pause_check"] == "pause_reported_source_age_unknown"
    assert report["source_age_unknown"] and report["vehicle_observed_at"] is None
    assert report["current_pause_verified"] is report["durable_pause_verified"] is report["handover_authority"] is False
    assert not {"revision", "release_only", "pause_valid_until", "permission_digest"}.intersection(report)
    assert VIN not in json.dumps(report)


# Intent: ready, off and completed charging under ordinary modes do not get labelled an owner pause.
# Why: a finished/full/stopped charger can show zero without the car having withdrawn permission.
@pytest.mark.parametrize("mode", ["manual", "off", "timer"])
@pytest.mark.parametrize("state", ["readyForCharging", "off", "chargePurposeReachedAndNotConservationCharging"])
def test_zero_power_is_not_a_pause(mode: str, state: str) -> None:
    read, started = readback(BODY | {"charging": BODY["charging"] | {"mode": mode, "state": state}})
    assert assess_pause(read, VEHICLE, "nonce", started).classification == "pause_not_reported"


# Intent: a contradictory, missing or unsupported charging response is never accepted as a stopped car.
# Why: partial/defaulted values and concurrent contradictory readings cannot safely authorise handover.
@pytest.mark.parametrize(
    "delta,expected",
    [
        ({"charging": BODY["charging"] | {"state": "charging"}}, "contradictory_response"),
        ({"charging": BODY["charging"] | {"chargedPowerInKw": 1.4}}, "contradictory_response"),
        ({"state": "charging"}, "contradictory_response"),
        ({"state": []}, "unsupported_response"),
        ({"state": {}}, "unsupported_response"),
        ({"plug": {"connection": "disconnected"}}, "contradictory_response"),
        ({"charging": BODY["charging"] | {"chargedPowerInKw": False}}, "unsupported_response"),
        ({"charging": BODY["charging"] | {"chargedPowerInKw": float("nan")}}, "unsupported_response"),
        ({"charging": BODY["charging"] | {"chargedPowerInKw": 10**400}}, "unsupported_response"),
        ({"charging": BODY["charging"] | {"mode": True}}, "unsupported_response"),
        ({"charging": BODY["charging"] | {"state": "unknown"}}, "unsupported_response"),
        ({"charging": {}}, "unsupported_response"),
        ({"plug": None}, "unsupported_response"),
    ],
)
def test_ambiguous_readings_fail_closed(delta: dict[str, Any], expected: str) -> None:
    read, started = readback(BODY | delta)
    assert assess_pause(read, VEHICLE, "nonce", started).classification == expected


# Intent: old in-flight, cross-vehicle, cross-request, future and stale reads cannot be rebound to a new check.
# Why: a provider callback cannot manufacture freshness by returning the last cached successful operation.
@pytest.mark.parametrize(
    "delta",
    [
        {"nonce": "old"},
        {"vehicle_digest": "0" * 64},
        {"started_at": time.time() - 60},
        {"fetched_at": time.time() + 60},
        {"started_at": float("nan")},
        {"fetched_at": time.time() - 60},
    ],
)
def test_read_interval_and_identity_binding(delta: dict[str, Any]) -> None:
    read, started = readback(BODY)
    with pytest.raises(SecurityError):
        assess_pause(replace(read, **delta), VEHICLE, "nonce", started)


# Intent: the observed live idle response omits optional power, while still reporting an ordinary manual mode.
# Why: an absent power value must remain unknown without hiding the explicit mode or inventing zero power.
def test_idle_response_with_missing_power() -> None:
    body = BODY | {"state": "notReady", "charging": {"mode": "manual", "state": "notReadyForCharging"}}
    read, started = readback(body)
    report = assess_pause(read, VEHICLE, "nonce", started).summary()
    assert report["car_pause_check"] == "pause_not_reported" and report["reported_mode"] == "manual"
    assert report["reported_power_kw"] is None and report["handover_authority"] is False


# Serve real synthetic TLS HTTP replies while recording every actual request line and header.
@asynccontextmanager
async def source_server(tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], replies: list[bytes]):
    calls: list[bytes] = []

    # Record a complete request and return one fixed raw response from this independent HTTP peer.
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            calls.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(replies[min(len(calls) - 1, len(replies) - 1)])
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async with await asyncio.start_server(handler, "127.0.0.1", 0, ssl=tls[0]) as server:
        port = server.sockets[0].getsockname()[1]

        # Only the test injects a loopback peer; production has no configurable cloud URL or trust override.
        async def connect():
            return await asyncio.open_connection("127.0.0.1", port, ssl=tls[1], limit=65536)

        yield SeatChargingSource(VEHICLE, access, connect), calls


# Encode an ordinary HTTP response without changing the endpoint's data shape.
def reply(body: bytes, status: bytes = b"200 OK") -> bytes:
    return (
        b"HTTP/1.1 "
        + status
        + b"\r\nContent-Type: application/json\r\nContent-Length: "
        + str(len(body)).encode()
        + b"\r\nDate: Sun, 08 Nov 2026 22:00:00 GMT\r\n\r\n"
        + body
    )


# Intent: live checks use only the exact inspected GET and fresh requests, then drop previous success on failure.
# Why: a second authentication error must not return the earlier stopped response or refresh the shared grant.
async def test_real_tls_get_and_no_stale_fallback(tls: tuple[ssl.SSLContext, ssl.SSLContext, Path]) -> None:
    async with source_server(tls, [reply(json.dumps(BODY).encode()), reply(b"{}", b"401 Unauthorized")]) as (
        source,
        calls,
    ):
        verifier = SeatPauseVerifier(source)
        assert (await verifier.check()).classification == "pause_reported_source_age_unknown"
        assert (await verifier.check()).classification == "live_read_unavailable"
        assert len(calls) == 2
        for call in calls:
            assert call.startswith(f"GET /v1/vehicles/{VIN}/charging/status HTTP/1.1\r\n".encode())
            assert ("Host: " + HOST + "\r\n").encode() in call
            assert b"X-Firebase-AppCheck: synthetic-proof\r\n" in call
            assert b"VIN: " + VIN.encode() + b"\r\n" in call
            assert b"refresh" not in call and b"requests/" not in call and call.endswith(b"\r\n\r\n")


# Intent: redirects, forbidden access and ambiguous JSON all fail without a follow-up request.
# Why: account headers cannot leave the fixed endpoint, and malformed bodies cannot bypass the parser.
@pytest.mark.parametrize(
    "response",
    [
        reply(b"{}", b"302 Found"),
        reply(b"{}", b"403 Forbidden"),
        reply(b"{}", b"429 Too Many Requests"),
        reply(b'{"charging":{},"charging":{}}'),
        reply(b'{"power":NaN}'),
        reply(b"[]"),
    ],
)
async def test_http_and_json_fail_closed(tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], response: bytes) -> None:
    async with source_server(tls, [response]) as (source, calls):
        assert (await SeatPauseVerifier(source).check()).classification == "live_read_unavailable"
        assert len(calls) == 1


# Save a synthetic production store with the same durable envelope and no real credentials.
def store(path: Path) -> dict[str, Any]:
    credential = access()
    future = datetime.fromtimestamp(time.time() + 600, UTC).isoformat()
    value = {
        "version": 1,
        "minor_version": 1,
        "key": "seat_readonly." + VEHICLE,
        "data": {
            "vin": VIN,
            "vehicle_sha256": VEHICLE,
            "proof_rejected": False,
            "pending_refresh": None,
            "auth": {
                "access_token": credential.access_token,
                "user_id": credential.user_id,
                "access_expires_at": future,
                "refresh_token": "must-never-be-used",
            },
            "device_proof": {"token": credential.device_proof, "expires_at": future, "public_headers": {}},
        },
    }
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return value


# Intent: reading shared production access borrows only a current grant and never returns a refresh token.
# Why: a second refresh owner would race Home Assistant's rotated grant and disrupt the existing integration.
def test_current_private_access_is_read_only(tmp_path: Path) -> None:
    path = tmp_path / "session.json"
    store(path)
    before = path.read_bytes()
    credential = read_access(path, VEHICLE)
    assert credential.access_token == "synthetic-access" and not hasattr(credential, "refresh_token")
    assert path.read_bytes() == before and "synthetic-access" not in repr(credential)


# Intent: expired, pending, wrong-vehicle, redirected and overly public private access cannot contact a service.
# Why: unavailable shared authentication must fail without exporting a new session or attempting rotation.
@pytest.mark.parametrize("failure", ["expired", "pending", "rejected", "vehicle", "public", "symlink", "headers"])
def test_bad_private_access_fails(tmp_path: Path, failure: str) -> None:
    path = tmp_path / "session.json"
    value = store(path)
    if failure == "expired":
        value["data"]["auth"]["access_expires_at"] = "2000-01-01T00:00:00Z"
    elif failure == "pending":
        value["data"]["pending_refresh"] = {"auth": {}}
    elif failure == "rejected":
        value["data"]["proof_rejected"] = True
    elif failure == "vehicle":
        value["data"]["vin"] = "VSSZZZ5FZJR000002"
    elif failure == "headers":
        value["data"]["device_proof"]["public_headers"] = {"Host": "evil.invalid"}
    path.write_text(json.dumps(value))
    if failure == "public":
        path.chmod(0o644)
    elif failure == "symlink":
        link = tmp_path / "link.json"
        link.symlink_to(path)
        path = link
    with pytest.raises(SecurityError):
        read_access(path, VEHICLE)


# Intent: the standalone command defaults to an offline check and prints no credential material.
# Why: supplying a private configuration must never implicitly contact the real service with test access.
def test_standalone_default_is_offline(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    session, config = tmp_path / "session.json", tmp_path / "config.json"
    store(session)
    config.write_text(json.dumps({"session": str(session), "vehicle_digest": VEHICLE}))
    config.chmod(0o600)
    tools = Path(__file__).resolve().parents[1] / "tools"
    monkeypatch.syspath_prepend(str(tools))
    monkeypatch.setattr(sys, "argv", ["seat_pause.py", "--config", str(config)])
    command = runpy.run_path(str(tools / "seat_pause.py"))
    before = session.read_bytes()
    assert command["main"]() == 0
    output = capsys.readouterr().out
    assert json.loads(output) == {"private_access_valid": True, "network_contacted": False, "handover_authority": False}
    assert "synthetic" not in output and VIN not in output and session.read_bytes() == before


# Intent: actual automatic checks block connected handover before any freeze, journal transition or Clear.
# Why: an undated cloud pause must not become authority even alongside the synthetic strong pause provider.
async def test_handover_preflight_checks_without_changing_trial(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with source_server(tls, [reply(json.dumps(BODY).encode())]) as (source, calls):
        async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
            rig.session.handover.pause_verifier = SeatPauseVerifier(source)
            phase = rig.session.journal.get("trial_phase")
            before = len(rig.bench.device_calls)
            report = await rig.session.command({"command": "verify-car-pause"})
            assert report["car_pause_check"] == "pause_reported_source_age_unknown"
            for command in ("connected-handover-plan", "connected-handover"):
                with pytest.raises(UnavailableError, match="source age"):
                    await rig.session.command({"command": command})
                assert rig.session.journal.get("trial_phase") == phase
                assert rig.session.journal.get("connected_handover") is None
            assert len(rig.bench.device_calls) == before and len(calls) == 3
            assert rig.peer.restore_calls == 0 and 7001 in rig.bench.firmware.profiles
