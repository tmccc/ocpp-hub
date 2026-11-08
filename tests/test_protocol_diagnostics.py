"""Synthetic format-policy failures over both TLS legs; these are mechanisms, not a claim about the real iCS server."""

from __future__ import annotations

import asyncio
import json
import re
import ssl
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.diagnostics import FaultDiagnostics
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.rpc import Peer
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission
from websockets.asyncio.server import ServerConnection, serve

from tests.test_prototype import BOOT
from tests.test_prototype_runtime import NOW, Bench, until
from tests.test_prototype_tls import accept_cap
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls

UTC_SECONDS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
LEGACY_TIME = "2026-01-01T01:02:03.456+00:00"
SECRET = "SYNTHETIC_PRIVATE_REASON_DO_NOT_EXPOSE"


class FormatCloud:
    """An independent synthetic backend chooses a strict policy solely to reproduce the captured failure pattern."""

    # No real account, endpoint, identity or historical household timestamp is loaded by this peer.
    def __init__(self, close_code: int = 1008) -> None:
        self.close_code = close_code
        self.accept_legacy = False
        self.boots = 0
        self.seen: list[list[Any]] = []

    # Accept registration, then reject selected status formats; a later policy change permits exact retained replay.
    async def serve(self, connection: ServerConnection) -> None:
        async for raw in connection:
            frame = decode(raw)
            if frame[0] != 2:
                continue
            self.seen.append(frame)
            if frame[2] == "BootNotification":
                self.boots += 1
                answer = {"status": "Accepted", "interval": 1, "currentTime": NOW}
            elif (
                frame[2] == "StatusNotification"
                and not self.accept_legacy
                and not UTC_SECONDS.fullmatch(frame[3]["timestamp"])
            ):
                await connection.close(code=self.close_code, reason=SECRET)
                return
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": NOW}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))


# Check a clock response rather than a historical fact: the instant is current and its representation is canonical UTC.
def clock_reply(value: str) -> None:
    assert UTC_SECONDS.fullmatch(value)
    assert 0 <= (datetime.now(UTC) - datetime.fromisoformat(value)).total_seconds() < 2


# Intent: new local clocks give an Auto-format fake charger the same whole-second UTC form as observed from iCS.
# Why: this exercises the suspected compatibility mechanism without imposing that synthetic policy on historical events.
async def test_new_clock_replies_keep_format_sensitive_simulated_cloud_online(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "state.db", tls, admission)
    vendor = FormatCloud()
    try:
        async with serve(
            vendor.serve, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            runtime = await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await charger.send(build_call("boot", "BootNotification", BOOT))
                response = decode(await charger.recv())[2]
                clock_reply(response["currentTime"])
                await accept_cap(charger, decode(await charger.recv()), bench.firmware)
                await until(lambda: bench.gateway.controls.ready)
                # A synthetic Auto formatter copies the representation it learned from its local backend.
                stamp = (
                    datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    if UTC_SECONDS.fullmatch(response["currentTime"])
                    else LEGACY_TIME
                )
                status = {"connectorId": 1, "status": "Available", "errorCode": "NoError", "timestamp": stamp}
                await charger.send(build_call("status", "StatusNotification", status))
                assert decode(await charger.recv())[0] == 3
                await until(
                    lambda: bench.store.next_delivery() is None and bench.gateway.vendor_registration == "Accepted"
                )
                await charger.send(build_call("heartbeat", "Heartbeat", {}))
                clock_reply(decode(await charger.recv())[2]["currentTime"])
                await asyncio.sleep(0.08)
                assert runtime.status.vendor_connected and vendor.boots == 1
                assert bench.gateway.vendor.diagnostics.accepted_registrations == 1
                assert not runtime.status.vendor_fault.count
    finally:
        await bench.close()


# Intent: registration, policy/clean closes and unchanged replay remain visible across reconnect and restart.
# Why: disconnected/queued alone hid successful Boot in the live trial, while normal close1000 bypassed error handlers.
@pytest.mark.parametrize("close_code", [1008, 1000, 1001])
async def test_cloud_close_diagnostics_and_unchanged_history_replay(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission, close_code: int
) -> None:
    path = tmp_path / "state.db"
    bench = Bench(path, tls, admission)
    vendor = FormatCloud(close_code)
    originals = [
        {"connectorId": connector, "status": "Available", "errorCode": "NoError", "timestamp": LEGACY_TIME}
        for connector in (1, 0)
    ]
    tokens: list[str] = []
    async with serve(vendor.serve, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        try:
            runtime = await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await bench.boot(charger)
                for index, status in enumerate(originals):
                    await charger.send(build_call("status-" + str(index), "StatusNotification", status))
                    assert decode(await charger.recv())[0] == 3
                await until(lambda: runtime.status.vendor_fault.count >= 2)
                runtime.vendor_enabled = lambda: False
                await until(lambda: not runtime.status.vendor_connected and not bench.gateway.vendor._pending)
                diagnostic = runtime.status.vendor_fault.snapshot()
                rpc = bench.gateway.vendor.diagnostics.snapshot()
                assert diagnostic["received_close_code"] == diagnostic["sent_close_code"] == close_code
                assert diagnostic["classification"] == (
                    "ConnectionClosedOK" if close_code in (1000, 1001) else "ConnectionClosedError"
                )
                assert diagnostic["stage"] == "history" and diagnostic["action"] == "StatusNotification"
                assert rpc["accepted_registrations"] >= 2 and rpc["last_registration"] == "Accepted"
                assert rpc["failed_phase"] == "awaiting_reply"
                rows = bench.store.db.execute("SELECT token,payload,state FROM outbox ORDER BY seq").fetchall()
                tokens = [row[0] for row in rows]
                assert [json.loads(row[1]) for row in rows] == originals and all(row[2] == "queued" for row in rows)
                # Local receive and physical Stop remain usable while cloud delivery is deliberately disabled.
                await charger.send(build_call("local-heartbeat", "Heartbeat", {}))
                clock_reply(decode(await charger.recv())[2]["currentTime"])
                stop = asyncio.create_task(bench.gateway.stop())
                await accept_cap(charger, decode(await charger.recv()), bench.firmware)
                await stop
                assert bench.firmware.current == 0
                # A fault on the other leg cannot erase the previously captured vendor close cause.
                await charger.send(SECRET.encode())
                await until(lambda: runtime.status.charger_fault.count > 0 and not runtime.status.charger_connected)
                assert runtime.status.vendor_fault.snapshot() == diagnostic
                public = json.dumps(
                    {"vendor": diagnostic, "charger": runtime.status.charger_fault.snapshot(), "rpc": rpc}
                )
                assert SECRET not in public and LEGACY_TIME not in public and "OFFLINE001" not in public
        finally:
            await bench.close()
        vendor.accept_legacy = True
        second = Bench(path, tls, admission)
        second.firmware = bench.firmware
        try:
            await second.start(cloud.sockets[0].getsockname()[1])
            async with second.charger() as charger:
                await second.boot(charger)
                await until(lambda: second.store.next_delivery() is None)
                replayed = [frame for frame in vendor.seen if frame[0] == 2 and frame[1] in tokens]
                assert replayed and all(frame[3] in originals for frame in replayed)
                assert all(frame[3]["timestamp"] == LEGACY_TIME for frame in replayed)
                assert second.store.get("control")["inhibit"] and second.firmware.current == 0
        finally:
            await second.close()


# Intent: unmatched replies and unknown action labels reveal correlation failure without leaking their opaque values.
# Why: diagnosing a vendor identifier quirk must not turn the status file into a credential or payload log.
async def test_rpc_diagnostics_only_retain_classifications() -> None:
    peer = Peer("vendor", 0.1)
    peer.receive([3, SECRET, {"password": SECRET}])

    # The independent synthetic peer sends a mismatched reply before the exact correlated result.
    async def sender(raw: str) -> None:
        frame = decode(raw)
        peer.receive([3, "unmatched", {}])
        peer.receive([3, frame[1], {"private": SECRET}])

    peer.sender = sender
    await peer.call(SECRET, {"secret": SECRET})
    diagnostic = peer.diagnostics.snapshot()
    assert diagnostic["action"] == "Other" and diagnostic["reply_matched"]
    assert diagnostic["matched_replies"] == 1 and diagnostic["unmatched_replies"] == 2
    assert diagnostic["call_id_length"] == diagnostic["reply_id_length"] == 34
    assert SECRET not in json.dumps(diagnostic)
    fault = FaultDiagnostics()
    fault.record(ValueError(SECRET), "untrusted-" + SECRET, SECRET)
    assert fault.stage == "session" and fault.action == "Other" and SECRET not in json.dumps(fault.snapshot())


# Intent: an observed remote close remains printable before our own close echo exists.
# Why: cleanup can race the remote close, so the numeric-only diagnostic must accept a missing sent frame.
def test_remote_close_diagnostic_allows_missing_echo() -> None:
    fault = FaultDiagnostics()
    fault.record_close("history", "StatusNotification", 1000, None)
    assert fault.classification == "ConnectionClosedOK"
    assert fault.received_close_code == 1000 and fault.sent_close_code is None
    fault.record_close(SECRET, SECRET, 1008, 5000)
    assert fault.classification == "ConnectionClosedError" and fault.sent_close_code is None
    assert SECRET not in json.dumps(fault.snapshot())
