"""Known ordinary sessions and temporary native outages recover through real TLS and native HTTP."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_introduction_retry import InterruptedNativePeer, selected_ready
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_test_charge import owner_start, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Close only the fake device's real reported session after the behavior under test has been observed.
async def close_session(rig, session, charger, local):
    rig.peer.permission_enabled = False
    rig.bench.firmware.requested_current = 0
    await rig.bench.call(
        charger,
        uuid.uuid4().hex,
        "StopTransaction",
        {
            "transactionId": local,
            "timestamp": START["timestamp"],
            "meterStop": 1100,
        },
    )
    await until(lambda: session.journal.active() is None and session.journal.next_delivery() is None)


# Intent: ordinary app/daily known sessions continue at their original6/32 cap after reconnect/replacement.
# Why: current Boot/native source and zero proof permit continuation, without replaying Start or guessing Stop.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("source", ["daily", "cloud"])
@pytest.mark.parametrize("restart", [False, True])
async def test_known_ordinary_session_recovers_without_temporary_lease(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, source, restart
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        if source == "cloud":
            await owner_start(b)
        local = await positive_flow(b, source)
        if source == "cloud":
            b.rig.peer.plan_session = None
        await healthy(b.controller)
        starts_before = len(starts(b.rig))
        profiles = len(b.rig.bench.received_limits)
        assert b.session.journal.get("managed_temporary_schedule") is None
        if restart:
            await crash(b.session)
            session = managed_session(b.rig)
            await session.start()
            c = ManagedController(session)
        else:
            await b.charger.close()
            await until(lambda: not b.session.runtime.status.charger_connected)
            session, c = b.session, b.controller
        async with b.rig.charger(session) as charger:
            await introduced(b.rig, session, charger, "known-ordinary-recovery")
            assert b.rig.bench.firmware.effective_limit() == 0
            status = await healthy(c)
            assert status["current_a"] == current and status["station_ceiling_a"] == current
            assert session.journal.active() == local and session.journal.vendor_id(local) is not None
            assert len(starts(b.rig)) == starts_before
            assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)
            assert b.rig.bench.received_limits[profiles:][0] == 0
            assert session.journal.get("managed_temporary_schedule") is None
            await close_session(b.rig, session, charger, local)
            await healthy(c)


# Intent: a bounded native outage holds ordinary idle/open operation at zero without a permanent authority latch.
# Why: missing/stale observation is not positive configuration/history drift; fresh proof restores exact owner intent.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("open_session", [False, True])
async def test_ordinary_http_outage_retries_zero_and_recovers(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, open_session
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        local = await positive_flow(b, "daily") if open_session else None
        await healthy(b.controller)
        revision = b.controller.requests.intent().revision
        before_starts = len(starts(b.rig))
        b.rig.peer.unavailable = True
        for _ in range(5):
            status = await b.controller.tick()
            assert status["state"] != "attention"
        assert b.rig.bench.firmware.effective_limit() == 0
        assert not b.session.journal.get("managed_control_fault")
        assert b.controller.requests.intent().mode == "app"
        assert b.controller.requests.intent().revision == revision
        b.rig.peer.unavailable = False
        status = await healthy(b.controller)
        assert status["station_ceiling_a"] == current
        assert b.controller.requests.intent().revision == revision
        assert b.session.journal.active() == local
        assert len(starts(b.rig)) == before_starts
        assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)
        if local is not None:
            assert status["current_a"] == current
            await close_session(b.rig, b.session, b.charger, local)
            await healthy(b.controller)


class SlowOrTruncatedPeer(InterruptedNativePeer):
    """Independent HTTP delay and premature EOF supply missing or genuinely stale complete observations."""

    # Keep the endpoint behavior separate from engine clocks and native values.
    def __init__(self, bench):
        super().__init__(bench)
        self.transport_mode = None

    # Delay complete real replies or close a bounded partial response, without altering their physical content.
    async def handle(self, reader, writer):
        if self.transport_mode == "eof":
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 900\r\n\r\n{")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        if self.transport_mode == "stale":
            await asyncio.sleep(0.55)
        await super().handle(reader, writer)


# Intent: interrupted HTTP bodies and actually elapsed stale complete audits take the same bounded zero recovery.
# Why: native availability failures must never become permanent authority drift merely because they repeat three times.
@pytest.mark.parametrize("current,mode", [(6, "eof"), (32, "eof"), (6, "stale")])
async def test_ordinary_native_eof_and_real_stale_audit_recover(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, mode
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", SlowOrTruncatedPeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        revision = b.controller.requests.intent().revision
        b.rig.peer.transport_mode = mode
        for _ in range(3):
            status = await b.controller.tick()
            assert status["state"] != "attention"
        assert b.rig.bench.firmware.effective_limit() == 0
        assert not b.session.journal.get("managed_control_fault")
        expected = "StaleEvidenceError" if mode == "stale" else "NativeTransportUnavailableError"
        assert b.session.journal.get("managed_native_failure")["type"] == expected
        b.rig.peer.transport_mode = None
        status = await healthy(b.controller)
        assert status["station_ceiling_a"] == current
        assert b.controller.requests.intent().revision == revision
        assert b.session.journal.active() is None and not starts(b.rig)
