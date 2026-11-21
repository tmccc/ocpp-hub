"""Known ordinary sessions and temporary native outages recover through real TLS and native HTTP."""

import asyncio
import socket
import uuid
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit

import pytest
from ocpp_hub_core.prototype.connection_scope import current_scope
from ocpp_hub_core.prototype.managed import ManagedController

from tests.charge_rig import owner_start, positive_flow
from tests.current_rig import selected_ready
from tests.handover_helpers import crash
from tests.managed_rig import healthy, introduced, managed_session, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: an ordinary same-connection zero hold during a genuine cloud outage preserves the owner's app revision.
# Why: recovering phase alone does not mean a replacement introduction retired the owner's authority.
@pytest.mark.parametrize("completion_refused", [False, True])
async def test_same_connection_outage_hold_preserves_app_authority(
    tmp_path, tls, unused_tcp_port, monkeypatch, completion_refused
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 32, native_peer=InterruptedNativePeer) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        intent = c.requests.intent()
        epoch, boot = g._charger_epoch, g.boot_observation
        transport = b.session.runtime.transport
        original = transport.config
        faults = b.session.runtime.status.vendor_fault.count
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            url = urlsplit(original.vendor_url)
            transport.config = replace(
                original,
                vendor_url=urlunsplit(url._replace(netloc="127.0.0.1:" + str(unavailable.getsockname()[1]))),
            )
            try:
                await b.rig.bench.cloud.close()
                await until(
                    lambda: (
                        b.session.runtime.status.vendor_fault.count > faults
                        and b.session.runtime.status.vendor_failed_scope == current_scope(g)
                        and not b.session.runtime.status.vendor_connected
                    )
                )
                b.rig.peer.unavailable = True
                for _ in range(3):
                    await c.tick()
                requests = []
                while not b.rig.peer.failed_reads.empty():
                    requests.append(b.rig.peer.failed_reads.get_nowait())
                assert requests and all(request["action"] == "getConfig" for request in requests)
                assert j.get("trial_phase") == "recovering"
                assert b.rig.bench.firmware.effective_limit() == 0
                b.rig.peer.unavailable = False
                await c.tick()
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert c.requests.intent() == intent
                assert not j.get("managed_control_fault")
            finally:
                b.rig.peer.unavailable = False
                transport.config = original
        await until(lambda: g.vendor_registration == "Accepted")
        if completion_refused:
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_recovery_completion BEFORE INSERT ON meta "
                "WHEN NEW.key='managed_ordinary_recovery' AND json_extract(NEW.value,'$.state')='complete' "
                "BEGIN SELECT RAISE(ABORT,'completion storage temporarily busy'); END"
            )
            try:
                async with asyncio.timeout(5):
                    while c.ordinary.completion is None:
                        await c.tick()
                        await asyncio.sleep(0.02)
                assert g.controls.ready and g.controls.desired() == 32
                hold = j.get("managed_ordinary_recovery")
                mark = list(b.rig.bench.received_limits)
                b.rig.peer.unavailable = True
                status = await c.tick()
                assert status["state"] == "degraded" and status["fault"] == "native_observation_unavailable"
                assert not b.rig.peer.failed_reads.empty() and c.native_unavailable == 1
                assert c.ordinary.completion is not None
                assert j.get("managed_ordinary_recovery") == hold and b.rig.bench.received_limits == mark
            finally:
                b.rig.peer.unavailable = False
                j.db.execute("DROP TRIGGER refuse_recovery_completion")
        status = await healthy(c)
        assert c.requests.intent() == intent and status["station_ceiling_a"] == 32
        if completion_refused:
            assert j.get("managed_ordinary_recovery")["state"] == "complete"
            assert b.rig.bench.received_limits == mark
        assert not starts(b.rig) and not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)


# Close only the fake device's real reported session after the behavior under test has been observed.
async def close_session(rig, session, charger, local, *, delivered=True):
    rig.peer.permission_enabled = False
    rig.bench.firmware.requested_current = 0
    reply = await rig.bench.call(
        charger,
        uuid.uuid4().hex,
        "StopTransaction",
        {
            "transactionId": local,
            "timestamp": START["timestamp"],
            "meterStop": 1100,
        },
    )
    assert reply[0] == 3
    await until(lambda: session.journal.active() is None and (not delivered or session.journal.next_delivery() is None))


# Intent: a genuine OCPP Stop followed by late native closure holds an ordinary mapped session at zero.
# Why: a matched closure gap must not become a persistent authority fault or grant positive current early.
@pytest.mark.parametrize("connection", ["plugged", "unplugged"])
@pytest.mark.parametrize("current", [6, 32])
async def test_ocpp_first_mapped_closure_recovers_only_after_native_closure(
    tmp_path, tls, unused_tcp_port, monkeypatch, connection, current
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        intent = c.requests.intent()
        local = await positive_flow(b, "daily")
        assert (await healthy(c))["current_a"] == current
        open_session = rig.peer.native("SESSION")
        command_count = len(rig.bench.device_calls)
        limit_count = len(rig.bench.received_limits)
        await close_session(rig, b.session, b.charger, local)
        assert j.vendor_id(local) is not None
        rig.bench.cp, rig.bench.connection = ("B", "Plug") if connection == "plugged" else ("A", "Unplug")
        native = rig.peer.native

        with monkeypatch.context() as patch:
            patch.setattr(rig.peer, "native", lambda key: open_session if key == "SESSION" else native(key))
            for _ in range(4):
                status = await c.tick()
                assert status["state"] == "reconciling"
                assert not status.get("owner_control_enabled") and not j.fault.latched()
                assert rig.bench.firmware.effective_limit() == 0
                assert c.requests.intent() == intent
            assert rig.bench.received_limits[limit_count:] == [0]

        status = await healthy(c)
        assert status["owner_control_enabled"] and status["station_ceiling_a"] == current
        assert status["charging_session_open"] is False and c.requests.intent() == intent
        assert rig.bench.received_limits[limit_count:] == [0, current]
        assert not any(
            f[2] in ("RemoteStartTransaction", "RemoteStopTransaction") for f in rig.bench.device_calls[command_count:]
        )


# Intent: a real Pause remains at zero while an unplugged charger reports the last closed session.
# Why: the Stop report and the native confirmation can arrive several polls apart without a charger fault.
async def test_pause_waits_for_unplugged_native_closure(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        local = await positive_flow(b, "daily")
        assert (await healthy(c))["charging_session_open"]
        open_session = rig.peer.native("SESSION")
        identifier = uuid.uuid4().hex
        c.command(identifier, "pause", c.requests.intent().revision)
        await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        await close_session(rig, b.session, b.charger, local)
        assert j.vendor_id(local) is not None
        rig.bench.cp, rig.bench.connection = "A", "Unplug"
        native = rig.peer.native
        mark = len(rig.bench.device_calls)
        with monkeypatch.context() as patch:
            patch.setattr(rig.peer, "native", lambda key: open_session if key == "SESSION" else native(key))
            for _ in range(4):
                status = await c.tick()
                assert status["state"] == "reconciling" and not j.fault.latched()
                assert rig.bench.firmware.effective_limit() == 0
                assert not status.get("local_hold_verified")
                assert c.requests.receipt(identifier)["state"] != "completed"
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(identifier)["state"] == "completed"
        later_calls = rig.bench.device_calls[mark:]
        assert not any(frame[2] in ("RemoteStartTransaction", "RemoteStopTransaction") for frame in later_calls)


# Intent: unrelated, unmapped and nonzero native reports remain strict refusals after OCPP closure.
# Why: only zero flow from the latest exact mapped lifecycle can enter the ordinary closure wait.
@pytest.mark.parametrize("sample", ["unknown", "older", "unmapped", "nonzero"])
async def test_ocpp_first_closure_does_not_adopt_other_native_sessions(
    tmp_path, tls, unused_tcp_port, monkeypatch, sample
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        older = None
        if sample == "older":
            older = await positive_flow(b, "daily")
            await close_session(rig, b.session, b.charger, older)
            rig.bench.cp, rig.bench.connection = "B", "Plug"
        if sample == "unmapped":
            monkeypatch.setattr(b.session.runtime, "vendor_enabled", lambda: False)
            await rig.bench.cloud.close()
            await until(lambda: not b.session.runtime.status.vendor_connected)
            start = await rig.bench.call(
                b.charger, uuid.uuid4().hex, "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
            )
            assert start[0] == 3 and start[2]["idTagInfo"]["status"] == "Accepted"
            local = start[2]["transactionId"]
            assert j.vendor_id(local) is None
        else:
            local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex, meter_start=1001)
            assert j.vendor_id(local) is not None
        await close_session(rig, b.session, b.charger, local, delivered=sample != "unmapped")
        rig.bench.cp, rig.bench.connection = "C", "Charging"
        native = rig.peer.native
        reported_local = older if older is not None else local
        if sample == "unknown":
            reported_local += 1

        # Keep independent native observations coherent while varying only the session identity or actual draw.
        def open_native(key):
            value = native(key)
            if key == "SESSION":
                value["saveData"]["sessionList"][0]["_transactionId"] = reported_local
            elif key == "METER" and sample == "nonzero":
                value[0]["meterData"].update(cur=[2, 0, 0], power=[48, 0, 0, 48])
            return value

        with monkeypatch.context() as patch:
            patch.setattr(rig.peer, "native", open_native)
            status = await c.tick()
            assert status["state"] == "degraded" and status["fault"] == "native_authority_or_preservation_failed"
            assert not status.get("owner_control_enabled") and j.active() is None
            assert j.get("managed_ordinary_recovery", {}).get("state") != "holding"
        rig.bench.cp, rig.bench.connection = "B", "Plug"


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
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=InterruptedNativePeer
    ) as b:
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
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=SlowOrTruncatedPeer
    ) as b:
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
