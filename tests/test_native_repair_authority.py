"""Real held native HTTP replies cannot revoke a newer owner authority or reuse a changed Pause proof."""

import asyncio
import copy
import json
import sqlite3
import sys
import uuid

import pytest
from ocpp_hub_core.prototype.managed_native_repair import AUDITS, repair_pending, repair_revoked
from ocpp_hub_core.prototype.managed_release_recovery import ACK, PROOF, recoverable_fault
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_release_recovery import explicit_release_pause, release_bench
from tests.test_native_repair_storage import refused_write
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_prototype import BOOT
from tests.test_prototype_tls import tls as tls


class HeldRepairPeer(InterruptedNativePeer):
    """Hold a captured last-getConfig wire reply; later genuine connections retain ordinary peer facts."""

    # Only the test endpoint owns its capture/release events; no native engine observer is replaced.
    def __init__(self, bench):
        super().__init__(bench)
        self.armed = False
        self.config_reads = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    # Replay unchanged actual request bytes to the existing peer except for one captured final response.
    async def handle(self, reader, writer):
        if not self.armed:
            await super().handle(reader, writer)
            return
        header = await reader.readuntil(b"\r\n\r\n")
        fields = {
            line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
            for line in header.split(b"\r\n")[1:]
            if b":" in line
        }
        raw = await reader.readexactly(int(fields[b"content-length"]))
        request = json.loads(raw)
        if request["action"] == "getConfig":
            self.config_reads += 1
        if request["action"] == "getConfig" and self.config_reads == 2:
            self.armed = False
            body = json.dumps({"code": 200, "data": copy.deepcopy(self.configuration)}).encode()
            self.entered.set()
            try:
                await self.release.wait()
                writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
            return
        replay = asyncio.StreamReader()
        replay.feed_data(header + raw)
        replay.feed_eof()
        await super().handle(replay, writer)


# Intent: an old captured contradiction is deferred after actual newer Pause/Boot, without revoking newer authority.
# Why: read generation and real owner authority must be checked before a raw sample becomes a strict fact.
@pytest.mark.parametrize("change", ["pause", "boot"])
async def test_new_authority_supersedes_held_old_native_content(tmp_path, tls, unused_tcp_port, monkeypatch, change):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", HeldRepairPeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, peer, g = b.c, b.session.journal, b.rig.peer, b.session.gateway
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        original = copy.deepcopy(peer.configuration)
        peer.configuration["untouched"] = "captured old-source contradiction"
        peer.armed = True
        read = asyncio.create_task(c.native.read(closed=True))
        await asyncio.wait_for(peer.entered.wait(), 3)
        assert repair_pending(j, fault)
        peer.configuration = original
        if change == "pause":
            pause = uuid.uuid4().hex
            c.command(pause, "pause", c.requests.intent().revision)
        else:
            old_boot = g.boot_observation
            assert (await b.rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            assert g.boot_observation != old_boot
        intent, ticket = c.requests.intent(), j.get("control")["ticket"]
        peer.release.set()
        with pytest.raises(AuditSupersededError):
            await read
        assert not repair_pending(j, fault) and not repair_revoked(j, fault)
        assert c.requests.intent() == intent and j.get("control")["ticket"] == ticket
        if c.pause_task is not None:
            await c.pause_task
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed"
        assert j.get(PROOF)["pause_request"] == pause and j.get("managed_control_fault") is None
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await mp.healthy(c))["owner_control_enabled"]
        b.controller = c
        await two_later_sessions(b, selected=6, original_starts=0, first_minute=20)


# Intent: a changed diagnostic cannot complete an older acknowledgement, while a newly bound valid Pause can recover.
# Why: fresh native zero does not itself authorize reusing a mismatched diagnostic/proof snapshot.
async def test_changed_diagnostic_cannot_borrow_old_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j = b.c, b.session.journal
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        diagnostic, ack = copy.deepcopy(j.get("managed_native_failure")), copy.deepcopy(j.get(ACK))
        j.put("managed_native_failure", {**diagnostic, "detail": "another independent unavailable diagnostic"})
        for _ in range(3):
            assert (await c.tick())["state"] == "attention"
        assert j.get(ACK) == ack and c.requests.receipt(pause)["state"] != "completed"
        assert j.get("managed_control_fault") == fault and b.rig.bench.firmware.effective_limit() == 0
        new_pause = uuid.uuid4().hex
        c.command(new_pause, "pause", c.requests.intent().revision)
        if c.pause_task is not None:
            await c.pause_task
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(new_pause)["state"] == "completed" and j.get(PROOF)["pause_request"] == new_pause


# Intent: an occupied malformed current-fault audit slot remains uncertain instead of being overwritten by a fresh read.
# Why: absent/mismatched private preimages are not evidence that the earlier attempt never existed.
@pytest.mark.parametrize("row", [None, {}, {"fault": {"classification": "different"}}])
async def test_malformed_current_fault_fence_stays_pending(tmp_path, tls, unused_tcp_port, monkeypatch, row):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j = b.c, b.session.journal
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        await explicit_release_pause(b)
        value = {fingerprint(fault): row}
        j.put(AUDITS, value)
        requests = len(b.rig.peer.requests)
        with pytest.raises(RecoveryRequiredError, match="prior audit outcome"):
            await c.native.read()
        assert j.get(AUDITS) == value and len(b.rig.peer.requests) == requests
        assert (await c.tick())["state"] == "attention"
        assert j.get(AUDITS) == value and b.rig.bench.firmware.effective_limit() == 0


# Intent: real SQL refusal recording an owned-profile contradiction cannot hide the typed strict resource fact.
# Why: an observed native conflict remains authoritative even if its independent ResourceBook diagnostic cannot persist.
@pytest.mark.parametrize("failure", ["statement", "commit"])
async def test_resource_conflict_survives_its_diagnostic_sql_failure(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, peer = b.c, b.session.journal, b.rig.peer
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        await explicit_release_pause(b)
        original = peer.native
        observations = []

        # Only native SMART bytes change; the independently modelled station stays at actual zero.
        def changed_owned_profile(key):
            value = original(key)
            if key == "SMART":
                value["conn0"]["ChargePointMaxProfile"][0]["periods"][0]["limit"] = 10
                observations.append(copy.deepcopy(value))
            return value

        monkeypatch.setattr(peer, "native", changed_owned_profile)
        failures = []
        prior_trace = sys.gettrace()

        # Observe the real diagnostic write failure without replacing a resource decision or returned native value.
        def observed_sql(frame, event, argument):
            if (
                event == "exception"
                and frame.f_code.co_name == "_inventory_failure"
                and frame.f_code.co_filename.endswith("/trial_resources.py")
                and isinstance(argument[1], sqlite3.Error)
            ):
                failures.append(type(argument[1]).__name__)
            return observed_sql

        try:
            sys.settrace(observed_sql)
            with refused_write(j, "trial_inventory_fault", failure):
                assert (await c.tick())["state"] == "attention"
        finally:
            sys.settrace(prior_trace)
        assert failures, "the intended real SQLite diagnostic refusal must actually execute"
        assert observations and repair_revoked(j, fault) and not recoverable_fault(c, fault)
        monkeypatch.setattr(peer, "native", original)
        for _ in range(3):
            assert (await c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault and b.rig.bench.firmware.effective_limit() == 0


# Intent: a real pending safe TLS read postpones native repair, then the exact Pause recovers after its actual reply.
# Why: native readiness must not turn ordinary in-flight protocol work into a permanent strict or unknown cause.
async def test_actual_pending_charger_read_remains_recoverable(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.c, b.session.journal, b.rig
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        ack = copy.deepcopy(j.get(ACK))
        entered, release = asyncio.Event(), asyncio.Event()
        actual_send = b.charger.send

        # Hold only the independent fake charger's response to a real cloud-originated safe GetConfiguration.
        async def hold_read_reply(raw):
            frame = json.loads(raw)
            request = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
            if frame[0] == 3 and request is not None and request[2] == "GetConfiguration":
                entered.set()
                await release.wait()
            await actual_send(raw)

        monkeypatch.setattr(b.charger, "send", hold_read_reply)
        cloud_read = asyncio.create_task(
            rig.bench.cloud_call(uuid.uuid4().hex, "GetConfiguration", {"key": ["HeartbeatInterval"]})
        )
        try:
            await asyncio.wait_for(entered.wait(), 3)
            assert b.session.gateway.charger._pending and not cloud_read.done()
            with pytest.raises(RecoveryRequiredError, match="settled resource operations"):
                await c.native.read(closed=True)
            assert not repair_pending(j, fault) and not repair_revoked(j, fault)
            assert (await c.tick())["state"] == "attention"
            assert j.get("managed_control_fault") == fault and j.get(ACK) == ack
            assert c.requests.receipt(pause)["state"] == "accepted"
            assert rig.bench.firmware.effective_limit() == rig.bench.firmware.current == 0
        finally:
            release.set()
            response = await cloud_read
            monkeypatch.setattr(b.charger, "send", actual_send)
        assert response[0] == 3 and not b.session.gateway.charger._pending
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed" and j.get("managed_control_fault") is None
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await mp.healthy(c))["owner_control_enabled"]
        b.controller = c
        await two_later_sessions(b, selected=6, original_starts=0, first_minute=20)
