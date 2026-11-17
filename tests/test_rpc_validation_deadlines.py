"""Separate native validation and wire budgets retain revocation, cleanup and physical uncertainty."""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.rpc import Peer, UnavailableError

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


class NativeAuditPeer(NativeVehiclePeer):
    """The independent HTTP endpoint delays or refuses one complete received request."""

    # Arm only after ordinary commissioning; the peer never reads the engine's decisions or receipts.
    def __init__(self, bench):
        super().__init__(bench)
        self.arm = None
        self.delay = 0.22
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.client_closed = asyncio.Event()
        self.finished = asyncio.Event()
        self.selected_request = None
        self.selected_count = 0

    # Preserve exact request bytes and credentials while controlling only this independent server's response.
    async def handle(self, reader, writer):
        mode, self.arm = self.arm, None
        if mode is None:
            await super().handle(reader, writer)
            return
        header = await reader.readuntil(b"\r\n\r\n")
        fields = {
            line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
            for line in header.split(b"\r\n")[1:]
            if b":" in line
        }
        body = await reader.readexactly(int(fields[b"content-length"]))
        self.selected_request = json.loads(body)
        self.selected_count += 1
        self.entered.set()

        # A cancelled production reader must actually close its HTTP socket before this server releases a reply.
        async def watch_close():
            assert await reader.read() == b""
            self.client_closed.set()

        closed = asyncio.create_task(watch_close())
        try:
            if mode == "hold":
                await self.release.wait()
            elif mode == "delay":
                await asyncio.sleep(self.delay)
            elif mode == "503":
                writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            else:
                raise AssertionError("unknown independent native response mode")
            replay = asyncio.StreamReader()
            replay.feed_data(header + body)
            replay.feed_eof()
            await super().handle(replay, writer)
        except ConnectionError:
            pass  # Cancelling an old audit legitimately retires its independent peer socket before a late reply.
        finally:
            closed.cancel()
            await asyncio.gather(closed, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.finished.set()


# The real peer must retire its correlation entry and leave the outgoing gate available after terminal work.
def assert_retired(peer):
    assert not peer._pending
    assert not peer.gate.busy


# Intent: slow valid native HTTP and a valid charger reply each receive their own complete bounded allowance.
# Why: their combined elapsed time may exceed the wire deadline without either boundary failing.
@pytest.mark.parametrize(("audit_delay", "reply_delay"), [(0.22, 0.22), (0.40, 0.05)])
async def test_native_audit_and_wire_reply_have_independent_budgets(
    tmp_path, tls, unused_tcp_port, monkeypatch, audit_delay, reply_delay
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", NativeAuditPeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        c, g, peer = test.controller, test.session.gateway, test.rig.peer
        g.charger.timeout = 0.30
        peer.delay = audit_delay
        assert g.charger.validate_send.__self__ is c.native
        assert g.charger.validate_send.__func__ is type(c.native).validate_send

        # The independent device arms the next HTTP audit only after its positive station cap was actually applied.
        async def before_reply(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit > 0:
                    peer.arm = "delay"
            elif action == "RemoteStartTransaction":
                await asyncio.sleep(reply_delay)

        test.rig.bench.before_reply = before_reply
        request = uuid.uuid4().hex
        c.command(request, "start", c.requests.intent().revision)
        await c.charging.settle()
        assert peer.selected_count == 1 and peer.selected_request["action"] == "getConfig"
        assert len(starts(test.rig)) == 1
        assert test.session.journal.get("local_start_receipt")["stage"] == "accepted"
        assert c.charging.state()["start_rpc_accepted"] is True
        assert c.requests.intent().mode == "app"
        assert not test.session.journal.get("managed_test_failure")
        assert_retired(g.charger)


# Intent: a real newer Pause applies zero and retires a held pre-Start audit before HTTP is released.
# Why: extending the audit allowance must not make a protective control wait behind fifteen seconds of old evidence.
async def test_pause_cancels_held_native_audit_before_start(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", NativeAuditPeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        c, g, peer, j = test.controller, test.session.gateway, test.rig.peer, test.session.journal

        # Trigger latency from actual device effects, after positive cap validation but before RemoteStart validation.
        async def before_reply(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit > 0:
                    peer.arm = "hold"

        test.rig.bench.before_reply = before_reply
        request = uuid.uuid4().hex
        c.command(request, "start", c.requests.intent().revision)
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            assert peer.selected_request["action"] == "getConfig" and not starts(test.rig)
            pause = uuid.uuid4().hex
            c.command(pause, "pause", c.requests.intent().revision)
            assert c.pause_task is not None
            await asyncio.wait_for(asyncio.shield(c.pause_task), 1)
            await asyncio.wait_for(c.charging.settle(), 1)
            await asyncio.wait_for(peer.client_closed.wait(), 1)
            assert not peer.release.is_set()
            assert not starts(test.rig) and test.rig.bench.firmware.effective_limit() == 0
            assert j.get("local_start_receipt")["stage"] == "unsent"
            assert not j.get("control").get("start_pending")
            assert c.requests.intent().mode == "paused"
            assert_retired(g.charger)
            assert (await healthy(c))["local_hold_verified"]
            assert c.requests.receipt(pause)["state"] == "completed"
        finally:
            peer.release.set()
            await c.charging.settle()
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            await asyncio.wait_for(peer.finished.wait(), 2)


# Intent: an actual dispatched Start whose reply misses the wire deadline keeps its durable uncertain outcome.
# Why: separate preparation time cannot turn absent physical acknowledgement into proof that nothing was sent.
async def test_missing_wire_reply_keeps_start_uncertain(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        g, j, bench = test.session.gateway, test.session.journal, test.rig.bench
        g.charger.timeout = 0.20
        entered, release = asyncio.Event(), asyncio.Event()

        # The independent firmware has applied the command before withholding only its actual TLS acknowledgement.
        async def before_reply(action, payload):
            if action == "RemoteStartTransaction":
                entered.set()
                await release.wait()

        bench.before_reply = before_reply
        task = asyncio.create_task(
            bench.cloud_call("wire-timeout-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            answer = await asyncio.wait_for(asyncio.shield(task), 1)
            assert answer[0] == 4 and answer[2] == "InternalError"
            assert len(starts(test.rig)) == 1 and j.get("vendor_start_grant")
            assert j.get("uncertain_vendor_command")
            rows = j.db.execute("SELECT state FROM mutations WHERE origin='vendor'").fetchall()
            assert [row[0] for row in rows] == ["uncertain"]
            assert_retired(g.charger)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


# Intent: cancelling real native validation retires only prepared vendor work and closes its actual HTTP reader.
# Why: no orphan validation task may retain the gate, grant or correlation entry after its caller exits.
async def test_cancelled_native_validation_retires_unsent_vendor_work(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", NativeAuditPeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        g, j, peer = test.session.gateway, test.session.journal, test.rig.peer
        peer.arm = "hold"
        frame = [2, "cancel-native-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}]
        task = asyncio.create_task(g._vendor_call(frame, g._vendor_epoch))
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            assert j.get("vendor_start_grant") and not starts(test.rig)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(peer.client_closed.wait(), 1)
            assert not peer.release.is_set()
            assert not starts(test.rig) and not j.get("vendor_start_grant")
            assert not j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor'").fetchone()
            assert not j.get("uncertain_vendor_command")
            assert_retired(g.charger)
        finally:
            peer.release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.wait_for(peer.finished.wait(), 2)


# Intent: retiring the real charger connection cannot dispatch a Start from its outstanding native audit.
# Why: the independently completed HTTP snapshot cannot revive an obsolete socket generation or leave its gate owned.
async def test_retired_charger_cancels_native_validation_without_dispatch(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", NativeAuditPeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        g, j, peer = test.session.gateway, test.session.journal, test.rig.peer
        epoch = g._charger_epoch
        peer.arm = "hold"
        frame = [2, "retired-native-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}]
        task = asyncio.create_task(g._vendor_call(frame, g._vendor_epoch))
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            await test.charger.close()
            await until(lambda: g._charger_epoch != epoch and g.charger.sender is None)
            await asyncio.wait_for(asyncio.shield(task), 1)
            await asyncio.wait_for(peer.client_closed.wait(), 1)
            assert not peer.release.is_set() and not starts(test.rig)
            assert not j.get("vendor_start_grant")
            assert not j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor'").fetchone()
            assert_retired(g.charger)
        finally:
            peer.release.set()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.wait_for(peer.finished.wait(), 2)


# Intent: a real native HTTP refusal retires its prepared mutation without weakening the exact validation boundary.
# Why: deadline separation must preserve both fast validation failures and later fresh command admission.
async def test_native503_retires_prepared_vendor_work(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", NativeAuditPeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        g, j, peer = test.session.gateway, test.session.journal, test.rig.peer
        peer.arm = "503"
        result = await test.rig.bench.cloud_call(
            "refused-native-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
        )
        assert result[0] == 4 and result[2] == "InternalError"
        assert peer.selected_count == 1 and peer.selected_request["action"] == "getConfig"
        assert not starts(test.rig) and not j.get("vendor_start_grant")
        assert not j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor'").fetchone()
        assert not j.get("uncertain_vendor_command")
        assert_retired(g.charger)
        result = await test.rig.bench.cloud_call(
            "fresh-native-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
        )
        assert result[0] == 3 and result[2] == {"status": "Accepted"}
        assert len(starts(test.rig)) == 1


# Intent: a bounded audit timeout proves no dispatch and frees the gate for the next independent request.
# Why: removing the wire timeout from validation must not permit an unbounded validator or an orphan child task.
async def test_audit_deadline_is_known_unsent_and_releases_gate():
    peer = Peer("charger", 0.30)
    peer.validation_timeout = 0.05
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    sent = []

    # Retain an actual independent request until cancellation; harmless requests still validate immediately.
    async def validate(action, payload):
        if action == "RemoteStartTransaction":
            entered.set()
            try:
                await release.wait()
            finally:
                cancelled.set()

    # Supply a correlated independent reply only for messages which the real Peer actually dispatches.
    async def sender(raw):
        frame = decode(raw)
        sent.append(frame)
        peer.receive(decode(build_call_result(frame[1], {"status": "Accepted"})))

    peer.validate_send, peer.sender = validate, sender
    try:
        with pytest.raises(UnavailableError, match="not sent|before dispatch"):
            await peer.call("RemoteStartTransaction", {"idTag": "SYNTHETIC"})
        assert entered.is_set() and cancelled.is_set() and not sent
        assert_retired(peer)
        assert (await peer.call("GetConfiguration", {}))[0] == 3
        assert [frame[2] for frame in sent] == ["GetConfiguration"]
    finally:
        release.set()


# Intent: priority Stop revokes outstanding validation but preserves an already dispatched request's real outcome.
# Why: preemption is safe only before transport invocation, while reply correlation must remain live after dispatch.
@pytest.mark.parametrize("boundary", ["validation", "reply"])
async def test_priority_stop_preempts_only_validation(boundary):
    peer = Peer("charger", 2)
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    sent = []

    # Keep only the positive audit pending, with cancellation observed independently from the Peer implementation.
    async def validate(action, payload):
        if action == "RemoteStartTransaction" and boundary == "validation":
            entered.set()
            try:
                await release.wait()
            finally:
                cancelled.set()

    # Observe exact actual dispatch and retain a sent positive request's reply until the test supplies it.
    async def sender(raw):
        frame = decode(raw)
        sent.append(frame)
        if frame[2] == "RemoteStartTransaction" and boundary == "reply":
            entered.set()
        else:
            peer.receive(decode(build_call_result(frame[1], {"status": "Accepted"})))

    peer.validate_send, peer.sender = validate, sender
    positive = asyncio.create_task(peer.call("RemoteStartTransaction", {"idTag": "SYNTHETIC"}))
    stopped = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        stopped = asyncio.create_task(peer.call("RemoteStopTransaction", {"transactionId": 1}, priority=0))
        if boundary == "validation":
            assert (await asyncio.wait_for(asyncio.shield(stopped), 0.5))[0] == 3
            with pytest.raises(UnavailableError, match="not sent|before dispatch"):
                await positive
            assert cancelled.is_set() and not release.is_set()
            assert [frame[2] for frame in sent] == ["RemoteStopTransaction"]
        else:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not stopped.done() and [frame[2] for frame in sent] == ["RemoteStartTransaction"]
            peer.receive(decode(build_call_result(sent[0][1], {"status": "Accepted"})))
            assert (await positive)[0] == 3 and (await stopped)[0] == 3
            assert [frame[2] for frame in sent] == ["RemoteStartTransaction", "RemoteStopTransaction"]
        assert_retired(peer)
    finally:
        release.set()
        if boundary == "reply" and sent:
            peer.receive(decode(build_call_result(sent[0][1], {"status": "Accepted"})))
        await asyncio.gather(positive, *([] if stopped is None else [stopped]), return_exceptions=True)


# Intent: a queued zero at equal priority cannot cancel the current restrictive validation before its own reply.
# Why: several restrictive sequences may queue together, and priority cancellation must not make them defeat each other.
async def test_queued_zero_does_not_preempt_active_restrictive_validation():
    peer = Peer("charger", 2)
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    sent = []

    # Hold the first actual zero-profile audit while the second zero is genuinely queued on the same gate.
    async def validate(action, payload):
        if payload["csChargingProfiles"]["chargingProfileId"] == 1:
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    # Confirm only requests actually delivered by the real Peer, preserving the first and second profile identities.
    async def sender(raw):
        frame = decode(raw)
        sent.append(frame)
        peer.receive(decode(build_call_result(frame[1], {"status": "Accepted"})))

    peer.validate_send, peer.sender = validate, sender
    first = asyncio.create_task(
        peer.call(
            "SetChargingProfile",
            commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 0, 1),
            priority=0,
        )
    )
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(
            peer.call(
                "SetChargingProfile",
                commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 0, 2),
                priority=0,
            )
        )
        await asyncio.wait_for(peer.gate.restrictive_waiting.wait(), 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(first), 0.05)
        assert not cancelled.is_set() and not sent and not second.done()
        release.set()
        assert (await first)[0] == 3 and (await second)[0] == 3
        assert [frame[3]["csChargingProfiles"]["chargingProfileId"] for frame in sent] == [1, 2]
        assert not peer.gate.restrictive_waiting.is_set()
        assert_retired(peer)
    finally:
        release.set()
        await asyncio.gather(first, *([] if second is None else [second]), return_exceptions=True)


# Intent: cancelling a queued urgent waiter clears its signal and permits a later positive validation to succeed.
# Why: a vanished Stop must not leave a sticky preemption event that rejects every subsequent ordinary request.
async def test_cancelled_urgent_waiter_does_not_poison_later_validation():
    peer = Peer("charger", 2)
    entered = asyncio.Event()
    sent = []
    validated = []

    # Record real validation attempts independently while allowing their native stand-in to finish immediately.
    async def validate(action, payload):
        validated.append(action)

    # Retain one already dispatched read so the urgent request is cancelled while genuinely queued.
    async def sender(raw):
        frame = decode(raw)
        sent.append(frame)
        if frame[2] == "GetConfiguration":
            entered.set()
        else:
            peer.receive(decode(build_call_result(frame[1], {"status": "Accepted"})))

    peer.validate_send, peer.sender = validate, sender
    first = asyncio.create_task(peer.call("GetConfiguration", {}))
    urgent = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        urgent = asyncio.create_task(peer.call("RemoteStopTransaction", {"transactionId": 1}, priority=0))
        await asyncio.wait_for(peer.gate.restrictive_waiting.wait(), 1)
        urgent.cancel()
        with pytest.raises(asyncio.CancelledError):
            await urgent
        assert not peer.gate.restrictive_waiting.is_set()
        peer.receive(decode(build_call_result(sent[0][1], {})))
        assert (await first)[0] == 3
        assert (await peer.call("RemoteStartTransaction", {"idTag": "SYNTHETIC"}))[0] == 3
        assert [frame[2] for frame in sent] == ["GetConfiguration", "RemoteStartTransaction"]
        assert validated == ["GetConfiguration", "RemoteStartTransaction"]
        assert_retired(peer)
    finally:
        if sent:
            peer.receive(decode(build_call_result(sent[0][1], {})))
        if urgent is not None and not urgent.done():
            urgent.cancel()
        await asyncio.gather(first, *([] if urgent is None else [urgent]), return_exceptions=True)
