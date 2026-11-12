"""A genuine daily wire Start during an ordinary zero hold obtains no current before native source proof."""

import asyncio
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.test_managed_introduction_retry import InterruptedNativePeer, selected_ready
from tests.test_managed_proxy import healthy, starts
from tests.test_ordinary_recovery import close_session
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Establish the real bounded outage path rather than manufacturing a control-hold receipt.
async def idle_outage(b):
    b.rig.peer.unavailable = True
    for _ in range(5):
        await b.controller.tick()
    assert b.rig.bench.firmware.effective_limit() == 0
    assert b.controller.ordinary.active() and b.session.journal.active() is None


# Intent: protocol-first daily Starts record the actual wire lifecycle while zero survives unavailable/wrong source.
# Why: allocating an OCPP number grants no current and cannot adopt an unsolicited native transaction number.
@pytest.mark.parametrize("current", [6, 32])
async def test_daily_wire_start_during_ordinary_hold_requires_exact_native_source(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        await idle_outage(b)
        body = START | {"idTag": "NATIVE_TIMER"}
        unavailable = await b.rig.bench.call(b.charger, uuid.uuid4().hex, "StartTransaction", body)
        assert unavailable[0] == 4 and b.session.journal.active() is None
        b.rig.peer.unavailable = False
        reply = await b.rig.bench.call(b.charger, uuid.uuid4().hex, "StartTransaction", body)
        assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
        local = reply[2]["transactionId"]
        assert b.session.journal.get("managed_ordinary_recovery")["local"] == local
        assert b.rig.bench.firmware.effective_limit() == 0
        await until(
            lambda: b.session.journal.vendor_id(local) is not None and b.session.journal.next_delivery() is None
        )
        b.rig.peer.plan_session = (local, 987654)
        b.rig.peer.paused, b.rig.peer.permission_enabled = False, True
        b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
        b.rig.bench.firmware.fire_plan(9)
        for _ in range(3):
            await b.controller.tick()
        assert b.rig.bench.firmware.current == 0
        assert b.controller.ordinary.active() and not b.session.journal.get("managed_control_fault")
        b.rig.peer.plan_session = (local, 9)
        status = await healthy(b.controller)
        assert status["current_a"] == current and status["station_ceiling_a"] == current
        assert not starts(b.rig)
        assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)
        await close_session(b.rig, b.session, b.charger, local)
        await healthy(b.controller)


# Intent: foreign tags and an explicit newer Pause cannot borrow ordinary daily lifecycle authority.
# Why: outage recovery must retain the owner's exact app intent and immutable original schedule identity.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("superseding", ["foreign_tag", "pause"])
async def test_ordinary_idle_hold_does_not_authorize_foreign_or_paused_start(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, superseding
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        await idle_outage(b)
        b.rig.peer.unavailable = False
        tag = "FOREIGN" if superseding == "foreign_tag" else "NATIVE_TIMER"
        if superseding == "pause":
            b.controller.command(uuid.uuid4().hex, "pause", b.controller.requests.intent().revision)
        answer = await b.rig.bench.call(b.charger, uuid.uuid4().hex, "StartTransaction", START | {"idTag": tag})
        assert answer[0] == 4 and b.session.journal.active() is None
        assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)


class GatedNativePeer(InterruptedNativePeer):
    """Hold one actual HTTP connection while a genuine newer owner/Boot event crosses the read."""

    # Keep the transport latch entirely in the independent fake native peer.
    def __init__(self, bench):
        super().__init__(bench)
        self.gate_next = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    # The next actual HTTP request waits without changing any native value or proxy decision.
    async def handle(self, reader, writer):
        if self.gate_next:
            self.gate_next = False
            self.entered.set()
            await self.release.wait()
        await super().handle(reader, writer)


# Intent: a real unknown native ID or a newer owner/Boot event cannot enter the provisional daily report route.
# Why: fresh closed-zero evidence and its admission snapshot must remain valid through the actual wire Start commit.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("superseding", ["unknown_native", "crossed_pause", "crossed_boot"])
async def test_ordinary_daily_report_rejects_unknown_or_superseded_authority(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, superseding
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", GatedNativePeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        await idle_outage(b)
        b.rig.peer.unavailable = False
        if superseding == "unknown_native":
            b.rig.bench.firmware.active = 987654
            b.rig.peer.plan_session = (987654, 9)
            answer = await b.rig.bench.call(
                b.charger, uuid.uuid4().hex, "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
            )
            b.rig.bench.firmware.active = None
        else:
            b.rig.peer.gate_next = True
            pending = asyncio.create_task(
                b.rig.bench.call(b.charger, uuid.uuid4().hex, "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
            )
            await asyncio.wait_for(b.rig.peer.entered.wait(), 1)
            if superseding == "crossed_pause":
                b.controller.command(uuid.uuid4().hex, "pause", b.controller.requests.intent().revision)
            else:
                reply = await b.rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT)
                assert reply[0] == 3
            b.rig.peer.release.set()
            answer = await pending
        assert answer[0] == 4 and b.session.journal.active() is None
        assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
