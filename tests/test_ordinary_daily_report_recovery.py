"""A genuine daily wire Start during an ordinary zero hold obtains no current before native source proof."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.control import HUB_PROFILE_ID
from ocpp_hub_core.frames import build_call

from tests.current_rig import selected_ready
from tests.managed_rig import healthy, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import BOOT, START
from tests.runtime_rig import until
from tests.test_ordinary_recovery import close_session
from tests.tls_support import tls as tls


async def test_ordinary_timer_report_requires_the_confirmed_ceiling(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        gateway, journal = b.session.gateway, b.session.journal
        bench = b.rig.bench
        boot = gateway.boot_observation
        assert gateway.controls.ready and bench.firmware.effective_limit() == 6
        profile = bench.firmware.profiles[HUB_PROFILE_ID]
        limit = profile["chargingSchedule"]["chargingSchedulePeriod"][0]
        original_limit = limit["limit"]
        body = START | {"idTag": "NATIVE_TIMER"}

        limit["limit"] = 32
        refused = await bench.call(b.charger, "daily-incorrect-profile", "StartTransaction", body)
        assert refused[0] == 4 and journal.active() is None and not journal.known_start(body)
        assert gateway.boot_observation == boot

        limit["limit"] = original_limit
        accepted = await bench.call(b.charger, "daily-matching-profile", "StartTransaction", body)
        assert accepted[0] == 3 and accepted[2]["idTagInfo"]["status"] == "Accepted", accepted
        await close_session(b.rig, b.session, b.charger, accepted[2]["transactionId"])


# Establish the real bounded outage path rather than manufacturing a control-hold receipt.
async def idle_outage(b):
    b.rig.peer.unavailable = True
    for _ in range(5):
        await b.controller.tick()
    assert b.rig.bench.firmware.effective_limit() == 0
    assert b.controller.ordinary.active() and b.session.journal.active() is None


# Admit a later Boot before the event loop can run either queued Start handler.
async def test_buffered_daily_and_vendor_starts_cannot_inherit_later_boot(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        g, j = b.session.gateway, b.session.journal
        bench = b.rig.bench
        boot = g.boot_observation
        body = START | {"idTag": "NATIVE_TIMER"}
        old_reply, boot_reply = asyncio.get_running_loop().create_future(), asyncio.get_running_loop().create_future()
        bench.replies.update({"buffered-start": old_reply, "later-boot": boot_reply})
        await g.receive("charger", build_call("buffered-start", "StartTransaction", body), g._charger_epoch)
        cloud = build_call("buffered-cloud", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
        await g.receive("vendor", cloud, g._vendor_epoch)
        await g.receive("charger", build_call("later-boot", "BootNotification", BOOT), g._charger_epoch)
        assert g.boot_observation != boot
        await g.drain()
        assert (await asyncio.wait_for(old_reply, 2))[0] == 4
        assert (await asyncio.wait_for(boot_reply, 2))[0] == 3
        await until(lambda: any(frame[1] == "buffered-cloud" for frame in bench.calls))
        assert next(frame[0] for frame in bench.calls if frame[1] == "buffered-cloud") == 4
        assert not starts(b.rig) and not j.get("vendor_start_grant")
        assert j.active() is None and not j.known_start(body)

        retry = await bench.call(b.charger, "new-boot-start", "StartTransaction", body)
        assert retry[0] == 3 and retry[2]["idTagInfo"]["status"] == "Accepted"
        local = retry[2]["transactionId"]
        assert j.active() == local
        await close_session(b.rig, b.session, b.charger, local)


# Intent: protocol-first daily Starts record the actual wire lifecycle while zero survives unavailable/wrong source.
# Why: allocating an OCPP number grants no current and cannot adopt an unsolicited native transaction number.
@pytest.mark.parametrize("current", [6, 32])
async def test_daily_wire_start_during_ordinary_hold_requires_exact_native_source(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=InterruptedNativePeer
    ) as b:
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


# Intent: a daily report missing its timestamp receives a correlated error even when cloud approval has expired.
# Why: native evidence under an ordinary zero hold cannot make an incomplete Start eligible for renewal or allocation.
async def test_held_daily_start_requires_timestamp(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6, native_peer=InterruptedNativePeer) as b:
        await idle_outage(b)
        b.rig.peer.unavailable = False
        g, j, bench = b.session.gateway, b.session.journal, b.rig.bench
        g.auth_info.put("NATIVE_TIMER", {"status": "Expired"})
        body = START | {"idTag": "NATIVE_TIMER"}
        malformed = {key: value for key, value in body.items() if key != "timestamp"}
        counter, native_reads = j.get("next_transaction"), b.controller.native.read_sequence

        reply = await bench.call(b.charger, "missing-timestamp", "StartTransaction", malformed)
        assert reply[:3] == [4, "missing-timestamp", "PropertyConstraintViolation"]
        assert b.controller.native.read_sequence == native_reads
        assert j.get("next_transaction") == counter and j.active() is None
        assert g.auth_info.stored("NATIVE_TIMER") == {"status": "Expired"}
        assert (await bench.call(b.charger, "same-connection", "Heartbeat", {}))[0] == 3

        accepted = await bench.call(b.charger, "valid-timestamp", "StartTransaction", body)
        assert accepted[0] == 3 and accepted[2]["idTagInfo"]["status"] == "Accepted"
        local = accepted[2]["transactionId"]
        assert j.active() == local
        await close_session(b.rig, b.session, b.charger, local)


# Intent: foreign tags and an explicit newer Pause cannot borrow ordinary daily lifecycle authority.
# Why: outage recovery must retain the owner's exact app intent and immutable original schedule identity.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("superseding", ["foreign_tag", "pause"])
async def test_ordinary_idle_hold_does_not_authorize_foreign_or_paused_start(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, superseding
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=InterruptedNativePeer
    ) as b:
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
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=GatedNativePeer) as b:
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
