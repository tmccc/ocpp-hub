"""Foreground recovery cannot finish a reconnect introduction that still owns pending native work."""

import asyncio
import copy
import uuid

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.native_bounded_handover import NativeBoundedHandover

from tests.charge_rig import ChargeRig, positive_flow
from tests.current_rig import selected_ready
from tests.managed_rig import healthy, starts
from tests.native_fault_support import coroutine_locals
from tests.prototype_rig import BOOT
from tests.recovery_barriers import returned_to_app
from tests.runtime_rig import until
from tests.test_introduction_storage_retry import GatedIntroductionPeer, current_probe
from tests.test_ordinary_recovery import close_session
from tests.tls_support import tls as tls


# Intent: ordinary zero/source repair waits for the real reconnect introducer before restoring selected current.
# Why: independent fresh zero proof cannot publish completion while that introducer still owns a later zero write.
@pytest.mark.parametrize("current", [6, 32])
async def test_ordinary_zero_cannot_publish_unfinished_introduction(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=GatedIntroductionPeer
    ) as b:
        c, s, rig = b.controller, b.session, b.rig
        g, j, peer = s.gateway, s.journal, rig.peer
        original_plan, original_config = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(peer.configuration)
        local = await positive_flow(b, "daily")
        peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await healthy(c)
        intent = c.requests.intent()
        await b.charger.close()
        await until(lambda: not s.runtime.status.charger_connected)
        peer.gate_next = True
        async with rig.charger(s) as charger:
            try:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                await asyncio.wait_for(peer.arrived.wait(), 2)
                epoch, boot = g._charger_epoch, g.boot_observation
                probe = current_probe(s.runtime, epoch)
                assert probe is not None and not probe.done()
                assert coroutine_locals(probe, NativeBoundedHandover.introduce.__code__)["epoch"] == epoch
                before = len(rig.bench.device_calls)
                assert (await c.tick())["state"] not in ("attention", "stopping")
                await until(lambda: g.vendor_registration == "Accepted")
                for _ in range(3):
                    status = await c.tick()
                    assert status["state"] not in ("attention", "stopping"), status
                source = j.get("managed_ordinary_source")
                assert source["local"] == local and source["source"]["kind"] == "daily"
                assert source["epoch"] == epoch and source["boot"] == boot
                assert not probe.done() and not peer.release.is_set()
                assert c.native.introduced_epoch != epoch
                assert g.controls.ready and g.controls.desired() == rig.bench.firmware.effective_limit() == 0
                assert c.requests.intent() == intent and j.active() == local
                assert j.get("managed_control_fault") is None and g.controls._release_ticket is None
                zeros = [row for row in rig.bench.device_calls[before:] if row[2] == "SetChargingProfile"]
                assert zeros and all(
                    period["limit"] == 0
                    for row in zeros
                    for period in row[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
                )
                peer.release.set()
                status = await healthy(c)
                assert probe.done() and c.native.introduced_epoch == epoch
                assert c.native.introduced_revision == g.controls._revision
                assert status["station_ceiling_a"] == rig.bench.firmware.effective_limit() == current
                assert c.requests.intent() == intent and j.active() == local
                assert rig.bench.firmware.plans == original_plan and peer.configuration == original_config
                assert not starts(rig) and not any(row[2] == "RemoteStopTransaction" for row in rig.bench.device_calls)
                await close_session(rig, s, charger, local)
                assert (await healthy(c))["station_ceiling_a"] == current
                # Finishing another owner cannot hide one that still has live introduction work.
                assert not c.native.introduction_pending
                with c.native._introduction_work():
                    with c.native._introduction_work():
                        assert c.native.introduction_pending
                    assert c.native.introduction_pending
                assert not c.native.introduction_pending
            finally:
                peer.release.set()


# Intent: a newer Pause can complete fresh closed-zero admission after superseding a late introduction zero.
# Why: separating completion from audit must retain the old Pause repair path after the superseded body retires.
@pytest.mark.parametrize("current", [6, 32])
async def test_new_pause_repairs_superseded_late_introduction_zero(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, s, rig = b.controller, b.session, b.rig
        g, j = s.gateway, s.journal
        local = await positive_flow(b, "daily")
        rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await healthy(c)
        await b.charger.close()
        await until(lambda: not s.runtime.status.charger_connected)
        entered, release = asyncio.Event(), asyncio.Event()
        async with rig.charger(s) as charger:
            original_send, held = charger.send, []

            # Delay the actual Accepted zero reply, retaining its genuine correlation and physical application.
            async def send(raw):
                answer = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == answer[1]), None)
                if not held and answer[0] == 3 and call is not None and call[2] == "SetChargingProfile":
                    periods = call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
                    if all(period["limit"] == 0 for period in periods):
                        assert answer[2]["status"] == "Accepted"
                        held.append(copy.deepcopy(call))
                        entered.set()
                        await release.wait()
                await original_send(raw)

            monkeypatch.setattr(charger, "send", send)
            try:
                before = len(rig.bench.device_calls)
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                await asyncio.wait_for(entered.wait(), 2)
                epoch, boot = g._charger_epoch, g.boot_observation
                probe = current_probe(s.runtime, epoch)
                assert probe is not None and coroutine_locals(probe, NativeBoundedHandover.introduce.__code__)
                assert c.native.audited_epoch == epoch and c.native.introduction_pending
                assert c.native.introduced_epoch != epoch
                old_ticket = j.get("control").get("ticket")
                pause = uuid.uuid4().hex
                c.command(pause, "pause", c.requests.intent().revision)
                await until(lambda: j.get("control").get("ticket") != old_ticket)
                release.set()
                await c.pause_task
                await until(lambda: probe.done() and not c.native.introduction_pending)
                assert not c.native.introduction_pending and c.native.audited_epoch == epoch
                assert c.native.introduced_epoch != epoch
                assert s.runtime.status.charger_fault.classification == "ControlSupersededError"
                await close_session(rig, s, charger, local)
                status = await healthy(c)
                assert status["local_hold_verified"] and c.requests.receipt(pause)["state"] == "completed"
                assert c.native.introduced_epoch == epoch and c.native.introduced_revision == g.controls._revision
                assert g.boot_observation == boot and j.get("managed_control_fault") is None
                assert rig.bench.firmware.effective_limit() == 0
                assert all(
                    period["limit"] == 0
                    for row in rig.bench.device_calls[before:]
                    if row[2] == "SetChargingProfile"
                    for period in row[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
                )
                stops = [row for row in rig.bench.device_calls[before:] if row[2] == "RemoteStopTransaction"]
                assert len(stops) == 1 and stops[0][3]["transactionId"] == local
                await returned_to_app(ChargeRig(rig, s, c, charger), current)
                assert rig.bench.firmware.effective_limit() == current and not starts(rig)
            finally:
                release.set()


# Intent: zero confirmation survives storage healing on the same connection and Boot.
# Why: an actual open scheduled session must not stay at zero just because the first zero receipt failed.
async def test_zero_receipt_storage_failure_does_not_strand_reconnect(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, s, rig = b.controller, b.session, b.rig
        g, j = s.gateway, s.journal
        local = await positive_flow(b, "daily")
        rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await healthy(c)
        await b.charger.close()
        await until(lambda: not s.runtime.status.charger_connected)
        entered, release = asyncio.Event(), asyncio.Event()
        async with rig.charger(s) as charger:
            send = charger.send

            # Hold the real peer's first Accepted zero reply until the exact receipt write can fail.
            async def hold_zero(raw):
                answer = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == answer[1]), None)
                if not entered.is_set() and answer[0] == 3 and call is not None and call[2] == "SetChargingProfile":
                    periods = call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
                    if all(period["limit"] == 0 for period in periods):
                        assert answer[2] == {"status": "Accepted"}
                        entered.set()
                        await release.wait()
                await send(raw)

            monkeypatch.setattr(charger, "send", hold_zero)
            try:
                assert (await rig.bench.call(charger, "reconnect-storage", "BootNotification", BOOT))[0] == 3
                await asyncio.wait_for(entered.wait(), 2)
                epoch, boot = g._charger_epoch, g.boot_observation
                probe = current_probe(s.runtime, epoch)
                assert probe is not None and c.native.audited_epoch == epoch
                refused = []
                j.db.create_function("witness_refused_zero", 0, lambda: refused.append(True) or 0)
                j.db.execute(
                    "CREATE TEMP TRIGGER refuse_zero_receipt BEFORE UPDATE ON trial_resources "
                    "WHEN NEW.key='profile:7001' AND OLD.state='pending' AND NEW.state='confirmed' "
                    "BEGIN SELECT witness_refused_zero(); SELECT RAISE(ABORT,'zero receipt unavailable'); END"
                )
                try:
                    release.set()
                    await until(lambda: len(refused) >= 2)
                    assert not probe.done() and rig.bench.firmware.effective_limit() == 0
                finally:
                    j.db.execute("DROP TRIGGER refuse_zero_receipt")
                assert (await healthy(c))["station_ceiling_a"] == 6
                assert g.boot_observation == boot and c.native.introduced_epoch == epoch and j.active() == local
                assert not starts(rig) and not rig.bench.remote_stop.is_set()
                await close_session(rig, s, charger, local)
            finally:
                release.set()


# Intent: a real deferred probe retains introduction ownership while its Native body has retired for retry sleep.
# Why: completion repair cannot borrow the quiet gap before that same service starts another restrictive attempt.
@pytest.mark.parametrize("current", [6, 32])
async def test_deferred_retry_sleep_retains_introduction_service(tmp_path, tls, unused_tcp_port, monkeypatch, current):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=GatedIntroductionPeer
    ) as b:
        c, s, rig = b.controller, b.session, b.rig
        g, j, peer = s.gateway, s.journal, rig.peer
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        await b.charger.close()
        await until(lambda: not s.runtime.status.charger_connected)
        peer.unavailable = True
        async with rig.charger(s) as charger:
            try:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch, boot = g._charger_epoch, g.boot_observation
                assert (await asyncio.wait_for(peer.failed_reads.get(), 2))["action"] == "getConfig"
                async with asyncio.timeout(3):
                    while True:
                        probe = current_probe(s.runtime, epoch)
                        awaited = None if probe is None else getattr(probe.get_coro(), "cr_await", None)
                        if (
                            probe is not None
                            and getattr(awaited, "cr_code", None) is asyncio.sleep.__code__
                            and not coroutine_locals(probe, NativeBoundedHandover.introduce.__code__)
                        ):
                            break
                        await asyncio.sleep(0)
                assert not c.native._introductions and not probe.done()
                assert s.runtime.introduction_pending and c.native.introduction_pending
                assert c.native.introduced_epoch != epoch and c.native.audited_epoch != epoch
                assert j.get("managed_control_fault") is None
                peer.unavailable = False
                status = await healthy(c)
                assert status["local_hold_verified"] and not c.native.introduction_pending
                assert probe.done() and c.native.introduced_epoch == epoch
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert not starts(rig) and not any(row[2] == "RemoteStopTransaction" for row in rig.bench.device_calls)
                await returned_to_app(ChargeRig(rig, s, c, charger), current)
                assert rig.bench.firmware.effective_limit() == current
            finally:
                peer.unavailable = False
