"""Expected-behaviour regression: ordinary introduction survives a repeated identical Boot."""

import asyncio
import sqlite3
import uuid
from dataclasses import replace

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.control import HUB_PROFILE_ID
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_view import refresh_for_read
from ocpp_hub_core.prototype.native_faults import MEASURED_FAULT, STATION_CEILING_LOST

from tests.charge_rig import positive_flow
from tests.current_rig import commissioned
from tests.handover_helpers import crash
from tests.managed_rig import healthy, managed_bench, managed_session
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import BOOT, START
from tests.runtime_rig import until
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Delay the first correlated reply without changing the actual independent charger response.
def delay_reply(monkeypatch, rig, charger, action):
    send, held, entered = charger.send, [], asyncio.Event()

    # The full peer reply remains available for the test to deliver after a newer Boot.
    async def delayed(raw):
        answer = decode(raw)
        call = next((frame for frame in rig.bench.device_calls if frame[1] == answer[1]), None)
        if answer[0] == 3 and call is not None and call[2] == action and not held:
            held.append(raw)
            entered.set()
            return
        await send(raw)

    monkeypatch.setattr(charger, "send", delayed)
    return send, held, entered


# Intent: a newer identical Boot restarts the full restrictive proof without losing normal introduction.
# Why: zero/composite acknowledgements cannot strand the listener or be reused across Boot observations.
@pytest.mark.parametrize("boundary", ["SetChargingProfile", "GetCompositeSchedule"])
@pytest.mark.parametrize("duplicate", [False, True], ids=["normal-control", "identical-boot"])
async def test_introduction_eventually_healthy(tmp_path, tls, unused_tcp_port, monkeypatch, duplicate, boundary):
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            peer_send, captured, zero_dispatched = delay_reply(monkeypatch, rig, charger, boundary)
            assert (await rig.bench.call(charger, "first-boot", "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(zero_dispatched.wait(), 2)
            if duplicate:
                for number in range(2):
                    assert (await rig.bench.call(charger, f"retry-boot-{number}", "BootNotification", BOOT))[0] == 3
            after_boot = len(rig.bench.device_calls)
            await peer_send(captured[0])

            # The repeated boot changes no identity, configuration, physical state or requested current.
            # Both cases should complete the safe zero proof and subsequently release normal 6 A control.
            status = await healthy(controller)
            assert status["state"] == "healthy"
            assert session.gateway.controls.ready
            assert rig.bench.received_limits[0] == 0
            assert rig.bench.received_limits[-1] == 6
            if duplicate:
                fresh = rig.bench.device_calls[after_boot:]
                assert fresh[0][2] == "SetChargingProfile"
                assert fresh[0][3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                assert fresh[1][2] == "GetCompositeSchedule"
            assert not any(frame[2] == "RemoteStartTransaction" for frame in rig.bench.device_calls)


# Intent: a known daily session needs a fresh full zero proof if a newer Boot overtakes its introduction.
# Why: neither the old Set acknowledgement nor its old composite can restore the existing positive ceiling.
@pytest.mark.parametrize("boundary", ["SetChargingProfile", "GetCompositeSchedule"])
async def test_open_daily_introduction_rechecks_zero_after_new_boot(
    tmp_path, tls, unused_tcp_port, monkeypatch, boundary
):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        c, g, rig = b.controller, b.session.gateway, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await healthy(c))["current_a"] == 32
        vendor = b.session.journal.vendor_id(local)
        await b.charger.close()
        await until(lambda: g.charger.sender is None)
        async with rig.charger(b.session) as charger:
            send, held, entered = delay_reply(monkeypatch, rig, charger, boundary)
            assert (await rig.bench.call(charger, "open-first-boot", "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(entered.wait(), 3)
            assert (await rig.bench.call(charger, "open-second-boot", "BootNotification", BOOT))[0] == 3
            mark = len(rig.bench.device_calls)
            assert not g.controls.ready and rig.bench.firmware.effective_limit() == 0
            await send(held[0])
            assert (await healthy(c))["current_a"] == 32
            fresh = rig.bench.device_calls[mark:]
            assert [frame[2] for frame in fresh[:2]] == ["SetChargingProfile", "GetCompositeSchedule"]
            assert fresh[0][3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
            assert b.session.journal.active() == local and b.session.journal.vendor_id(local) == vendor
            assert not any(frame[2] in ("RemoteStartTransaction", "RemoteStopTransaction") for frame in fresh)


# Intent: after a failed Boot, the open-session zero retry cannot attach to changed metadata.
# Why: waiting for a saved observation cannot turn a different Boot body into the one already audited.
async def test_open_introduction_does_not_retry_zero_under_changed_boot(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        c, g, rig, j = b.controller, b.session.gateway, b.rig, b.session.journal
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await healthy(c)
        await b.charger.close()
        await until(lambda: g.charger.sender is None)
        async with rig.charger(b.session) as charger:
            send, held, entered = delay_reply(monkeypatch, rig, charger, "SetChargingProfile")
            assert (await rig.bench.call(charger, "audited-open-boot", "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(entered.wait(), 3)
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_boot BEFORE INSERT ON meta WHEN NEW.key='boot' "
                "BEGIN SELECT RAISE(ABORT,'Boot storage temporarily unavailable'); END"
            )
            try:
                assert (await rig.bench.call(charger, "unsaved-open-boot", "BootNotification", BOOT))[0] == 4
            finally:
                j.db.execute("DROP TRIGGER refuse_boot")
            await send(held[0])
            await until(lambda: c.native.introduced_epoch == g._charger_epoch)
            mark = len(rig.bench.device_calls)
            changed = BOOT | {"firmwareVersion": "unverified firmware"}
            assert (await rig.bench.call(charger, "different-open-boot", "BootNotification", changed))[0] == 3
            await until(lambda: not b.session.runtime.introduction_pending)
            assert rig.bench.device_calls[mark:] == [] and not g.controls.ready
            assert j.active() == local


# Intent: even a failed Boot admission withdraws the older ready ceiling, which a fresh retry can reprove.
# Why: matching metadata and an unchanged socket cannot stand in for a new physical limit check.
@pytest.mark.parametrize("charging", [False, True], ids=["idle", "daily"])
async def test_retained_ceiling_is_rechecked_without_rewriting(tmp_path, tls, unused_tcp_port, monkeypatch, charging):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        c, g, rig, j = b.controller, b.session.gateway, b.rig, b.session.journal
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex) if charging else None
        await healthy(c)
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_boot BEFORE INSERT ON meta WHEN NEW.key='boot' "
            "BEGIN SELECT RAISE(ABORT,'Boot storage temporarily unavailable'); END"
        )
        try:
            assert (await rig.bench.call(b.charger, "new-boot", "BootNotification", BOOT))[0] == 4
            assert not g.controls.ready and not g.current_boot_seen(g._charger_epoch)
        finally:
            j.db.execute("DROP TRIGGER refuse_boot")
        assert (await rig.bench.call(b.charger, "new-boot", "BootNotification", BOOT))[0] == 3
        assert not g.controls.ready
        mark = len(rig.bench.device_calls)
        status = await healthy(c)
        assert status["station_ceiling_a"] == 16 and g.controls.ready
        assert j.active() == local
        fresh = [frame[2] for frame in rig.bench.device_calls[mark:]]
        assert fresh == ["GetCompositeSchedule"]
        vendor_epoch = g._vendor_epoch
        assert (await rig.bench.call(b.charger, "another-saved-boot", "BootNotification", BOOT))[0] == 3
        assert (await healthy(c))["station_ceiling_a"] == 16 and g._vendor_epoch == vendor_epoch


# Intent: a Boot that loses the owned station profile cannot borrow the previous proof for a cloud Start.
# Why: a real idle measurement by itself does not show that the station would limit the next session.
@pytest.mark.parametrize("new_boot", [False, True], ids=["same-boot", "new-boot"])
async def test_lost_ceiling_refuses_cloud_start(tmp_path, tls, unused_tcp_port, monkeypatch, new_boot):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        g, rig = b.session.gateway, b.rig
        published = await healthy(b.controller)
        assert g.controls.ready
        rig.bench.firmware.profiles.clear()
        if new_boot:
            assert (await rig.bench.call(b.charger, "profile-lost-boot", "BootNotification", BOOT))[0] == 3
        mark = len(rig.bench.device_calls)
        reply = await rig.bench.cloud_call(
            "start-after-profile-loss", "RemoteStartTransaction", {"idTag": "CLOUD_OWNER", "connectorId": 1}
        )
        assert reply[0] == 4 and not g.controls.ready
        assert rig.bench.device_calls[mark:] == [] and b.session.journal.active() is None
        status = refresh_for_read(b.controller, published)
        assert status["state"] == "attention" and status["fault"] == STATION_CEILING_LOST
        assert status["station_ceiling_a"] is None and not status["owner_control_enabled"]


# Intent: the vendor Start verifies the exact owned device profile again on the same Boot.
# Why: the reserved ID can remain installed with a different limit after the ready proof.
@pytest.mark.parametrize("drifted_limit", [12, 24], ids=["below-confirmed-limit", "above-confirmed-limit"])
async def test_changed_present_ceiling_refuses_cloud_start(tmp_path, tls, unused_tcp_port, monkeypatch, drifted_limit):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        g, rig = b.session.gateway, b.rig
        firmware = rig.bench.firmware
        assert (await healthy(b.controller))["station_ceiling_a"] == 16
        assert g.controls.ready and firmware.effective_limit() == 16
        boot = g.boot_observation
        profile = firmware.profiles[HUB_PROFILE_ID]
        profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = drifted_limit
        assert firmware.effective_limit() == drifted_limit and firmware.current == 0

        mark = len(rig.bench.device_calls)
        reply = await rig.bench.cloud_call(
            "start-after-profile-change", "RemoteStartTransaction", {"idTag": "CLOUD_OWNER", "connectorId": 1}
        )

        assert reply[:3] == [4, "start-after-profile-change", "SecurityError"]
        assert g.boot_observation == boot and b.session.journal.active() is None
        assert HUB_PROFILE_ID in firmware.profiles
        assert firmware.effective_limit() == drifted_limit and firmware.current == 0
        assert rig.bench.device_calls[mark:] == []


# Intent: positive current selection cannot recreate a profile that its own fresh audit found absent.
# Why: a cached readiness result from before the loss does not grant a new physical write.
async def test_lost_ceiling_refuses_positive_selection(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        c, rig = b.controller, b.rig
        await healthy(c)
        rig.bench.firmware.profiles.pop(HUB_PROFILE_ID)
        mark = len(rig.bench.device_calls)
        request = uuid.uuid4().hex
        assert c.command(request, "set_current", c.requests.intent().revision, current_a=24)["state"] == "accepted"
        await c.current.settle()
        assert c.requests.receipt(request)["state"] != "completed"
        assert not any(call[2] == "SetChargingProfile" for call in rig.bench.device_calls[mark:])
        assert b.session.book.station_ceiling_lost() and HUB_PROFILE_ID not in rig.bench.firmware.profiles


# Intent: loss of the owned ceiling stops exactly the known charge during normal operation or after restart.
# Why: a real Stop reply does not establish closure, recreate the missing profile, or silently verify Pause.
@pytest.mark.parametrize("boundary", ["live", "reconnect", "restart", "loss_write"])
async def test_lost_ceiling_stops_the_known_open_session(tmp_path, tls, unused_tcp_port, monkeypatch, boundary):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        rig, firmware = b.rig, b.rig.bench.firmware
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await healthy(b.controller))["current_a"] == 16
        intent = b.controller.requests.intent()
        firmware.profiles.pop(HUB_PROFILE_ID)
        assert firmware.current == 32
        mark = len(rig.bench.device_calls)

        # Require a genuine Stop exchange, then deliver the independent device's lifecycle closure separately.
        async def expect_stop(c, charger):
            g, j = c.session.gateway, c.session.journal
            vendor = j.vendor_id(local)
            async with asyncio.timeout(8):
                while not g.controls.stop_acknowledged(local):
                    await c.tick()
                    await asyncio.sleep(0.02)
            assert c.requests.intent() == intent and j.active() == local
            assert j.fault.classification() == STATION_CEILING_LOST
            assert firmware.current == 0 and firmware.effective_limit() == 32
            status = await c.tick()
            assert status["state"] == "attention" and not status.get("local_hold_verified")
            fresh = rig.bench.device_calls[mark:]
            assert [call[3] for call in fresh if call[2] == "RemoteStopTransaction"] == [{"transactionId": local}]
            assert not any(
                call[2] in ("SetChargingProfile", "ClearChargingProfile", "RemoteStartTransaction") for call in fresh
            )
            assert j.get("control")["uncertain"] and not g.controls.ready
            rig.peer.permission_enabled = False
            rig.bench.cp, rig.bench.connection = "B", "Plug"
            stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
            assert (await rig.bench.call(charger, uuid.uuid4().hex, "StopTransaction", stop))[0] == 3
            assert j.active() is None
            await until(lambda: j.next_delivery() is None)
            assert any(call[2] == "StopTransaction" and call[3]["transactionId"] == vendor for call in rig.bench.calls)
            status = await c.tick()
            assert status["fault"] == STATION_CEILING_LOST and not status.get("local_hold_verified")

        if boundary == "loss_write":
            j = b.session.journal
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_absence BEFORE UPDATE ON trial_resources "
                "WHEN NEW.key='profile:7001' AND NEW.state='absent_verified' "
                "BEGIN SELECT RAISE(ABORT,'station loss observation storage refused'); END"
            )
            try:
                await expect_stop(b.controller, b.charger)
                row = j.db.execute("SELECT state FROM trial_resources WHERE key='profile:7001'").fetchone()
                assert row["state"] == "confirmed" and b.session.book.station_ceiling_lost()
            finally:
                j.db.execute("DROP TRIGGER refuse_absence")
            return
        if boundary == "live":
            await expect_stop(b.controller, b.charger)
            return

        session, controller = b.session, b.controller
        if boundary == "restart":
            await b.controller.charging.close()
            await crash(b.session)
            session = managed_session(rig)
            await session.start()
            controller = ManagedController(session)
        else:
            await b.charger.close()
            await until(lambda: session.gateway.charger.sender is None)
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "profile-lost-during-charge", "BootNotification", BOOT))[0] == 3
                await expect_stop(controller, charger)
        finally:
            if boundary == "restart":
                await controller.charging.close()
                await session.close()


# Intent: a lost ceiling and an unrelated native ID never justify stopping the journal's known charge.
# Why: the automatic Stop needs the same independently observed physical session as the accepted lifecycle.
async def test_lost_ceiling_does_not_stop_a_mismatched_session(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        j, rig = b.session.journal, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await healthy(b.controller)
        firmware = rig.bench.firmware
        firmware.profiles.pop(HUB_PROFILE_ID)
        firmware.active = local + 1
        try:
            mark = len(rig.bench.device_calls)
            for _ in range(4):
                await b.controller.tick()
            assert j.active() == local and firmware.active == local + 1
            assert not any(call[2] == "RemoteStopTransaction" for call in rig.bench.device_calls[mark:])
        finally:
            firmware.active = local


# Intent: a poll waiting for a vendor profile receipt still needs a composite for a Boot received during that wait.
# Why: a fresh native inventory alone does not give the new Boot a confirmed ceiling in status.
async def test_boot_during_vendor_profile_wait_is_verified_before_status(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        c, g, rig = b.controller, b.session.gateway, b.rig
        send, held, entered = delay_reply(monkeypatch, rig, b.charger, "SetChargingProfile")
        profile = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 16, 1001, stack_level=0)
        vendor = asyncio.create_task(rig.bench.cloud_call("waiting-profile", "SetChargingProfile", profile))
        await asyncio.wait_for(entered.wait(), 3)
        poll = asyncio.create_task(c.tick())
        await asyncio.sleep(0)
        assert not poll.done() and b.session._resource_mutation_inflight()
        assert (await rig.bench.call(b.charger, "boot-during-profile", "BootNotification", BOOT))[0] == 3
        mark = len(rig.bench.device_calls)
        assert not g.controls.ready
        await send(held[0])
        assert (await vendor)[2] == {"status": "Accepted"}
        status = await poll
        assert status["state"] == "healthy" and g.controls.ready, status
        assert [frame[2] for frame in rig.bench.device_calls[mark:]] == ["GetCompositeSchedule"]


# Intent: an unsaved Boot revokes old proof even when the first database read fails, then prompts a new Boot.
# Why: the charger need not retry a rejected Boot on its own to recover on this same socket.
async def test_failed_boot_read_requests_another_boot(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        g, rig, j = b.session.gateway, b.rig, b.session.journal
        await healthy(b.controller)
        epoch = g._charger_epoch
        b.session.runtime.settings = replace(b.session.runtime.settings, boot_probe_after=0.05)
        rig.bench.boot_on_probe = True

        # Refuse the real first journal read during this Boot and restore storage after its failure reply.
        def refuse_read(action, first, second, database, trigger):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and first == "meta" else sqlite3.SQLITE_OK

        j.db.set_authorizer(refuse_read)
        try:
            assert (await rig.bench.call(b.charger, "temporary-boot-read", "BootNotification", BOOT))[0] == 4
            assert not g.current_boot_seen(epoch)
        finally:
            j.db.set_authorizer(None)
        assert not g.controls.ready
        mark = len(rig.bench.device_calls)
        assert (await healthy(b.controller))["station_ceiling_a"] == 16
        assert g._charger_epoch == epoch and g.controls.ready
        assert [frame[2] for frame in rig.bench.device_calls[mark:]] == ["TriggerMessage", "GetCompositeSchedule"]


# Intent: initial introduction waits for a saved Boot when a newer Boot is refused during its native audit.
# Why: losing proof temporarily is distinct from a charger with mismatched commissioning metadata.
async def test_failed_boot_during_introduction_waits_for_new_proof(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16, native_peer=InterruptedNativePeer) as b:
        c, g, rig, j = b.controller, b.session.gateway, b.rig, b.session.journal
        await b.charger.close()
        await until(lambda: g.charger.sender is None)
        rig.peer.unavailable = True
        rig.bench.boot_on_probe = True
        b.session.runtime.settings = replace(b.session.runtime.settings, boot_probe_after=0.08)
        async with rig.charger(b.session) as charger:
            assert (await rig.bench.call(charger, "initial-boot", "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(rig.peer.failed_reads.get(), 3)
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_boot BEFORE INSERT ON meta WHEN NEW.key='boot' "
                "BEGIN SELECT RAISE(ABORT,'Boot storage temporarily unavailable'); END"
            )
            try:
                assert (await rig.bench.call(charger, "refused-newer-boot", "BootNotification", BOOT))[0] == 4
                assert not g.current_boot_seen(g._charger_epoch)
            finally:
                j.db.execute("DROP TRIGGER refuse_boot")
                rig.peer.unavailable = False
            assert (await healthy(c))["station_ceiling_a"] == 16
            assert g.controls.ready and not j.fault.latched()


# Intent: a complete new composite above the selected limit triggers a protective hold even while idle.
# Why: the real configured profile and zero current at idle cannot overrule an unsafe effective schedule.
@pytest.mark.parametrize("first_observer", ["poll", "cloud"])
async def test_excessive_composite_after_boot_holds_zero(tmp_path, tls, unused_tcp_port, monkeypatch, first_observer):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        c, rig = b.controller, b.rig
        firmware = rig.bench.firmware
        answer = firmware.answer

        # Override just the independently reported effective schedule until the actual profile reaches zero.
        def excessive_offer(action, payload):
            result = answer(action, payload)
            if action == "GetCompositeSchedule" and firmware.effective_limit() != 0:
                result["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 32
            return result

        monkeypatch.setattr(firmware, "answer", excessive_offer)
        assert (await rig.bench.call(b.charger, "composite-excess-boot", "BootNotification", BOOT))[0] == 3
        if first_observer == "cloud":
            reply = await rig.bench.cloud_call(
                "cloud-start-with-excessive-offer", "RemoteStartTransaction", {"idTag": "CLOUD_OWNER", "connectorId": 1}
            )
            assert reply[0] == 4 and b.session.journal.fault.classification() == "station_composite_current_exceeded"
        status = await c.tick()
        assert status["fault"] == "station_composite_current_exceeded"
        assert firmware.effective_limit() == 0 and b.session.journal.active() is None


# Intent: a real measured excess after Boot keeps the selected-current fault's existing recoverable classification.
# Why: the read-only profile verification must preserve the result of the complete native measurement audit.
async def test_meter_excess_after_boot_keeps_measured_fault(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16) as b:
        c, rig, j = b.controller, b.rig, b.session.journal
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await healthy(c))["current_a"] == 16
        actual = rig.peer.native

        # Change just the independent current meter while the real known daily session still draws power.
        def excessive_meter(key):
            result = actual(key)
            if key == "METER" and rig.bench.firmware.current:
                result[0]["meterData"]["cur"] = [166, 0, 0]
            return result

        monkeypatch.setattr(rig.peer, "native", excessive_meter)
        assert (await rig.bench.call(b.charger, "meter-excess-boot", "BootNotification", BOOT))[0] == 3
        for _ in range(3):
            await c.tick()
        assert j.fault.classification() == MEASURED_FAULT and j.active() == local
        assert not rig.bench.remote_stop.is_set()
