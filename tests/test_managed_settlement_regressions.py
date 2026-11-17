"""Exact terminal operations converge through real TLS/native peers without another owner command."""

import asyncio
import copy
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_release_recovery import ACK, reconcile_owner_release
from ocpp_hub_core.prototype.managed_settlement import current_candidate, internal_pause_matches, require_record
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.native_schedule_support import SelectiveConfigPeer
from tests.test_managed_current import adjustable
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_test_charge import ChargeRig, owner_start, positive_flow, ready
from tests.test_native_bounded_recovery import crash
from tests.test_ordinary_recovery import close_session
from tests.test_pause_stop_recovery import genuine_stop
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import fail_boundary, failing_cleanup

pytestmark = pytest.mark.asyncio


# Intent: await only normal production supervision, never create another Pause or complete a saved result in test code.
# Why: these regressions concern missing convergence after genuine independent evidence has become sufficient.
async def settled_without_another_command(c):
    status = None
    for _ in range(10):
        status = await c.tick()
        if status["state"] == "healthy":
            return status
        await asyncio.sleep(0)
    assert status["state"] == "healthy", (status, c.session.journal.get("control"))


# Intent: both normal and timed owner-local Starts settle under the same connection's real fault/Pause/Stop proof.
# Why: the pre-fault introduction revision must not make metadata settlement wait on the reconciliation it blocks.
@pytest.mark.parametrize("action", ["start", "start_test"])
@pytest.mark.parametrize("rollback", [False, True])
async def test_same_socket_fault_retires_closed_local_start(
    tmp_path, tls, unused_tcp_port, monkeypatch, action, rollback
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60, startup=60) as b:
        c, j, g, peer = b.controller, b.session.journal, b.session.gateway, b.rig.peer
        identifier = uuid.uuid4().hex
        c.command(identifier, action, c.requests.intent().revision)
        await c.charging.settle()
        local = await positive_flow(b)
        assert (await healthy(c))["current_a"] == 6
        historical = copy.deepcopy(c.requests.receipt(identifier))
        assert historical["state"] == "completed"
        assert c.charging.state()["state"] == ("running" if action == "start" else "charging")
        epoch, boot = g._charger_epoch, g.boot_observation
        actual_native = peer.native

        # The fake physical device retains its genuine identity/history/resources; only its meter exceeds the cap.
        def excessive_meter(key):
            value = actual_native(key)
            if key == "METER" and b.rig.bench.firmware.current > 0:
                value[0]["meterData"].update(cur=[66, 0, 0], power=[1584, 0, 0, 1584])
            return value

        monkeypatch.setattr(peer, "native", excessive_meter)
        for _ in range(3):
            assert (await c.tick())["state"] == "degraded"
        assert j.get("managed_control_fault")["classification"] == "measured_output_limit_exceeded"
        assert c.native.introduced_revision != g.controls._revision
        assert (await c.tick())["state"] == "attention"
        await until(lambda: b.rig.bench.firmware.effective_limit() == b.rig.bench.firmware.current == 0)
        mark = len(b.rig.bench.received_limits)
        pause_id = uuid.uuid4().hex
        c.command(pause_id, "pause", c.requests.intent().revision)
        await genuine_stop(b, local)
        if c.pause_task is not None:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        monkeypatch.setattr(peer, "native", actual_native)
        native = await c.native.read(closed=True)
        assert not native.physical.transaction_open
        assert native.output_limit_a == 0 and all(v == 0 for v in native.currents_a + native.powers_w)
        assert not j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL").fetchone()
        assert not j.db.execute("SELECT 1 FROM outbox").fetchone()
        assert not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed'").fetchone()
        if rollback:
            before = {
                key: copy.deepcopy(j.get(key))
                for key in ("managed_control_fault", ACK, "managed_test_charge", "control")
            }
            introduced_revision, lease = c.native.introduced_revision, c.charging.lease
            j.db.execute(
                "CREATE TRIGGER refuse_terminal_settlement BEFORE INSERT ON meta "
                "WHEN NEW.key='managed_test_charge' AND json_extract(NEW.value,'$.state')='completed' "
                "BEGIN SELECT RAISE(ABORT,'synthetic terminal settlement failure'); END"
            )
            try:
                with pytest.raises(sqlite3.Error):
                    await reconcile_owner_release(c)
            finally:
                j.db.execute("DROP TRIGGER refuse_terminal_settlement")
            assert {key: j.get(key) for key in before} == before
            assert c.native.introduced_revision == introduced_revision and c.charging.lease is lease
            assert c.requests.receipt(identifier) == historical

        for _ in range(10):
            await c.tick()
            assert b.rig.bench.firmware.effective_limit() == 0
            assert all(v == 0 for v in b.rig.bench.received_limits[mark:])
            if j.get("managed_control_fault") is None:
                break
        assert j.get("managed_control_fault") is None
        assert c.charging.state()["state"] == "completed" and c.charging.lease is None
        assert c.requests.receipt(identifier) == historical
        assert g._charger_epoch == epoch and g.boot_observation == boot
        assert c.native.introduced_revision == g.controls._revision
        assert (await settled_without_another_command(c))["local_hold_verified"]
        assert c.requests.receipt(pause_id)["state"] == "completed"
        assert len(starts(b.rig)) == 1
        assert len([f for f in b.rig.bench.device_calls if f[2] == "RemoteStopTransaction"]) == 1

        # Positive authority remains a separate new owner action after the exact fault has been retired at zero.
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await healthy(c))["station_ceiling_a"] == 6
        assert len(starts(b.rig)) == 1


# Intent: a replaced timed/cloud test retires only after actual closed lifecycle or independently unused cloud arm.
# Why: an interrupted test record cannot disable the two later genuine ordinary sessions' outage recovery.
@pytest.mark.parametrize("source", ["local", "cloud", "cloud-arm"])
async def test_interrupted_test_retires_after_real_closed_zero(tmp_path, tls, unused_tcp_port, monkeypatch, source):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60, startup=60) as b:
        identifier = uuid.uuid4().hex
        b.controller.command(identifier, "start_test" if source == "local" else "arm_cloud_test", 0)
        if source == "local":
            await b.controller.charging.settle()
        elif source == "cloud":
            await owner_start(b)
        local = None
        if source != "cloud-arm":
            local = await positive_flow(b, source)
            assert (await healthy(b.controller))["power_w"] == 1440
        historical = copy.deepcopy(b.controller.requests.receipt(identifier))
        assert historical["state"] == "completed"
        expected_starts = 0 if source == "cloud-arm" else 1
        assert len(starts(b.rig)) == expected_starts
        await b.controller.charging.close()
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            assert recovered.charging.state()["state"] == "interrupted"
            assert recovered.requests.intent().mode == "paused"
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, "closed-test-replacement-" + source)
                fresh = ChargeRig(b.rig, replacement, recovered, charger)
                if local is not None:
                    # This is a real natural device Stop and cloud ACK, not a synthetic controller completion.
                    await close_session(b.rig, replacement, charger, local)
                await settled_without_another_command(recovered)
                assert recovered.charging.state()["state"] == "completed"
                assert recovered.charging.lease is None
                assert recovered.requests.receipt(identifier) == historical
                assert len(starts(b.rig)) == expected_starts
                assert b.rig.bench.firmware.effective_limit() == 0
                await two_later_sessions(fresh, selected=6, original_starts=expected_starts)
                assert recovered.requests.receipt(identifier) == historical
        finally:
            await recovered.charging.close()
            if recovered.pause_task is not None:
                await asyncio.gather(recovered.pause_task, return_exceptions=True)
            await replacement.close()


# Intent: replacement settles each exact action without turning closure into a missing power-test success.
# Why: local Start, timed power verification and cloud arming have different durable confirmation contracts.
@pytest.mark.parametrize("action", ["start", "start_test", "arm_cloud_test"])
async def test_interrupted_request_retains_its_action_outcome(tmp_path, tls, unused_tcp_port, monkeypatch, action):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60, startup=60) as b:
        c, j = b.controller, b.session.journal
        identifier, local = uuid.uuid4().hex, None
        if action == "arm_cloud_test":
            # Fail the actual arm receipt UPDATE after its independently committed permission/request metadata.
            j.db.execute(
                "CREATE TRIGGER refuse_arm_confirmation BEFORE UPDATE ON managed_requests "
                f"WHEN NEW.id='{identifier}' AND NEW.state='completed' "
                "BEGIN SELECT RAISE(ABORT,'synthetic arm confirmation failure'); END"
            )
            try:
                with pytest.raises(sqlite3.Error):
                    c.command(identifier, action, c.requests.intent().revision)
            finally:
                j.db.execute("DROP TRIGGER refuse_arm_confirmation")
            assert c.charging.state()["state"] == "armed"
        else:
            c.command(identifier, action, c.requests.intent().revision)
            await c.charging.settle()
            local = await positive_flow(b)
            # The actual device has flowed, but no supervisor observation has published that proof.
            assert c.charging.state()["state"] == "awaiting_power"
            assert not c.charging.state().get("start_source_verified")
            assert "flow_started_at" not in c.charging.state()
        assert c.requests.receipt(identifier)["state"] == "accepted"
        expected_starts = int(local is not None)
        assert len(starts(b.rig)) == expected_starts
        await c.charging.close()
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            assert recovered.requests.receipt(identifier)["state"] == "uncertain"
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, uuid.uuid4().hex)
                fresh = ChargeRig(b.rig, replacement, recovered, charger)
                recovered.command(uuid.uuid4().hex, "pause", recovered.requests.intent().revision)
                if local is not None:
                    await close_session(b.rig, replacement, charger, local)
                assert (await settled_without_another_command(recovered))["local_hold_verified"]
                terminal = copy.deepcopy(recovered.requests.receipt(identifier))
                if action == "start_test":
                    assert terminal["state"] == "failed"
                    assert terminal["result"] == {"classification": "test_interrupted_at_verified_zero"}
                else:
                    assert terminal["state"] == "completed"
                    assert terminal["result"] == (
                        {"local_session_verified": True, "charging": False, "timed": False}
                        if action == "start"
                        else {"armed": True, "charging_started": False}
                    )
                assert not terminal["result"].get("positive_power_verified")
                assert recovered.charging.state()["state"] == "completed" and recovered.charging.lease is None
                assert len(starts(b.rig)) == expected_starts
                await two_later_sessions(fresh, selected=6, original_starts=expected_starts)
                assert recovered.requests.receipt(identifier) == terminal
        finally:
            await recovered.charging.close()
            if recovered.pause_task is not None:
                await asyncio.gather(recovered.pause_task, return_exceptions=True)
            await replacement.close()


# Intent: the exact failed selection retires after fresh verified Pause without claiming its unknown target succeeded.
# Why: physical zero may heal while stale metadata would still disable ordinary or later fault recovery.
async def test_failed_current_record_retires_after_verified_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        fail_boundary(j, "current", "statement")
        reached, _ = failing_cleanup(
            j,
            "managed_current_failure",
            minimum_attempts=1,
            on_first_attempt=lambda: setattr(b.rig.peer, "unavailable", True),
        )
        identifier = uuid.uuid4().hex
        c.command(identifier, "set_current", c.requests.intent().revision, current_a=16)
        await asyncio.wait_for(reached.wait(), 3)
        await c.current.settle()
        if c.pause_task is not None:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        historical = copy.deepcopy(c.requests.receipt(identifier))
        assert historical["state"] == "uncertain" and c.current.failed_idle_hold()
        mark = len(b.rig.bench.device_calls)
        b.rig.peer.unavailable = False
        try:
            assert (await settled_without_another_command(c))["local_hold_verified"]
            assert j.get("managed_current_change")["id"] == identifier
            assert j.get("managed_current_change")["state"] == "cancelled"
            assert c.requests.receipt(identifier) == historical
            assert c.current.selected() == 6 and b.rig.bench.firmware.effective_limit() == 0
            assert not starts(b.rig)
            assert all(
                f[2] not in ("RemoteStartTransaction", "RemoteStopTransaction") for f in b.rig.bench.device_calls[mark:]
            )
        finally:
            j.db.execute("DROP TRIGGER review_worker_boundary")
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")


# Intent: an exact internal Pause claim converges when HTTP heals even if its one immediate follow-up read failed.
# Why: neither a selection before its first source read nor a failed timed Start creates an owner Pause marker.
@pytest.mark.parametrize("case", ["app-current", "paused-current", "timed-start"])
async def test_internal_pause_recovers_after_first_read_failure(tmp_path, tls, unused_tcp_port, monkeypatch, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    factory = adjustable if case != "timed-start" else ready
    async with factory(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        if case == "paused-current":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            assert (await healthy(c))["local_hold_verified"]
        identifier = uuid.uuid4().hex
        profile_mark = len(b.rig.bench.received_limits)
        if case == "timed-start":
            # The device first applies the real initial zero; only subsequent native HTTP requests fail.
            async def drop_after_zero(action, payload):
                if action == "SetChargingProfile":
                    periods = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
                    if periods and all(p["limit"] == 0 for p in periods):
                        b.rig.peer.unavailable = True

            b.rig.bench.before_reply = drop_after_zero
            c.command(identifier, "start_test", c.requests.intent().revision)
            await c.charging.settle()
            assert c.charging.state()["state"] == "stopping"
        else:
            # Last health/native evidence is genuinely fresh, so admission succeeds before the first failing read.
            b.rig.peer.unavailable = True
            c.command(identifier, "set_current", c.requests.intent().revision, current_a=16)
            await c.current.settle()
            assert j.get("managed_current_change")["state"] == "uncertain"
        if c.pause_task is not None:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        assert b.rig.peer.failed_reads.qsize() >= 2
        assert c.requests.intent().mode == "paused" and j.get("control")["uncertain"]
        ticket, intent = j.get("control")["ticket"], c.requests.intent()
        b.rig.bench.before_reply = None
        b.rig.peer.unavailable = False
        status = await settled_without_another_command(c)
        assert status["local_hold_verified"] and j.get("control")["uncertain"] is False
        assert j.get("control")["ticket"] == ticket and c.requests.intent() == intent
        assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
        assert all(v == 0 for v in b.rig.bench.received_limits[profile_mark:])
        assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)


# Intent: an explicit device Rejected result can settle only its exact existing Start ticket after fresh zero.
# Why: a rejected Start is known not to have created a session, but a failed follow-up read must not strand it.
async def test_explicit_rejected_start_settles_its_current_ticket(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, startup=60) as b:
        c, j = b.controller, b.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        await healthy(c)
        answer = b.rig.bench.firmware.answer

        # Refuse at the independent firmware before applying Start; all profile commands retain real behavior.
        def rejected(action, payload):
            return {"status": "Rejected"} if action == "RemoteStartTransaction" else answer(action, payload)

        # The actual response is delivered, then all immediate native follow-up requests genuinely return HTTP503.
        async def fail_follow_up(action, payload):
            if action == "RemoteStartTransaction":
                b.rig.peer.unavailable = True

        monkeypatch.setattr(b.rig.bench.firmware, "answer", rejected)
        b.rig.bench.before_reply = fail_follow_up
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        await c.charging.settle()
        if c.pause_task is not None:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        assert j.get("local_start_receipt")["stage"] == "rejected"
        assert j.get("local_start_receipt")["local"] is None
        assert j.get("control")["start_pending"] is False and j.get("control")["uncertain"]
        ticket, intent = j.get("control")["ticket"], c.requests.intent()
        mark = len(b.rig.bench.device_calls)
        b.rig.bench.before_reply = None
        b.rig.peer.unavailable = False
        assert (await settled_without_another_command(c))["local_hold_verified"]
        assert j.get("control")["ticket"] == ticket and c.requests.intent() == intent
        assert j.get("control")["uncertain"] is False
        assert j.get("local_start_receipt")["settled_ticket"] == ticket
        assert len(starts(b.rig)) == 1 and j.active() is None
        assert all(
            f[2] not in ("RemoteStartTransaction", "RemoteStopTransaction") for f in b.rig.bench.device_calls[mark:]
        )


# Intent: an exact terminal candidate is revoked if its durable target or request changes during real native I/O.
# Why: metadata cleanup cannot apply a proof to a request whose separate argument binding has been replaced.
@pytest.mark.parametrize("change", ["target", "request", "new_pause"])
async def test_terminal_current_snapshot_is_revoked_during_native_read(
    tmp_path, tls, unused_tcp_port, monkeypatch, change
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", SelectiveConfigPeer)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        peer.fail_at = peer.config_requests + 1
        c.command(identifier, "set_current", revision, current_a=16)
        await c.current.settle()
        if c.pause_task is not None:
            await c.pause_task
        candidate = current_candidate(c)
        assert candidate is not None
        original = copy.deepcopy(j.get("managed_current_change"))
        historical = copy.deepcopy(c.requests.receipt(identifier))
        peer.gate_at = peer.config_requests + 2
        read = asyncio.create_task(c.native.read(closed=True))
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            if change == "target":
                j.db.execute("UPDATE managed_current_requests SET current_a=32 WHERE id=?", (identifier,))
            elif change == "request":
                j.db.execute("UPDATE managed_requests SET revision=revision+1 WHERE id=?", (identifier,))
            else:
                old_ticket = j.get("managed_internal_pause")["ticket"]
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                assert j.get("control")["ticket"] != old_ticket and not internal_pause_matches(c)
            peer.release.set()
            outcome = (await asyncio.gather(read, return_exceptions=True))[0]
            if change != "new_pause":
                assert not isinstance(outcome, BaseException)
                with pytest.raises(RecoveryRequiredError):
                    require_record(c, candidate)
                assert j.get("managed_current_change") == original
            assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
        finally:
            peer.release.set()
            await asyncio.gather(read, return_exceptions=True)
            if change == "target":
                j.db.execute("UPDATE managed_current_requests SET current_a=16 WHERE id=?", (identifier,))
            elif change == "request":
                j.db.execute("UPDATE managed_requests SET revision=? WHERE id=?", (revision, identifier))
        assert (await healthy(c))["local_hold_verified"]
        assert j.get("managed_current_change")["state"] == "cancelled"
        assert c.requests.receipt(identifier) == historical
