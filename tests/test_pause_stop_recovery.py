"""Focused real-peer witnesses for Return gate availability and faulted Pause Stop progress."""

import asyncio
import copy
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.gateway import decode

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_release_recovery import release_bench
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import positive_flow
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Close only after the independent peer really receives the correct known-session Stop.
async def genuine_stop(b, local):
    await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 2)
    response = await b.rig.bench.call(
        b.charger,
        uuid.uuid4().hex,
        "StopTransaction",
        {
            "transactionId": local,
            "timestamp": START["timestamp"],
            "meterStop": 1100,
        },
    )
    assert response[0] == 3
    b.rig.peer.permission_enabled = False
    await until(lambda: b.session.journal.active() is None and b.session.journal.next_delivery() is None)


# Intent: an actual identical Boot retires an earlier Stop ACK, including a reply arriving after that Boot.
# Why: a late accepted reply must retain its original generation instead of suppressing the current Pause's Stop.
@pytest.mark.parametrize("timing", ["during_reply", "after_reply"])
async def test_boot_cannot_reuse_earlier_stop_ack(tmp_path, tls, unused_tcp_port, monkeypatch, timing):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, rig, g = b.c, b.rig, b.session.gateway
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await mp.healthy(c)
        await legacy_fault(b, "NativeTransportUnavailableError")
        entered, release = asyncio.Event(), asyncio.Event()

        # The charger applies the first genuine Stop before delaying only its real accepted response.
        async def hold_stop(action, payload):
            if action == "RemoteStopTransaction" and not entered.is_set():
                entered.set()
                await release.wait()

        rig.bench.before_reply = hold_stop
        request = uuid.uuid4().hex
        c.command(request, "pause", c.requests.intent().revision)
        boot = g.boot_observation
        try:
            await asyncio.wait_for(entered.wait(), 3)
            if timing == "after_reply":
                release.set()
                await c.pause_task
                assert g.controls.stop_acknowledged(local)
            boot_call = asyncio.create_task(rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))
            await until(lambda: g.boot_observation != boot)
            release.set()
            assert (await boot_call)[0] == 3
            await c.pause_task
            assert not g.controls.stop_acknowledged(local)
            await c.tick()
            stops = [f for f in rig.bench.device_calls if f[2] == "RemoteStopTransaction"]
            assert len(stops) == 2 and all(f[3] == {"transactionId": local} for f in stops)
            assert g.controls.stop_acknowledged(local) and c.requests.receipt(request)["state"] == "accepted"
            await genuine_stop(b, local)
            assert (await mp.healthy(c))["local_hold_verified"] and not mp.starts(rig)
        finally:
            release.set()
            rig.bench.before_reply = None


# Intent: native availability healing should permit a previously accepted known-session Pause to complete Stop.
# Why: a faulted supervisor currently follows a different branch from ordinary maintain_pause.
@pytest.mark.parametrize("pause503", [False, True])
@pytest.mark.parametrize("stale_revision", [False, True])
async def test_faulted_pause_availability_witness(
    tmp_path, tls, unused_tcp_port, monkeypatch, pause503, stale_revision
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.c, b.session.journal, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await mp.healthy(c))["current_a"] == 6
        await legacy_fault(b, "NativeTransportUnavailableError")
        assert j.active() == local and rig.bench.firmware.effective_limit() == 0
        assert not rig.bench.remote_stop.is_set()
        request = uuid.uuid4().hex
        rig.peer.unavailable = pause503
        c.command(request, "pause", 0 if stale_revision else c.requests.intent().revision)
        if c.pause_task is not None:
            await asyncio.wait_for(c.pause_task, 3)
        before_heal = len([x for x in rig.bench.device_calls if x[2] == "RemoteStopTransaction"])
        rig.peer.unavailable = False
        assert [(await c.tick())["state"] for _ in range(3)] == ["attention"] * 3
        stops = [x for x in rig.bench.device_calls if x[2] == "RemoteStopTransaction"]
        assert before_heal == (0 if pause503 else 1)
        assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
        assert j.active() == local and rig.bench.firmware.effective_limit() == 0
        assert c.requests.receipt(request)["state"] == "accepted"
        await genuine_stop(b, local)
        if c.pause_task is not None:
            await c.pause_task
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(request)["state"] == "completed"
        assert c.requests.intent().mode == "paused"


# Intent: persistent validated absolute measurement excess must not starve the accepted owner's known-session Stop.
# Why: the existing below-absolute selected-overdraw test does not cross the native reader's own error boundary.
@pytest.mark.parametrize("selected,measured", [(6, 8), (32, 32.6)], ids=["selected-bound-control", "absolute-bound"])
async def test_faulted_pause_persistent_meter_witness(tmp_path, tls, unused_tcp_port, monkeypatch, selected, measured):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True) as b:
        c, j, rig, peer = b.controller, b.session.journal, b.rig, b.rig.peer
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await mp.healthy(c))["current_a"] == selected
        actual_native = peer.native
        samples = []

        # The genuine device/resource/session identity is unchanged; only the independent reported meter persists.
        def persistent_native(key):
            value = actual_native(key)
            if key == "METER" and rig.bench.firmware.active == local:
                power = int(round(measured * 240))
                value[0]["meterData"].update(cur=[int(round(measured * 10)), 0, 0], power=[power, 0, 0, power])
                samples.append(copy.deepcopy(value))
            return value

        monkeypatch.setattr(peer, "native", persistent_native)
        for _ in range(3):
            assert (await c.tick())["state"] == "degraded"
        assert (await c.tick())["state"] == "attention"
        await until(lambda: rig.bench.firmware.effective_limit() == rig.bench.firmware.current == 0)
        assert j.get("managed_control_fault")["classification"] == "measured_output_limit_exceeded"
        assert not rig.bench.remote_stop.is_set()
        request = uuid.uuid4().hex
        c.command(request, "pause", c.requests.intent().revision)
        if c.pause_task is not None:
            await asyncio.wait_for(c.pause_task, 3)
        states = [(await c.tick())["state"] for _ in range(2)]
        stops = [x for x in rig.bench.device_calls if x[2] == "RemoteStopTransaction"]
        assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
        assert j.active() == local and c.requests.receipt(request)["state"] == "accepted"
        assert rig.bench.firmware.current == rig.bench.firmware.effective_limit() == 0
        assert samples and states == ["attention", "attention"]
        monkeypatch.setattr(peer, "native", actual_native)
        await genuine_stop(b, local)
        if c.pause_task is not None:
            await c.pause_task
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(request)["state"] == "completed"


# Intent: one clean owner Pause settles its exact failed Stop claim after independent genuine closed zero.
# Why: replacing the original Pause ticket must not strand uncertainty when its ordinary session really closes.
@pytest.mark.parametrize("failure", ["rejected", "unanswered"])
@pytest.mark.parametrize("return_before_tick", [False, True])
async def test_clean_pause_settles_failed_stop_after_genuine_closure(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure, return_before_tick
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig, controls = b.c, b.session.journal, b.rig, b.session.gateway.controls
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await mp.healthy(c)
        assert j.get("trial_phase") == "active" and not j.get("managed_control_fault")
        assert not j.get("local_start_receipt") and not mp.starts(rig)
        answer, send, failed = rig.bench.firmware.answer, b.charger.send, []

        # Reject at the independent device before applying Stop; the station-zero write is answered normally.
        def device_answer(action, payload):
            if action == "RemoteStopTransaction" and failure == "rejected":
                failed.append(payload)
                return {"status": "Rejected"}
            return answer(action, payload)

        # Lose only the genuine physical Stop response, leaving native reads and later lifecycle frames intact.
        async def device_send(raw):
            frame = decode(raw)
            call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
            if failure == "unanswered" and frame[0] == 3 and call is not None and call[2] == "RemoteStopTransaction":
                failed.append(call[3])
                return
            await send(raw)

        monkeypatch.setattr(rig.bench.firmware, "answer", device_answer)
        monkeypatch.setattr(b.charger, "send", device_send)
        request = uuid.uuid4().hex
        c.command(request, "pause", c.requests.intent().revision)
        original_pause = copy.deepcopy(j.get("managed_owner_pause"))
        revision = c.requests.intent().revision
        assert c.pause_task is not None
        await asyncio.wait_for(c.pause_task, 8)
        assert failed == [{"transactionId": local}]
        control = j.get("control")
        assert control["ticket"] != original_pause["ticket"] and control["uncertain"]
        assert j.get("managed_owner_pause") == original_pause
        assert j.get("managed_owner_stop") == {"ticket": control["ticket"], "revision": revision}
        assert j.active() == local and not controls.stop_acknowledged(local)
        assert rig.bench.firmware.current == rig.bench.firmware.effective_limit() == 0
        assert not j.get("managed_control_fault")

        # A real charger lifecycle closes the session independently of whether its Stop command was accepted.
        await genuine_stop(b, local)
        returning = uuid.uuid4().hex
        if return_before_tick:
            c.command(returning, "app_control", revision)
        for _ in range(3):
            status = await c.tick()
        assert c.requests.receipt(request)["state"] == "completed"
        assert not j.get("control")["uncertain"]
        assert status["local_healthy"] and status["normal_start_ready"]
        if return_before_tick:
            assert c.requests.receipt(returning)["state"] == "completed"
            assert c.requests.intent().mode == "app" and c.requests.intent().revision == revision + 1
            assert rig.bench.firmware.effective_limit() == 6
        else:
            assert status["local_hold_verified"] and c.requests.intent().mode == "paused"
            assert c.requests.intent().revision == revision and rig.bench.firmware.effective_limit() == 0
        assert j.get("managed_owner_pause") == original_pause
        assert j.get("trial_phase") == "active" and not j.get("managed_control_fault")
        assert not mp.starts(rig)
        stops = [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"]
        assert len(stops) == 1 and stops[0][3] == {"transactionId": local}


# Intent: refusing the exact owner-Stop binding rolls back its claim and later genuine closure settles the same Pause.
# Why: a separately committed replacement ticket would strand uncertainty once closed zero skips another Stop.
@pytest.mark.parametrize("failure", ["statement", "commit"])
async def test_pause_stop_binding_storage_refusal_rolls_back_claim_and_recovers(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig, controls = b.c, b.session.journal, b.rig, b.session.gateway.controls
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await mp.healthy(c)
        request = uuid.uuid4().hex
        c.command(request, "pause", c.requests.intent().revision)
        original_pause = copy.deepcopy(j.get("managed_owner_pause"))
        original_control = copy.deepcopy(j.get("control"))
        original_ticket = original_control["ticket"]
        original_stop_ticket = controls._stop_ticket
        original_stop = copy.deepcopy(j.get("managed_owner_stop"))
        revision = c.requests.intent().revision
        attempts = []

        # Observe the attempted replacement binding inside SQLite before the real statement is refused.
        def binding_attempt(ticket):
            attempts.append(ticket)
            return 0

        j.db.create_function("review_pause_binding_attempt", 1, binding_attempt)
        if failure == "statement":
            effect = "SELECT RAISE(ABORT,'independent owner Stop binding storage refusal');"
        else:
            # The new claim and binding must also roll back when a deferred reference fails their actual COMMIT.
            j.db.execute("CREATE TABLE review_pause_binding_parent(id INTEGER PRIMARY KEY)")
            j.db.execute(
                "CREATE TABLE review_pause_binding_child("
                "id REFERENCES review_pause_binding_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
            effect = "INSERT INTO review_pause_binding_child VALUES(123);"
        j.db.execute(
            "CREATE TEMP TRIGGER review_pause_binding_refusal BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_owner_stop' BEGIN "
            "SELECT review_pause_binding_attempt(json_extract(NEW.value,'$.ticket')); "
            + effect
            + " END"
        )
        try:
            assert c.pause_task is not None
            with pytest.raises(sqlite3.IntegrityError):
                await asyncio.wait_for(c.pause_task, 8)
        finally:
            j.db.execute("DROP TRIGGER review_pause_binding_refusal")
        assert len(attempts) == 1 and attempts[0] != original_ticket
        assert not j.db.in_transaction
        assert j.get("control") == original_control
        assert controls._stop_ticket == original_stop_ticket
        assert j.get("managed_owner_pause") == original_pause
        assert j.get("managed_owner_stop") == original_stop
        assert c.requests.intent().mode == "paused" and c.requests.intent().revision == revision
        assert j.active() == local and j.get("control")["uncertain"]
        assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
        assert rig.bench.firmware.current == rig.bench.firmware.effective_limit() == 0

        # The car finishes independently; no remote Stop was launched, so there is no command event to await.
        reply = await rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        assert reply[0] == 3
        rig.peer.permission_enabled = False
        await until(lambda: j.active() is None and j.next_delivery() is None)
        status = await mp.healthy(c)
        assert status["local_hold_verified"] and status["local_healthy"] and status["normal_start_ready"]
        assert c.requests.receipt(request)["state"] == "completed" and not j.get("control")["uncertain"]
        assert c.requests.intent().mode == "paused" and c.requests.intent().revision == revision
        assert not j.get("managed_control_fault") and j.get("trial_phase") == "active"
        assert not mp.starts(rig) and not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
        assert rig.bench.firmware.effective_limit() == 0
