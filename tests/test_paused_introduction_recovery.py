"""A partial paused introduction resumes only from fresh genuine closure and confirmed current-Boot zero."""

import asyncio
import copy
import uuid
from contextlib import AsyncExitStack
from datetime import timedelta

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.handover_helpers import crash
from tests.recovery_barriers import fresh_daily_values, returned_to_app
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Retire each actual service with its own socket still available; tested process loss explicitly uses crash instead.
async def retire(session, controller):
    await controller.tests.close()
    if controller.pause_task is not None:
        await asyncio.gather(controller.pause_task, return_exceptions=True)
    await session.close()


# Start a new real journal owner and intercept only a named independent peer response before Boot is reported.
async def reconnect(stack, rig, monkeypatch, fault):
    session = mp.managed_session(rig)
    await session.start()
    controller = ManagedController(session)
    sock = await stack.enter_async_context(rig.charger(session))
    stack.push_async_callback(retire, session, controller)
    send = sock.send
    gate = {"seen": [], "entered": asyncio.Event()}

    # The actual firmware has already applied each request; suppress only one response on its actual TLS connection.
    async def reply(raw):
        frame = decode(raw)
        call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
        zero = (
            call is not None
            and call[2] == "SetChargingProfile"
            and call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
        )
        target = call is not None and (
            (fault.startswith("stop-") and call[2] == "RemoteStopTransaction") or fault == "zero-timeout" and zero
        )
        if frame[0] == 3 and target and not gate["seen"]:
            gate["seen"].append(copy.deepcopy(call))
            gate["entered"].set()
            if fault == "stop-error":
                await send(build_call_error(frame[1], "InternalError", "independent restrictive reply refusal"))
            return
        await send(raw)

    monkeypatch.setattr(sock, "send", reply)
    assert (await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
    await asyncio.wait_for(gate["entered"].wait(), 5)
    return ChargeRig(rig, session, controller, sock), gate


# Intent: two successive failed paused introductions recover on the same sockets, then two later ordinary sessions work.
# Why: markers alone, a lost Stop ACK or an old zero must never strand the owner or falsely complete the current proof.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("fault", ["stop-timeout", "stop-error", "zero-timeout"])
async def test_partial_paused_introduction_recovers_after_genuine_stop(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, fault
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True) as original:
        rig, b = original.rig, original
        original_config, original_plans = copy.deepcopy(rig.peer.configuration), copy.deepcopy(rig.bench.firmware.plans)
        async with AsyncExitStack() as stack:
            closed = []
            for cycle in range(2):
                beginning, meter = fresh_daily_values(rig, meter_floor=1400 + cycle * 100)
                local = await positive_flow(
                    b,
                    "daily",
                    call_id=uuid.uuid4().hex,
                    meter_start=meter,
                    timestamp=beginning.isoformat().replace("+00:00", "Z"),
                )
                await mp.healthy(b.controller)
                pause_id = uuid.uuid4().hex
                b.controller.command(pause_id, "pause", b.controller.requests.intent().revision)
                await b.controller.pause_task
                assert rig.bench.firmware.effective_limit() == 0 and b.session.journal.active() == local
                await b.controller.tests.close()
                await crash(b.session)
                b, gate = await reconnect(stack, rig, monkeypatch, fault)
                c, g, j = b.controller, b.session.gateway, b.session.journal
                epoch, boot = g._charger_epoch, g.boot_observation
                # Neither a pre-existing physical zero nor an applied-but-unanswered RPC publishes new readiness.
                assert c.native.introduced_revision != g.controls._revision
                assert c.requests.intent().mode == "paused" and rig.bench.firmware.effective_limit() == 0
                assert j.get("managed_control_fault") is None
                if fault == "zero-timeout":
                    assert not g.controls.ready
                # Only the device's actual Stop report closes the exact ledger lifecycle.
                stop = {
                    "transactionId": local,
                    "timestamp": (beginning + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
                    "meterStop": meter + 100,
                }
                assert (await rig.bench.call(b.charger, uuid.uuid4().hex, "StopTransaction", stop))[0] == 3
                rig.peer.permission_enabled = False
                await until(lambda: not g.charger._pending and not g.controls._operations, timeout=7)
                # The service's normal supervisor must reconfirm zero before a not-yet-open cloud channel can drain.
                status = await mp.healthy(c)
                await until(lambda: j.next_delivery() is None)
                assert status["local_hold_verified"] and not status["owner_control_enabled"]
                assert c.native.introduced_epoch == epoch and c.native.introduced_revision == g.controls._revision
                assert g.boot_observation == boot and g._charger_epoch == epoch
                assert j.active() is None and j.get("managed_control_fault") is None
                assert len(gate["seen"]) == 1 and rig.bench.firmware.effective_limit() == 0
                closed.append(local)
                # Queue a real Return behind a new explicit Pause; both exact receipts must finish without a new Boot.
                pause = uuid.uuid4().hex
                c.command(pause, "pause", c.requests.intent().revision)
                await returned_to_app(b, selected)
                assert c.requests.receipt(pause)["state"] == "completed"
                assert c.requests.intent().mode == "app" and rig.bench.firmware.effective_limit() == selected
                assert not mp.starts(rig)
            assert len(set(closed)) == 2
            await two_later_sessions(b, selected=selected, original_starts=0, first_minute=10)
            assert rig.peer.configuration == original_config and rig.bench.firmware.plans == original_plans
            assert len(rig.bench.backend_starts) == 4 and all(entry["closed"] for entry in rig.bench.backend_starts)
            assert not rig.bench.backend_errors and not mp.starts(rig)
