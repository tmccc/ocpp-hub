"""A real measured-output trip must have a narrow explicit-Pause recovery at independently verified zero."""

import asyncio
import copy
import json
import uuid

import pytest
from ocpp_hub_core.control import HUB_PROFILE_ID
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.recovery_barriers import completed_control
from tests.test_managed_api import TOKEN, call
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import positive_flow, ready
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: a pure measured current trip recovers only after new Pause, actual closure/ACK and full native zero.
# Why: the existing over-selected test proves the restrictive latch but never checks its supported recovery suffix.
@pytest.mark.parametrize(
    "selected,measured,trip,power_only,native32",
    [(16, 17, True, False, True), (32, 32.6, True, False, True), (16, 16, False, False, True)]
    + [(selected, selected, True, True, True) for selected in (6, 16, 32)]
    + [(selected, selected, False, True, True) for selected in (6, 16, 32)]
    + [(6, 6.6, True, False, False), (6, 6, True, True, False)],
    ids=["selected-bound", "absolute-bound", "normal-meter-control"]
    + ["power-over-" + str(selected) for selected in (6, 16, 32)]
    + ["power-equal-" + str(selected) for selected in (6, 16, 32)]
    + ["native6-current-over", "native6-power-over"],
)
async def test_measured_current_trip_fresh_pause_recovers_after_genuine_closed_zero(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, selected, measured, trip, power_only, native32
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    factory = (
        commissioned(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, selected, cli_route=True)
        if native32
        else ready(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, cli_route=True)
    )
    async with factory as b:
        c, j, peer, firmware = b.controller, b.session.journal, b.rig.peer, b.rig.bench.firmware
        original_configuration = copy.deepcopy(peer.configuration)
        original_plans = copy.deepcopy(firmware.plans)
        epoch, boot = b.session.gateway._charger_epoch, b.session.gateway.boot_observation
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        published = await mp.healthy(c)
        assert published["current_a"] == selected and j.vendor_id(local) is not None
        assert not mp.starts(b.rig)
        actual_native, excessive_samples = peer.native, []

        # Only the independent meter changes, while HTTP/configuration/history and firmware effects stay real.
        def excessive_meter(key):
            value = actual_native(key)
            if key == "METER" and firmware.current > 0:
                value[0]["meterData"]["cur"] = [int(round(measured * 10)), 0, 0]
                power = (
                    (min(8500, selected * 270 + 100) if native32 else 2000) + int(trip)
                    if power_only
                    else int(round(measured * 240))
                )
                value[0]["meterData"]["power"] = [power, 0, 0, power]
                excessive_samples.append(copy.deepcopy(value))
            return value

        monkeypatch.setattr(peer, "native", excessive_meter)
        for _ in range(3):
            assert (await c.tick())["state"] == ("degraded" if trip else "healthy")
        assert len(excessive_samples) >= 3
        if trip:
            assert (await c.tick())["state"] == "attention"
            await until(lambda: firmware.effective_limit() == firmware.current == 0)
            fault = copy.deepcopy(j.get("managed_control_fault"))
            assert fault and b.session.gateway.controls.ready
            assert (await c.tick())["state"] == "attention" and j.get("managed_control_fault") == fault
        else:
            assert j.get("managed_control_fault") is None and firmware.current == selected
        assert c.current.selected() == selected
        zero_boundary = len(b.rig.bench.received_limits)

        settings = ManagementSettings(unused_tcp_port_factory(), tmp_path / "unused-management-token", TOKEN)
        api = ManagementServer(settings, c, lambda: dict(published, running=True, certificate_remaining_seconds=86400))
        await api.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{settings.port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as socket:
                pause_id, revision = uuid.uuid4().hex, c.requests.intent().revision
                accepted = await call(socket, "pause", id=pause_id, revision=revision)
                assert accepted["ok"] and accepted["result"]["id"] == pause_id
                assert accepted["result"]["state"] == "accepted"
                await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 5)
                assert firmware.current == 0
                stop_id = uuid.uuid4().hex
                stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
                response = await b.rig.bench.call(b.charger, stop_id, "StopTransaction", stop)
                assert response[0] == 3
                peer.permission_enabled = False
                await until(lambda: j.active() is None and j.next_delivery() is None)
                if c.pause_task is not None:
                    await asyncio.wait_for(asyncio.shield(c.pause_task), 5)
                monkeypatch.setattr(peer, "native", actual_native)

                # The independent cloud transcript must contain this actual Stop and its actual emitted ACK.
                vendor_id = j.vendor_id(local)
                wire_stops = [
                    frame
                    for frame in b.rig.bench.calls
                    if frame[0] == 2 and frame[2] == "StopTransaction" and frame[3]["transactionId"] == vendor_id
                ]
                assert len(wire_stops) == 1
                assert wire_stops[0][3] == {**stop, "transactionId": vendor_id}
                assert any(frame[0] == 3 and frame[1] == wire_stops[0][1] for frame in b.rig.bench.sent_vendor_replies)
                backend = next(row for row in b.rig.bench.backend_starts if row["answer"]["transactionId"] == vendor_id)
                assert backend["closed"] and backend["stop"]["body"] == wire_stops[0][3]

                native = await c.native.read(closed=True)
                assert not native.physical.transaction_open and native.output_limit_a == 0
                assert all(value == 0 for value in native.currents_a + native.powers_w)
                owned = b.session.book.owned_present(native.physical)
                station = [row for row in owned if row.identifier == HUB_PROFILE_ID]
                assert len(station) == 1 and station[0].canonical.get("limit") == 0
                receipt = j.db.execute(
                    "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
                ).fetchone()
                assert receipt["state"] == "confirmed" and json.loads(receipt["confirmed"]) == station[0].canonical
                assert b.session.gateway.controls.ready and b.session.gateway.controls.desired() == 0
                assert not j.db.execute("SELECT 1 FROM outbox").fetchone()
                assert not j.db.execute("SELECT 1 FROM delivery_replies").fetchone()
                assert not j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL").fetchone()
                assert not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed'").fetchone()
                assert peer.configuration == original_configuration and firmware.plans == original_plans
                assert b.session.gateway._charger_epoch == epoch and b.session.gateway.boot_observation == boot
                print(json.dumps({"stage": "fresh-pause-real-stop-ack-and-closed-zero", "selected": selected}))

                # Bounded ordinary supervision may reconcile, but no test clears a fault or completes a receipt.
                for _ in range(10):
                    published = await c.tick()
                    if j.get("managed_control_fault") is None and c.requests.receipt(pause_id)["state"] == "completed":
                        break
                    await asyncio.sleep(0)
                assert j.get("managed_control_fault") is None, (
                    "fresh Pause cannot recover a pure measured-current trip after genuine Stop ACK and closed zero",
                    j.get("managed_control_fault"),
                    c.requests.receipt(pause_id),
                )
                await completed_control(b, pause_id, mode="paused", revision=revision + 1, ceiling=0)
                assert firmware.effective_limit() == firmware.current == 0
                assert all(value == 0 for value in b.rig.bench.received_limits[zero_boundary:])
                assert not mp.starts(b.rig) and c.current.selected() == selected

                return_id, revision = uuid.uuid4().hex, c.requests.intent().revision
                accepted = await call(socket, "app_control", id=return_id, revision=revision)
                assert accepted["ok"] and accepted["result"]["id"] == return_id
                await completed_control(b, return_id, mode="app", revision=revision + 1, ceiling=selected)
        finally:
            await api.close()

        await two_later_sessions(b, selected=selected, original_starts=0, first_minute=10)
        assert peer.configuration == original_configuration and firmware.plans == original_plans
        assert len(b.rig.bench.backend_starts) == 3 and all(row["closed"] for row in b.rig.bench.backend_starts)
