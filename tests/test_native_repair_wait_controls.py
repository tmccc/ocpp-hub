"""Ordinary availability and closure waits during legacy recovery remain pending and eventually recover."""

import copy
import uuid

import pytest
from ocpp_hub_core.prototype.managed_release_recovery import ACK, recoverable_fault

from tests import test_native_bounded_session as native_bench
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_release_recovery import explicit_release_pause, release_bench
from tests.test_managed_test_charge import positive_flow
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_ordinary_recovery import SlowOrTruncatedPeer
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: an actual HTTP503 or elapsed stale audit during repair cannot become a permanent strict-native refusal.
# Why: the strict-refusal fix must preserve ordinary availability retries through the same fresh Pause proof.
@pytest.mark.parametrize("mode", ["http503", "stale"])
async def test_legacy_native_repair_availability_wait_remains_recoverable(
    tmp_path, tls, unused_tcp_port, monkeypatch, mode
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", SlowOrTruncatedPeer)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, peer = b.c, b.session.journal, b.rig.peer
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        ack = copy.deepcopy(j.get(ACK))
        if mode == "http503":
            peer.unavailable = True
        else:
            peer.transport_mode = "stale"
        try:
            assert (await c.tick())["state"] == "attention"
            assert j.get("managed_control_fault") == fault and j.get(ACK) == ack
            assert recoverable_fault(c, fault) and c.requests.receipt(pause)["state"] == "accepted"
            assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
        finally:
            peer.unavailable, peer.transport_mode = False, None
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed" and j.get("managed_control_fault") is None


# Intent: genuine open-zero and coherent native closure lag, including small nonzero draw, remain pending at zero.
# Why: operation readiness must not be confused with contradictory identity/configuration/history authority.
async def test_legacy_native_repair_closure_waits_do_not_revoke_availability_proof(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.c, b.session.journal, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await healthy(c))["current_a"] == 6
        open_history = copy.deepcopy(rig.bench.history)
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        ack = copy.deepcopy(j.get(ACK))
        assert j.active() == local and rig.bench.firmware.current == 0
        for _ in range(2):
            assert (await c.tick())["state"] == "attention"
            assert j.get("managed_control_fault") == fault and j.get(ACK) == ack and recoverable_fault(c, fault)
        response = await rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        assert response[0] == 3
        rig.peer.permission_enabled = False
        await until(lambda: j.active() is None and j.next_delivery() is None)
        actual = rig.peer.native
        reported_current = 0.0

        # The native endpoint alone lags its genuine protocol Stop; all identities/settings/resources stay original.
        def lagging_native(key):
            value = actual(key)
            if key == "SESSION":
                value["saveData"]["sessionList"][0].update(_transactionId=local, _setPlanId=9)
            elif key == "CONNECTOR":
                value[0]["info"]["CP"], value[0]["connStatus"] = "C", "Charging"
                value[0]["outLimit"] = 0
            elif key == "METER":
                value[0]["meterData"]["cur"] = [reported_current * 10, 0, 0]
                value[0]["meterData"]["power"] = [reported_current * 240, 0, 0, reported_current * 240]
            elif key == "TRANSACTIONS":
                value = copy.deepcopy(open_history)
            return value

        monkeypatch.setattr(rig.peer, "native", lagging_native)
        try:
            for reported_current in (0.0, 0.2):
                assert (await c.tick())["state"] == "attention"
                assert j.get("managed_control_fault") == fault and j.get(ACK) == ack
                assert recoverable_fault(c, fault) and c.requests.receipt(pause)["state"] == "accepted"
                assert rig.bench.firmware.effective_limit() == rig.bench.firmware.current == 0
        finally:
            monkeypatch.setattr(rig.peer, "native", actual)
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed" and j.get("managed_control_fault") is None
        assert not starts(rig)
