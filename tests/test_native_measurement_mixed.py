"""A strict native contradiction cannot borrow recovery permission from a later measured-only observation."""

import copy
import uuid

import pytest
from ocpp_hub_core.prototype.managed_release_recovery import ACK, recoverable_fault

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: any explicit strict member of a three-failure run prevents measured-only recovery provenance.
# Why: choosing the last error alone would erase a real configuration/history refusal before a fresh Pause.
@pytest.mark.parametrize("strict_kind", ["configuration", "history"])
@pytest.mark.parametrize("strict_at", [0, 1, 2, "coincident"])
@pytest.mark.parametrize("measurement", ["current", "power"])
async def test_mixed_native_run_keeps_strict_fault(
    tmp_path, tls, unused_tcp_port, monkeypatch, strict_kind, strict_at, measurement
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16, cli_route=True) as b:
        c, j, peer, firmware = b.controller, b.session.journal, b.rig.peer, b.rig.bench.firmware
        original_configuration = copy.deepcopy(peer.configuration)
        original_plans = copy.deepcopy(firmware.plans)
        original_history_row = copy.deepcopy(b.rig.bench.history[0])
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await mp.healthy(c))["owner_control_enabled"] and j.vendor_id(local) is not None
        ordinary_native = peer.native
        measured = {"enabled": False, "samples": 0}

        # Only the independent native meter changes; firmware and protocol observations remain real.
        def changed_meter(key):
            value = ordinary_native(key)
            if key == "METER" and firmware.current > 0 and measured["enabled"]:
                current = 17 if measurement == "current" else 16
                power = 4080 if measurement == "current" else 4421
                value[0]["meterData"]["cur"] = [current * 10, 0, 0]
                value[0]["meterData"]["power"] = [power, 0, 0, power]
                measured["samples"] += 1
            return value

        monkeypatch.setattr(peer, "native", changed_meter)
        for index in range(3):
            peer.configuration = copy.deepcopy(original_configuration)
            b.rig.bench.history[0] = copy.deepcopy(original_history_row)
            strict = strict_at == "coincident" or index == strict_at
            measured["enabled"] = not strict or strict_at == "coincident"
            if strict_kind == "configuration" and strict:
                peer.configuration["untouched"] = "independent valid contradiction"
            elif strict:
                b.rig.bench.history[0]["original_record"]["preserve"].append(4)
            assert (await c.tick())["state"] == "degraded"
        fault = copy.deepcopy(j.get("managed_control_fault"))
        assert fault["classification"] == "native_authority_or_preservation_failed"
        assert not recoverable_fault(c, fault)
        assert measured["samples"] >= 1
        peer.configuration = original_configuration
        b.rig.bench.history[0] = original_history_row
        measured["enabled"] = False
        assert (await c.tick())["state"] == "attention"
        await until(lambda: firmware.effective_limit() == firmware.current == 0)
        boundary = len(b.rig.bench.received_limits)
        pause_id = uuid.uuid4().hex
        c.command(pause_id, "pause", c.requests.intent().revision)
        await until(lambda: b.rig.bench.remote_stop.is_set())
        await b.rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        peer.permission_enabled = False
        await until(lambda: j.active() is None and j.next_delivery() is None)
        if c.pause_task is not None:
            await c.pause_task
        for _ in range(4):
            assert (await c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault and j.get(ACK) is None
        assert c.requests.receipt(pause_id)["state"] != "completed"
        assert all(value == 0 for value in b.rig.bench.received_limits[boundary:])
        assert not mp.starts(b.rig) and firmware.effective_limit() == firmware.current == 0
        assert peer.configuration == original_configuration and firmware.plans == original_plans
