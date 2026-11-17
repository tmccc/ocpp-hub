"""A continuation domain must still own its source after real native send-validation waits."""

import asyncio
import copy
import uuid

from ocpp_hub_core.prototype.recovery_contracts import Prerequisite

from tests.native_schedule_support import SelectiveConfigPeer, timer_bench
from tests.test_prototype_tls import tls as tls


# Count only actual positive profile writes delivered to the independent charger.
def positive_writes(b, mark):
    return [
        frame
        for frame in b.rig.bench.device_calls[mark:]
        if frame[2] == "SetChargingProfile"
        and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
    ]


# Intent: losing the current-recovery domain during actual native validation revokes its continuation permission.
# Why: a known original daily session alone cannot inherit positive authority after its recovery owner retires.
async def test_continuation_source_rechecked_after_native_send_wait(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch, peer_type=SelectiveConfigPeer) as b:
        c, g, j, peer = b.c, b.session.gateway, b.session.journal, b.rig.peer
        wire, reads = len(b.rig.bench.device_calls), peer.config_requests
        unknown = await c.recovery.restore(continuing_local=123456)
        assert unknown.pending is not None and unknown.pending.reason is Prerequisite.SOURCE
        assert peer.config_requests == reads and not positive_writes(b, wire) and not b.stops()

        # Hold the worker's actual first HTTP audit while an independent original daily session starts.
        peer.gate_at = peer.config_requests + 2
        identifier = uuid.uuid4().hex
        c.command(identifier, "set_current", c.requests.intent().revision, current_a=32)
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            local = await b.start_plan(9)
            await c.native.read()
        finally:
            peer.release.set()
            await c.current.settle()
        assert c.requests.receipt(identifier)["state"] == "failed"
        assert j.get("managed_current_change")["state"] == "recovering"
        assert b.device.effective_limit() == 0 and not b.stops()
        await c.current.protect_overlap()
        await c.native.read()
        assert j.get("trial_phase") == "active"
        assert j.get("managed_current_change")["continuing_local"] == local
        intent, boot, revision = c.requests.intent(), g.boot_observation, g.controls._revision
        wire = len(b.rig.bench.device_calls)

        # Two preflight audits finish before the final configuration of the real positive RPC's audit is held.
        peer.entered.clear()
        peer.release.clear()
        peer.gate_at = peer.config_requests + 6
        restoring = asyncio.create_task(c.recovery.restore(continuing_local=local))
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            attempt = g.controls.release_attempt
            assert attempt is not None and not attempt.finished and not attempt.positive_may_have_dispatched
            assert b.device.effective_limit() == 0 and not positive_writes(b, wire)
            before = c.native.authority()
            saved = copy.deepcopy(j.get("managed_current_change"))
            # Retire only the source domain: current intent, Boot, ticket and HTTP authority stay unchanged.
            j.put("managed_current_change", {**saved, "state": "cancelled"})
            assert c.native.authority() == before
        finally:
            peer.release.set()
        progress = await asyncio.wait_for(restoring, 5)
        assert progress.pending is not None and progress.pending.reason is Prerequisite.SOURCE
        assert not progress.shared and progress.operation is not None
        assert not progress.operation.effects.positive_dispatched
        assert progress.operation.effects.zero_verified
        assert c.requests.intent() == intent and g.boot_observation == boot and g.controls._revision == revision
        assert j.active() == local and b.device.effective_limit() == 0
        assert not positive_writes(b, wire) and not b.stops() and not j.get("managed_control_fault")
