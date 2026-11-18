"""Explicit Return preflight failures end only their actual request before any positive physical claim."""

import asyncio
import copy
import uuid
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.managed_diagnostics import snapshot
from ocpp_hub_core.prototype.native_bounded_handover import NativeBoundedHandover
from ocpp_hub_core.prototype.recovery_contracts import Prerequisite, RestoreState
from ocpp_hub_core.prototype.recovery_coordinator import RecoveryCoordinator

from tests.charge_rig import ChargeRig, positive_flow
from tests.current_rig import locally_ready
from tests.managed_rig import healthy, starts
from tests.managed_scenario_helpers import next_daily, originals
from tests.native_fault_support import FinalReleaseHttpPeer, coroutine_locals
from tests.native_schedule_support import timer_bench
from tests.recovery_barriers import completed_control, require_zero_held
from tests.runtime_rig import until
from tests.test_managed_normal_start import natural_stop
from tests.test_managed_offline_recovery import local_boot, replacement_offline
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Verify the completed production preflight and its actual public receipt; no operation result is synthesized.
def require_unconfirmed_preclaim(test, identifier, reason, intent, control, wire_mark):
    c, j, g = test.controller, test.session.journal, test.session.gateway
    owned = c.recovery._task
    assert owned is not None and owned.done() and not owned.cancelled()
    assert owned.exception() is None
    progress = owned.result()
    assert progress.state is RestoreState.REQUEST_UNCONFIRMED and not progress.shared
    assert progress.pending is not None and progress.pending.reason is reason
    assert progress.operation is None
    receipt = c.requests.receipt(identifier)
    assert receipt["id"] == identifier and receipt["state"] == "uncertain"
    assert receipt["result"] == {"classification": "return_not_confirmed"}
    assert c.requests.intent() == intent and j.get("control") == control
    assert not c.requests.return_in_progress() and c.requests.pending_return() is None
    assert g.controls.release_attempt is None and not g.controls._operations
    assert not j.get("managed_control_fault") and j.active() is None
    require_zero_held(test.rig, wire_mark)
    assert all(
        period["limit"] == 0
        for frame in test.rig.bench.device_calls[wire_mark:]
        if frame[2] == "SetChargingProfile"
        for period in frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
    )
    assert not any(
        frame[2] in ("RemoteStartTransaction", "RemoteStopTransaction")
        for frame in test.rig.bench.device_calls[wire_mark:]
    )
    return copy.deepcopy(receipt)


# Observe recovery without admitting another request, then complete one distinct owner Return at the same revision.
async def new_return_after_preflight_recovery(test, identifier, original, intent, wire_mark):
    c, j = test.controller, test.session.journal
    for _ in range(3):
        status = await healthy(c)
        assert status["local_hold_verified"] and c.requests.intent() == intent
        assert c.requests.receipt(identifier) == original and c.requests.pending_return() is None
        assert not j.get("managed_control_fault") and j.active() is None
        require_zero_held(test.rig, wire_mark)
    fresh = uuid.uuid4().hex
    accepted = c.command(fresh, "app_control", intent.revision)
    assert accepted["id"] == fresh and accepted["state"] == "accepted"
    await completed_control(test, fresh, mode="app", revision=intent.revision + 1, ceiling=c.current.selected())
    assert c.requests.receipt(identifier) == original and not starts(test.rig)
    status = await healthy(c)
    assert snapshot(c, status)["pending_command_count"] == 0
    return fresh, copy.deepcopy(c.requests.receipt(fresh))


# Intent: a genuine HTTP503 in explicit Return preflight makes that UUID unconfirmed without a positive claim.
# Why: fresh telemetry cannot silently replay an accepted positive request whose preflight already terminated.
async def test_explicit_return_native503_preflight_requires_a_new_request(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, peer_type=FinalReleaseHttpPeer) as b:
        test = ChargeRig(b.rig, b.session, b.c, b.sock)
        c, g, j, peer = b.c, b.session.gateway, b.session.journal, b.rig.peer
        assert isinstance(peer, FinalReleaseHttpPeer)
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed"
        intent, control = c.requests.intent(), copy.deepcopy(j.get("control"))
        saved = originals(b)
        original_plans = copy.deepcopy(b.device.plans)
        original_configuration = copy.deepcopy(peer.configuration)
        wire_mark = len(b.rig.bench.device_calls)
        identifier = uuid.uuid4().hex
        assert c.command(identifier, "app_control", intent.revision)["state"] == "accepted"

        # Select the parsed real HTTP request from this exact owned Return before a release child exists.
        def select_return_preflight():
            owned = c.recovery._task
            if owned is None or owned not in g.controls._completions:
                return None
            restore = coroutine_locals(owned, RecoveryCoordinator._restore_owned.__code__)
            read = coroutine_locals(owned, NativeBoundedHandover._read.__code__)
            if (
                restore.get("self") is not c.recovery
                or restore.get("expected_intent") != intent
                or restore.get("return_request") != identifier
                or read.get("self") is not c.native
                or g.controls.release_attempt is not None
                or g.controls._operations
                or c.requests.receipt(identifier)["state"] != "running"
            ):
                return None
            return {"owned": owned, "sequence": read["sequence"], "intent": intent}

        peer.select = select_return_preflight
        returning = asyncio.create_task(c.return_requested(identifier, intent.revision))
        try:
            await asyncio.wait_for(peer.entered.wait(), 5)
            assert peer.witness is not None and peer.witness["http_action"] == "getConfig"
            owned = peer.witness["owned"]
            assert owned in g.controls._completions and not owned.done()
            assert g.controls.release_attempt is None and not g.controls._operations
            assert j.get("control") == control and b.device.effective_limit() == 0
            peer.release.set()
            await asyncio.wait_for(returning, 5)
            assert peer.responded and owned is c.recovery._task
            original = require_unconfirmed_preclaim(test, identifier, Prerequisite.NATIVE, intent, control, wire_mark)
            assert j.get("control")["uncertain"] is False
            peer.select = None
            fresh, fresh_receipt = await new_return_after_preflight_recovery(
                test, identifier, original, intent, wire_mark
            )

            # The original native timetable still opens and closes a genuine mapped session after the fresh Return.
            await next_daily(b, c.current.selected(), saved)
            assert (await healthy(c))["owner_control_enabled"]
            assert c.requests.receipt(identifier) == original and c.requests.receipt(fresh) == fresh_receipt
            assert b.device.plans == original_plans and peer.configuration == original_configuration
            assert not starts(b.rig) and not b.stops() and not j.get("managed_control_fault")
        finally:
            peer.select = None
            peer.release.set()
            await asyncio.gather(returning, return_exceptions=True)


# Intent: actual cloud connection refusal ends explicit Return preflight while native/physical zero remains valid.
# Why: only a real acknowledgement wait retains the same UUID; a disconnected backend is not CLOUD_ACK.
async def test_explicit_return_refused_cloud_preflight_requires_a_new_request(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with replacement_offline(
        tmp_path, tls, unused_tcp_port, monkeypatch, initial="paused", selected=6, debt=None
    ) as recovery:
        test = recovery.test
        c, j, g = test.controller, test.session.journal, test.session.gateway
        await local_boot(recovery)
        status = await locally_ready(c)
        assert status["phase"] == "active" and status["local_hold_verified"]
        assert c.requests.intent().mode == "paused" and not status["cloud_connected"]
        assert not test.session.runtime.status.vendor_connected and g.vendor_registration != "Accepted"
        assert test.rig.bench.firmware.effective_limit() == 0 and not j.get("managed_control_fault")
        intent, control = c.requests.intent(), copy.deepcopy(j.get("control"))
        original_plans = copy.deepcopy(test.rig.bench.firmware.plans)
        original_configuration = copy.deepcopy(test.rig.peer.configuration)
        wire_mark = len(test.rig.bench.device_calls)
        identifier = uuid.uuid4().hex
        assert c.command(identifier, "app_control", intent.revision)["state"] == "accepted"
        await asyncio.wait_for(c.return_requested(identifier, intent.revision), 5)
        # Current zero_decision groups the absent cloud connection into ZERO_PROOF, never CLOUD_ACK.
        original = require_unconfirmed_preclaim(test, identifier, Prerequisite.ZERO_PROOF, intent, control, wire_mark)
        assert g.controls.ready and g.controls.desired() == 0 and j.get("control")["uncertain"] is False

        # Restore only the actual external destination; normal runtime registration and queue ACKs do the recovery.
        transport = test.session.runtime.transport
        transport.config = replace(transport.config, vendor_url=recovery.original_vendor_url)
        await until(
            lambda: (
                test.session.runtime.status.vendor_connected
                and g.vendor_registration == "Accepted"
                and j.next_delivery() is None
                and not g.vendor._pending
            )
        )
        fresh, fresh_receipt = await new_return_after_preflight_recovery(test, identifier, original, intent, wire_mark)

        # Drive the original plan's actual independent lifecycle; Return itself never starts another transaction.
        local = await positive_flow(test, "daily", call_id=uuid.uuid4().hex)
        status = await healthy(c)
        assert status["power_w"] > 0 and status["current_a"] == c.current.selected()
        assert j.active() == local and j.vendor_id(local) is not None
        await natural_stop(test, local)
        assert (await healthy(c))["owner_control_enabled"] and j.active() is None
        assert c.requests.receipt(identifier) == original and c.requests.receipt(fresh) == fresh_receipt
        assert test.rig.bench.firmware.plans == original_plans
        assert test.rig.peer.configuration == original_configuration
        assert not starts(test.rig) and not j.get("managed_control_fault")
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
