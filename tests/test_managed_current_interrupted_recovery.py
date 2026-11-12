"""Explicit Pause reconciles interrupted legacy current collisions without promoting the unverified target."""

import asyncio
import copy
import json
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current_recovery import INTERRUPTED
from ocpp_hub_core.prototype.managed_release_recovery import ACK, reconcile_owner_release
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.handover_helpers import crash
from tests.test_managed_current import adjustable
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_managed_release_recovery import explicit_release_pause, recover_then_return
from tests.test_prototype_tls import tls as tls


# Persist the old tick's real fault/request shape before the current worker can write a terminal diagnostic.
async def interrupt_old_collision(b, previous, paused):
    c = b.controller
    if previous != 6:
        c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=previous)
        await c.current.settle()
        await healthy(c)
    request = uuid.uuid4().hex
    c.command(request, "set_current", c.requests.intent().revision, current_a=32)
    c.fault("owner_control_or_delivery_uncertain")
    c.current.worker.cancel()
    await asyncio.gather(c.current.worker, return_exceptions=True)
    if paused:
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        if c.pause_task is not None:
            await c.pause_task
    assert b.session.journal.get("managed_current_change")["state"] == "pending"
    assert c.requests.receipt(request)["state"] == "accepted"
    await crash(b.session)
    return request


# Intent: process loss before the old worker's outcome preserves the previous setting through Pause and Return.
# Why: both old app intent and an already newer owner Pause require durable narrow evidence across replacement.
@pytest.mark.parametrize("previous", [6, 16])
@pytest.mark.parametrize("paused", [False, True])
@pytest.mark.parametrize("again", [False, True])
async def test_interrupted_collision_requires_new_pause_and_preserves_previous_selection(
    tmp_path, tls, unused_tcp_port, monkeypatch, previous, paused, again
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        request = await interrupt_old_collision(b, previous, paused)
        replacement = managed_session(b.rig)
        await replacement.start()
        try:
            c = ManagedController(replacement)
            saved = copy.deepcopy(replacement.journal.get(INTERRUPTED))
            assert saved["change"]["id"] == request and saved["previous_a"] == previous
            if again:
                await crash(replacement)
                replacement = managed_session(b.rig)
                await replacement.start()
                c = ManagedController(replacement)
                assert replacement.journal.get(INTERRUPTED) == saved
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, "current-interrupted-reboot")
                for _ in range(2):
                    assert (await c.tick())["state"] == "attention"
                assert not replacement.journal.get(ACK)
                fresh = SimpleNamespace(rig=b.rig, session=replacement, c=c, charger=charger)
                await recover_then_return(fresh)
                assert c.current.selected() == b.rig.bench.firmware.effective_limit() == previous
                assert replacement.journal.get(INTERRUPTED) is None
                receipt = c.requests.receipt(request)
                assert receipt["state"] == "failed" and receipt["result"]["reconciled_at_zero"] is True
                assert receipt["result"]["requested_current_a"] == 32
                assert receipt["result"]["previous_current_a"] == previous
        finally:
            await replacement.close()


# Intent: an old tick fault during actual native HTTP records its real local-authority supersession diagnostic.
# Why: the native reader can fail before the current guard emits the more familiar superseded-operation error.
async def test_interrupted_native_http_diagnostic_can_reconcile_only_through_pause(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    from tests import test_native_bounded_session as native_bench

    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        peer.arm = True
        request = uuid.uuid4().hex
        c.command(request, "set_current", c.requests.intent().revision, current_a=16)
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            c.fault("owner_control_or_delivery_uncertain")
            peer.release.set()
            await c.current.settle()
            assert j.get("managed_current_failure") == {
                "type": "AuditSupersededError",
                "detail": "native bounded audit was superseded by local authority",
            }
            if c.pause_task is not None:
                await c.pause_task
            assert (await c.tick())["state"] == "attention"
            await recover_then_return(SimpleNamespace(rig=b.rig, session=b.session, c=c))
            assert c.current.selected() == b.rig.bench.firmware.effective_limit() == 6
            assert c.requests.receipt(request)["state"] == "failed"
        finally:
            peer.release.set()


# Intent: changed interrupted markers, faults, request outcomes and bindings during HTTP retain the fault.
# Why: the constructor's narrow marker must be rechecked as part of the particular owner's Pause snapshot.
@pytest.mark.parametrize("changed", ["marker", "fault", "request", "binding"])
async def test_interrupted_collision_rechecks_marker_and_request_after_http(
    tmp_path, tls, unused_tcp_port, monkeypatch, changed
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        request = await interrupt_old_collision(b, 6, False)
        replacement = managed_session(b.rig)
        await replacement.start()
        try:
            c = ManagedController(replacement)
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, "current-interrupted-revoked")
                fresh = SimpleNamespace(rig=b.rig, session=replacement, c=c, charger=charger)
                await c.tick()
                await explicit_release_pause(fresh)
                j = replacement.journal
                prior = copy.deepcopy(j.get("managed_control_fault"))
                native = b.rig.peer.native
                tasks = []

                # Introduce a competing durable fact through the real native HTTP event loop.
                async def cross():
                    if changed == "marker":
                        j.put(INTERRUPTED, {**j.get(INTERRUPTED), "later_observation": True})
                    elif changed == "fault":
                        j.put("managed_control_fault", {"classification": "unrelated_fault", "at_epoch": 123})
                    elif changed == "request":
                        j.db.execute(
                            "UPDATE managed_requests SET result=? WHERE id=?",
                            (json.dumps({"classification": "unrelated_uncertainty"}), request),
                        )
                    else:
                        j.db.execute("UPDATE managed_current_requests SET current_a=31 WHERE id=?", (request,))

                # Preserve the original independent peer's response and schedule only one competing change.
                def observe(key):
                    value = native(key)
                    if not tasks:
                        tasks.append(asyncio.create_task(cross()))
                    return value

                monkeypatch.setattr(b.rig.peer, "native", observe)
                try:
                    assert not await reconcile_owner_release(c)
                except RecoveryRequiredError, SecurityError, UnavailableError:
                    pass
                await asyncio.gather(*tasks)
                assert tasks and j.get(INTERRUPTED) is not None
                assert j.get("managed_control_fault") == (
                    {"classification": "unrelated_fault", "at_epoch": 123} if changed == "fault" else prior
                )
                assert c.current.selected() == 6 and b.rig.bench.firmware.effective_limit() == 0
        finally:
            await replacement.close()


# Intent: a pending record without its exact original request binding or revision cannot receive a startup marker.
# Why: fresh zero can reconcile only an attested old operation, never an uncorrelated generic owner fault.
@pytest.mark.parametrize("changed", ["missing_binding", "different_revision"])
async def test_interrupted_collision_requires_original_pending_request_correlation(
    tmp_path, tls, unused_tcp_port, monkeypatch, changed
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        request = uuid.uuid4().hex
        c.command(request, "set_current", c.requests.intent().revision, current_a=32)
        c.fault("owner_control_or_delivery_uncertain")
        c.current.worker.cancel()
        await asyncio.gather(c.current.worker, return_exceptions=True)
        if changed == "missing_binding":
            j.db.execute("DELETE FROM managed_current_requests WHERE id=?", (request,))
        else:
            j.db.execute("UPDATE managed_requests SET revision=revision+1 WHERE id=?", (request,))
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        try:
            c = ManagedController(replacement)
            assert replacement.journal.get(INTERRUPTED) is None
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, "current-uncorrelated-reboot")
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                if c.pause_task is not None:
                    await c.pause_task
                for _ in range(2):
                    assert (await c.tick())["state"] == "attention"
                assert not replacement.journal.get(ACK)
                assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            await replacement.close()
