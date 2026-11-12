"""Failed paused current selections restore the previously verified future setting."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_current import adjustable
from tests.test_managed_current_tick_race import OneNativeResponseGate, chain
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_prototype_tls import tls as tls


# Intent: compare normal paused selection with a newer Pause during its genuine native verification.
# Why: separate retained desired preference and failed receipt semantics from any unrequested positive effect.
@pytest.mark.parametrize("supersede", [False, True])
async def test_future_selection_after_pause(tmp_path, tls, unused_tcp_port, monkeypatch, supersede):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, peer = b.controller, b.rig.peer
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"] and c.current.selected() == 6
        peer.arm = True
        change_id = uuid.uuid4().hex
        c.command(change_id, "set_current", c.requests.intent().revision, current_a=16)
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            assert "_apply" in chain(c.current.worker)
            before = {
                "receipt": c.requests.receipt(change_id),
                "selected_a": c.current.selected(),
                "effective_a": b.rig.bench.firmware.effective_limit(),
            }
            assert before["selected_a"] == 16 and before["effective_a"] == 0
            if supersede:
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            peer.release.set()
            await asyncio.wait_for(c.current.settle(), 5)
            if c.pause_task is not None:
                await c.pause_task
            status = await healthy(c)
            receipt = c.requests.receipt(change_id)
            assert receipt["state"] == ("failed" if supersede else "completed")
            assert c.current.selected() == (6 if supersede else 16) and status["local_hold_verified"]
            assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
            returning = uuid.uuid4().hex
            c.command(returning, "app_control", c.requests.intent().revision)
            final = await healthy(c)
            assert c.requests.receipt(returning)["state"] == "completed"
            assert final["owner_control_enabled"] and b.rig.bench.firmware.effective_limit() == (6 if supersede else 16)
            assert not starts(b.rig)
            if supersede:
                assert b.session.journal.get("managed_current_change")["state"] == "cancelled"
            original_port = b.session.prepared.observer.port

            # A genuine read outage after Return must still activate ordinary zero recovery.
            async def unavailable(reader, writer):
                await reader.readuntil(b"\r\n\r\n")
                writer.close()
                await writer.wait_closed()

            async with await asyncio.start_server(unavailable, "127.0.0.1", 0) as server:
                b.session.prepared.observer.port = server.sockets[0].getsockname()[1]
                try:
                    for _ in range(3):
                        await c.tick()
                finally:
                    b.session.prepared.observer.port = original_port
            assert b.rig.bench.firmware.effective_limit() == 0
            assert (await healthy(c))["owner_control_enabled"]
            assert b.rig.bench.firmware.effective_limit() == (6 if supersede else 16) and not starts(b.rig)
        finally:
            peer.release.set()


# Intent: losing the process during paused-current verification preserves the last confirmed selection.
# Why: an uncertain command cannot become future permission through process replacement and explicit Return.
@pytest.mark.parametrize("previous,target", [(6, 16), (16, 32)])
async def test_pending_paused_selection_restarts_at_previous_current(
    tmp_path, tls, unused_tcp_port, monkeypatch, previous, target
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, peer = b.controller, b.rig.peer
        if previous != 6:
            c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=previous)
            await c.current.settle()
            await healthy(c)
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        peer.arm = True
        request = uuid.uuid4().hex
        c.command(request, "set_current", c.requests.intent().revision, current_a=target)
        await asyncio.wait_for(peer.entered.wait(), 2)
        c.current.worker.cancel()
        await asyncio.gather(c.current.worker, return_exceptions=True)
        assert b.session.journal.get("managed_current_change")["state"] == "pending"
        peer.release.set()
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        try:
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, "paused-selection-replacement")
                status = await healthy(recovered)
                assert status["local_hold_verified"] and recovered.current.selected() == previous
                assert recovered.requests.receipt(request)["state"] == "uncertain"
                assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
                returning = uuid.uuid4().hex
                recovered.command(returning, "app_control", recovered.requests.intent().revision)
                assert (await healthy(recovered))["owner_control_enabled"]
                assert recovered.requests.receipt(returning)["state"] == "completed"
                assert recovered.current.selected() == b.rig.bench.firmware.effective_limit() == previous
                assert recovered.requests.receipt(request)["state"] == "uncertain"
                assert replacement.journal.get("managed_current_change")["state"] == "cancelled"
                assert not starts(b.rig)
        finally:
            await recovered.current.settle()
            await recovered.tests.close()
            await replacement.close()
