"""Pending physical work and real restrictive SQL failures publish honest status without cancelling their worker."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

from tests import test_native_bounded_session as native_bench
from tests.test_managed_api import TOKEN, call
from tests.test_managed_current import adjustable
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.test_managed_proxy import healthy
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import fail_boundary
from tests.test_worker_storage_supersession import current_settlement_unavailable


# Intent: a pending physical response or refused restrictive transaction cannot freeze status or invent zero proof.
# Why: the real worker retains its outcome and a newer Pause keeps its own revision and ticket through settlement.
@pytest.mark.parametrize("boundary", ["native_http", "statement", "commit"])
async def test_current_poll_during_pending_physical_or_sql_work(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, boundary
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        prior_checked = published["checked_at"]
        if boundary == "native_http":
            peer.arm = True
            reached = peer.entered
        else:
            fail_boundary(j, "current", boundary)
            reached, attempts = current_settlement_unavailable(j, boundary)
        request = uuid.uuid4().hex
        c.command(request, "set_current", c.requests.intent().revision, current_a=16)
        worker = c.current.worker
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-token", TOKEN), c, lambda: published)
        await api.start()
        try:
            await asyncio.wait_for(reached.wait(), 3)
            assert worker is not None and not worker.done()
            if boundary != "native_http":
                assert len(attempts) >= 2
            async with connect(
                f"ws://127.0.0.1:{port}/management", additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"]
            ) as socket:
                for _ in range(2):
                    pending = await asyncio.wait_for(c.tick(), 1)
                    assert pending["checked_at"] > prior_checked
                    prior_checked = pending["checked_at"]
                    assert pending["state"] == "reconciling" and pending["fault"] == "current_update_pending"
                    assert c.current.worker is worker and not worker.done() and not worker.cancelled()
                    assert c.latest_native is None
                    assert all(pending[key] is None for key in (
                        "current_a", "power_w", "native_observed_at", "station_ceiling_a", "charging_session_open"
                    ))
                    published = {**pending, "running": True, "certificate_remaining_seconds": 86400}
                    status = (await call(socket, "status"))["result"]
                    assert not status["healthy"] and not status["local_healthy"]
                    assert status["desired_control"] == "app" and status["requested_current_a"] == 16
                    assert status["can_pause"] and not any(status[key] for key in (
                        "can_start", "can_start_test", "can_return", "can_set_current"
                    ))
                # A separate, real owner Pause can enforce zero even while the old operation's settlement waits.
                newer = await call(socket, "pause", revision=status["control_revision"])
                assert newer["ok"]
                if c.pause_task is not None:
                    await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
                intent, control = c.requests.intent(), j.get("control")
                assert intent.mode == "paused" and b.rig.bench.firmware.effective_limit() == 0
                if boundary == "native_http":
                    peer.release.set()
                else:
                    j.db.execute("DROP TRIGGER review_worker_boundary")
                    j.db.execute("DROP TRIGGER review_cleanup_unavailable")
                await asyncio.wait_for(asyncio.shield(worker), 3)
                assert c.requests.intent() == intent and j.get("control")["ticket"] == control["ticket"]
                assert (await healthy(c))["local_hold_verified"]
                assert c.requests.receipt(newer["result"]["id"])["state"] == "completed"
                assert c.requests.receipt(request)["state"] == "failed"
                assert c.current.selected() == 6 and b.rig.bench.firmware.effective_limit() == 0
        finally:
            peer.release.set()
            j.db.execute("DROP TRIGGER IF EXISTS review_worker_boundary")
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
            if worker is not None:
                await asyncio.wait_for(asyncio.gather(worker, return_exceptions=True), 3)
            await api.close()
