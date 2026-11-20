"""Pending physical work and real restrictive SQL failures publish honest status without cancelling their worker."""

import asyncio
import uuid
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_view import publish_poll
from websockets.asyncio.client import connect

from tests.current_rig import adjustable
from tests.managed_rig import healthy
from tests.management_peer import TOKEN, call
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.test_worker_storage_recovery import fail_boundary
from tests.test_worker_storage_supersession import current_settlement_unavailable
from tests.tls_support import tls as tls
from tools.managed_reporting import Reporting


# Intent: a completed current receipt and the status read agree before the next charger observation.
# Why: retiring a pending label cannot certify readings or enable controls from the previous poll.
async def test_completed_current_status_retires_pending_before_next_poll(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    async with adjustable(
        tmp_path, tls, unused_tcp_port_factory(), monkeypatch, native_peer=OneNativeResponseGate
    ) as b:
        c, peer = b.controller, b.rig.peer
        reporter = Reporting(Path(__file__).resolve().parents[1])
        before = await healthy(c)
        started = before["checked_at"]
        expires = started + 86400
        published = publish_poll(c, before, pid=1, started=started, expires=expires, observe=reporter.observe)
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-token", TOKEN), c, lambda: published)
        await api.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as socket:
                peer.arm = True
                request = await call(socket, "set_current", revision=before["control_revision"], current_a=16)
                receipt = request["result"]
                assert receipt["state"] == "accepted"
                await asyncio.wait_for(peer.entered.wait(), 3)
                poll = await asyncio.wait_for(c.tick(), 3)
                assert poll["fault"] == "current_update_pending"
                published = publish_poll(c, poll, pid=1, started=started, expires=expires, observe=reporter.observe)
                pending = (await call(socket, "status"))["result"]
                assert pending["control_mode"] == "updating_current"
                assert pending["diagnostics"]["pending_command_count"] == 1
                assert pending["diagnostics"]["pending_commands"][0]["action"] == "set_current"
                assert pending["diagnostics"]["current_fault"] == "current_update_pending"
                assert pending["diagnostics"]["workers"]["current"] == "running"

                peer.release.set()
                await asyncio.wait_for(c.current.settle(), 5)
                outcome = (await call(socket, "receipt", request_id=receipt["id"]))["result"]
                assert outcome["state"] == "completed"
                assert outcome["result"] == {"current_a": 16, "charging_started": False}
                database = c.session.journal.db
                counts = (database.total_changes, len(peer.requests), len(b.rig.bench.device_calls))
                completed = (await call(socket, "status"))["result"]
                assert counts == (database.total_changes, len(peer.requests), len(b.rig.bench.device_calls))
                assert completed["control_mode"] == "waiting" and completed["requested_current_a"] == 16
                assert completed["checked_at"] == poll["checked_at"]
                assert all(completed[key] is None for key in ("native_observed_at", "power_w", "station_ceiling_a"))
                assert completed.get("fault") is None
                assert not any(
                    completed[key] for key in ("can_start", "can_start_test", "can_return", "can_set_current")
                )
                diagnostic = completed["diagnostics"]
                assert diagnostic["current_fault"] is None and "current_update_pending" not in diagnostic["blockers"]
                assert diagnostic["pending_command_count"] == 0 and diagnostic["pending_commands"] == []
                assert diagnostic["workers"]["current"] == "done"
                assert diagnostic["timeline"] == pending["diagnostics"]["timeline"]
                assert diagnostic["timeline"][-1]["fault"] == "current_update_pending"
                assert published["fault"] == "current_update_pending"
                assert published["diagnostics"]["pending_command_count"] == 1

                observed = await healthy(c)
                published = publish_poll(c, observed, pid=1, started=started, expires=expires, observe=reporter.observe)
                verified = (await call(socket, "status"))["result"]
                assert verified["control_mode"] == "app" and verified["station_ceiling_a"] == 16
                assert verified["can_set_current"] and b.rig.bench.firmware.effective_limit() == 16
        finally:
            peer.release.set()
            await api.close()


# Intent: a pending physical response or refused restrictive transaction cannot freeze status or invent zero proof.
# Why: the real worker retains its outcome and a newer Pause keeps its own revision and ticket through settlement.
@pytest.mark.parametrize("boundary", ["native_http", "statement", "commit"])
async def test_current_poll_during_pending_physical_or_sql_work(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, boundary
):
    async with adjustable(
        tmp_path, tls, unused_tcp_port_factory(), monkeypatch, native_peer=OneNativeResponseGate
    ) as b:
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
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as socket:
                # The management read must recognise the accepted work before the next service poll.
                immediate = (await call(socket, "status"))["result"]
                assert immediate["checked_at"] == prior_checked
                assert immediate["fault"] == "current_update_pending" and immediate["requested_current_a"] == 16
                assert immediate["native_age_seconds"] is None
                assert all(immediate[key] is None for key in ("current_a", "power_w", "vehicle_connected"))
                for _ in range(2):
                    pending = await asyncio.wait_for(c.tick(), 1)
                    assert pending["checked_at"] > prior_checked
                    prior_checked = pending["checked_at"]
                    assert pending["state"] == "reconciling" and pending["fault"] == "current_update_pending"
                    assert c.current.worker is worker and not worker.done() and not worker.cancelled()
                    assert c.latest_native is None
                    assert all(
                        pending[key] is None
                        for key in (
                            "current_a",
                            "power_w",
                            "native_observed_at",
                            "station_ceiling_a",
                            "charging_session_open",
                        )
                    )
                    published = {**pending, "running": True, "certificate_remaining_seconds": 86400}
                    status = (await call(socket, "status"))["result"]
                    assert not status["healthy"] and not status["local_healthy"]
                    assert status["desired_control"] == "app" and status["requested_current_a"] == 16
                    assert status["can_pause"] and not any(
                        status[key] for key in ("can_start", "can_start_test", "can_return", "can_set_current")
                    )
                # A separate, real owner Pause can enforce zero even while the old operation's settlement waits.
                newer = await call(socket, "pause", revision=status["control_revision"])
                assert newer["ok"]
                pausing = (await call(socket, "status"))["result"]
                assert pausing["desired_control"] == "paused" and pausing["pause_pending"] is True
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
