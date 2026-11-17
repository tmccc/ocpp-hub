"""Retained cloud permissions and timed-test closure remain visible without releasing their safeguards."""

import asyncio
import copy
import time
import uuid

import pytest
from ocpp_hub_core.prototype.managed_diagnostics import snapshot
from ocpp_hub_core.prototype.managed_view import health_snapshot
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_service_cli import healthy_status
from tests.test_managed_test_charge import owner_start, positive_flow, ready
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: a real accepted but unbound cloud Start is explicitly unhealthy before and after its grant expires.
# Why: expiry never proves that a previously accepted physical Start cannot still report its session.
@pytest.mark.parametrize("expired", [False, True])
async def test_unbound_cloud_start_explains_health_without_retiring_debt(
    tmp_path, tls, unused_tcp_port, monkeypatch, expired
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60) as b:
        c, j = b.controller, b.session.journal
        await owner_start(b)
        if expired:
            j.put("vendor_start_grant", j.get("vendor_start_grant") | {"expires": 0})
        grant = copy.deepcopy(j.get("vendor_start_grant"))
        mutations = [tuple(row) for row in j.db.execute("SELECT * FROM mutations")]
        status = await c.tick()
        assert status["cloud_start_pending"] is True and status["state"] == "degraded"
        before = j.db.total_changes, list(b.rig.bench.device_calls)
        detail = snapshot(c, status)
        assert "cloud_start_pending" in detail["blockers"]
        now = time.time()
        health = health_snapshot(healthy_status(now) | status | {"checked_at": now}, now)
        assert health["healthy"] is False and health["local_healthy"] is False
        assert "cloud_start_pending" in health["health_blockers"]
        assert (j.db.total_changes, b.rig.bench.device_calls) == before
        assert j.get("vendor_start_grant") == grant
        assert [tuple(row) for row in j.db.execute("SELECT * FROM mutations")] == mutations
        if not expired:
            await positive_flow(b, "cloud")
            bound = await healthy(c)
            assert bound["cloud_start_pending"] is False and j.get("vendor_start_grant")["local"] == j.active()


# Intent: a timed local/cloud test settles closed zero before a new or previously accepted Return can release.
# Why: Return must not switch intent to app while the final test observation still requires Pause.
@pytest.mark.parametrize("source", ["local", "cloud"])
@pytest.mark.parametrize("legacy_return", [False, True])
async def test_return_waits_for_timed_test_settlement(
    tmp_path, tls, unused_tcp_port, monkeypatch, source, legacy_return
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60) as b:
        c, j = b.controller, b.session.journal
        c.command(uuid.uuid4().hex, "start_test" if source == "local" else "arm_cloud_test", 0)
        if source == "local":
            await c.charging.settle()
        else:
            await owner_start(b)
        local = await positive_flow(b, source)
        await healthy(c)
        # Advance the live monotonic lease so the real independent deadline task owns Stop for both sources.
        assert c.charging.lease is not None
        c.charging.lease.deadline = time.monotonic() - 1
        c.charging.changed.set()
        await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 3)
        assert c.charging.state()["state"] == "stopping" and c.requests.intent().mode == "paused"
        reply = await b.rig.bench.call(
            b.charger, "test-closed-before-return", "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        assert reply[0] == 3
        b.rig.peer.permission_enabled = False
        await until(lambda: j.active() is None and j.next_delivery() is None)
        if c.pause_task is not None:
            await c.pause_task
        count = len(b.rig.bench.received_limits)
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        if legacy_return:
            # Durable input compatibility: an older service could already have accepted Return at this boundary.
            c.requests.accept(identifier, "app_control", revision)
            await c.tick()
            assert c.requests.receipt(identifier)["state"] == "accepted"
            assert c.requests.intent().mode == "paused" and c.charging.state()["state"] == "completed"
        else:
            with pytest.raises(RecoveryRequiredError, match="test settlement"):
                c.command(identifier, "app_control", revision)
            await healthy(c)
            assert c.charging.state()["state"] == "completed"
        assert all(limit == 0 for limit in b.rig.bench.received_limits[count:])
        if not legacy_return:
            c.command(identifier, "app_control", revision)
        await healthy(c)
        assert c.requests.receipt(identifier)["state"] == "completed" and c.requests.intent().mode == "app"
        assert b.rig.bench.firmware.effective_limit() == 6 and len(starts(b.rig)) == 1
