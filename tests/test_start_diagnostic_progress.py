"""A refused optional Start diagnostic cannot occupy native/status supervision after independent zero."""

import asyncio
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import two_later_sessions
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import fail_boundary, failing_cleanup


# Intent: real pre-dispatch SQL and persistent diagnostic refusal retain zero while native loss is supervised.
# Why: the independent deadline's zero does not excuse indefinitely stale status or a recovery needing replacement.
@pytest.mark.parametrize("failure", ["statement", "commit"])
async def test_start_diagnostic_refusal_does_not_block_native_recovery(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=1) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        first = await healthy(c)
        fail_boundary(j, "start", failure)
        reached, attempts = failing_cleanup(j, "managed_test_failure", minimum_attempts=1)
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        worker = c.tests.worker
        try:
            await asyncio.wait_for(reached.wait(), 3)
            assert attempts and starts(b.rig) == []
            assert j.get("local_start_receipt")["stage"] in ("prepared", "unsent")
            await until(lambda: b.rig.bench.firmware.effective_limit() == 0)
            # Only the native peer fails; neither SQL fixture is removed to help ordinary foreground supervision.
            peer.unavailable = True
            for _ in range(3):
                status = await asyncio.wait_for(c.tick(), 1)
                assert status["checked_at"] > first["checked_at"]
                assert status["state"] != "healthy" and b.rig.bench.firmware.effective_limit() == 0
            assert peer.failed_reads.qsize() >= 3
            assert worker is not None and worker.done() and not worker.cancelled()
            assert not j.get("managed_control_fault")
            peer.unavailable = False
            restored = await healthy(c)
            assert restored["local_hold_verified"] and b.rig.bench.firmware.effective_limit() == 0
            assert attempts and j.db.execute(
                "SELECT 1 FROM sqlite_temp_master WHERE type='trigger' AND name='review_cleanup_unavailable'"
            ).fetchone()
            j.db.execute("DROP TRIGGER review_worker_boundary")
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            assert (await healthy(c))["local_hold_verified"]
            assert c.requests.receipt(identifier)["state"] == "failed"
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            await two_later_sessions(b, selected=6, original_starts=0)
            assert j.next_delivery() is None and not starts(b.rig)
        finally:
            peer.unavailable = False
            j.db.execute("DROP TRIGGER IF EXISTS review_worker_boundary")
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
            if worker is not None:
                await asyncio.wait_for(asyncio.gather(worker, return_exceptions=True), 3)
