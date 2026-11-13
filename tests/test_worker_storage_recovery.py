"""Live worker cleanup survives real SQLite refusal without repeating positive effects or replacing the service."""

import asyncio
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import two_later_sessions
from tests.test_managed_current import adjustable
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_stop_reply_recovery import pause
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# An actual SQLite trigger records separate cleanup attempts before refusing that write; no result is mocked.
def failing_cleanup(j, key):
    reached = asyncio.Event()
    attempts = []

    # Observe a genuine trigger execution without changing its real SQLite failure.
    def observed():
        attempts.append(len(attempts) + 1)
        if len(attempts) >= 2:
            reached.set()
        return 0

    j.db.create_function("review_cleanup_attempt", 0, observed)
    j.db.execute(
        "CREATE TEMP TRIGGER review_cleanup_unavailable BEFORE INSERT ON meta "
        "WHEN NEW.key='" + key + "' BEGIN SELECT review_cleanup_attempt(); "
        "SELECT RAISE(ABORT,'independent cleanup storage unavailable'); END"
    )
    return reached, attempts


# Only the real target dispatch/completion transaction fails; a deferred reference exercises actual COMMIT.
def fail_boundary(j, boundary, failure):
    if failure == "statement":
        effect = "SELECT RAISE(ABORT,'independent worker boundary storage refusal');"
    else:
        j.db.execute("CREATE TABLE review_worker_parent(id INTEGER PRIMARY KEY)")
        j.db.execute(
            "CREATE TABLE review_worker_child(id REFERENCES review_worker_parent(id) DEFERRABLE INITIALLY DEFERRED)"
        )
        effect = "INSERT INTO review_worker_child VALUES(123);"
    if boundary == "start":
        target = (
            "BEFORE INSERT ON meta WHEN NEW.key='local_start_receipt' AND json_extract(NEW.value,'$.stage')='issued'"
        )
    else:
        target = "BEFORE UPDATE OF state ON managed_requests WHEN NEW.action='set_current' AND NEW.state='completed'"
    j.db.execute("CREATE TEMP TRIGGER review_worker_boundary " + target + " BEGIN " + effect + " END")


# Intent: actual prepared-to-issued failure stays unsent and retries cleanup while storage is still refused.
# Why: a finished worker or a foreground wait must not require replacement to complete later Pause/Return.
@pytest.mark.parametrize("tick_awaits", [False, True])
async def test_unsent_start_eventually_recovers_same_service(tmp_path, tls, unused_tcp_port, monkeypatch, tick_awaits):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=1) as b:
        c, j = b.controller, b.session.journal
        original_session = b.session
        for cycle, failure in enumerate(("statement", "commit")):
            fail_boundary(j, "start", failure)
            reached, attempts = failing_cleanup(j, "managed_test_failure")
            identifier = uuid.uuid4().hex
            c.command(identifier, "start", c.requests.intent().revision)
            tick = asyncio.create_task(c.tick()) if tick_awaits else None
            await asyncio.wait_for(reached.wait(), 3)
            assert len(attempts) >= 2 and c.tests.worker is not None and not c.tests.worker.done()
            assert starts(b.rig) == []
            assert j.get("local_start_receipt")["stage"] in ("prepared", "unsent")
            # Only the real independent deadline/Stop task drives zero while failure cleanup remains refused.
            await until(lambda: b.rig.bench.firmware.effective_limit() == 0)
            j.db.execute("DROP TRIGGER review_worker_boundary")
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")
            await asyncio.wait_for(asyncio.shield(c.tests.worker), 3)
            if tick is not None:
                await asyncio.wait_for(tick, 3)
            if c.pause_task is not None:
                await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
            await pause(c)
            assert (await healthy(c))["local_hold_verified"]
            assert j.get("local_start_receipt")["stage"] == "unsent"
            assert j.get("control")["start_pending"] is False
            assert c.requests.receipt(identifier)["state"] == "failed"
            assert starts(b.rig) == [] and b.rig.bench.firmware.effective_limit() == 0
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            await two_later_sessions(b, selected=6, original_starts=0, first_minute=10 + cycle * 10)
            assert b.session is original_session and not b.session.closed and j.next_delivery() is None


# Intent: completion rollback is retried as restrictive failure settlement, never as another positive profile write.
# Why: both already-finished and concurrently awaited worker orderings must restore ordinary later operation.
@pytest.mark.parametrize("tick_awaits", [False, True])
async def test_current_completion_eventually_recovers_same_service(
    tmp_path, tls, unused_tcp_port, monkeypatch, tick_awaits
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        original_session = b.session
        for cycle, failure in enumerate(("statement", "commit")):
            fail_boundary(j, "current", failure)
            reached, attempts = failing_cleanup(j, "managed_current_failure")
            identifier = uuid.uuid4().hex
            before = len(b.rig.bench.device_calls)
            c.command(identifier, "set_current", c.requests.intent().revision, current_a=16)
            tick = asyncio.create_task(c.tick()) if tick_awaits else None
            await asyncio.wait_for(reached.wait(), 3)
            assert len(attempts) >= 2 and c.current.worker is not None and not c.current.worker.done()
            assert c.requests.receipt(identifier)["state"] == "accepted"
            assert j.get("managed_current_change")["state"] == "pending"
            assert b.rig.bench.firmware.effective_limit() == 16
            actual_positive = [
                f
                for f in b.rig.bench.device_calls[before:]
                if f[2] == "SetChargingProfile"
                and f[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 16
            ]
            assert len(actual_positive) == 1
            j.db.execute("DROP TRIGGER review_worker_boundary")
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")
            await asyncio.wait_for(asyncio.shield(c.current.worker), 3)
            if tick is not None:
                await asyncio.wait_for(tick, 3)
            if c.pause_task is not None:
                await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
            await pause(c)
            assert (await healthy(c))["local_hold_verified"]
            assert c.current.pending() is False and b.rig.bench.firmware.effective_limit() == 0
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            assert c.requests.receipt(identifier)["state"] == "failed"
            assert j.get("managed_current_change")["state"] == "cancelled"
            await two_later_sessions(b, selected=6, original_starts=0, first_minute=10 + cycle * 10)
            assert b.session is original_session and not b.session.closed and j.next_delivery() is None
