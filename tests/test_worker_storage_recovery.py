"""Live worker cleanup survives real SQLite refusal without repeating positive effects or replacing the service."""

import asyncio
import sqlite3
import uuid

import pytest

from tests.charge_rig import owner_rig
from tests.cloud_start_helpers import two_later_sessions
from tests.current_rig import adjustable
from tests.managed_rig import healthy, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.runtime_rig import until
from tests.test_managed_stop_reply_recovery import pause
from tests.tls_support import tls as tls


# Intent: a real Start preflight read can fail before any physical effect and still settle its receipt.
# Why: one failed worker must not fail the supervisor or turn an unsent Start into uncertain device work.
async def test_start_preflight_storage_refusal_settles(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        refused = []

        # Refuse only the candidate table statement, before the worker reaches any OCPP call.
        def authorize(action, table, column, database, trigger):
            if not refused and action == sqlite3.SQLITE_READ and table == "managed_current_requests":
                refused.append(table)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        j.db.set_authorizer(authorize)
        try:
            await asyncio.wait_for(asyncio.shield(c.charging.worker), 3)
        finally:
            j.db.set_authorizer(None)
        assert refused and c.requests.receipt(identifier)["state"] == "failed"
        assert c.charging.state()["failure"]["effects_started"] is False
        assert not starts(b.rig)
        assert (await healthy(c))["owner_control_enabled"]
        await pause(c)
        assert (await healthy(c))["local_hold_verified"]


# Intent: a cloud-arm receipt and its armed metadata commit together, with no live lease on a failed COMMIT.
# Why: a partial acceptance can arm a later cloud request even though the caller only sees an error.
async def test_cloud_arm_storage_is_atomic(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        j.db.execute("CREATE TABLE arm_parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE arm_child(id REFERENCES arm_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        for event in ("INSERT", "UPDATE"):
            j.db.execute(
                f"CREATE TEMP TRIGGER fail_arm_{event} AFTER {event} ON managed_requests "
                "WHEN NEW.action='arm_cloud_test' AND NEW.state='completed' "
                "BEGIN INSERT INTO arm_child VALUES(1); END"
            )
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        before = c.charging.state()
        with pytest.raises(sqlite3.IntegrityError):
            c.command(identifier, "arm_cloud_test", revision)
        assert c.charging.state() == before and c.requests.receipt(identifier)["state"] == "not_found"
        assert c.charging.lease is None and c.charging.timer is None
        for event in ("INSERT", "UPDATE"):
            j.db.execute(f"DROP TRIGGER fail_arm_{event}")
        receipt = c.command(identifier, "arm_cloud_test", revision)
        timer = c.charging.timer
        assert receipt["state"] == "completed" and receipt["result"] == {"armed": True, "charging_started": False}
        assert c.command(identifier, "arm_cloud_test", revision) == receipt and c.charging.timer is timer
        assert c.charging.state()["state"] == "armed" and not starts(b.rig)


# An actual SQLite trigger records separate cleanup attempts before refusing that write; no result is mocked.
def failing_cleanup(j, key, *, minimum_attempts=2, on_first_attempt=None):
    reached = asyncio.Event()
    attempts = []

    # Observe a genuine trigger execution without changing its real SQLite failure.
    def observed():
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1 and on_first_attempt is not None:
            on_first_attempt()
        if len(attempts) >= minimum_attempts:
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


# Intent: prepared-to-issued failure stays unsent and restricts zero before its optional diagnostic is repaired.
# Why: a diagnostic write cannot occupy the worker or foreground poll after critical settlement succeeds.
@pytest.mark.parametrize("tick_awaits", [False, True])
async def test_unsent_start_eventually_recovers_same_service(tmp_path, tls, unused_tcp_port, monkeypatch, tick_awaits):
    async with owner_rig(
        tmp_path, tls, unused_tcp_port, monkeypatch, startup=1, native_peer=InterruptedNativePeer
    ) as b:
        c, j = b.controller, b.session.journal
        original_session = b.session
        for cycle, failure in enumerate(("statement", "commit")):
            fail_boundary(j, "start", failure)
            reached, attempts = failing_cleanup(j, "managed_test_failure", minimum_attempts=1)
            identifier = uuid.uuid4().hex
            c.command(identifier, "start", c.requests.intent().revision)
            tick = asyncio.create_task(c.tick()) if tick_awaits else None
            await asyncio.wait_for(reached.wait(), 3)
            assert attempts and c.charging.worker is not None
            await asyncio.wait_for(asyncio.shield(c.charging.worker), 3)
            assert c.charging.state()["failure"]["id"] == identifier
            assert starts(b.rig) == []
            assert j.get("local_start_receipt")["stage"] in ("prepared", "unsent")
            # Real restrictive tasks drive zero while the diagnostic write remains continuously refused.
            await until(lambda: b.rig.bench.firmware.effective_limit() == 0)
            j.db.execute("DROP TRIGGER review_worker_boundary")
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")
            await asyncio.wait_for(asyncio.shield(c.charging.worker), 3)
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


# Intent: completion rollback restricts zero before an optional diagnostic can be repaired or another tick waits.
# Why: a diagnostic outage cannot leave the failed positive offer active or require replacement for later operation.
@pytest.mark.parametrize("tick_awaits", [False, True])
async def test_current_completion_eventually_recovers_same_service(
    tmp_path, tls, unused_tcp_port, monkeypatch, tick_awaits
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=InterruptedNativePeer) as b:
        c, j = b.controller, b.session.journal
        original_session = b.session
        for cycle, failure in enumerate(("statement", "commit")):
            fail_boundary(j, "current", failure)
            reached, attempts = failing_cleanup(j, "managed_current_failure", minimum_attempts=1)
            identifier = uuid.uuid4().hex
            before = len(b.rig.bench.device_calls)
            c.command(identifier, "set_current", c.requests.intent().revision, current_a=16)
            tick = asyncio.create_task(c.tick()) if tick_awaits else None
            await asyncio.wait_for(reached.wait(), 3)
            assert attempts and c.current.worker is not None
            await asyncio.wait_for(asyncio.shield(c.current.worker), 3)
            assert c.requests.receipt(identifier)["state"] == "uncertain"
            change = j.get("managed_current_change")
            assert change["state"] in ("uncertain", "cancelled") and change["failure"]["id"] == identifier
            if change["state"] == "cancelled":
                # The concurrent poll may already retire metadata under fresh Pause; its command stays unknown.
                assert tick is not None
                await asyncio.wait_for(tick, 3)
                c.require_paused(await c.native.read(closed=True))
                assert c.requests.receipt(identifier)["state"] == "uncertain"
            assert b.rig.bench.firmware.effective_limit() == 0
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
