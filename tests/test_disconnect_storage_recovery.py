"""Actual transport retirement cannot depend on a successful durable generation update."""

import asyncio
import json
import sqlite3
import uuid
from contextlib import AsyncExitStack, closing, contextmanager

import pytest

from tests.test_managed_proxy import healthy, introduced
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Observe actual physical flow, cloud frames and an independent committed lifecycle before each retirement.
async def ordinary_session(rig, session, controller, charger, cycle, current):
    assert (await healthy(controller))["owner_control_enabled"]
    stamp = f"2026-01-0{cycle + 1}T00:00:00Z"
    start = START | {"idTag": "NATIVE_TIMER", "timestamp": stamp}
    assert (await rig.bench.call(charger, uuid.uuid4().hex, "Authorize", {"idTag": "NATIVE_TIMER"}))[0] == 3
    reply = await rig.bench.call(charger, uuid.uuid4().hex, "StartTransaction", start)
    assert reply[0] == 3
    local = reply[2]["transactionId"]
    await until(lambda: session.journal.vendor_id(local) is not None and session.journal.next_delivery() is None)
    vendor = session.journal.vendor_id(local)
    rig.peer.plan_session = (local, 9)
    rig.peer.paused, rig.peer.permission_enabled = False, True
    rig.bench.cp, rig.bench.connection = "C", "Charging"
    rig.bench.firmware.fire_plan(9)
    assert (await healthy(controller))["current_a"] == current
    assert rig.bench.firmware.current == current
    meter = {
        "connectorId": 1,
        "transactionId": local,
        "meterValue": [{"timestamp": stamp, "sampledValue": [{"value": "1100"}]}],
    }
    assert (await rig.bench.call(charger, uuid.uuid4().hex, "MeterValues", meter))[0] == 3
    stop = {"transactionId": local, "timestamp": stamp, "meterStop": 1100}
    assert (await rig.bench.call(charger, uuid.uuid4().hex, "StopTransaction", stop))[0] == 3
    rig.peer.permission_enabled = False
    await until(lambda: session.journal.active() is None and session.journal.next_delivery() is None)
    assert rig.bench.firmware.current == 0
    actual = [
        f for f in rig.bench.calls if f[0] == 2 and f[2] in ("StartTransaction", "MeterValues", "StopTransaction")
    ]
    assert len([f for f in actual if f[2:] == ["StartTransaction", start]]) == 1
    assert len([f for f in actual if f[2:] == ["MeterValues", meter | {"transactionId": vendor}]]) == 1
    assert len([f for f in actual if f[2:] == ["StopTransaction", stop | {"transactionId": vendor}]]) == 1
    with closing(sqlite3.connect(session.journal.path)) as committed:
        row = committed.execute("SELECT vendor,stop FROM transactions WHERE local=?", (local,)).fetchone()
        assert row[0] == vendor and json.loads(row[1]) == stop
    assert (await healthy(controller))["owner_control_enabled"]


# Fail the actual generation transaction without replacing a gateway, runtime or journal method.
@contextmanager
def failed_epoch(journal, boundary):
    db = journal.db
    if boundary == "begin":
        # Only real BEGIN authorisation fails; reads and unrelated non-transactional diagnostics remain usable.
        def authorizer(action, first, second, database, trigger):
            return (
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and first == "BEGIN" else sqlite3.SQLITE_OK
            )

        db.set_authorizer(authorizer)
    elif boundary == "statement":
        db.execute(
            "CREATE TRIGGER fail_epoch BEFORE INSERT ON meta WHEN NEW.key='connection_epoch' "
            "BEGIN SELECT RAISE(ABORT,'synthetic generation write failure'); END"
        )
    else:
        db.execute("CREATE TABLE epoch_parent(id INTEGER PRIMARY KEY)")
        db.execute("CREATE TABLE epoch_child(id INTEGER REFERENCES epoch_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        db.execute(
            "CREATE TRIGGER fail_epoch BEFORE INSERT ON meta WHEN NEW.key='connection_epoch' "
            "BEGIN INSERT INTO epoch_child VALUES(1); END"
        )
    try:
        yield
    finally:
        db.set_authorizer(None)
        db.execute("DROP TRIGGER IF EXISTS fail_epoch")
        if boundary == "commit":
            assert not db.in_transaction and db.execute("SELECT COUNT(*) FROM epoch_child").fetchone()[0] == 0
            db.execute("DROP TABLE epoch_child")
            db.execute("DROP TABLE epoch_parent")


# Intent: disconnect during real generation BEGIN/write/COMMIT failure retires Boot, controls and cloud liveness.
# Why: sender retirement and truthful status cannot be skipped when durable cleanup rolls back.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("boundary", ["begin", "statement", "commit"])
async def test_disconnect_storage_failure_recovers_twice(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, boundary
):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected) as b:
        rig, session, controller = b.rig, b.session, b.controller
        async with AsyncExitStack() as connections:
            for cycle in range(2):
                rig.bench.online = True
                charger = b.charger if cycle == 0 else await connections.enter_async_context(rig.charger(session))
                if cycle:
                    await introduced(rig, session, charger, "disconnect-recovery-" + str(cycle))
                assert (await healthy(controller))["owner_control_enabled"]
                await ordinary_session(rig, session, controller, charger, cycle, selected)
                epoch = session.gateway._charger_epoch
                failures = session.runtime.status.charger_fault.count
                with failed_epoch(session.journal, boundary):
                    rig.bench.online = False
                    await charger.close()
                    await until(lambda: session.gateway.charger.sender is None)
                    assert session.runtime.status.charger_fault.count > failures
                    assert session.runtime.status.charger_fault.stage == "charger_retirement"
                    assert not session.gateway.current_boot_seen(epoch)
                    assert not session.gateway.current_boot_seen(session.gateway._charger_epoch)
                    assert session.gateway.boot_observation is None
                    assert not session.gateway.controls.ready
                    assert session.runtime._charger is None
                    assert not session.runtime.status.charger_connected and not session.runtime.status.boot_observed
                    await until(lambda: not session.runtime.status.vendor_connected)
                    heartbeats = sum(frame[0] == 2 and frame[2] == "Heartbeat" for frame in rig.bench.calls)
                    await asyncio.sleep(1.1)
                    assert sum(frame[0] == 2 and frame[2] == "Heartbeat" for frame in rig.bench.calls) == heartbeats
                session.journal.put("storage-repaired", cycle)
                with closing(sqlite3.connect(session.journal.path)) as reader:
                    saved = reader.execute("SELECT value FROM meta WHERE key='connection_epoch'").fetchone()
                    assert json.loads(saved[0]) == epoch
                status = await controller.tick()
                assert not status["charger_connected"] and not status["boot_verified"]
                assert not status["owner_control_enabled"] and status["state"] == "starting"
            rig.bench.online = True
            charger = await connections.enter_async_context(rig.charger(session))
            await introduced(rig, session, charger, "disconnect-final-recovery")
            assert session.gateway._charger_epoch > epoch
            await ordinary_session(rig, session, controller, charger, 2, selected)
            assert session.journal.next_delivery() is None and session.gateway.controls.ready
