"""Real lost replies and failed SQLite cleanup must leave the cloud supervisor recoverable."""

import asyncio
import sqlite3
import uuid
from contextlib import contextmanager

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from websockets.asyncio.server import serve

from tests.test_prototype import START
from tests.test_prototype_runtime import Bench, until
from tests.test_prototype_runtime import admission as admission
from tests.test_prototype_runtime import tls as tls


class LostDeliveryBench(Bench):
    """Only the independent cloud drops an explicitly armed response, retaining its actual wire body."""

    # Keep all protocol, scheduling and persistence in the real gateway and runtime.
    def __init__(self, *args):
        super().__init__(*args)
        self.drop_action = None
        self.arrived = asyncio.Event()
        self.dropped = None

    # Answer every unrelated real RPC normally so only one selected delivery loses its response.
    async def vendor(self, connection):
        self.connections.append(connection)
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            if frame[2] == self.drop_action:
                self.drop_action, self.dropped = None, frame
                self.arrived.set()
                continue
            if frame[2] == "BootNotification":
                answer = {"status": "Accepted", "interval": 1, "currentTime": "2030-01-01T00:00:00Z"}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": "2030-01-01T00:00:00Z"}
            elif frame[2] == "StartTransaction":
                answer = {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))


# Exercise actual transaction entry, update and deferred commit failures without replacing a journal method.
@contextmanager
def refused_requeue(journal, token, boundary):
    db, refused = journal.db, []
    if boundary == "begin":
        # Count real denied transaction admissions, leaving reads available to the receiver and supervisor.
        def authorizer(action, first, second, database, trigger):
            if action == sqlite3.SQLITE_TRANSACTION and first == "BEGIN":
                refused.append(boundary)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorizer)
    else:
        db.create_function("witness_requeue", 0, lambda: refused.append(boundary) or 1)
        if boundary == "commit":
            db.execute("CREATE TABLE requeue_parent(id INTEGER PRIMARY KEY)")
            db.execute(
                "CREATE TABLE requeue_child(id INTEGER REFERENCES requeue_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
            effect = "INSERT INTO requeue_child VALUES(1);"
        else:
            effect = "SELECT RAISE(ABORT,'synthetic requeue refusal');"
        assert token.startswith("e-") and len(token) == 34 and all(c in "0123456789abcdef" for c in token[2:])
        db.execute(
            "CREATE TRIGGER refuse_requeue BEFORE UPDATE OF state ON outbox WHEN OLD.token='"
            + token
            + "' AND NEW.state='queued' BEGIN SELECT witness_requeue(); "
            + effect
            + " END"
        )
    try:
        yield refused
    finally:
        db.set_authorizer(None)
        db.execute("DROP TRIGGER IF EXISTS refuse_requeue")
        assert not db.in_transaction
        if boundary == "commit":
            assert db.execute("SELECT COUNT(*) FROM requeue_child").fetchone()[0] == 0
            db.execute("DROP TABLE requeue_child")
            db.execute("DROP TABLE requeue_parent")


# Receive the genuine charger response without borrowing any internal gateway correlation or persistence result.
async def charger_call(charger, action, payload):
    token = uuid.uuid4().hex
    await charger.send(build_call(token, action, payload))
    reply = decode(await asyncio.wait_for(charger.recv(), 2))
    assert reply[:2] == [3, token]
    return reply


# Intent: lost routine/Stop responses recover on the same live runtime despite actual cleanup SQL failures.
# Why: cancellation must reach retirement, while only an unanswered non-Start head may be retried automatically.
@pytest.mark.parametrize("action", ["StatusNotification", "StopTransaction"])
@pytest.mark.parametrize("route", ["timeout", "disconnect"])
@pytest.mark.parametrize("boundary", ["begin", "statement", "commit"])
async def test_unanswered_delivery_retries_after_cleanup_storage_heals(
    tmp_path, tls, admission, action, route, boundary
):
    bench = LostDeliveryBench(tmp_path / "journal.db", tls, admission)
    bench.gateway.vendor.timeout = 0.4 if route == "timeout" else 5
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            runtime = await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")
                for cycle in range(2):
                    if action == "StopTransaction":
                        started = await charger_call(
                            charger, "StartTransaction", START | {"timestamp": f"2026-01-0{cycle + 1}T00:00:00Z"}
                        )
                        local = started[2]["transactionId"]
                        await until(lambda: bench.store.next_delivery() is None)
                        payload = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
                    else:
                        payload = {"connectorId": 1, "status": "Available", "errorCode": "NoError"}
                    bench.arrived.clear()
                    bench.drop_action = action
                    await charger_call(charger, action, payload)
                    await asyncio.wait_for(bench.arrived.wait(), 2)
                    head = bench.store.next_delivery()
                    assert head is not None and head.state == "inflight" and head.token == bench.dropped[1]
                    original = bench.dropped
                    with refused_requeue(bench.store, head.token, boundary) as refusals:
                        if route == "disconnect":
                            await bench.connections[-1].close()
                        await until(lambda: bool(refusals))
                        assert not runtime._runner.done()
                        assert bench.store.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 0
                    await charger_call(charger, "Heartbeat", {})
                    await until(lambda: bench.store.next_delivery() is None and runtime.status.vendor_connected)
                    copies = [f for f in bench.calls if f[0] == 2 and f[1] == head.token]
                    assert copies == [original, original]
                    assert runtime._runner is not None and not runtime._runner.done()
                    assert runtime.status.charger_connected and bench.gateway.vendor_registration == "Accepted"
                assert runtime.status.reconnects >= 3
    finally:
        await bench.close()


# Intent: a lost Start with the same failed cleanup must remain quarantined after storage and transport recover.
# Why: at-least-once Stop/report retry rules cannot authorize another Start or invent its vendor mapping.
@pytest.mark.parametrize("route", ["timeout", "disconnect"])
async def test_unknown_start_is_not_replayed_after_cleanup_failure(tmp_path, tls, admission, route):
    bench = LostDeliveryBench(tmp_path / "journal.db", tls, admission)
    bench.gateway.vendor.timeout = 0.4 if route == "timeout" else 5
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            runtime = await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")
                bench.drop_action = "StartTransaction"
                await charger_call(charger, "StartTransaction", START)
                await asyncio.wait_for(bench.arrived.wait(), 2)
                head = bench.store.next_delivery()
                assert head is not None and head.state == "inflight"
                refusals = []
                bench.store.db.create_function("witness_start_cleanup", 0, lambda: refusals.append(1) or 1)
                bench.store.db.execute(
                    "CREATE TRIGGER refuse_start_cleanup BEFORE UPDATE OF state ON outbox "
                    "WHEN NEW.state='ambiguous' BEGIN SELECT witness_start_cleanup(); "
                    "SELECT RAISE(ABORT,'synthetic Start cleanup refusal'); END"
                )
                try:
                    if route == "disconnect":
                        await bench.connections[-1].close()
                    await until(lambda: bool(refusals))
                finally:
                    bench.store.db.execute("DROP TRIGGER refuse_start_cleanup")
                await until(lambda: runtime.status.delivery_blocked_cause == "reconciliation")
                await charger_call(charger, "Heartbeat", {})
                assert bench.store.next_delivery().state == "ambiguous"
                assert bench.store.vendor_id(head.transaction) is None
                assert len([f for f in bench.calls if f[0] == 2 and f[1] == head.token]) == 1
                assert not runtime._runner.done() and runtime.status.charger_connected
    finally:
        await bench.close()
