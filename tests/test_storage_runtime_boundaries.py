"""Actual SQLite attachment and savepoint failures recover without replacing the engine's state."""

import asyncio
import json
import sqlite3
from contextlib import closing

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.native_history import NativeHistoryArchive, history_savepoint
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.server import serve

from tests.test_native_history_archive import row
from tests.test_prototype import START
from tests.test_prototype_runtime import Bench, until
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls


# Deny only the real vendor receipt retirement statement and retain the exact attempts for independent assertions.
def refuse_retirement(journal, failures):
    def authorizer(action, table, column, database, trigger):
        if action == sqlite3.SQLITE_DELETE and table == "inbound_receipts":
            failures.append((action, table))
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    journal.db.set_authorizer(authorizer)


# Complete one actual local/cloud lifecycle after recovery; cloud identifiers are translated by the real engine.
async def lifecycle(bench, charger, cycle):
    stamp = f"2026-01-0{cycle + 1}T00:00:00Z"
    start = START | {"timestamp": stamp, "meterStart": cycle * 100}
    token = f"attachment-start-{cycle}"
    await charger.send(build_call(token, "StartTransaction", start))
    response = decode(await charger.recv())
    assert response[0:2] == [3, token]
    local = response[2]["transactionId"]
    await until(lambda: bench.store.vendor_id(local) == 9001 and bench.store.next_delivery() is None)
    meter = {
        "connectorId": 1,
        "transactionId": local,
        "meterValue": [{"timestamp": stamp, "sampledValue": [{"value": str(cycle * 100 + 20)}]}],
    }
    await charger.send(build_call(f"attachment-meter-{cycle}", "MeterValues", meter))
    assert decode(await charger.recv())[0] == 3
    stop = {"transactionId": local, "timestamp": stamp, "meterStop": cycle * 100 + 30}
    await charger.send(build_call(f"attachment-stop-{cycle}", "StopTransaction", stop))
    assert decode(await charger.recv())[0] == 3
    await until(lambda: bench.store.next_delivery() is None and bench.store.active() is None)
    actual = [f for f in bench.calls if f[0] == 2 and f[2] in ("StartTransaction", "MeterValues", "StopTransaction")]
    assert actual[-3][2:] == ["StartTransaction", start]
    assert actual[-2][2:] == ["MeterValues", meter | {"transactionId": 9001}]
    assert actual[-1][2:] == ["StopTransaction", stop | {"transactionId": 9001}]
    with closing(sqlite3.connect(bench.store.path)) as reader:
        saved = reader.execute("SELECT vendor,stop FROM transactions WHERE local=?", (local,)).fetchone()
        assert saved[0] == 9001 and json.loads(saved[1]) == stop


# Intent: two repeated real receipt DELETE failures automatically reconnect and register, then deliver later lifecycles.
# Why: a pump-only failure test misses attachment before its cleanup handler and cannot establish runner survival.
async def test_vendor_attachment_storage_failure_twice(tmp_path, tls, admission):
    bench = Bench(tmp_path / "journal.db", tls, admission)
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            runtime = await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")
                for cycle in range(2):
                    failures = []
                    previous_connections = len(bench.connections)
                    refuse_retirement(bench.store, failures)
                    try:
                        await bench.connections[-1].close()
                        await until(lambda: bool(failures))
                        assert not runtime._runner.done()
                        assert not runtime.status.vendor_connected
                        assert bench.gateway.vendor.sender is None
                        assert bench.gateway.vendor_registration != "Accepted"
                        await until(lambda: len(failures) >= 2)
                        # Local reception is still independent while queued harmless work cannot reach the cloud.
                        token = f"queued-status-{cycle}"
                        payload = {"connectorId": 1, "errorCode": "NoError", "status": "Available"}
                        await charger.send(build_call(token, "StatusNotification", payload))
                        assert decode(await charger.recv())[0:2] == [3, token]
                        assert bench.store.next_delivery() is not None
                    finally:
                        bench.store.db.set_authorizer(None)
                    await until(
                        lambda: bench.gateway.vendor_registration == "Accepted" and bench.store.next_delivery() is None
                    )
                    assert len(bench.connections) > previous_connections
                    assert runtime._runner is not None and not runtime._runner.done()
                    assert runtime.status.vendor_connected and runtime.status.charger_connected
                    await lifecycle(bench, charger, cycle)
    finally:
        bench.store.db.set_authorizer(None)
        await bench.close()


# Install a real deferred FK violation at the archive insert; only transaction completion is refused.
def defer_archive_commit(journal):
    journal.db.execute("CREATE TABLE fail_history_parent(id INTEGER PRIMARY KEY)")
    journal.db.execute(
        "CREATE TABLE fail_history_child(id REFERENCES fail_history_parent(id) DEFERRABLE INITIALLY DEFERRED)"
    )
    journal.db.execute(
        "CREATE TRIGGER fail_history_commit AFTER INSERT ON native_history_records "
        "BEGIN INSERT INTO fail_history_child VALUES(1); END"
    )


# Intent: actual outer history COMMIT failure publishes nothing and permits later transactions on the same connection.
# Why: reopening the database hides the poisoned connection which previously blocked the protective shutdown writes.
def test_history_commit_failure_cleans_connection(tmp_path):
    journal = Journal(tmp_path / "journal.db", "HISTORY_STORAGE001")
    try:
        archive = NativeHistoryArchive(journal)
        defer_archive_commit(journal)
        statements = []
        journal.db.set_trace_callback(statements.append)
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            archive.observe([], [row(1001)], allow_missing=False)
        assert any(s in ("COMMIT", "RELEASE native_history_observation") for s in statements)
        assert not journal.db.in_transaction
        assert journal.get("native_history_summary") is None
        assert journal.get("native_history_archive_binding") is None
        assert journal.db.execute("SELECT COUNT(*) FROM native_history_records").fetchone()[0] == 0
        assert journal.db.execute("SELECT COUNT(*) FROM fail_history_child").fetchone()[0] == 0
        journal.db.execute("DROP TRIGGER fail_history_commit")
        with journal.atomic():
            journal.put("same_connection_repaired", True)
        assert archive.observe([], [row(1001)], allow_missing=False)["archived_records"] == 1
        with closing(sqlite3.connect(journal.path)) as reader:
            assert json.loads(
                reader.execute("SELECT value FROM meta WHERE key='same_connection_repaired'").fetchone()[0]
            )
    finally:
        journal.close()


# Intent: one actual nested RELEASE failure discards only history work and preserves the caller's open transaction.
# Why: clearing the whole connection here would erase an enclosing operator recovery's atomic ownership boundary.
def test_nested_history_release_failure_preserves_outer_boundary(tmp_path):
    journal = Journal(tmp_path / "journal.db", "HISTORY_STORAGE002")
    failures = []
    try:
        with journal.atomic():
            journal.put("outer_before", {"preserve": True})

            # One real SQLite SAVEPOINT RELEASE is refused; cleanup operations are allowed after that refusal.
            def authorizer(action, first, second, database, trigger):
                if action == sqlite3.SQLITE_SAVEPOINT and first == "RELEASE" and not failures:
                    failures.append((first, second))
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            journal.db.set_authorizer(authorizer)
            try:
                with pytest.raises(sqlite3.DatabaseError):
                    with history_savepoint(journal):
                        journal.put("nested_uncommitted", True)
                assert failures == [("RELEASE", "native_history_observation")]
                assert journal.db.in_transaction
                assert journal.get("nested_uncommitted") is None
                assert journal.get("outer_before") == {"preserve": True}
            finally:
                journal.db.set_authorizer(None)
            journal.put("outer_after", True)
        assert not journal.db.in_transaction
        with closing(sqlite3.connect(journal.path)) as reader:
            assert (
                reader.execute("SELECT COUNT(*) FROM meta WHERE key IN ('outer_before','outer_after')").fetchone()[0]
                == 2
            )
            assert reader.execute("SELECT value FROM meta WHERE key='nested_uncommitted'").fetchone() is None
    finally:
        journal.close()


# Intent: failure inside history observation retains enclosing writes while rolling back every inner observation write.
# Why: protecting RELEASE must preserve the existing savepoint-body rollback contract.
def test_nested_history_body_failure_preserves_outer_boundary(tmp_path):
    journal = Journal(tmp_path / "journal.db", "HISTORY_STORAGE003")
    try:
        with journal.atomic():
            journal.put("outer_before", True)
            journal.db.execute(
                "CREATE TEMP TRIGGER fail_nested_body BEFORE INSERT ON meta WHEN NEW.key='refused_history' "
                "BEGIN SELECT RAISE(ABORT,'actual history write refused'); END"
            )
            with pytest.raises(sqlite3.IntegrityError, match="actual history write refused"):
                with history_savepoint(journal):
                    journal.put("nested_uncommitted", True)
                    journal.put("refused_history", True)
            assert journal.db.in_transaction and journal.get("outer_before") is True
            assert journal.get("nested_uncommitted") is None
            journal.put("outer_after", True)
        assert not journal.db.in_transaction and journal.get("outer_after") is True
    finally:
        journal.close()


# Intent: local storage refusal before first registration retries without inventing a failed cloud introduction.
# Why: the offline-Pause authority predicate consumes actual vendor-failure epochs, so SQL failure must stay distinct.
@pytest.mark.parametrize("boundary", ["boot-read", "attachment"])
async def test_initial_vendor_storage_refusal_is_not_a_cloud_failure(tmp_path, tls, admission, monkeypatch, boundary):
    original_connect = sqlite3.connect

    # Disabling SQLite's statement cache makes every real SELECT authorization visible without replacing the journal.
    def connect(*args, **kwargs):
        kwargs["cached_statements"] = 0
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    bench = Bench(tmp_path / "journal.db", tls, admission)
    failures = []
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            runtime = await bench.start(cloud.sockets[0].getsockname()[1])

            # Refuse only the real runner's storage statement; independent receiver and control writes remain genuine.
            def authorizer(action, table, column, database, trigger):
                targeted = (
                    action == sqlite3.SQLITE_READ and table == "meta" and column == "value"
                    if boundary == "boot-read"
                    else action == sqlite3.SQLITE_DELETE and table == "inbound_receipts"
                )
                if asyncio.current_task() is runtime._runner and targeted:
                    failures.append((action, table, column))
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            bench.store.db.set_authorizer(authorizer)
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: len(failures) >= 2 or runtime._runner.done())
                assert not runtime._runner.done()
                assert not runtime.status.vendor_connected and bench.gateway.vendor.sender is None
                assert runtime.status.vendor_fault.count == 0
                assert runtime.status.vendor_failed_charger_epoch is None
                assert runtime.status.last_failure in ("DatabaseError", "OperationalError")
                if boundary == "boot-read":
                    assert runtime.status.vendor_attempts == 0 and not bench.connections
                else:
                    assert runtime.status.vendor_attempts >= 2 and len(bench.connections) >= 2
                await charger.send(build_call("storage-still-local", "Heartbeat", {}))
                assert decode(await charger.recv())[0:2] == [3, "storage-still-local"]
                bench.store.db.set_authorizer(None)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")
                assert runtime.status.vendor_fault.count == 0 and runtime.status.vendor_failed_charger_epoch is None
                await lifecycle(bench, charger, 0)
    finally:
        bench.store.db.set_authorizer(None)
        await bench.close()


# Intent: SQLite's full rollback preserves the actual failed statement rather than a missing-savepoint error.
# Why: an I/O error or RAISE(ROLLBACK) may already have removed every savepoint and the enclosing transaction.
def test_history_sqlite_automatic_rollback_keeps_original_failure(tmp_path):
    journal = Journal(tmp_path / "journal.db", "HISTORY_STORAGE004")
    try:
        journal.db.execute(
            "CREATE TEMP TRIGGER fail_rollback_body BEFORE INSERT ON meta WHEN NEW.key='refused_history' "
            "BEGIN SELECT RAISE(ROLLBACK,'actual original rollback failure'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="actual original rollback failure"):
            with journal.atomic():
                journal.put("outer_uncommitted", True)
                with history_savepoint(journal):
                    journal.put("nested_uncommitted", True)
                    journal.put("refused_history", True)
        assert not journal.db.in_transaction
        assert journal.get("outer_uncommitted") is None and journal.get("nested_uncommitted") is None
        with journal.atomic():
            journal.put("repaired", True)
        assert journal.get("repaired") is True
    finally:
        journal.close()


# Intent: a deferred constraint inside a nested history observation fails only the caller's actual outer COMMIT.
# Why: the helper must not commit or partially preserve any enclosing operator-recovery writes.
def test_nested_history_deferred_commit_retains_callers_atomic_boundary(tmp_path):
    journal = Journal(tmp_path / "journal.db", "HISTORY_STORAGE005")
    try:
        archive = NativeHistoryArchive(journal)
        defer_archive_commit(journal)
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            with journal.atomic():
                journal.put("outer_uncommitted", True)
                assert archive.observe([], [row(1001)], allow_missing=False)["archived_records"] == 1
                assert journal.db.in_transaction
                journal.put("outer_after", True)
        assert not journal.db.in_transaction
        assert journal.get("outer_uncommitted") is None and journal.get("outer_after") is None
        assert journal.get("native_history_summary") is None
        assert journal.db.execute("SELECT COUNT(*) FROM native_history_records").fetchone()[0] == 0
        journal.db.execute("DROP TRIGGER fail_history_commit")
        with journal.atomic():
            journal.put("repaired", True)
        assert journal.get("repaired") is True
    finally:
        journal.close()


# Intent: a failed savepoint rollback keeps the original write error primary and propagates to the outer rollback.
# Why: cleanup refusal cannot silently accept a partial observation or conceal the failure which triggered shutdown.
def test_nested_history_cleanup_refusal_propagates_original_to_outer_owner(tmp_path):
    journal = Journal(tmp_path / "journal.db", "HISTORY_STORAGE006")
    failures = []
    try:
        journal.db.execute(
            "CREATE TEMP TRIGGER fail_nested_original BEFORE INSERT ON meta WHEN NEW.key='refused_history' "
            "BEGIN SELECT RAISE(ABORT,'original history statement refused'); END"
        )

        # Deny one actual savepoint rollback; the enclosing transaction's full ROLLBACK remains available.
        def authorizer(action, first, second, database, trigger):
            if action == sqlite3.SQLITE_SAVEPOINT and first == "ROLLBACK" and not failures:
                failures.append((first, second))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        journal.db.set_authorizer(authorizer)
        with pytest.raises(sqlite3.IntegrityError, match="original history statement refused") as caught:
            with journal.atomic():
                journal.put("outer_uncommitted", True)
                with history_savepoint(journal):
                    journal.put("nested_uncommitted", True)
                    journal.put("refused_history", True)
        assert failures == [("ROLLBACK", "native_history_observation")]
        assert isinstance(caught.value.__cause__, sqlite3.DatabaseError)
        assert not journal.db.in_transaction
        assert journal.get("outer_uncommitted") is None and journal.get("nested_uncommitted") is None
        journal.db.set_authorizer(None)
        with journal.atomic():
            journal.put("repaired", True)
    finally:
        journal.db.set_authorizer(None)
        journal.close()
