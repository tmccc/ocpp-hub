"""Independent SQLite trace and exact-response negative controls for the storage/FIFO driver."""

import json
import sqlite3
from types import SimpleNamespace

import pytest

from tests_model.test_storage_fifo_sequences import (
    CHILD,
    install_failure,
    require_ack_rollback,
    require_saved_stop_reply,
    rolled_back_ack_transactions,
)

TOKEN = "e-" + "a" * 32
OTHER = "e-" + "b" * 32
DELETE = "DELETE FROM outbox WHERE token='" + TOKEN + "'"


# Intent: a genuine reply-save COMMIT must not be mistaken for the later failing ACK boundary.
# Why: trace COMMIT precedes execution and can itself fail, so only the exact DELETE rollback group proves the cut.
@pytest.mark.parametrize("fault", ["write", "commit"])
def test_storage_fifo_witness_uses_real_sqlite_ack_boundary(tmp_path, fault):
    db = sqlite3.connect(tmp_path / "trace.db", isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("CREATE TABLE outbox(token TEXT PRIMARY KEY)")
        db.execute("CREATE TABLE delivery_replies(token TEXT PRIMARY KEY REFERENCES outbox ON DELETE CASCADE)")
        db.execute("INSERT INTO outbox VALUES(?)", (TOKEN,))
        install_failure(db, SimpleNamespace(cut="ack", sqlite_fault=fault), TOKEN)
        trace = []
        db.set_trace_callback(trace.append)
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO delivery_replies VALUES(?)", (TOKEN,))
        db.execute("COMMIT")
        with pytest.raises(sqlite3.IntegrityError) as error:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(DELETE)
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
        expected = sqlite3.SQLITE_CONSTRAINT_TRIGGER if fault == "write" else sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
        assert error.value.sqlite_errorcode == expected
        assert len(require_ack_rollback(trace, TOKEN, fault)) == 1
        with pytest.raises(AssertionError):
            require_ack_rollback(trace, TOKEN, "write" if fault == "commit" else "commit")
        assert db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 1
        assert not db.in_transaction
        if fault == "commit":
            assert db.execute(f"SELECT COUNT(*) FROM {CHILD}").fetchone()[0] == 0
    finally:
        db.close()


# Intent: neither a saved response, foreign rollback, successful ACK nor unfinished ACK proves target failure.
# Why: these plausible traces must not permit storage_failures_observed to pass without an actual ACK rollback.
@pytest.mark.parametrize(
    "trace",
    [
        ["BEGIN IMMEDIATE", "INSERT INTO delivery_replies VALUES('x')", "COMMIT", "BEGIN IMMEDIATE", "ROLLBACK"],
        ["BEGIN IMMEDIATE", DELETE.replace(TOKEN, OTHER), "ROLLBACK"],
        ["BEGIN IMMEDIATE", DELETE, "COMMIT"],
        ["BEGIN IMMEDIATE", DELETE],
    ],
)
def test_storage_fifo_witness_rejects_non_ack_failure(trace):
    assert not rolled_back_ack_transactions(trace, TOKEN)
    with pytest.raises(AssertionError):
        require_ack_rollback(trace, TOKEN, "write")


# Intent: each saved-response identity component is necessary and the independent wire Frame must match.
# Why: an unrelated or changed response cannot serve as restart evidence for the retained Stop.
@pytest.mark.parametrize("field", ["token", "action", "payload", "local", "answer"])
def test_storage_fifo_saved_reply_rejects_changed_identity(field):
    db = sqlite3.connect(":memory:", isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        db.execute("CREATE TABLE delivery_replies(token TEXT,action TEXT,payload TEXT,local INTEGER,answer TEXT)")
        frame = [3, TOKEN, {}]
        head = {"token": TOKEN, "payload": '{"transactionId":9000001}', "local": 9000001}
        row = {
            "token": TOKEN,
            "action": "StopTransaction",
            "payload": head["payload"],
            "local": 9000001,
            "answer": json.dumps(frame),
        }
        db.execute("INSERT INTO delivery_replies VALUES(?,?,?,?,?)", tuple(row.values()))
        assert require_saved_stop_reply(db, head, frame) == row
        changes = {
            "token": OTHER,
            "action": "MeterValues",
            "payload": "{}",
            "local": 9000002,
            "answer": json.dumps([3, TOKEN, {"changed": True}]),
        }
        db.execute(f"UPDATE delivery_replies SET {field}=?", (changes[field],))
        with pytest.raises(AssertionError):
            require_saved_stop_reply(db, head, frame)
    finally:
        db.close()
