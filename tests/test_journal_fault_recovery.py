"""Real SQLite transaction, storage exhaustion and writer contention failures retain durable recovery semantics."""

import sqlite3

import pytest
from ocpp_hub_core.prototype.store import Journal


# Intent: SQLite's automatic rollback on storage exhaustion preserves the original error and every prior fact.
# Why: attempting a second rollback must not mask SQLITE_FULL or falsely leave a partial committed transition.
def test_actual_sqlite_full_preserves_cause_and_can_recover(tmp_path):
    j = Journal(tmp_path / "bounded.db", "synthetic")
    try:
        j.put("preserved", {"before": True})
        pages = j.db.execute("PRAGMA page_count").fetchone()[0]
        j.db.execute("PRAGMA max_page_count=" + str(pages))
        with pytest.raises(sqlite3.OperationalError) as error:
            with j.atomic():
                j.put("atomic-first", True)
                j.put("exhaust", "x" * (256 * 1024))
        assert error.value.sqlite_errorcode == sqlite3.SQLITE_FULL
        assert not j.db.in_transaction and j.get("atomic-first") is None and j.get("exhaust") is None
        assert j.get("preserved") == {"before": True}
        j.db.execute("PRAGMA max_page_count=1000000")
        with j.atomic():
            j.put("atomic-first", "recovered")
        assert j.get("atomic-first") == "recovered"
    finally:
        j.close()


# Intent: a genuine deferred constraint fails at COMMIT and rolls back all real changes before a successful retry.
# Why: statement-abort tests alone do not establish commit-time atomicity.
def test_actual_commit_failure_rolls_back_and_retries(tmp_path):
    j = Journal(tmp_path / "commit.db", "synthetic")
    try:
        j.db.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)")
        with pytest.raises(sqlite3.IntegrityError) as error:
            with j.atomic():
                j.put("commit-transition", True)
                j.db.execute("INSERT INTO child VALUES(1)")
                assert j.db.in_transaction and j.get("commit-transition") is True
        assert error.value.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
        assert not j.db.in_transaction and j.get("commit-transition") is None
        assert j.db.execute("SELECT COUNT(*) FROM child").fetchone()[0] == 0
        j.db.execute("INSERT INTO parent VALUES(1)")
        with j.atomic():
            j.put("commit-transition", True)
            j.db.execute("INSERT INTO child VALUES(1)")
        assert j.get("commit-transition") is True
    finally:
        j.close()


# Intent: real writer contention leaves both owners' transaction state intact and permits later ordinary commit.
# Why: BEGIN failure must not rollback another connection or become a synthetic successful transition.
def test_actual_writer_busy_does_not_change_journal(tmp_path):
    j = Journal(tmp_path / "busy.db", "synthetic")
    other = sqlite3.connect(j.path, isolation_level=None, timeout=0)
    try:
        j.put("preserved", True)
        j.db.execute("PRAGMA busy_timeout=0")
        other.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError) as error:
            with j.atomic():
                j.put("busy-transition", True)
        assert error.value.sqlite_errorcode == sqlite3.SQLITE_BUSY
        assert other.in_transaction and not j.db.in_transaction
        assert j.get("busy-transition") is None and j.get("preserved") is True
        other.execute("ROLLBACK")
        with j.atomic():
            j.put("busy-transition", True)
        assert j.get("busy-transition") is True
    finally:
        other.close()
        j.close()
