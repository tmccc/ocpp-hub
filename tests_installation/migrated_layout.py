"""Recreate the audited journal layout from synthetic records and the real migration."""

import sqlite3
from contextlib import closing

AUDITED_SCHEMA = "0b08c43048c89c897d8224d2335e43294652dd9ba58fd940030a61437c776f0d"
OLD_SCHEDULE = ("d" * 32, 1_735_689_600)


# Start before the vendor-ID migration and retain the retired tables from the audited installation.
def seed_old_layout(path):
    with closing(sqlite3.connect(path, isolation_level=None)) as db:
        db.executescript(
            "CREATE TABLE transactions(local INTEGER PRIMARY KEY,connector INTEGER NOT NULL,"
            "start TEXT UNIQUE NOT NULL,stop TEXT,vendor INTEGER UNIQUE,live INTEGER NOT NULL DEFAULT 0,"
            "start_info TEXT,stop_info TEXT);"
            "CREATE TABLE managed_pending_native_reports("
            "native_id INTEGER PRIMARY KEY, local INTEGER NOT NULL UNIQUE REFERENCES transactions(local));"
            "CREATE TABLE managed_schedule_requests(id TEXT PRIMARY KEY, start_epoch INTEGER NOT NULL);"
            "CREATE TABLE managed_unnumbered_reports("
            "attempt TEXT NOT NULL, started_ms INTEGER NOT NULL,"
            "local INTEGER NOT NULL UNIQUE REFERENCES transactions(local),PRIMARY KEY(attempt,started_ms));"
            "CREATE TABLE native_history_records("
            "transaction_id INTEGER NOT NULL,digest TEXT NOT NULL,body TEXT NOT NULL,"
            "first_archived REAL NOT NULL,last_archived REAL NOT NULL,original INTEGER NOT NULL,"
            "PRIMARY KEY(transaction_id,digest));"
            "CREATE TABLE native_start_attempts(digest TEXT PRIMARY KEY,body TEXT NOT NULL);"
        )
        db.execute("INSERT INTO managed_schedule_requests VALUES(?,?)", OLD_SCHEDULE)
    path.chmod(0o600)


# Check the whole layout, old marker and journal integrity independently of either running build.
def verify_layout(path, expected=AUDITED_SCHEMA):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        if expected is not None:
            from tools.check_proxy_rollback import schema_digest

            assert schema_digest(db) == expected
        assert db.execute("SELECT id,start_epoch FROM managed_schedule_requests").fetchall() == [OLD_SCHEDULE]
        assert db.execute("PRAGMA quick_check(1)").fetchone() == ("ok",)
        assert db.execute("PRAGMA foreign_key_check").fetchone() is None
