"""Sequential cloud IDs retain unambiguous active controls and exact preserved historical records."""

import json
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.store import ConflictError, Journal

from tests.observation_helpers import ObservationTLS, observation_payload
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


# Build only the actual pre-upgrade schema and immutable facts needed to exercise its real SQLite migration.
def legacy_database(path):
    db = sqlite3.connect(path, isolation_level=None)
    db.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE transactions(local INTEGER PRIMARY KEY,connector INTEGER NOT NULL,start TEXT UNIQUE NOT NULL,
            stop TEXT,vendor INTEGER UNIQUE,live INTEGER NOT NULL DEFAULT 0,start_info TEXT,stop_info TEXT);
        CREATE UNIQUE INDEX one_open_connector ON transactions(connector) WHERE stop IS NULL;
        CREATE TABLE transaction_clock_discontinuities(local INTEGER PRIMARY KEY REFERENCES transactions(local),
            start_timestamp TEXT NOT NULL,stop_timestamp TEXT NOT NULL,observed_at_epoch REAL NOT NULL);
    """)
    db.execute("INSERT INTO meta VALUES('identity',?)", (json.dumps("MAPPING_TEST"),))
    db.execute(
        "INSERT INTO transactions VALUES(1001,1,?,?,0,0,?,?)",
        (
            json.dumps(START, sort_keys=True),
            json.dumps({"transactionId": 1001, "meterStop": 1100}),
            '{"idTagInfo":{"status":"Blocked"}}',
            '{"status":"Accepted"}',
        ),
    )
    db.execute("INSERT INTO transaction_clock_discontinuities VALUES(1001,'2026-01-02Z','2026-01-01Z',123.5)")
    before = (
        db.execute("SELECT * FROM transactions").fetchall(),
        db.execute("SELECT * FROM transaction_clock_discontinuities").fetchall(),
    )
    db.close()
    return before


# Intent: upgrading the global vendor-ID constraint preserves every lifecycle/auth/clock byte and reference.
# Why: a denial ID such as zero may recur, but migration must not lose genuine history or transaction counters.
def test_vendor_id_schema_migration_preserves_exact_history(tmp_path):
    path = tmp_path / "legacy.db"
    before = legacy_database(path)
    j = Journal(path, "MAPPING_TEST")
    try:
        assert [tuple(r) for r in j.db.execute("SELECT * FROM transactions")] == before[0]
        assert [tuple(r) for r in j.db.execute("SELECT * FROM transaction_clock_discontinuities")] == before[1]
        assert j.db.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert j.db.execute("PRAGMA foreign_key_check").fetchall() == []
        assert j.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert j.vendor_id(1001) == 0
    finally:
        j.close()


# Intent: a failed required index creation rolls back the entire rebuild, releasing its private owner lock.
# Why: a partially upgraded history must never be presented as a successful or silently weakened construction.
def test_vendor_id_schema_migration_rolls_back_on_storage_refusal(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    before = legacy_database(path)
    connect = sqlite3.connect

    # The real SQLite engine refuses the exact required index operation during its migration transaction.
    def guarded_connect(*args, **kwargs):
        db = connect(*args, **kwargs)

        # All ordinary reads/copies succeed so the failure exercises rollback after the original table was dropped.
        def refuse_index(operation, name, table, database, trigger):
            return (
                sqlite3.SQLITE_DENY
                if operation == sqlite3.SQLITE_CREATE_INDEX and name == "one_open_vendor"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(refuse_index)
        return db

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", guarded_connect)
        with pytest.raises(sqlite3.DatabaseError):
            Journal(path, "MAPPING_TEST")
    with connect(path) as db:
        assert db.execute("SELECT * FROM transactions").fetchall() == before[0]
        assert db.execute("SELECT * FROM transaction_clock_discontinuities").fetchall() == before[1]
        assert (
            "vendor INTEGER UNIQUE"
            in db.execute("SELECT sql FROM sqlite_master WHERE name='transactions'").fetchone()[0]
        )
    j = Journal(path, "MAPPING_TEST")
    j.close()


# Intent: a wrong configured identity is rejected before schema migration or any history rewrite.
# Why: opening an unrelated charger journal must not transform it even when its schema is an older supported one.
def test_wrong_identity_cannot_migrate_journal(tmp_path):
    path = tmp_path / "legacy.db"
    legacy_database(path)
    with sqlite3.connect(path) as db:
        before = db.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").fetchall()
    with pytest.raises(ConflictError):
        Journal(path, "OTHER_MAPPING_TEST")
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").fetchall() == before


# Intent: a reused ID names only the new active session and never forwards an old sample into its history.
# Why: global historical uniqueness is too strict, while guessing among historical matches cross-attributes data.
@pytest.mark.parametrize("boundary", ["same", "reconnect", "restart", "reconnect-boot", "restart-boot"])
async def test_sequential_zero_id_reuse_and_late_old_meter(tmp_path, tls, boundary):
    async with ObservationTLS(tmp_path / "mapping.db", tls) as peer:
        peer.fixed_vendor = 0
        first = (await peer.call("start", "StartTransaction", START))[2]["transactionId"]
        await peer.flush()
        assert peer.j.vendor_id(first) == 0 and peer.j.historical_local_id(0) == first
        action, old = observation_payload("meter-transaction", first)
        assert (await peer.call("original-meter", action, old))[0] == 3
        await peer.flush()
        assert (
            await peer.call(
                "stop",
                "StopTransaction",
                {"transactionId": first, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"},
            )
        )[0] == 3
        await peer.flush()
        second = (await peer.call("second-start", "StartTransaction", START | {"timestamp": "2026-01-02T00:00:00Z"}))[
            2
        ]["transactionId"]
        if boundary != "same":
            await peer.replace(restart=boundary.startswith("restart"), boot=boundary.endswith("-boot"))
        assert (await peer.call("original-meter", action, old))[0] == 3
        # While the later mapping is unknown the charger receives a retryable failure, not a false publication ACK.
        assert (await peer.call("old-before-map", action, old))[:3] == [4, "old-before-map", "InternalError"]
        await peer.flush()
        assert peer.j.vendor_id(second) == 0 and peer.j.local_id(0) == second
        assert peer.j.historical_local_id(0) is None
        assert (await peer.call("original-meter", action, old))[0] == 3
        assert (await peer.call("old-after-map", action, old))[0] == 4
        _, current = observation_payload("meter-transaction", second)
        current["meterValue"][0]["timestamp"] = "2026-01-02T00:00:30Z"
        assert (await peer.call("current-meter", action, current))[0] == 3
        assert (
            await peer.call(
                uuid.uuid4().hex,
                "StopTransaction",
                {"transactionId": second, "meterStop": 1300, "timestamp": "2026-01-02T00:01:00Z"},
            )
        )[0] == 3
        await peer.flush()
        meters = [f for f in peer.cloud_calls if f[2] == "MeterValues"]
        assert len(meters) == 2 and all(frame[3]["transactionId"] == 0 for frame in meters)
        assert not peer.cloud_errors
        assert peer.j.next_delivery() is None and peer.j.local_id(0) is None
