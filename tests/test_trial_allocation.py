"""Fresh numbers come from independent native history; recovery never changes accepted local facts."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_allocation import initialize_allocation, observe_history
from ocpp_hub_core.prototype.trial_evidence import Evidence
from ocpp_hub_core.prototype.trial_resources import Baseline, ResourceBook

from tests.test_prototype import START


# Construct only ordinary preparation metadata before the real allocator is exercised.
def prepared_ledger(path: Path) -> Journal:
    journal = Journal(path, "OFFLINE001")
    evidence = Evidence(fingerprint("OFFLINE001"), time.time(), "A", "Unplug", 0, 0, False, "config", ())
    ResourceBook(journal, Baseline.capture(journal.identity, evidence))
    return journal


# Intent: use the highest retained identity, persist it across restart and leave later rolling history powerless.
# Why: fresh default1001 collided with prior proxy history during the physical test preparation.
def test_allocation_persists_without_reseeding(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    journal = prepared_ledger(path)
    history = observe_history(
        [{"transactionId": 9000000}, {"transactionId": 1001}], journal.identity, "HW", time.time()
    )
    initialize_allocation(journal, history, journal.identity, "HW")
    proof = journal.get("trial_allocation")
    assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
    result = journal.start("start", START)
    assert result["transactionId"] == 9000001
    journal.close()
    journal = Journal(path, "OFFLINE001")
    try:
        initialize_allocation(journal, observe_history([], journal.identity, "HW", time.time()), journal.identity, "HW")
        assert journal.get("next_transaction") == 9000001 and journal.get("trial_allocation") == proof
        assert journal.start("different-token", START) == result
        assert journal.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    finally:
        journal.close()


# Intent: malformed, duplicate and exhausted history never becomes a fresh allocation boundary.
# Why: treating an error or truncated identifier as empty history would reuse saved native session numbers.
@pytest.mark.parametrize(
    "value",
    [None, {}, [None], [{}]]
    + [[{"transactionId": value}] for value in (True, False, "1", 1.0, 0, -1, 2**31, 2**31 - 1)]
    + [[{"transactionId": 7}, {"transactionId": 7}], [{"transactionId": value + 1} for value in range(10001)]]
    + [[{"transactionId": 1, "invalid": float("nan")}]],
)
def test_invalid_native_history_cannot_reserve(value: Any) -> None:
    with pytest.raises(SecurityError):
        observe_history(value, "OFFLINE001", "HW", time.time())


# Intent: failure after the counter write rolls the whole reservation back without creating any lifecycle fact.
# Why: a power loss or database failure must not leave an apparently initialized but unproven number range.
def test_allocation_is_atomic(tmp_path: Path) -> None:
    journal = prepared_ledger(tmp_path / "state.db")
    try:
        journal.db.execute(
            "CREATE TRIGGER fail_proof BEFORE INSERT ON meta WHEN NEW.key='trial_allocation' "
            "BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            initialize_allocation(
                journal, observe_history([], journal.identity, "HW", time.time()), journal.identity, "HW"
            )
        assert journal.get("next_transaction") is None and journal.get("trial_allocation") is None
        assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
    finally:
        journal.close()


# Intent: a crash before activation cannot reuse an initialized floor after native history advances elsewhere.
# Why: the charger may have created a record while the first bootstrap was committed but no trial was running.
def test_prepared_restart_rejects_a_stale_allocation_floor(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    journal = prepared_ledger(path)
    initialize_allocation(journal, observe_history([], journal.identity, "HW", time.time()), journal.identity, "HW")
    journal.close()
    journal = Journal(path, "OFFLINE001")
    try:
        advanced = observe_history([{"transactionId": 1001}], journal.identity, "HW", time.time())
        with pytest.raises(RecoveryRequiredError):
            initialize_allocation(journal, advanced, journal.identity, "HW")
        assert journal.get("next_transaction") == 1000
        assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
    finally:
        journal.close()


# Intent: wrong identities, stale snapshots and future observations cannot initialize even an empty ledger.
# Why: a valid history from another device or time cannot authorize this charger's local number range.
@pytest.mark.parametrize("change", ["identity", "hardware", "stale", "future", "nan"])
def test_history_requires_current_device_binding(tmp_path: Path, change: str) -> None:
    journal = prepared_ledger(tmp_path / "state.db")
    history = observe_history([], journal.identity, "HW", time.time())
    changes: dict[str, Any] = {
        "identity": {"identity_digest": fingerprint("OTHER")},
        "hardware": {"hardware_digest": fingerprint("OTHER")},
        "stale": {"observed_at": time.time() - 60},
        "future": {"observed_at": time.time() + 10},
        "nan": {"observed_at": float("nan")},
    }
    try:
        with pytest.raises(SecurityError):
            initialize_allocation(journal, replace(history, **changes[change]), journal.identity, "HW")
        assert journal.get("next_transaction") is None
    finally:
        journal.close()


# Intent: existing ambiguous delivery and legacy receipts remain untouched when new allocation is unavailable.
# Why: missing new provenance must not turn a previously accepted Start into a duplicate or synthetic success.
def test_legacy_receipt_replay_survives_missing_allocation(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "state.db", "OFFLINE001")
    result = journal.start("old", START)
    journal.put("trial_phase", "recovering")
    journal.db.execute("UPDATE outbox SET state='ambiguous'")
    before = [tuple(row) for row in journal.db.execute("SELECT * FROM outbox")]
    try:
        assert journal.start("retry", START) == result
        with pytest.raises(RecoveryRequiredError):
            journal.start("new", {**START, "connectorId": 2})
        assert [tuple(row) for row in journal.db.execute("SELECT * FROM outbox")] == before
        assert json.loads(journal.db.execute("SELECT start FROM transactions").fetchone()[0]) == START
    finally:
        journal.close()


# Intent: an existing allocator remains bound to the exact physical charger and journal identity that initialized it.
# Why: same OCPP identity on another physical unit must not silently adopt the old counter's provenance.
@pytest.mark.parametrize("mismatch", ["hardware", "journal"])
def test_existing_allocator_rejects_changed_device(tmp_path: Path, mismatch: str) -> None:
    journal = prepared_ledger(tmp_path / "state.db")
    initialize_allocation(journal, observe_history([], journal.identity, "HW-A", time.time()), journal.identity, "HW-A")
    proof, counter = journal.get("trial_allocation"), journal.get("next_transaction")
    identity, hardware = (journal.identity, "HW-B") if mismatch == "hardware" else ("OTHER", "HW-A")
    try:
        with pytest.raises(SecurityError):
            initialize_allocation(journal, observe_history([], identity, hardware, time.time()), identity, hardware)
        assert journal.get("trial_allocation") == proof and journal.get("next_transaction") == counter
    finally:
        journal.close()


# Intent: persisted counter and provenance corruption blocks new Start without rewriting existing facts.
# Why: exact integer checks and a valid original timestamp must survive process restarts, not just input parsing.
@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("count", True),
        ("floor", True),
        ("observed_at", "invalid"),
        ("observed_at", float("inf")),
        ("observed_at", 0),
        ("hardware_digest", "invalid"),
        ("counter", True),
        ("counter", "1000"),
        ("counter", 1000.0),
        ("counter", 999),
    ],
)
def test_corrupted_allocation_is_not_repaired(tmp_path: Path, field: str, value: Any) -> None:
    journal = prepared_ledger(tmp_path / "state.db")
    initialize_allocation(journal, observe_history([], journal.identity, "HW", time.time()), journal.identity, "HW")
    try:
        if field == "counter":
            journal.put("next_transaction", value)
        else:
            proof = journal.get("trial_allocation")
            proof[field] = value
            if value == float("inf"):
                journal.db.execute("UPDATE meta SET value=? WHERE key='trial_allocation'", (json.dumps(proof),))
            else:
                journal.put("trial_allocation", proof)
        with pytest.raises(RecoveryRequiredError):
            journal.start("new", START)
        assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
    finally:
        journal.close()
