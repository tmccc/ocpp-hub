"""Durable management intent revokes only an older Return and keeps other unresolved effects intact."""

import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.store import Journal


# Intent: a new Pause revokes an older uncertain Return while every different unresolved action remains unknown.
# Why: orphan Return receipts must not block future recovery, but Pause cannot settle one-shot Start/current debt.
@pytest.mark.parametrize("future_corruption", [False, True], ids=["older-return", "future-revision-negative"])
def test_pause_revokes_only_older_uncertain_return(tmp_path, future_corruption):
    j = Journal(tmp_path / "intent.db", "synthetic")
    try:
        requests = ManagedRequests(j)
        identifiers = {action: uuid.uuid4().hex for action in ("app_control", "start", "start_test", "set_current")}
        for action, identifier in identifiers.items():
            requests.accept(identifier, action, 0)
        requests = ManagedRequests(j)
        if future_corruption:
            # Labeled private-state corruption is a negative control, never a supported execution history.
            j.db.execute("UPDATE managed_requests SET revision=7 WHERE id=?", (identifiers["app_control"],))
        pause = requests.accept(uuid.uuid4().hex, "pause", 0)
        assert pause["state"] == "accepted" and requests.intent().mode == "paused"
        for action, identifier in identifiers.items():
            receipt = requests.receipt(identifier)
            if action == "app_control" and not future_corruption:
                assert receipt["state"] == "failed"
                assert receipt["result"]["classification"] == "superseded_by_pause"
            else:
                assert receipt["state"] == "uncertain"
    finally:
        j.close()


# Intent: actual SQLite refusal of Return retirement rolls back the entire new Pause acceptance and allows retry.
# Why: an orphan fix must not publish a paused intent or partial revocation when its acceptance transaction fails.
def test_pause_return_revocation_is_atomic_under_real_storage_failure(tmp_path):
    j = Journal(tmp_path / "atomic-intent.db", "synthetic")
    try:
        requests = ManagedRequests(j)
        previous, pause = uuid.uuid4().hex, uuid.uuid4().hex
        requests.accept(previous, "app_control", 0)
        requests = ManagedRequests(j)
        before = [tuple(row) for row in j.db.execute("SELECT * FROM managed_requests ORDER BY rowid")]
        j.db.execute("CREATE TRIGGER refuse_return_revocation BEFORE UPDATE ON managed_requests "
                     "WHEN OLD.action='app_control' AND NEW.state='failed' "
                     "BEGIN SELECT RAISE(ABORT,'synthetic revocation refusal'); END")
        with pytest.raises(sqlite3.IntegrityError) as error:
            requests.accept(pause, "pause", 0)
        assert error.value.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_TRIGGER
        assert not j.db.in_transaction and requests.intent().mode == "app" and requests.intent().revision == 0
        assert [tuple(row) for row in j.db.execute("SELECT * FROM managed_requests ORDER BY rowid")] == before
        assert requests.receipt(pause)["state"] == "not_found"
        j.db.execute("DROP TRIGGER refuse_return_revocation")
        assert requests.accept(pause, "pause", 0)["state"] == "accepted"
        assert requests.receipt(previous)["state"] == "failed" and requests.intent().mode == "paused"
    finally:
        j.close()
