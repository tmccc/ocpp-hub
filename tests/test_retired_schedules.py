"""Retired one-off schedules remain passive evidence and cannot reach journal normalization."""

import copy
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError

START = {"connectorId": 1, "idTag": "OLD_TIMER", "meterStart": 1000, "timestamp": "2026-01-01T00:00:00Z"}


# Build a definitively closed old occurrence, with enough immutable identity to detect conflicting snapshots.
def completed(identifier="old", plan=17, **changes):
    return {
        "id": identifier,
        "revision": 0,
        "accepted_epoch": 90,
        "start_epoch": 100,
        "body": {"planId": plan},
        "state": "complete",
        "result": "session_closed",
    } | changes


# A refusal must release its owner lock and preserve the actual SQLite file, not merely selected metadata.
def refused(path: Path):
    before = path.read_bytes()
    for _ in range(2):
        with pytest.raises(RecoveryRequiredError, match="retired one-off"):
            Journal(path, "OFFLINE001")
        assert path.read_bytes() == before


# Intent: unfinished occurrences refuse before schema migration, live reset or in-flight Start normalization.
# Why: removing the implementation must not acknowledge or rewrite physical and cloud obligations it cannot settle.
@pytest.mark.parametrize("state", ["requested", "installing", "armed", "clearing", "unknown", None])
def test_unfinished_journal_refuses_without_changing_bytes(tmp_path, state):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.start("real-start", START)
        j.db.execute("UPDATE transactions SET live=1")
        item = j.next_delivery()
        assert item is not None
        j.mark_sent(item)
        j.put("managed_temporary_schedule", completed(state=state))
        j.put("sentinel", {"preserve": [1, 2, 3]})
    refused(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT live FROM transactions").fetchone()[0] == 1
        assert db.execute("SELECT state FROM outbox").fetchone()[0] == "inflight"


# Intent: completed inventory cannot hide an independent source/report or cutoff obligation.
# Why: a previous cleanup label alone never proves the timer's lifecycle and station duties discharged.
@pytest.mark.parametrize(
    "flag",
    [
        "idle_cutoff_hold",
        "recovery_session",
        "closure_gap",
        "unreported_native",
        "pending_native_reports",
        "temporary_attempts",
        "unidentified_local",
    ],
)
def test_completed_occurrence_with_remaining_debt_refuses(tmp_path, flag):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.put("managed_temporary_schedule", completed(**{flag: True}))
    refused(path)


# Intent: the working completed record supersedes its stale archive while old completed IDs stay reserved.
# Why: archived snapshots must neither revive an already discharged timer nor spend a later owner's command.
def test_current_completion_overlays_stale_archive(tmp_path):
    path = tmp_path / "state.db"
    current = completed()
    archive = {"old": completed(state="armed", recovery_session=True), "earlier": completed("earlier", 23)}
    with closing(Journal(path, "OFFLINE001")) as j:
        j.put("managed_temporary_schedule", current)
        j.put("managed_temporary_occurrences", archive)
    with closing(Journal(path, "OFFLINE001")) as j:
        assert j.retired_plan_ids == frozenset({17, 23})
        assert j.get("managed_temporary_schedule") == current
        assert j.get("managed_temporary_occurrences") == archive
    with closing(Journal(path, "OFFLINE001")) as j:
        assert j.retired_plan_ids == frozenset({17, 23})


# Intent: a completed head cannot discard an unfinished older occurrence with a different identity.
# Why: replacing the working slot is not evidence that every archived installation and deadline ended.
def test_older_unfinished_occurrence_is_not_overlaid_by_another_id(tmp_path):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.put("managed_temporary_schedule", completed("newer", 23))
        j.put("managed_temporary_occurrences", {"older": completed("older", 17, state="armed")})
    refused(path)


# Intent: a reused occurrence ID cannot replace its immutable request, window or plan body.
# Why: overlaying snapshots must preserve the original identity check instead of laundering another timer as closed.
@pytest.mark.parametrize("change", [{"revision": 1}, {"start_epoch": 101}, {"body": {"planId": 23}}])
def test_conflicting_current_and_archive_identity_refuses(tmp_path, change):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.put("managed_temporary_schedule", completed(**change))
        j.put("managed_temporary_occurrences", {"old": completed(state="armed")})
    refused(path)


# Intent: malformed old records refuse without inventing empty history or guessing an allocated timer ID.
# Why: unsupported evidence cannot become ordinary daily-schedule permission through permissive defaults.
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("managed_temporary_schedule", None),
        ("managed_temporary_occurrences", []),
        ("managed_temporary_occurrences", {"old": None}),
        ("managed_temporary_occurrences", {"different": completed()}),
        ("managed_temporary_schedule", completed(id=None)),
        ("managed_temporary_schedule", completed(body=None)),
        ("managed_temporary_schedule", completed(plan=True)),
        ("managed_temporary_schedule", completed(plan=0)),
        ("managed_temporary_schedule", completed(result="verified_session")),
    ],
)
def test_malformed_or_undischarged_history_refuses(tmp_path, key, value):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.put(key, copy.deepcopy(value))
    refused(path)


# Intent: invalid JSON in a retired slot refuses before normal initialization opens a writable connection.
# Why: parse failure is unknown retired evidence, not the absence of a timer record.
def test_invalid_history_json_refuses(tmp_path):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.db.execute("INSERT INTO meta VALUES('managed_temporary_schedule','{')")
    refused(path)


# Intent: every nonterminal old timer request refuses even when its metadata slot is absent.
# Why: missing history cannot acknowledge an accepted, running or uncertain command.
@pytest.mark.parametrize("state", ["accepted", "running", "uncertain", "unknown", None])
def test_unsettled_timer_request_refuses(tmp_path, state):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.db.execute("CREATE TABLE managed_requests(id TEXT,action TEXT,revision INTEGER,state TEXT,result TEXT)")
        j.db.execute("INSERT INTO managed_requests VALUES('old','temporary_schedule',0,?,'{}')", (state,))
    refused(path)


# Intent: terminal timer receipts remain historical, and a normal owner's pending request is still admissible.
# Why: retirement must target one-off scheduling without changing ordinary request startup handling.
@pytest.mark.parametrize("state", ["completed", "failed"])
def test_terminal_receipts_and_ordinary_requests_remain(tmp_path, state):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.db.execute("CREATE TABLE managed_requests(id TEXT,action TEXT,revision INTEGER,state TEXT,result TEXT)")
        j.db.execute("INSERT INTO managed_requests VALUES('old','temporary_schedule',0,?,'{}')", (state,))
        j.db.execute("INSERT INTO managed_requests VALUES('owner','start',0,'accepted','{}')")
    with closing(Journal(path, "OFFLINE001")) as j:
        assert j.db.execute("SELECT state FROM managed_requests WHERE id='old'").fetchone()[0] == state
        assert j.db.execute("SELECT state FROM managed_requests WHERE id='owner'").fetchone()[0] == "accepted"


# Intent: closed unnumbered mappings stay historical while open and orphan mappings remain blocking debt.
# Why: a permanent dedup table is not itself a pending timer, and a missing closure cannot be guessed.
@pytest.mark.parametrize("outcome", ["closed", "open", "orphan", "no-transactions-table"])
def test_unnumbered_mapping_checks_real_lifecycle(tmp_path, outcome):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.db.execute("CREATE TABLE managed_unnumbered_reports(attempt TEXT,started_ms INTEGER,local INTEGER)")
        if outcome in ("closed", "open"):
            answer = j.start("original", START)
            local = answer["transactionId"]
            if outcome == "closed":
                j.db.execute("UPDATE transactions SET stop=? WHERE local=?", (json.dumps({"meterStop": 1100}), local))
        else:
            local = 999
        j.db.execute("INSERT INTO managed_unnumbered_reports VALUES('original',100,?)", (local,))
        if outcome == "no-transactions-table":
            j.db.execute("DROP TABLE transactions")
    if outcome != "closed":
        refused(path)
    else:
        with closing(Journal(path, "OFFLINE001")) as j:
            assert j.db.execute("SELECT local FROM managed_unnumbered_reports").fetchone()[0] == local


# Intent: pending native report aliases refuse independently of completed occurrence metadata.
# Why: source identity still needs its original implementation even when no plan remains installed.
def test_pending_native_report_refuses(tmp_path):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.db.execute("CREATE TABLE managed_pending_native_reports(native_id INTEGER,local INTEGER)")
        j.db.execute("INSERT INTO managed_pending_native_reports VALUES(999,1)")
    refused(path)


# Intent: any owned plan without verified absence refuses, while an absent plan and ordinary profile stay usable.
# Why: retirement cannot treat an old plan's desired, confirmed or uncertain physical state as cleanup proof.
@pytest.mark.parametrize("state", ["desired", "confirmed", "uncertain", None, "absent_verified"])
def test_old_owned_plan_requires_absence_receipt(tmp_path, state):
    path = tmp_path / "state.db"
    with closing(Journal(path, "OFFLINE001")) as j:
        j.db.execute("CREATE TABLE trial_resources(key TEXT PRIMARY KEY,state TEXT)")
        j.db.execute("INSERT INTO trial_resources VALUES('plan:17',?)", (state,))
        j.db.execute("INSERT INTO trial_resources VALUES('profile:7001','confirmed')")
    if state != "absent_verified":
        refused(path)
    else:
        with closing(Journal(path, "OFFLINE001")) as j:
            assert j.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0] == 2


# Intent: a fresh ordinary journal does not acquire the retired feature's tables or timer metadata.
# Why: passive old-history support must not reconstruct a timer subsystem in new installations.
def test_new_journal_has_no_oneoff_schema(tmp_path):
    with closing(Journal(tmp_path / "state.db", "OFFLINE001")) as j:
        assert j.retired_plan_ids == frozenset()
        names = {row[0] for row in j.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert not names & {"managed_schedule_requests", "managed_pending_native_reports", "managed_unnumbered_reports"}
        assert j.get("managed_temporary_schedule") is None and j.get("managed_temporary_occurrences") is None
