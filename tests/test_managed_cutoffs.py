"""Small production guard checks using a real file-backed journal, alongside the full wire regressions."""

from contextlib import nullcontext

import pytest
from ocpp_hub_core.prototype.managed_cutoffs import idle_cutoff_pending, release_idle_cutoffs, resume_cutoff
from ocpp_hub_core.prototype.managed_occurrences import KEY, occurrences, remember
from ocpp_hub_core.prototype.store import Journal


# Provide ordered input receipts; callers under test perform all selection and discharge transitions.
@pytest.fixture
def journal(tmp_path):
    j = Journal(tmp_path / "cutoffs.db", "CUTOFF_TEST")
    j.db.execute("CREATE TABLE managed_schedule_requests(id TEXT PRIMARY KEY,start_epoch INTEGER NOT NULL)")
    for index, name in enumerate(("older", "closed", "newer", "future")):
        j.db.execute("INSERT INTO managed_schedule_requests VALUES(?,?)", (name, 100 + index))
        j.put(
            KEY,
            {
                "id": name,
                "state": "armed" if name == "future" else "complete",
                "result": "no_scheduled_session",
                "start_epoch": 100 + index,
                "end_epoch": 600 if name != "future" else 900,
                "accepted_epoch": 100 - index,
                "body": {"planId": 10000 + index, "idTag": "CUTOFF_TEST"},
            },
        )
        remember(j)
    j.put(KEY, occurrences(j)["closed"])
    try:
        yield j
    finally:
        j.close()


# Intent: a verified independent continuation keeps the original cutoff pending, unlike terminal proof or Return.
# Why: one transaction's source proof cannot authorize another transaction or survive loss of that source.
@pytest.mark.parametrize(
    ("release", "expected"),
    [
        (None, True),
        ("verified_session", True),
        ("owner_return", False),
        ("session_closed", False),
        ("observed_closure", False),
    ],
)
def test_conditional_source_is_not_permanent_discharge(release, expected):
    assert idle_cutoff_pending({"state": "complete", "idle_cutoff_released": release}) is expected


# Intent: a later completed temporary receipt discharges only older retired duties, preserving later receipt identity.
# Why: equal cutoff times and reversed wall-clock acceptance do not make a later booking part of an older closure.
@pytest.mark.parametrize("outer_transaction", [False, True])
def test_temporary_closure_scope_uses_durable_receipt_order(journal, outer_transaction):
    before = occurrences(journal)
    with journal.atomic() if outer_transaction else nullcontext():
        release_idle_cutoffs(journal, "session_closed", 500)
    after = occurrences(journal)
    assert not idle_cutoff_pending(after["older"])
    assert not idle_cutoff_pending(after["closed"])
    assert idle_cutoff_pending(after["newer"]) and idle_cutoff_pending(after["future"])
    for name in before:
        for field in ("id", "state", "result", "accepted_epoch", "start_epoch", "end_epoch", "body"):
            assert after[name][field] == before[name][field]


# Intent: scope shared station closure to elapsed retired duties, keeping a newer enabled timer independent.
# Why: a genuinely closed idle station can settle an old hold but cannot cancel a future installed occurrence.
@pytest.mark.parametrize("now", [599, 600, 601])
def test_observed_closure_preserves_future_windows(journal, now):
    release_idle_cutoffs(journal, "observed_closure", now)
    saved = occurrences(journal)
    assert idle_cutoff_pending(saved["older"]) is (now < 600)
    assert idle_cutoff_pending(saved["newer"]) is (now < 600)
    assert idle_cutoff_pending(saved["future"])


# Intent: ordinary continuation leaves retired deadlines live; explicit Return discharges old retired duties together.
# Why: dispatching a positive profile under a source exception must not grant the same scope as an owner override.
@pytest.mark.parametrize("reason", ["verified_session", "owner_return"])
def test_return_and_continuation_have_distinct_scope(journal, reason):
    release_idle_cutoffs(journal, reason, 500)
    saved = occurrences(journal)
    assert idle_cutoff_pending(saved["older"]) is (reason == "verified_session")
    assert idle_cutoff_pending(saved["newer"]) is (reason == "verified_session")
    assert idle_cutoff_pending(saved["future"])


# Intent: an unsent cancellation restores an unresolved installed duty without changing the cancellation receipt.
# Why: the last request's result is not the station's complete set of remaining obligations.
@pytest.mark.parametrize("outer_transaction", [False, True])
def test_cancelled_head_restores_earliest_unresolved_cutoff(journal, outer_transaction):
    saved = occurrences(journal)
    saved["older"]["end_epoch"] = 550
    journal.put("managed_temporary_occurrences", saved)
    cancelled = {**saved["closed"], "result": "cancelled_before_install"}
    journal.put(KEY, cancelled)
    with journal.atomic() if outer_transaction else nullcontext():
        selected = resume_cutoff(journal)
    assert selected["id"] == "older"
    assert occurrences(journal)["closed"] == cancelled


# Intent: preserve the independently installed or not-yet-installed working head through ordinary observation.
# Why: an archive fallback cannot replace a live newer receipt merely because an older idle deadline also exists.
@pytest.mark.parametrize("state", ["requested", "installing", "armed", "clearing"])
def test_live_working_receipt_is_not_replaced(journal, state):
    current = {**journal.get(KEY), "state": state}
    journal.put(KEY, current)
    assert resume_cutoff(journal) == current
