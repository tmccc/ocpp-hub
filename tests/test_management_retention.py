"""Terminal UUID history remains idempotent while unresolved work and the reserved Pause stay bounded."""

from __future__ import annotations

import json
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError

from tests.test_managed_proxy import healthy
from tests.test_managed_test_charge import actual_stop, positive_flow, ready
from tests.test_prototype_tls import tls as tls


# Seed historical results or legacy unresolved work without altering the production admission/counting implementation.
def seed_requests(journal, count, state, action="start"):
    with journal.atomic():
        journal.db.executemany(
            "INSERT INTO managed_requests VALUES(?,?,0,?,?)",
            [
                (format(number, "032x"), action, state, json.dumps({"historical": number}))
                for number in range(1, count + 1)
            ],
        )


# Count only the three unresolved states, separately from retained UUID rows.
def unresolved(journal):
    return journal.db.execute(
        "SELECT COUNT(*) FROM managed_requests WHERE state IN ('accepted','running','uncertain')"
    ).fetchone()[0]


# Intent: thousands of completed UUID receipts survive restart and exact retries without exhausting new work.
# Why: durable idempotency must outlive active-capacity accounting rather than imposing a command lifetime budget.
def test_terminal_history_retains_uuid_results_without_consuming_capacity(tmp_path):
    path = tmp_path / "requests.db"
    journal = Journal(path, "OFFLINE001")
    requests = ManagedRequests(journal)
    seed_requests(journal, 4096, "completed", "pause")
    before = requests.receipt(format(1, "032x"))
    assert requests.has_capacity()
    assert requests.accept(format(1, "032x"), "pause", 0) == before
    assert requests.intent().revision == 0
    identifier = uuid.uuid4().hex
    requests.accept(identifier, "pause", 0)
    requests.confirm_pauses()
    assert requests.receipt(identifier)["state"] == "completed" and unresolved(journal) == 0
    journal.close()
    journal = Journal(path, "OFFLINE001")
    try:
        requests = ManagedRequests(journal)
        assert requests.has_capacity() and requests.receipt(format(1, "032x")) == before
        assert requests.receipt(identifier)["state"] == "completed"
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4097
        assert any(
            row[1] == "managed_requests_state" for row in journal.db.execute("PRAGMA index_list(managed_requests)")
        )
    finally:
        journal.close()


# Intent: every non-Pause command class reserves capacity for a later immediate restrictive intent.
# Why: timed or cloud charge admission must not consume a slot that normal Start and Return deliberately reserve.
@pytest.mark.parametrize(
    "action", ["start", "start_test", "arm_cloud_test", "set_current", "temporary_schedule", "app_control"]
)
def test_all_ordinary_admissions_reserve_the_pause_slot(tmp_path, action):
    journal = Journal(tmp_path / "requests.db", "OFFLINE001")
    try:
        requests = ManagedRequests(journal)
        seed_requests(journal, 4095, "uncertain")
        assert not requests.has_capacity()
        with pytest.raises(RecoveryRequiredError, match="capacity"):
            requests.accept(uuid.uuid4().hex, action, 0)
        assert unresolved(journal) == 4095
        pause = uuid.uuid4().hex
        requests.accept(pause, "pause", 0)
        assert requests.intent().mode == "paused" and requests.receipt(pause)["state"] == "accepted"
        assert unresolved(journal) == 4096
    finally:
        journal.close()


# Intent: even a legacy completely full unresolved pool accepts Pause, and repeated Pause cannot grow active work.
# Why: restrictive authority cannot depend on old capacity mistakes, while all old receipt identities remain queryable.
def test_legacy_full_pool_coalesces_pause_atomically_without_losing_uuid_rows(tmp_path):
    journal = Journal(tmp_path / "requests.db", "OFFLINE001")
    try:
        requests = ManagedRequests(journal)
        seed_requests(journal, 4096, "uncertain")
        first = uuid.uuid4().hex
        requests.accept(first, "pause", 0)
        assert unresolved(journal) == 4097 and requests.intent().revision == 1
        accepted = requests.receipt(first)
        assert requests.accept(first, "pause", 0) == accepted and requests.intent().revision == 1
        for expected_revision in range(2, 5):
            identifier = uuid.uuid4().hex
            requests.accept(identifier, "pause", requests.intent().revision)
            assert unresolved(journal) == 4097 and requests.intent().revision == expected_revision
        old = requests.receipt(first)
        assert old["state"] == "failed" and old["result"]["classification"] == "superseded_by_pause"
        assert requests.accept(first, "pause", 0) == old and requests.intent().revision == 4
        assert (
            journal.db.execute(
                "SELECT COUNT(*) FROM managed_requests WHERE action='start' AND state='uncertain'"
            ).fetchone()[0]
            == 4096
        )
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 4100
        assert (
            journal.db.execute(
                "SELECT COUNT(*) FROM managed_requests WHERE action='pause' AND state='accepted'"
            ).fetchone()[0]
            == 1
        )
    finally:
        journal.close()


# Intent: a failed new Pause receipt rolls back its supersession, desired intent and restrictive claim together.
# Why: an atomic coalescing implementation must not silently terminalize the previous user's request on storage failure.
def test_pause_coalescing_rolls_back_when_new_receipt_cannot_commit(tmp_path):
    journal = Journal(tmp_path / "requests.db", "OFFLINE001")
    try:
        requests = ManagedRequests(journal)
        prior = uuid.uuid4().hex
        requests.accept(prior, "pause", 0)
        new = "f" * 32
        journal.db.execute(
            "CREATE TRIGGER fail_pause BEFORE INSERT ON managed_requests WHEN NEW.id='"
            + new
            + "' BEGIN SELECT RAISE(ABORT,'blocked'); END"
        )
        before = list(journal.db.iterdump())

        # The actual controller's durable zero claim shares this same acceptance transaction.
        def claim():
            journal.put("synthetic_restrictive_claim", True)

        with pytest.raises(sqlite3.IntegrityError):
            requests.accept(new, "pause", 1, claim)
        assert list(journal.db.iterdump()) == before
        assert requests.receipt(prior)["state"] == "accepted" and requests.intent().revision == 1
        assert journal.get("synthetic_restrictive_claim") is None
    finally:
        journal.close()


# Intent: admitted real six-ampere test charging still accepts immediate Pause after4095 retained terminal receipts.
# Why: the reported owner-control failure must be verified at actual command admission, load and Stop boundaries.
async def test_real_test_charge_and_immediate_pause_ignore_terminal_history(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=10) as test:
        journal = test.session.journal
        seed_requests(journal, 4095, "completed", "pause")
        assert test.controller.tests.available(test.controller.latest_native)
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "start_test", test.controller.requests.intent().revision)
        await test.controller.tests.settle()
        local = await positive_flow(test)
        assert (await healthy(test.controller))["current_a"] == 6
        pause = uuid.uuid4().hex
        test.controller.command(pause, "pause", test.controller.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]
        assert test.controller.requests.receipt(pause)["state"] == "completed"
        assert test.controller.requests.intent().mode == "paused" and test.rig.bench.firmware.effective_limit() == 0
        assert journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] >= 4097
