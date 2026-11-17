"""Stored control transitions preserve UUID history, explicit publication and replacement without physical I/O."""

import json
import sqlite3

import pytest
from ocpp_hub_core.prototype.managed_intent import ControlIntent, ManagedRequests
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError

STATES = ("accepted", "running", "uncertain", "completed", "failed")
ORIGINAL = ' {"original": "opaque bytes"} '
RETURNED = '{"app_control_enabled": true, "charging_started": false}'


# Intent: every normal transition advances revision, while explicit legacy coalescing preserves an existing mode.
# Why: induced Pause and historical read-only recovery intentionally have different revision lifetimes.
@pytest.mark.parametrize("revision", [0, 7, 2**53 - 1])
@pytest.mark.parametrize(
    "current,mode,coalesce,increment",
    [
        ("app", "app", False, 1),
        ("app", "paused", False, 1),
        ("paused", "app", False, 1),
        ("paused", "paused", False, 1),
        ("app", "app", True, 0),
        ("app", "paused", True, 1),
        ("paused", "app", True, 1),
        ("paused", "paused", True, 0),
    ],
)
def test_shared_intent_revision_transition(revision, current, mode, coalesce, increment):
    original = ControlIntent(current, revision)
    assert original.next(mode, coalesce=coalesce) == ControlIntent(mode, revision + increment)
    assert original == ControlIntent(current, revision)


# Keep real SQLite ownership and cleanup around each independent stored-state witness.
@pytest.fixture
def owner(tmp_path):
    journal = Journal(tmp_path / "control.db", "OFFLINE001")
    try:
        yield ManagedRequests(journal)
    finally:
        journal.close()


# Seed an existing row without running admission or predicting its production transition.
def seed(owner, identifier="a" * 32, action="app_control", revision=7, state="accepted"):
    owner.journal.db.execute(
        "INSERT INTO managed_requests VALUES(?,?,?,?,?)", (identifier, action, revision, state, ORIGINAL)
    )
    return identifier


# Read exact stored bytes rather than decoding or normalizing terminal historical results.
def raw(owner, identifier):
    return tuple(
        owner.journal.db.execute("SELECT state,result FROM managed_requests WHERE id=?", (identifier,)).fetchone()
    )


# Intent: replacement marks only the five replay-sensitive actions accepted/running as uncertain.
# Why: Pause and schedule recovery retain their distinct lifetimes and all terminal UUID bytes survive startup.
@pytest.mark.parametrize(
    "action", ["app_control", "start", "start_test", "arm_cloud_test", "set_current", "pause", "temporary_schedule"]
)
@pytest.mark.parametrize("state", STATES)
def test_replacement_preserves_action_and_terminal_filters(tmp_path, action, state):
    path = tmp_path / "replacement.db"
    journal = Journal(path, "OFFLINE001")
    first = ManagedRequests(journal)
    journal.put("managed_control_intent", {"mode": "paused", "revision": 7})
    identifier = seed(first, action=action, state=state)
    journal.close()
    journal = Journal(path, "OFFLINE001")
    try:
        recovered = ManagedRequests(journal)
        expected = (
            ("uncertain", '{"classification": "process_replaced"}')
            if (action not in ("pause", "temporary_schedule") and state in ("accepted", "running"))
            else (state, ORIGINAL)
        )
        assert raw(recovered, identifier) == expected
        assert recovered.intent() == ControlIntent("paused", 7)
        assert recovered.pending_return() is None or (action == "app_control" and state == "accepted")
        assert journal.starts.lease is None
    finally:
        journal.close()


# Intent: malformed durable intent cannot gain a default app grant or normalize pending requests first.
# Why: the request owner is admitted only after validating the existing authority shape.
@pytest.mark.parametrize(
    "value",
    [
        {},
        {"mode": "other", "revision": 7},
        {"mode": "app", "revision": True},
        {"mode": "app", "revision": -1},
        {"mode": "app", "revision": 2**53},
        {"mode": "app", "revision": 7, "extra": False},
        {"mode": "app", "revision": "7"},
    ],
)
def test_invalid_intent_does_not_sweep_or_grant(owner, value):
    identifier = seed(owner)
    owner.journal.put("managed_control_intent", value)
    with pytest.raises(SecurityError, match="intent"):
        ManagedRequests(owner.journal)
    assert owner.journal.get("managed_control_intent") == value
    assert raw(owner, identifier) == ("accepted", ORIGINAL)


# Intent: send-time Return correlation uses its exact action, revision and accepted/running states.
# Why: a receipt lookup or an earlier accepted request cannot authorize a superseded or unrelated release.
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("action", ["app_control", "start"])
@pytest.mark.parametrize("revision", [7, 8])
def test_current_return_identity(owner, state, action, revision):
    identifier = seed(owner, action=action, revision=revision, state=state)
    assert owner.return_current(identifier, ControlIntent("paused", 7)) is (
        action == "app_control" and revision == 7 and state in ("accepted", "running")
    )
    assert owner.return_in_progress() is (action == "app_control" and state in ("accepted", "running"))
    assert owner.return_in_progress(7) is (
        action == "app_control" and revision == 7 and state in ("accepted", "running")
    )
    assert not owner.return_current(None, ControlIntent("paused", 7))
    assert owner.return_current(None, ControlIntent("app", 7))
    assert not owner.return_current(identifier, ControlIntent("app", 7))


# Intent: beginning an owned attempt changes only accepted rows without touching their opaque result.
# Why: completed, failed or replacement-uncertain requests never become fresh positive attempts.
@pytest.mark.parametrize("state", STATES)
def test_owned_return_begin_keeps_terminal_bytes(owner, state):
    identifier = seed(owner, state=state)
    owner.return_started(identifier)
    assert raw(owner, identifier) == ("running" if state == "accepted" else state, ORIGINAL)


# Intent: waiting requeues only under the original intent, while unconfirmed completion retains its old ID-only filter.
# Why: a newer Pause or domain revision must not be overwritten, and unknown outcomes cannot become replayable.
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("current", [ControlIntent("paused", 7), ControlIntent("paused", 8), ControlIntent("app", 7)])
@pytest.mark.parametrize("unconfirmed", [False, True])
def test_owned_return_disposition(owner, state, current, unconfirmed):
    identifier = seed(owner, state=state)
    owner.journal.put("managed_control_intent", {"mode": current.mode, "revision": current.revision})
    owner.return_waiting(identifier, ControlIntent("paused", 7), unconfirmed=unconfirmed)
    expected = (state, ORIGINAL)
    if unconfirmed and state in ("accepted", "running"):
        expected = ("uncertain", '{"classification": "return_not_confirmed"}')
    elif not unconfirmed and state == "running" and current == ControlIntent("paused", 7):
        expected = ("accepted", ORIGINAL)
    assert raw(owner, identifier) == expected
    assert owner.intent() == current


# Intent: old healthy-status completion preserves its accepted/running filter without advancing intent or epoch.
# Why: extracting atomic publication must retain the separate legacy bookkeeping contract.
@pytest.mark.parametrize("state", STATES)
def test_legacy_return_completion_changes_no_authority(owner, state):
    identifier = seed(owner, revision=1, state=state)
    owner.journal.put("managed_control_intent", {"mode": "app", "revision": 9})
    owner.journal.put("managed_owner_epoch", 4)
    owner.returned(identifier)
    assert raw(owner, identifier) == (
        ("completed", RETURNED) if state in ("accepted", "running") else (state, ORIGINAL)
    )
    assert owner.intent() == ControlIntent("app", 9)
    assert owner.journal.get("managed_owner_epoch") == 4


# Intent: an induced test Stop revokes only accepted/running Return rows and preserves other operation history.
# Why: replacement uncertainty and unrelated Start/Pause outcomes remain independent of that physical hold.
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("action", ["app_control", "start", "pause"])
def test_test_stop_retains_original_request_filter(owner, state, action):
    identifier = seed(owner, action=action, state=state)
    owner.charge_stopped()
    expected = (
        ("failed", '{"classification": "superseded_by_test_stop"}')
        if (action == "app_control" and state in ("accepted", "running"))
        else (state, ORIGINAL)
    )
    assert raw(owner, identifier) == expected


# Intent: publication cannot advance intent outside its transaction or under another owner's revision/mode.
# Why: real wire evidence is independent from permission to publish a positive app grant.
@pytest.mark.parametrize("kind", ["start", "return"])
@pytest.mark.parametrize("current", [ControlIntent("paused", 7), ControlIntent("paused", 8), ControlIntent("app", 7)])
def test_publication_requires_exact_transaction(owner, kind, current):
    identifier = seed(owner)
    owner.journal.put("managed_control_intent", {"mode": current.mode, "revision": current.revision})
    expected = ControlIntent("paused", 7)
    publish = (
        (lambda: owner.start_published(identifier, expected, 3))
        if kind == "start"
        else (lambda: owner.returned(identifier, expected))
    )
    with pytest.raises(RecoveryRequiredError, match="publication"):
        publish()
    if current != expected:
        with pytest.raises(RecoveryRequiredError, match="publication"), owner.journal.atomic():
            publish()
    assert owner.intent() == current
    assert raw(owner, identifier) == ("accepted", ORIGINAL)
    assert owner.journal.get("managed_owner_epoch") is None


# Intent: Start supersession and Return completion preserve separate filters and the original publication order.
# Why: app intent, request outcomes and a caller's current settlement must commit or roll back together.
@pytest.mark.parametrize("kind", ["start", "return"])
@pytest.mark.parametrize("failure", [None, "statement", "commit"])
def test_publication_order_and_actual_sqlite_rollback(owner, kind, failure):
    j = owner.journal
    j.put("managed_control_intent", {"mode": "paused", "revision": 7})
    identifier = seed(owner)
    for number, state in enumerate(STATES, 1):
        seed(owner, format(number, "032x"), "pause", 2, state)
    if failure == "statement":
        j.db.execute(
            "CREATE TRIGGER refuse_outcome BEFORE UPDATE ON managed_requests "
            "BEGIN SELECT RAISE(ABORT,'outcome refused'); END"
        )
    if failure == "commit":
        j.db.execute("PRAGMA foreign_keys=ON")
        j.db.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE deferred(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)")
    before = list(j.db.iterdump())

    # The domain callback must see new app intent and the still-unfinished Return in the child's transaction.
    def settle_current():
        assert j.db.in_transaction and owner.intent() == ControlIntent("app", 8)
        assert raw(owner, identifier) == ("accepted", ORIGINAL)
        j.put("current_settled", True)

    # Exercise real SQL and COMMIT without granting this stored-state fixture any physical command authority.
    def publish():
        with j.atomic():
            if failure == "commit":
                j.db.execute("INSERT INTO deferred VALUES(1)")
            if kind == "start":
                owner.start_published(identifier, ControlIntent("paused", 7), 3)
            else:
                owner.returned(identifier, ControlIntent("paused", 7), settle_current)

    if failure is not None:
        with pytest.raises(sqlite3.IntegrityError):
            publish()
        assert list(j.db.iterdump()) == before
    else:
        publish()
        assert owner.intent() == ControlIntent("app", 8)
        assert raw(owner, identifier) == (("completed", RETURNED) if kind == "return" else ("accepted", ORIGINAL))
        assert j.get("managed_owner_epoch") == (3 if kind == "start" else None)
        assert j.get("current_settled") is (True if kind == "return" else None)
        for number, state in enumerate(STATES, 1):
            observed = raw(owner, format(number, "032x"))
            if kind == "start" and state in ("accepted", "uncertain"):
                assert observed == (
                    "failed",
                    json.dumps({"classification": "superseded_by_start", "superseded_by": identifier}),
                )
            else:
                assert observed == (state, ORIGINAL)
