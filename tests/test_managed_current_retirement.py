"""Stored-state correlation and actual SQLite failures at the narrow automatic receipt settlement boundary."""

import copy
import json
import sqlite3
import uuid

import pytest

from tests.test_managed_current import adjustable
from tests.test_prototype_tls import tls as tls


# Build a labeled stored-state contract fixture; real wire reachability is covered by the SIGKILL family.
def retirement_state(b):
    c, j = b.controller, b.session.journal
    identifier = uuid.uuid4().hex
    revision = c.requests.intent().revision
    c.requests.accept(identifier, "set_current", revision)
    j.db.execute("INSERT INTO managed_current_requests VALUES(?,32)", (identifier,))
    j.db.execute(
        "UPDATE managed_requests SET state='uncertain',result=? WHERE id=?",
        (json.dumps({"classification": "process_replaced"}), identifier),
    )
    j.put("managed_control_intent", {"mode": "app", "revision": revision + 1})
    change = {
        "id": identifier,
        "state": "recovering",
        "mode": "app",
        "current_a": 32,
        "previous_a": 6,
        "recovery_revision": revision + 1,
        "recovery_reason": "process_replaced",
    }
    j.put("managed_current_change", change)
    assert b.rig.bench.firmware.effective_limit() == c.current.selected() == 6
    return copy.deepcopy(change)


# Snapshot exact encoded values so rollback and terminal preservation cannot hide behind equivalent JSON.
def settlement_bytes(j):
    return (
        [tuple(row) for row in j.db.execute("SELECT * FROM managed_requests ORDER BY id")],
        j.db.execute("SELECT value FROM meta WHERE key='managed_current_change'").fetchone()[0],
    )


# Intent: automatic settlement requires every immutable request, binding and current authority field.
# Why: these labeled stored-state negatives must not allow unrelated or superseded uncertainty to disappear.
@pytest.mark.parametrize(
    "changed",
    [
        "id",
        "current_a",
        "previous_a",
        "mode",
        "recovery_revision",
        "state",
        "binding",
        "missing_binding",
        "request_revision",
        "request_action",
        "request_state",
        "request_result",
        "intent_mode",
        "intent_revision",
        "stopping",
    ],
)
async def test_automatic_retirement_requires_exact_correlation(tmp_path, tls, unused_tcp_port, monkeypatch, changed):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        original = retirement_state(b)
        if changed in ("id", "current_a", "previous_a", "mode", "recovery_revision", "state"):
            altered = {
                "id": uuid.uuid4().hex,
                "current_a": 31,
                "previous_a": 7,
                "mode": "paused",
                "recovery_revision": original["recovery_revision"] + 1,
                "state": "interrupted",
            }
            j.put("managed_current_change", {**original, changed: altered[changed]})
        elif changed == "binding":
            j.db.execute("UPDATE managed_current_requests SET current_a=31 WHERE id=?", (original["id"],))
        elif changed == "missing_binding":
            j.db.execute("DELETE FROM managed_current_requests WHERE id=?", (original["id"],))
        elif changed == "request_result":
            j.db.execute(
                "UPDATE managed_requests SET result=? WHERE id=?",
                ('{"classification":"unrelated_uncertainty"}', original["id"]),
            )
        elif changed.startswith("request_"):
            column = changed.removeprefix("request_")
            value = {"revision": original["recovery_revision"], "action": "start", "state": "accepted"}[column]
            j.db.execute(f"UPDATE managed_requests SET {column}=? WHERE id=?", (value, original["id"]))
        elif changed.startswith("intent_"):
            value = {"mode": "app", "revision": original["recovery_revision"]}
            value[changed.removeprefix("intent_")] = "paused" if changed == "intent_mode" else value["revision"] + 1
            j.put("managed_control_intent", value)
        else:
            c.stopping = True
        before = settlement_bytes(j)
        with j.atomic():
            c.current.returned_to_previous(recovery=original)
        assert settlement_bytes(j) == before


# Intent: real statement and deferred-COMMIT refusal roll back both settlement facts before a successful retry.
# Why: a restored previous cap must never publish canceled metadata separately from its exact failed request.
@pytest.mark.parametrize("failure", ["metadata", "receipt", "commit"])
async def test_automatic_retirement_is_one_real_sqlite_transaction(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        original = retirement_state(b)
        before = settlement_bytes(j)
        if failure == "commit":
            j.db.execute("CREATE TABLE retirement_parent(id INTEGER PRIMARY KEY)")
            j.db.execute(
                "CREATE TABLE retirement_child(id REFERENCES retirement_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
            trigger = "AFTER UPDATE ON managed_requests WHEN NEW.state='failed'"
            effect = "INSERT INTO retirement_child VALUES(1)"
        else:
            trigger = (
                "BEFORE INSERT ON meta WHEN NEW.key='managed_current_change'"
                if failure == "metadata"
                else "BEFORE UPDATE ON managed_requests WHEN NEW.state='failed'"
            )
            effect = "SELECT RAISE(ABORT,'synthetic retirement storage refusal')"
        j.db.execute(f"CREATE TRIGGER refuse_retirement {trigger} BEGIN {effect}; END")
        with pytest.raises(sqlite3.IntegrityError):
            with j.atomic():
                c.current.returned_to_previous(recovery=original)
        assert settlement_bytes(j) == before and not j.db.in_transaction
        assert b.rig.bench.firmware.effective_limit() == 6
        j.db.execute("DROP TRIGGER refuse_retirement")
        with j.atomic():
            c.current.returned_to_previous(recovery=original)
        assert c.requests.receipt(original["id"])["state"] == "failed"
        assert j.get("managed_current_change")["state"] == "cancelled"


# Intent: existing terminal results remain byte-identical while only failed operations may cancel recovery metadata.
# Why: automatic cleanup cannot rewrite a previous failure diagnosis or reinterpret a completed target as abandoned.
@pytest.mark.parametrize("state", ["failed", "completed"])
async def test_automatic_retirement_preserves_terminal_receipt(tmp_path, tls, unused_tcp_port, monkeypatch, state):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        original = retirement_state(b)
        j.db.execute(
            "UPDATE managed_requests SET state=?,result=? WHERE id=?",
            (state, ' { "original" : [1,2,3], "detail": "retain exact bytes" } ', original["id"]),
        )
        before = settlement_bytes(j)
        with j.atomic():
            c.current.returned_to_previous(recovery=original)
        after = settlement_bytes(j)
        assert after[0] == before[0]
        assert j.get("managed_current_change")["state"] == ("cancelled" if state == "failed" else "recovering")


# Intent: refreshed native observations can settle the exact old request without clearing independent debt.
# Why: post-await checks must bind stable authority while preserving fresh source fields and unrelated uncertainty.
async def test_automatic_retirement_preserves_observations_and_unrelated_debt(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        original = retirement_state(b)
        other = uuid.uuid4().hex
        c.requests.accept(other, "start", c.requests.intent().revision)
        j.db.execute("UPDATE managed_requests SET state='uncertain' WHERE id=?", (other,))
        debt = c.requests.receipt(other)
        observations = {"source_boot": "later-real-observation", "continuing_local": 9000001, "native_open": True}
        j.put("managed_current_change", {**original, **observations})
        j.put("unrelated_stop_ack_debt", {"must_remain": True})
        with j.atomic():
            c.current.returned_to_previous(recovery=original)
        assert c.requests.receipt(original["id"])["state"] == "failed"
        assert c.requests.receipt(other) == debt
        assert j.get("unrelated_stop_ack_debt") == {"must_remain": True}
        assert all(j.get("managed_current_change")[key] == value for key, value in observations.items())
