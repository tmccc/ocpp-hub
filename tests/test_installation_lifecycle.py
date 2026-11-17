"""Installation observations and writes retain the original authority and transaction boundaries."""

from __future__ import annotations

import copy
import sqlite3
import time
from dataclasses import replace
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.gateway import Gateway
from ocpp_hub_core.prototype.installation_lifecycle import InstallationState
from ocpp_hub_core.prototype.managed_faults import latch_managed_fault
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.store import Journal, fingerprint
from ocpp_hub_core.prototype.trial_evidence import Evidence, Resource
from ocpp_hub_core.prototype.trial_resources import Baseline, ResourceBook


# Intent: exercise the real durable journal while leaving phase fixtures independent of the new owner.
# Why: historical and malformed states must not be normalized by the same API being checked.
@pytest.fixture
def journal(tmp_path: Path):
    value = Journal(tmp_path / "state.db", "OFFLINE001")
    try:
        yield value
    finally:
        value.close()


# Intent: distinguish active, restrictive, cleanup and absent legacy phases without coercing unknown JSON.
# Why: unhashable or falsey old values must not become either permission or a new TypeError.
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, (False, False, False, False, False, True, False)),
        ("prepared", (False, False, False, False, False, False, True)),
        ("active", (True, True, True, False, False, True, True)),
        ("recovering", (False, True, True, True, False, False, False)),
        ("blocked", (False, False, True, True, False, False, False)),
        ("rollback", (False, False, False, False, True, False, False)),
        ("backend_restore_pending", (False, False, False, False, True, False, False)),
        ("complete", (False, False, False, False, True, False, False)),
        ("unknown", (False, False, False, False, False, False, False)),
        (0, (False, False, False, False, False, False, False)),
        (False, (False, False, False, False, False, False, False)),
        ("", (False, False, False, False, False, False, False)),
        (["active"], (False, False, False, False, False, False, False)),
        ({"phase": "active"}, (False, False, False, False, False, False, False)),
    ],
)
def test_phase_observations_preserve_legacy_shapes(value, expected):
    state = InstallationState(copy.deepcopy(value))
    assert (
        state.active,
        state.operating,
        state.supervised,
        state.held,
        state.cleanup,
        state.legacy_active,
        state.admissible,
    ) == expected
    assert state.value == value


# Intent: preserve the different resource-fault and managed-fault policies for every retained phase.
# Why: resource faults block unknown phases, while managed faults may only change an existing supervised phase.
@pytest.mark.parametrize(
    ("phase", "resource_phase", "managed_phase"),
    [
        (None, "blocked", None),
        ("prepared", "blocked", "prepared"),
        ("active", "blocked", "blocked"),
        ("recovering", "blocked", "blocked"),
        ("blocked", "blocked", "blocked"),
        ("rollback", "rollback", "rollback"),
        ("backend_restore_pending", "backend_restore_pending", "backend_restore_pending"),
        ("complete", "complete", "complete"),
        ("unknown", "blocked", "unknown"),
        (0, "blocked", 0),
        (["active"], "blocked", ["active"]),
        ({"phase": "active"}, "blocked", {"phase": "active"}),
    ],
)
@pytest.mark.parametrize("managed", [False, True])
def test_faults_keep_their_original_phase_scope(journal, phase, resource_phase, managed_phase, managed):
    base = {"owner": "shared", "ticket": "captured", "resume_limit": 16, "start_pending": True, "opaque": [1, 2]}
    journal.put("control", {"owner": "local", "ticket": "other"})
    journal.put("trial_phase", phase)
    journal.put("trial_starting", {"old": "flag"})
    before_control = journal.get("control")
    if managed:
        journal.phase.fault(base)
        assert journal.get("control") == {**base, "owner": "local", "inhibit": True, "uncertain": True}
    else:
        journal.phase.block_unless_cleanup()
        assert journal.get("control") == before_control
    assert journal.get("trial_phase") == (managed_phase if managed else resource_phase)
    assert journal.get("trial_starting") == {"old": "flag"}
    assert base == {
        "owner": "shared",
        "ticket": "captured",
        "resume_limit": 16,
        "start_pending": True,
        "opaque": [1, 2],
    }


# Intent: phase-only transitions retain unconditional writes and never grant or replace physical authority.
# Why: adding legal-transition validation or a hold would change existing recovery and cleanup callers.
@pytest.mark.parametrize("old_phase", [None, "complete", {"legacy": "unknown"}])
@pytest.mark.parametrize(
    ("method", "new_phase", "new_starting"),
    [
        ("prepare", "prepared", None),
        ("activate", "active", None),
        ("recover", "recovering", None),
        ("block", "blocked", None),
        ("begin_cleanup", "rollback", None),
        ("await_backend", "backend_restore_pending", None),
        ("complete", "complete", None),
        ("begin_introduction", None, True),
        ("introduced", None, False),
    ],
)
def test_phase_only_writes_do_not_invent_authority(journal, old_phase, method, new_phase, new_starting):
    control = {"owner": "local", "inhibit": True, "uncertain": True, "ticket": "pause", "start_pending": True}
    journal.put("control", control)
    journal.put("trial_phase", old_phase)
    journal.put("trial_starting", {"opaque": "flag"})
    journal.put("independent_debt", {"unknown": True})
    before_control = journal.db.execute("SELECT value FROM meta WHERE key='control'").fetchone()[0]
    getattr(journal.phase, method)()
    assert journal.get("trial_phase") == (new_phase if new_phase is not None else old_phase)
    assert journal.get("trial_starting") == (new_starting if new_starting is not None else {"opaque": "flag"})
    assert journal.db.execute("SELECT value FROM meta WHERE key='control'").fetchone()[0] == before_control
    assert journal.get("independent_debt") == {"unknown": True}


# Intent: compound transitions preserve control/phase/starting publication order and supplied preimages.
# Why: the caller's SQL failure boundary and historical control fields must survive an ownership extraction.
@pytest.mark.parametrize(
    ("method", "phase", "uncertain", "keys"),
    [
        ("restrict", "recovering", True, ["control", "trial_phase"]),
        ("resume", "active", False, ["control", "trial_phase"]),
        ("recover_introduced", "recovering", True, ["control", "trial_phase", "trial_starting"]),
        ("fault", "blocked", True, ["control", "trial_phase"]),
    ],
)
def test_compound_transitions_keep_write_order(journal, method, phase, uncertain, keys):
    base = {"owner": "shared", "ticket": "captured", "start_pending": True, "opaque": {"keep": [4, 5]}}
    journal.put("trial_phase", "recovering")
    journal.put("control", {"ticket": "different"})
    journal.db.executescript(
        "CREATE TABLE write_log(seq INTEGER PRIMARY KEY,key TEXT);"
        "CREATE TRIGGER trace_phase AFTER INSERT ON meta "
        "WHEN NEW.key IN ('control','trial_phase','trial_starting') "
        "BEGIN INSERT INTO write_log(key) VALUES(NEW.key); END;"
    )
    getattr(journal.phase, method)(base)
    assert [row[0] for row in journal.db.execute("SELECT key FROM write_log ORDER BY seq")] == keys
    assert journal.get("control") == {**base, "owner": "local", "inhibit": True, "uncertain": uncertain}
    assert journal.phase.read().value == phase
    assert journal.phase.starting is (False if method == "recover_introduced" else None)


# Intent: real statement and COMMIT failures roll back phase, physical hold, flag and sibling receipts together.
# Why: the owner must join the existing transaction and publish no cached success after failed SQL completion.
@pytest.mark.parametrize("failure", ["phase", "starting", "commit"])
def test_lifecycle_joins_caller_rollback_and_real_commit(journal, failure):
    base = {"owner": "shared", "inhibit": False, "uncertain": False, "ticket": "old", "opaque": [3]}
    journal.put("control", base)
    journal.put("trial_phase", "active")
    journal.put("trial_starting", True)
    before = [tuple(row) for row in journal.db.execute("SELECT * FROM meta ORDER BY key")]
    journal.db.executescript(
        "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
        "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);"
    )
    if failure != "commit":
        key = "trial_phase" if failure == "phase" else "trial_starting"
        journal.db.execute(
            "CREATE TRIGGER refuse_lifecycle BEFORE INSERT ON meta WHEN NEW.key='" + key + "' "
            "BEGIN SELECT RAISE(ABORT,'injected lifecycle refusal'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        with journal.atomic():
            journal.put("sibling_receipt", {"accepted": True})
            journal.phase.recover_introduced(base)
            if failure == "commit":
                journal.db.execute("INSERT INTO child VALUES(7)")
    assert not journal.db.in_transaction
    assert [tuple(row) for row in journal.db.execute("SELECT * FROM meta ORDER BY key")] == before
    assert journal.phase.read().active and journal.phase.starting is True
    journal.db.execute("DROP TRIGGER IF EXISTS refuse_lifecycle")
    with journal.atomic():
        journal.put("sibling_receipt", {"accepted": True})
        journal.phase.recover_introduced(base)
        journal.db.execute("INSERT INTO parent VALUES(7)")
        journal.db.execute("INSERT INTO child VALUES(7)")
    assert journal.phase.read().value == "recovering" and journal.phase.starting is False
    assert journal.get("sibling_receipt") == {"accepted": True}
    assert journal.get("control") == {**base, "owner": "local", "inhibit": True, "uncertain": True}


# Intent: an unwrapped write failure keeps the caller's original partial-publication boundary.
# Why: silently adding a transaction would change recovery evidence at sites that deliberately do not open one.
def test_lifecycle_does_not_add_a_transaction(journal):
    base = {"owner": "shared", "ticket": "old"}
    journal.put("control", base)
    journal.put("trial_phase", "active")
    journal.db.execute(
        "CREATE TRIGGER refuse_phase BEFORE INSERT ON meta WHEN NEW.key='trial_phase' "
        "BEGIN SELECT RAISE(ABORT,'injected phase refusal'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected phase refusal"):
        journal.phase.restrict(base)
    assert not journal.db.in_transaction
    assert journal.get("control") == {**base, "owner": "local", "inhibit": True, "uncertain": True}
    assert journal.phase.read().active


# Intent: a phase observation reads only its key and preserves stale observations as data, never a cache.
# Why: eager flag reads or in-memory publication could change lazy guards and survive a later rollback.
def test_lifecycle_reads_are_lazy_and_never_normalize(journal):
    statements = []
    journal.db.set_trace_callback(statements.append)
    assert journal.phase.read().value is None
    assert len(statements) == 1 and "trial_phase" in statements[0]
    assert not journal.db.execute("SELECT 1 FROM meta WHERE key LIKE 'trial_%'").fetchone()
    journal.put("trial_phase", "active")
    journal.put("trial_starting", {"raw": "truthy"})
    statements.clear()
    old = journal.phase.read()
    assert len(statements) == 1 and "trial_starting" not in statements[0]
    statements.clear()
    assert journal.phase.starting == {"raw": "truthy"}
    assert len(statements) == 1 and "trial_starting" in statements[0]
    journal.put("trial_phase", ["unknown"])
    assert old.active and not journal.phase.read().supervised
    journal.db.execute("UPDATE meta SET value='not JSON' WHERE key='trial_phase'")
    with pytest.raises(ValueError):
        journal.phase.read()
    assert journal.db.execute("SELECT value FROM meta WHERE key='trial_phase'").fetchone()[0] == "not JSON"


# Intent: the actual managed-fault latch holds control without changing prepared or rollback phases.
# Why: wiring this caller to the broader resource-fault transition would alter its original scope.
@pytest.mark.parametrize("phase", ["prepared", "rollback"])
def test_managed_fault_caller_retains_phase_scope(journal, phase):
    gateway = Gateway(journal)
    control = {"owner": "shared", "inhibit": False, "ticket": "old", "start_pending": True}
    journal.put("control", control)
    journal.put("trial_phase", phase)
    revision = gateway.controls._revision
    latch_managed_fault(gateway, "native_refused", diagnostic={"kind": "strict"})
    assert journal.get("managed_control_fault")["classification"] == "native_refused"
    assert journal.get("managed_native_failure") == {"kind": "strict"}
    assert journal.get("control") == {**control, "owner": "local", "inhibit": True, "uncertain": True}
    assert journal.get("trial_phase") == phase
    assert gateway.controls._revision == revision + 1


# Intent: genuine missing-original evidence blocks preparation while leaving cleanup phases frozen.
# Why: the real resource caller must not accidentally use unconditional block or the narrower managed-fault rule.
@pytest.mark.parametrize(
    ("phase", "expected"),
    [
        ("prepared", "blocked"),
        ("rollback", "rollback"),
        ("backend_restore_pending", "backend_restore_pending"),
        ("complete", "complete"),
    ],
)
def test_resource_fault_caller_preserves_cleanup(journal, phase, expected):
    original = Resource("plan", 9, {"planId": 9, "limitCurrent": 32}, {"planId": 9, "limitCurrent": 32})
    observation = Evidence(
        fingerprint(journal.identity),
        time.time(),
        "A",
        "Unplug",
        0,
        0,
        False,
        fingerprint({"configuration": "original"}),
        (original,),
    )
    book = ResourceBook(journal, Baseline.capture(journal.identity, observation))
    journal.put("trial_phase", phase)
    control = {"owner": "local", "inhibit": True, "ticket": "cleanup", "uncertain": False}
    journal.put("control", control)
    with pytest.raises(NativeContentError, match="original resource preservation failed"):
        book.check_originals(replace(observation, resources=()))
    assert journal.get("trial_preservation_fault")["missing"] == ["plan:9"]
    assert journal.get("trial_phase") == expected
    assert journal.get("control") == control
