"""Physical authority is distinct from actual effects, lifecycle debt and requested intent."""

from __future__ import annotations

import copy
import sqlite3
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.control_state import ControlState
from ocpp_hub_core.prototype.store import Journal


# Intent: old falsey flags remain readable while explicit publication proof still requires JSON false.
# Why: a refactor must neither invalidate legacy holds nor strengthen incomplete evidence into confirmation.
@pytest.mark.parametrize(
    ("values", "shared", "confirmed", "hold", "explicit_hold"),
    [
        ({}, False, False, False, False),
        ({"owner": "shared"}, True, False, False, False),
        ({"owner": "shared", "inhibit": False, "uncertain": False}, True, True, False, False),
        ({"owner": "shared", "inhibit": 0, "uncertain": 0}, True, False, False, False),
        ({"owner": "shared", "inhibit": True}, False, False, False, False),
        ({"owner": "shared", "uncertain": True}, False, False, False, False),
        ({"owner": "local", "inhibit": True}, False, False, True, True),
        ({"owner": "local", "inhibit": 1}, False, False, True, False),
        ({"owner": "local", "inhibit": False}, False, False, False, False),
        ({"owner": "unknown", "inhibit": True}, False, False, False, False),
    ],
)
def test_authority_requires_its_own_evidence(values, shared, confirmed, hold, explicit_hold):
    state = ControlState(copy.deepcopy(values))
    assert (state.shared, state.confirmed_shared, state.local_hold, state.explicit_local_hold) == (
        shared,
        confirmed,
        hold,
        explicit_hold,
    )
    assert state.values == values


# Intent: a newer Pause makes an earlier successful physical completion unable to republish app authority.
# Why: effect truth survives separately; old Accepted evidence is not a reusable control permit.
def test_late_completion_cannot_release_newer_pause(tmp_path: Path):
    j = Journal(tmp_path / "state.db", "OFFLINE001")
    try:
        j.control.admit()
        first = j.control.read().values
        j.control.claim(first, "start", False, False, 32)
        j.control.project(j.control.read().values, start_pending=True)
        j.control.claim(j.control.read().values, "pause", True, True, 32)
        before = j.db.execute("SELECT value FROM meta WHERE key='control'").fetchone()[0]
        j.control.complete("start", owner="shared", inhibit=False, uncertain=False)
        assert j.db.execute("SELECT value FROM meta WHERE key='control'").fetchone()[0] == before
        assert j.control.read().local_hold
        assert j.control.read().values["start_pending"] is True
        j.control.complete("pause", uncertain=False)
        assert j.control.read().values["uncertain"] is False
        assert j.control.read().values["start_pending"] is True
    finally:
        j.close()


# Intent: fresh claims preserve a previously lower ceiling, unknown fields and unresolved Start evidence.
# Why: replacing physical authority must not silently increase current or discharge an independent debt.
@pytest.mark.parametrize("stopping", [False, True])
def test_claim_retains_ceiling_and_independent_evidence(stopping):
    before = {"resume_limit": 6, "start_pending": True, "opaque": {"receipt": [1, 2]}, "stop_epoch": 4}
    result = ControlState(copy.deepcopy(before)).claimed("new", True, stopping, 32)
    assert result.values["resume_limit"] == 6 and result.values["start_pending"] is True
    assert result.values["opaque"] == before["opaque"]
    assert result.values["stop_epoch"] == 4 + int(stopping)
    assert result.values["ticket"] == "new" and result.values["uncertain"] is True
    assert result.local_hold and not result.shared
    assert before == {"resume_limit": 6, "start_pending": True, "opaque": {"receipt": [1, 2]}, "stop_epoch": 4}
    fresh = ControlState({}).claimed("first", False, stopping, 32)
    assert fresh.values["resume_limit"] == 32


# Intent: authority writes share the caller's larger SQL transaction and publish no cached state on failed COMMIT.
# Why: one writer must preserve rollback of receipts and control together instead of making a nested transaction.
def test_authority_joins_failed_transaction(tmp_path: Path):
    j = Journal(tmp_path / "state.db", "OFFLINE001")
    try:
        j.control.admit()
        before = j.control.read().values
        j.db.execute(
            "CREATE TRIGGER refuse_commit BEFORE INSERT ON meta WHEN NEW.key='end' "
            "BEGIN SELECT RAISE(ABORT,'injected refusal'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="injected refusal"):
            with j.atomic():
                j.control.claim(before, "failed", False, False, 32)
                j.put("joined_receipt", {"accepted": True})
                j.put("end", True)
        assert j.control.read().values == before and j.get("joined_receipt") is None
        with j.atomic():
            j.control.claim(before, "real", True, True, 32)
            j.put("joined_receipt", {"accepted": True})
        assert j.control.read().values["ticket"] == "real"
        assert j.get("joined_receipt") == {"accepted": True}
    finally:
        j.close()


# Intent: recovery merges the supplied preimage and selection never releases a paused physical hold.
# Why: raw legacy bytes, captured-base semantics and future current selection have distinct contracts.
def test_recovery_and_selection_preserve_preimages(tmp_path: Path):
    j = Journal(tmp_path / "state.db", "OFFLINE001")
    try:
        j.control.admit()
        j.control.claim(j.control.read().values, "pause", True, True, 6)
        j.control.select(j.control.read().values, 32, app=False)
        assert j.control.read().local_hold and j.control.read().values["resume_limit"] == 32
        captured = j.control.read().values | {"opaque": {"keep": [3, 4]}}
        j.control.project(j.control.read().values, start_pending=True)
        j.control.hold(captured, uncertain=False)
        assert "start_pending" not in j.control.read().values
        assert j.control.read().values["opaque"] == {"keep": [3, 4]}
        j.control.select(j.control.read().values, 16, app=True)
        state = j.control.read()
        assert state.local_hold and state.values["uncertain"] is True and state.values["resume_limit"] == 32
        j.control.hold(state.values, start_pending=False)
        assert j.control.read().values["start_pending"] is False
        j.control.admit()
        assert j.control.read().values == {"owner": "local", "inhibit": True, "uncertain": False}
    finally:
        j.close()
