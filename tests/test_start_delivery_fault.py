"""A real cloud quarantine installs its recovery gate atomically before native supervision can return early."""

import sqlite3

import pytest
from ocpp_hub_core.prototype.managed_start_delivery import record_start_error, supersede_unknown_start_retry
from ocpp_hub_core.prototype.store import Journal

from tests.test_prototype import START


# The journal is independent of any supervisor poll, while its original positive control facts remain explicit.
def managed_journal(path, prior_fault):
    j = Journal(path, "CLOUD_START_LATCH")
    j.put("managed_mode", {"mode": "app_schedule"})
    control = {"owner": "shared", "inhibit": False, "uncertain": False, "ticket": "original", "resume_limit": 16}
    j.put("control", control)
    if prior_fault:
        j.put("managed_control_fault", {"classification": "independent_existing_fault", "at_epoch": 1})
    j.start("original", START)
    return j, control


# Intent: actual unknown/rejected transitions need no later healthy native poll to become independently recoverable.
# Why: native reconciliation may return early indefinitely while the exact delivery head remains quarantined.
@pytest.mark.parametrize("boundary", ["unknown", "startup", "exhaustion"])
@pytest.mark.parametrize("prior_fault", [False, True])
def test_start_quarantine_latches_without_supervisor(tmp_path, boundary, prior_fault):
    path = tmp_path / "quarantine.db"
    j, control = managed_journal(path, prior_fault)
    original_fault = j.get("managed_control_fault")
    try:
        if boundary == "exhaustion":
            for number in range(3):
                item = j.next_delivery()
                j.mark_sent(item)
                assert record_start_error(j, item, [4, item.token, "InternalError", "synthetic", {}]) == (number == 2)
                if number < 2:
                    assert j.next_delivery().state == "queued"
                    assert j.get("managed_control_fault") == original_fault
                    assert j.get("control") == control
        else:
            j.mark_sent(j.next_delivery())
            if boundary == "startup":
                j.close()
                j = Journal(path, "CLOUD_START_LATCH")
            else:
                with j.atomic():
                    j.db.execute("UPDATE outbox SET state='ambiguous'")
                    supersede_unknown_start_retry(j, j.next_delivery())
        assert j.next_delivery().state == ("rejected" if boundary == "exhaustion" else "ambiguous")
        if prior_fault:
            assert j.get("managed_control_fault") == original_fault and j.get("control") == control
        else:
            fault = j.get("managed_control_fault")
            assert fault["classification"] == "owner_control_or_delivery_uncertain"
            assert j.get("control") == control | {"owner": "local", "inhibit": True, "uncertain": True}
            with j.atomic():
                if boundary != "exhaustion":
                    supersede_unknown_start_retry(j, j.next_delivery())
            assert j.get("managed_control_fault") == fault
        assert j.vendor_id(j.next_delivery().transaction) is None
    finally:
        j.close()


# Intent: refusal at either write or deferred COMMIT rolls back quarantine, restrictive control and fault together.
# Why: a half-written latch must neither lose the exact delivery outcome nor invent independent recovery authority.
@pytest.mark.parametrize("boundary", ["control", "fault", "commit"])
def test_start_quarantine_fault_is_atomic(tmp_path, boundary):
    j, control = managed_journal(tmp_path / "atomic.db", False)
    try:
        j.mark_sent(j.next_delivery())
        before = [tuple(row) for row in j.db.execute("SELECT * FROM outbox")]
        if boundary == "commit":
            j.db.execute("CREATE TABLE latch_parent(id INTEGER PRIMARY KEY)")
            j.db.execute("CREATE TABLE latch_child(id REFERENCES latch_parent(id) DEFERRABLE INITIALLY DEFERRED)")
            effect = "INSERT INTO latch_child VALUES(1)"
        else:
            effect = "SELECT RAISE(ABORT,'synthetic latch write refusal')"
        key = "control" if boundary == "control" else "managed_control_fault"
        j.db.execute(f"CREATE TRIGGER fail_latch BEFORE INSERT ON meta WHEN NEW.key='{key}' BEGIN {effect}; END")
        with pytest.raises(sqlite3.IntegrityError):
            with j.atomic():
                j.db.execute("UPDATE outbox SET state='ambiguous'")
                supersede_unknown_start_retry(j, j.next_delivery())
        assert not j.db.in_transaction and j.get("managed_control_fault") is None
        assert j.get("control") == control
        assert [tuple(row) for row in j.db.execute("SELECT * FROM outbox")] == before
        j.db.execute("DROP TRIGGER fail_latch")
        with j.atomic():
            j.db.execute("UPDATE outbox SET state='ambiguous'")
            supersede_unknown_start_retry(j, j.next_delivery())
        assert j.get("managed_control_fault")["classification"] == "owner_control_or_delivery_uncertain"
    finally:
        j.close()
