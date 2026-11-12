"""Small real-journal contracts complement the complete charger/cloud recovery regressions."""

import copy
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import auth_info
from ocpp_hub_core.prototype.managed_occurrences import (
    ARCHIVE,
    HEAD,
    IDENTITY,
    KEY,
    installed_digest,
    remember,
    restore_head,
)
from ocpp_hub_core.prototype.managed_view import health_snapshot
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError

from tests.test_managed_service_cli import healthy_status
from tests.test_prototype import START


# Allocate an ordinary file-backed journal, leaving each production helper responsible for its own transitions.
@pytest.fixture
def journal(tmp_path):
    j = Journal(tmp_path / "boundaries.db", "BOUNDARY_TEST")
    try:
        yield j
    finally:
        j.close()


# Intent: every physical-zero predicate matters for both deliberate Pause and an idle timer hold.
# Why: fresh timestamps and connected cloud cannot turn a contradictory hold into healthy authority.
@pytest.mark.parametrize("idle", [False, True])
@pytest.mark.parametrize(
    "change",
    [
        None,
        {"local_hold_verified": False},
        {"station_ceiling_a": 6},
        {"current_a": 0.1},
        {"power_w": 1},
        {"charging_session_open": True},
    ],
)
def test_zero_health_requires_each_physical_predicate(idle, change):
    value = healthy_status(10000) | {
        "desired_control": "app" if idle else "paused",
        "local_hold_verified": True,
        "station_ceiling_a": 0,
        "current_a": 0,
        "power_w": 0,
        "charging_session_open": False,
        "owner_control_enabled": False,
    }
    if idle:
        value["temporary_schedule"] = {"idle_cutoff_hold": True}
    result = health_snapshot(value | (change or {}), 10000)
    assert result["healthy"] is (change is None)
    assert result["local_healthy"] is (change is None)


# Supply one complete immutable occurrence, independent of the helper's archive logic.
def occurrence(identifier="one"):
    return {
        "id": identifier,
        "revision": 1,
        "accepted_epoch": 100,
        "start_epoch": 200,
        "end_epoch": 300,
        "floor": 1,
        "body": {"planId": 10001},
        "install": {"digest": "installed"},
        "original_digest": "original",
        "augmented_digest": "augmented",
        "state": "armed",
    }


# Intent: identical repeats preserve receipts, while changing any same-occurrence immutable field is rejected.
# Why: archive selection must never replace the meaning of an already receipted window or installed payload.
@pytest.mark.parametrize("field", [None, *[key for key in IDENTITY if key != "id"]])
def test_occurrence_identity_is_immutable(journal, field):
    original = occurrence()
    journal.put(KEY, original)
    remember(journal)
    archive, head = journal.get(ARCHIVE), journal.get(HEAD)
    repeated = copy.deepcopy(original)
    if field is not None:
        repeated[field] = {"changed": True} if isinstance(repeated[field], dict) else "changed"
    journal.put(KEY, repeated)
    if field is None:
        remember(journal)
    else:
        with pytest.raises(RecoveryRequiredError, match="identity changed"):
            remember(journal)
    assert journal.get(ARCHIVE) == archive and journal.get(HEAD) == head


# Intent: inherited temporary hold fields are retired only when every independent closure guard permits it.
# Why: restoring a working receipt cannot discard a hold that still owns an active or reported session.
@pytest.mark.parametrize(
    "blocker", [None, "inherited_hold", "active", "bound_local", "temporary_start_reported", "retire_reason"]
)
def test_restore_head_preserves_each_inherited_hold_blocker(journal, blocker):
    old, head = occurrence("older"), occurrence("newer")
    head |= {
        "inherited_hold": True,
        "hold_since": 100,
        "next_cap": 0,
        "next_stop": 7,
        "unidentified_local": 7,
        "provisional_boot": "boot",
    }
    if blocker == "active":
        journal.start("active-boundary", START)
    elif blocker is not None:
        head[blocker] = False if blocker == "inherited_hold" else 7 if blocker == "bound_local" else True
    journal.put(ARCHIVE, {old["id"]: old, head["id"]: head})
    journal.put(HEAD, head["id"])
    journal.put(KEY, old)
    restore_head(journal)
    actual = journal.get(KEY)
    if blocker is None:
        assert actual == {
            key: value
            for key, value in head.items()
            if key
            not in ("inherited_hold", "hold_since", "next_cap", "next_stop", "unidentified_local", "provisional_boot")
        }
    else:
        assert actual == head
    assert journal.get(HEAD) == head["id"]


# Intent: installed inventory is recognized only for the matching original, augmented body and live lease state.
# Why: any independently changed component removes authority to treat another inventory as owned.
@pytest.mark.parametrize(
    "state,original,actual,expected",
    [
        ("armed", "original", "augmented", "augmented"),
        ("complete", "original", "augmented", "original"),
        ("armed", "other", "augmented", "other"),
        ("armed", "original", "other", "original"),
    ],
)
def test_installed_digest_requires_all_bindings(journal, state, original, actual, expected):
    journal.put(KEY, occurrence() | {"state": state})
    assert installed_digest(journal, original, actual) == expected


# Populate the genuine metadata book at an independently selected cloud observation time.
def metadata(journal, monkeypatch, observed):
    clock = SimpleNamespace(now=observed)
    monkeypatch.setattr(auth_info, "time", SimpleNamespace(time=lambda: clock.now))
    book = auth_info.InfoBook(journal)
    info = {"status": "Accepted", "expiryDate": "1970-01-01T00:01:40Z", "parentIdTag": "PARENT"}
    book.put("TAG", info)
    return book, info, clock


# Intent: delayed authorization permits only sub-resolution publication lag, with equality failing closed.
# Why: rounding cannot authorize an event that demonstrably preceded the cloud's permission observation.
@pytest.mark.parametrize(
    "fraction,precision", [("", 1), (".0", 0.1), (".000", 0.001), (".000000", 1e-6), (".000000000", 1e-6)]
)
@pytest.mark.parametrize("factor", [0, 0.75, 1, 1.25])
def test_delayed_auth_precision_is_strict(journal, monkeypatch, fraction, precision, factor):
    book, info, clock = metadata(journal, monkeypatch, precision * factor)
    clock.now = 200
    result = book.accepted_for_delayed_report("TAG", 0, "1970-01-01T00:00:00" + fraction + "Z")
    assert result == (info if factor < 1 else None)
    assert book.get("TAG")["status"] == "Expired" and book.stored("TAG") == info


# Intent: the event must precede expiry strictly, while current time may equal the original expiry.
# Why: a delayed historical exception must preserve the actual original permission and never renew it.
@pytest.mark.parametrize(
    "occurred,now,allowed", [(99, 100, True), (100, 200, False), (101, 200, False), (99, 99.5, False)]
)
def test_delayed_auth_expiry_bounds(journal, monkeypatch, occurred, now, allowed):
    book, info, clock = metadata(journal, monkeypatch, 0)
    clock.now = now
    result = book.accepted_for_delayed_report("TAG", occurred, "1970-01-01T00:01:39Z")
    assert result == (info if allowed else None)
