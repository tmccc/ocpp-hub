"""Small real-journal contracts complement the complete charger/cloud recovery regressions."""

from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import auth_info
from ocpp_hub_core.prototype.managed_view import health_snapshot
from ocpp_hub_core.prototype.store import Journal

from tests.test_managed_service_cli import healthy_status


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
def test_zero_health_requires_each_physical_predicate(change):
    value = healthy_status(10000) | {
        "desired_control": "paused",
        "local_hold_verified": True,
        "station_ceiling_a": 0,
        "current_a": 0,
        "power_w": 0,
        "charging_session_open": False,
        "owner_control_enabled": False,
    }
    result = health_snapshot(value | (change or {}), 10000)
    assert result["healthy"] is (change is None)
    assert result["local_healthy"] is (change is None)


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
