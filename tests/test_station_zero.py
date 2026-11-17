"""Exact resource-receipt correlation, independent of native I/O and permission policy."""

import json
import sqlite3
from contextlib import closing

import pytest
from ocpp_hub_core.prototype.trial_evidence import Resource
from ocpp_hub_core.prototype.trial_resources import ResourceBook

ZERO = {"limit": 0, "connector": 0, "purpose": "ChargePointMaxProfile", "sentinel": [1, 2]}


# Supply the actual named SQLite row shape without constructing a controller or network installation.
def receipt(state="confirmed", confirmed=None):
    with closing(sqlite3.connect(":memory:")) as db:
        db.row_factory = sqlite3.Row
        return db.execute("SELECT ? AS state, ? AS confirmed", (state, confirmed)).fetchone()


# Intent: only one zero station with its complete equal confirmed receipt establishes this narrow resource fact.
# Why: a positive profile, missing receipt or partial canonical match must never become a zero proof.
@pytest.mark.parametrize(
    "count,limit,state,body,expected",
    [
        (1, 0, "confirmed", json.dumps(ZERO), True),
        (0, 0, "confirmed", json.dumps(ZERO), False),
        (2, 0, "confirmed", json.dumps(ZERO), False),
        (1, 6, "confirmed", json.dumps(ZERO), False),
        (1, 32, "confirmed", json.dumps(ZERO), False),
        (1, 0, "pending", json.dumps(ZERO), False),
        (1, 0, "uncertain", json.dumps(ZERO), False),
        (1, 0, "absent_verified", json.dumps(ZERO), False),
        (1, 0, "clear_acknowledged", json.dumps(ZERO), False),
        (1, 0, "confirmed", None, False),
        (1, 0, "confirmed", "null", False),
        (1, 0, "confirmed", "[]", False),
        (1, 0, "confirmed", '{"limit": 0}', False),
        (1, 0, "confirmed", json.dumps(ZERO | {"sentinel": [2, 1]}), False),
    ],
)
def test_confirmed_station_zero(count, limit, state, body, expected):
    station = [Resource("profile", 7001, {}, ZERO | {"limit": limit}) for _ in range(count)]
    assert ResourceBook.confirmed_station_zero(station, receipt(state, body)) is expected


# Intent: receipt absence and structurally ineligible evidence do not decode unrelated bad saved bytes.
# Why: source/lifecycle Pending and earlier structural refusals retain their original precedence over JSON errors.
def test_zero_receipt_json_is_lazy():
    station = [Resource("profile", 7001, {}, ZERO)]
    assert not ResourceBook.confirmed_station_zero(station, None)
    assert not ResourceBook.confirmed_station_zero([], receipt(confirmed="malformed"))
    assert not ResourceBook.confirmed_station_zero(station * 2, receipt(confirmed="malformed"))
    assert not ResourceBook.confirmed_station_zero(station, receipt("pending", "malformed"))
    assert not ResourceBook.confirmed_station_zero(
        [Resource("profile", 7001, {}, ZERO | {"limit": 6})], receipt(confirmed="malformed")
    )
    with pytest.raises(json.JSONDecodeError):
        ResourceBook.confirmed_station_zero(station, receipt(confirmed="malformed"))
