"""Finite calendar and wire-codec tables, bound to the functions used by production admission.

These enumerate input decisions, not firmware executions or the five proposed event-order graphs.
The calendar oracle uses explicit UK transition offsets rather than deriving its expectation from the validator.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import product
from zoneinfo import ZoneInfo

import pytest
from ocpp_hub_core.prototype.ics_writes import validate_write
from ocpp_hub_core.prototype.policy import DeniedError, Policy
from ocpp_hub_core.prototype.schedule_time import temporary_window
from ocpp_hub_core.prototype.security import SecurityError

ZONE = ZoneInfo("Europe/London")
TAG = "TABLE_TEST"


@dataclass(frozen=True)
class CalendarDay:
    """Independent expected UTC boundaries for one ordinary or UK clock-change date."""

    date: str
    spring: bool = False
    autumn: bool = False

    # Convert a wall representation with the published UK transition rule, including gap aliases.
    def expected(self, minute: int, fold: int) -> tuple[int, int, int, int]:
        midnight = int(datetime.fromisoformat(self.date).replace(tzinfo=UTC).timestamp())
        offset = 0
        normalized = minute
        if self.spring:
            offset = 60 if minute >= 120 or 60 <= minute < 120 and fold == 1 else 0
            if 60 <= minute < 120:
                normalized += 60 if fold == 0 else -60
        if self.autumn:
            offset = 60 if minute < 60 or 60 <= minute < 120 and fold == 0 else 0
        epoch = midnight + (minute - offset) * 60
        beginning = midnight - (3600 if self.autumn else 0)
        ending = midnight + (5 if self.spring else 6) * 3600
        return epoch, normalized, beginning, ending


# Keep the exact supported wire shape independent of the production timetable builder.
def wire_plan(current: object, duration: object, kind: str = "Relative", mask: int = 0, stop: str = "Duration"):
    body = {
        "planId": 10001,
        "type": "Plan",
        "stopModel": stop,
        "limitCurrent": current,
        "stopValue": duration,
        "validTime": "2026-11-11T00:00:00Z",
        "startTime": 180,
        "connectorId": 1,
        "kindType": kind,
        "recurring": mask,
        "idTag": TAG,
        "eco": "Fast",
    }
    return {"vendorId": "IoCharger", "messageId": "setPlan", "data": json.dumps(body)}


# Intent: evaluate all 77,760 declared wall-minute/fold/lead/current rows against production time admission.
# Why: elapsed duration must end at local 06:00 on ordinary, short and long nights without admitting ambiguous starts.
def test_m01_calendar_table(record_property):
    days = (CalendarDay("2026-03-29", spring=True), CalendarDay("2026-10-25", autumn=True), CalendarDay("2026-11-11"))
    scanned = aliases = accepted = 0
    unique = set()
    observed_durations = {day.date: set() for day in days}
    policy = Policy(max_current_a=32, allowed_tags=frozenset({TAG}))
    for day, minute, fold, lead, selected in product(days, range(1440), (0, 1), (29, 30, 31), (6, 16, 32)):
        wall = datetime.fromisoformat(day.date).replace(tzinfo=ZONE, fold=fold) + timedelta(minutes=minute)
        # Arithmetic resets fold; select it on the finished wall representation.
        wall = wall.replace(fold=fold)
        epoch, normalized, beginning, ending = day.expected(minute, fold)
        assert int(wall.timestamp()) == epoch, (day, minute, fold)
        now = epoch - lead
        wanted = (
            lead >= 30
            and now >= beginning
            and normalized < 360
            and not (day.autumn and 60 <= minute < 120)
        )
        got = None
        try:
            got = temporary_window(epoch, now, ZONE)
        except SecurityError:
            pass
        assert (got is not None) == wanted, (day, minute, fold, lead, selected)
        if got is not None:
            start, end = got
            assert start.hour * 60 + start.minute == normalized
            assert end == ending and (end - epoch) % 60 == 0
            duration = (ending - epoch) // 60
            assert validate_write(wire_plan(selected, duration), policy)["stopValue"] == duration
            observed_durations[day.date].add(duration)
            accepted += 1
        aliases += day.spring and 60 <= minute < 120
        unique.add((epoch, now, selected))
        scanned += 1
    assert scanned == 77760 and aliases == 1080 and len(unique) == 38880
    assert {day: (min(values), max(values)) for day, values in observed_durations.items()} == {
        "2026-03-29": (1, 299),
        "2026-10-25": (1, 419),
        "2026-11-11": (1, 359),
    }
    record_property("model", "M01")
    record_property("candidate_rows", scanned)
    record_property("unique_absolute_requests", len(unique))
    record_property("nonexistent_wall_alias_rows", aliases)
    record_property("accepted_rows", accepted)


# Intent: evaluate all 18,576 declared recurrence/current/ceiling/stop-value combinations using the actual codec.
# Why: fractional finite current is supported, but fractional durations, coercion and nonfinite JSON are not.
def test_m07_native_codec_table(record_property):
    forms = [("Recurring", mask, "Duration") for mask in range(1, 128)]
    forms += [("Relative", 0, "Duration"), ("Relative", 0, "Full")]
    currents = (5, 6, 6.5, 16, 32, 33, True, "6", float("nan"))
    durations = (0, 0.1, 1, 135, 360, 1440, 1441, True)
    scanned = accepted = 0
    for (kind, mask, stop), ceiling, current, duration in product(forms, (6, 32), currents, durations):
        wanted_current = type(current) in (int, float) and 6 <= current <= ceiling
        wanted_duration = type(duration) is int and (
            1 <= duration <= 1440 if stop == "Duration" else duration == 0
        )
        wanted = wanted_current and wanted_duration
        got = False
        try:
            answer = validate_write(
                wire_plan(current, duration, kind, mask, stop),
                Policy(max_current_a=ceiling, allowed_tags=frozenset({TAG})),
            )
            assert answer["kindType"] == kind and answer["recurring"] == mask
            assert answer["limitCurrent"] == current and answer["stopValue"] == duration
            got = True
        except DeniedError:
            pass
        assert got == wanted, (kind, mask, stop, ceiling, current, duration)
        accepted += got
        scanned += 1
    assert scanned == 18576
    record_property("model", "M07")
    record_property("candidate_rows", scanned)
    record_property("accepted_rows", accepted)


# Intent: keep second alignment, the lead edge and current-day admission distinct from daylight-saving aliases.
# Why: a broad minute table alone would miss a schedule admitted at 29 seconds or on the following date.
@pytest.mark.parametrize(
    ("start", "now", "allowed"),
    [
        ("2026-11-11T03:00:00Z", "2026-11-11T02:59:30Z", True),
        ("2026-11-11T03:00:00Z", "2026-11-11T02:59:30.001Z", False),
        ("2026-11-11T03:00:01Z", "2026-11-11T02:00:00Z", False),
        ("2026-11-11T06:00:00Z", "2026-11-11T03:00:00Z", False),
        ("2026-11-12T03:00:00Z", "2026-11-11T23:59:00Z", False),
        ("2026-11-11T03:00:00Z", "2026-11-11T03:00:00Z", False),
        ("2026-11-11T03:00:00Z", "2026-11-11T03:00:01Z", False),
    ],
)
def test_calendar_request_boundary(start, now, allowed):
    epoch = int(datetime.fromisoformat(start).timestamp())
    current = datetime.fromisoformat(now).timestamp()
    if allowed:
        assert temporary_window(epoch, current, ZONE)[1] == int(datetime(2026, 11, 11, 6, tzinfo=UTC).timestamp())
    else:
        with pytest.raises(SecurityError):
            temporary_window(epoch, current, ZONE)
