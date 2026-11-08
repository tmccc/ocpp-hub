"""Synthetic reproduction of a 154 ms future schedule and strict validation before timing classification."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from ocpp_hub_core.prototype.composite import FutureScheduleError, verify_composite
from ocpp_hub_core.prototype.rpc import UnavailableError

START = datetime(2026, 11, 8, 16, 38, 45, tzinfo=UTC)


# Build a synthetic result with the same timing offset, without copying private identifiers or wire frames.
def result() -> list[Any]:
    return [
        3,
        "synthetic",
        {
            "status": "Accepted",
            "connectorId": 1,
            "scheduleStart": START.isoformat(),
            "chargingSchedule": {
                "duration": 60,
                "chargingRateUnit": "A",
                "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 6, "numberPhases": 1}],
            },
        },
    ]


# Intent: an otherwise valid future schedule is never accepted before its reported beginning.
# Why: the real failure was 154 ms early, rather than an expired one-second observation window.
def test_observed_future_offset_preserves_exact_coverage() -> None:
    with pytest.raises(FutureScheduleError) as caught:
        verify_composite(result(), 6, START - timedelta(microseconds=154274))
    assert caught.value.ahead_seconds == pytest.approx(0.154274)
    verify_composite(result(), 6, START)
    verify_composite(result(), 6, START + timedelta(seconds=59, microseconds=999999))
    with pytest.raises(UnavailableError) as expired:
        verify_composite(result(), 6, START + timedelta(seconds=60))
    assert not isinstance(expired.value, FutureScheduleError)


# Intent: unsafe limits, malformed coverage and invalid phase counts are not retryable clock jitter.
# Why: delaying a bad schedule must never turn it into proof that the station ceiling is safe.
@pytest.mark.parametrize("change", ["limit", "later-limit", "phase", "bool-phase", "period", "duration", "unit"])
def test_invalid_future_schedule_is_not_retryable(change: str) -> None:
    frame = result()
    schedule = frame[2]["chargingSchedule"]
    if change == "limit":
        schedule["chargingSchedulePeriod"][0]["limit"] = 32
    elif change == "later-limit":
        schedule["chargingSchedulePeriod"].append({"startPeriod": 30, "limit": 32})
    elif change == "phase":
        schedule["chargingSchedulePeriod"][0]["numberPhases"] = 0
    elif change == "bool-phase":
        schedule["chargingSchedulePeriod"][0]["numberPhases"] = True
    elif change == "period":
        schedule["chargingSchedulePeriod"][0]["startPeriod"] = False
    elif change == "duration":
        schedule["duration"] = True
    else:
        schedule["chargingRateUnit"] = "W"
    with pytest.raises(UnavailableError) as caught:
        verify_composite(frame, 6, START - timedelta(milliseconds=154))
    assert not isinstance(caught.value, FutureScheduleError)


# Intent: malformed or unsafe protocol fields always fail as ordinary validation errors rather than clock jitter.
# Why: future-only retry permission must not bypass the original full schema, bounds or ampere ceiling.
@pytest.mark.parametrize(
    "path,value",
    [
        ((), []),
        ((), [4, "synthetic", "NotSupported", "", {}]),
        ((2,), []),
        ((2, "status"), "Rejected"),
        ((2, "connectorId"), True),
        ((2, "connectorId"), 1.0),
        ((2, "connectorId"), 0),
        ((2, "chargingSchedule"), []),
        ((2, "chargingSchedule", "duration"), None),
        ((2, "chargingSchedule", "duration"), 0),
        ((2, "chargingSchedule", "duration"), 86401),
        ((2, "chargingSchedule", "duration"), 60.0),
        ((2, "chargingSchedule", "chargingSchedulePeriod"), None),
        ((2, "chargingSchedule", "chargingSchedulePeriod"), []),
        ((2, "chargingSchedule", "chargingSchedulePeriod"), [{}] * 129),
        ((2, "chargingSchedule", "chargingSchedulePeriod"), [None]),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "startPeriod"), -1),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "startPeriod"), 1),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "startPeriod"), 0.0),
        ((2, "chargingSchedule", "chargingSchedulePeriod"), [{"startPeriod": 0, "limit": 6}] * 2),
        (
            (2, "chargingSchedule", "chargingSchedulePeriod"),
            [{"startPeriod": 0, "limit": 6}, {"startPeriod": 60, "limit": 6}],
        ),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "numberPhases"), 4),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "limit"), None),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "limit"), -1),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "limit"), float("nan")),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "limit"), float("inf")),
        ((2, "chargingSchedule", "chargingSchedulePeriod", 0, "limit"), True),
        ((2, "scheduleStart"), None),
        ((2, "scheduleStart"), "invalid"),
        ((2, "scheduleStart"), "2026-11-08T16:38:45"),
    ],
)
def test_full_protocol_validation_rejects_invalid_shapes(path: tuple[Any, ...], value: Any) -> None:
    frame = result()
    if not path:
        frame = value
    else:
        parent = frame
        for component in path[:-1]:
            parent = parent[component]
        parent[path[-1]] = value
    with pytest.raises(UnavailableError) as caught:
        verify_composite(frame, 6, START)
    assert not isinstance(caught.value, FutureScheduleError)


# Intent: inclusive valid duration, phase and period bounds remain accepted under strict validation.
# Why: rejecting a boundary value is not a substitute for enforcing the real protocol limits.
@pytest.mark.parametrize("duration,phases,count", [(1, 1, 1), (86400, 3, 1), (128, 1, 128)])
def test_protocol_upper_and_lower_bounds_are_inclusive(duration: int, phases: int, count: int) -> None:
    frame = result()
    schedule = frame[2]["chargingSchedule"]
    schedule["duration"] = duration
    schedule["chargingSchedulePeriod"] = [
        {"startPeriod": offset, "limit": 6, "numberPhases": phases} for offset in range(count)
    ]
    verify_composite(frame, 6, START)
