"""Strict present-time ampere coverage, separate from any bounded read-only retry policy."""

from __future__ import annotations

from datetime import datetime

from ..frames import Frame
from .policy import DeniedError, number
from .rpc import UnavailableError


class FutureScheduleError(UnavailableError):
    """An otherwise valid bounded schedule has not begun at the actual response-completion instant."""

    # Retain only the numeric offset, never protocol bodies, identities or peer exception messages.
    def __init__(self, ahead_seconds: float) -> None:
        super().__init__("composite schedule does not yet cover response completion")
        self.ahead_seconds = ahead_seconds


# Validate every period and limit before distinguishing a future interval from an invalid or unsafe response.
def verify_composite(frame: Frame, limit: float, completed_at: datetime) -> None:
    if len(frame) != 3 or frame[0] != 3 or not isinstance(frame[2], dict):
        raise UnavailableError("charger did not confirm an effective ampere ceiling")
    result = frame[2]
    schedule = result.get("chargingSchedule")
    if (
        result.get("status") != "Accepted"
        or type(result.get("connectorId")) is not int
        or result["connectorId"] != 1
        or not isinstance(schedule, dict)
        or schedule.get("chargingRateUnit") != "A"
        or type(schedule.get("duration")) is not int
        or not 1 <= schedule["duration"] <= 86400
    ):
        raise UnavailableError("charger did not confirm present connector coverage")
    periods = schedule.get("chargingSchedulePeriod")
    if not isinstance(periods, list) or not 1 <= len(periods) <= 128:
        raise UnavailableError("invalid composite schedule periods")
    previous = -1
    for period in periods:
        if not isinstance(period, dict):
            raise UnavailableError("invalid composite schedule period")
        offset = period.get("startPeriod")
        phases = period.get("numberPhases", 1)
        if (
            type(offset) is not int
            or previous == -1
            and offset != 0
            or not previous < offset < schedule["duration"]
            or type(phases) is not int
            or not 1 <= phases <= 3
        ):
            raise UnavailableError("invalid composite schedule periods")
        previous = offset
        try:
            number(period.get("limit"), limit)
        except DeniedError:
            raise UnavailableError("effective current exceeds the requested ceiling") from None
    try:
        raw_start = result.get("scheduleStart")
        if not isinstance(raw_start, str):
            raise ValueError
        start = datetime.fromisoformat(raw_start.replace("Z", "+00:00"))
        if start.tzinfo is None or completed_at.tzinfo is None:
            raise ValueError
        age = (completed_at - start).total_seconds()
    except ValueError:
        raise UnavailableError("composite schedule requires valid timezone-aware coverage") from None
    if age < 0:
        raise FutureScheduleError(-age)
    if age >= schedule["duration"]:
        raise UnavailableError("composite schedule has already ended")
