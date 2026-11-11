"""Resolve an absolute temporary start against the charger's current local day and 06:00 end."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from .security import SecurityError


# End the actual occurrence at local 06:00 even when the UTC offset changes during its window.
def cutoff_epoch(start_epoch: int, zone: ZoneInfo) -> int:
    start = datetime.fromtimestamp(start_epoch, zone)
    return int(start.replace(hour=6, minute=0, second=0, microsecond=0, fold=0).timestamp())


# Keep time admission identical for the API and the finite calendar checks; epochs have no nonexistent wall times.
def temporary_window(start_epoch: int, now: float, zone: ZoneInfo) -> tuple[datetime, int]:
    if not 30 <= start_epoch - now <= 24 * 3600:
        raise SecurityError("a future start within the next day is required")
    start = datetime.fromtimestamp(start_epoch, zone)
    end_epoch = cutoff_epoch(start_epoch, zone)
    if (
        start_epoch % 60
        or start_epoch >= end_epoch
        or start.date() != datetime.fromtimestamp(now, zone).date()
        or start.replace(fold=0).utcoffset() != start.replace(fold=1).utcoffset()
    ):
        raise SecurityError("temporary timer requires a future whole minute on the current local date")
    return start, end_epoch
