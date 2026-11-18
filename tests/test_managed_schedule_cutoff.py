"""Native daily execution reads actual mutable charger storage."""

from datetime import datetime

import pytest
from ocpp_hub_core.prototype.security import SecurityError

from tests.managed_rig import starts
from tests.native_schedule_support import ZONE, timer_bench
from tests.tls_support import tls as tls


# A damaged saved plan changes both full native readback and the independent next occurrence.
# Intent: an altered original daily plan changes both its real readback and its next simulated occurrence.
# Why: a canonical constant substituted by the fake for plan 9 would silently mask a damaged timetable.
async def test_next_daily_reads_device_storage(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        b.device.plans[9].update(startTime=720, stopValue=1, recurring=0)
        with pytest.raises(SecurityError):
            await b.c.native.read()
        assert await b.advance_idle_to(int(datetime(2026, 11, 12, 6, tzinfo=ZONE).timestamp())) is None
        assert b.session.journal.active() is None and not starts(b.rig)
