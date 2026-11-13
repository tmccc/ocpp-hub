"""Independent physical recovery suffix shared by the finite control-boundary families."""

import uuid
from datetime import UTC, datetime, timedelta

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import positive_flow
from tests_model.test_start_recovery_sequences import close_daily, observe


# Verify fresh Pause/Return, two genuine ordinary lifecycles and real native loss without replayed owner commands.
async def ordinary_recovery(b, expected, demand, *, first_close="native"):
    c, rig, firmware = b.controller, b.rig, b.rig.bench.firmware
    c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
    assert (await healthy(c))["local_hold_verified"] and firmware.effective_limit() == 0
    c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
    assert (await healthy(c))["normal_start_ready"] and firmware.effective_limit() == expected
    locals_seen = []
    for number in range(2):
        beginning = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=number * 2)
        local = await positive_flow(
            b,
            "daily",
            call_id=uuid.uuid4().hex,
            meter_start=1000 + number * 100,
            timestamp=beginning.isoformat().replace("+00:00", "Z"),
        )
        assert local not in locals_seen
        locals_seen.append(local)
        if not demand:
            rig.peer.permission_enabled, firmware.requested_current = False, 0
        await healthy(c)
        assert firmware.current == (expected if demand else 0)
        rig.peer.unavailable = True
        await observe(c, 5)
        assert firmware.effective_limit() == 0 and b.session.journal.active() == local
        rig.peer.unavailable = False
        await healthy(c)
        assert firmware.effective_limit() == expected
        await close_daily(
            b,
            local,
            first_close,
            timestamp=(beginning + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            meter_stop=1100 + number * 100,
        )
        assert b.session.journal.active() is None and not starts(rig)
        assert not any(row[2] == "RemoteStopTransaction" for row in rig.bench.device_calls)
    assert b.session.journal.db.execute("SELECT COUNT(*) FROM transactions WHERE stop IS NOT NULL").fetchone()[0] == 2


# Identify only the real reserved station profile at an exact physical ceiling.
def station_limit(frame, limit):
    return (
        frame[2] == "SetChargingProfile"
        and frame[3]["csChargingProfiles"]["chargingProfileId"] == 7001
        and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == limit
    )
