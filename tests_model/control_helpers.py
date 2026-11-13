"""Independent physical recovery suffix shared by the finite control-boundary families."""

import uuid
from datetime import timedelta

from tests.recovery_barriers import completed_control, fresh_daily_values, returned_to_app, witnessed_native_loss
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import positive_flow
from tests_model.test_start_recovery_sequences import close_daily


# Verify fresh Pause/Return, two genuine ordinary lifecycles and real native loss without replayed owner commands.
async def ordinary_recovery(b, expected, demand, *, first_close="native"):
    c, rig, firmware = b.controller, b.rig, b.rig.bench.firmware
    closed_before = b.session.journal.db.execute("SELECT COUNT(*) FROM transactions WHERE stop IS NOT NULL").fetchone()[
        0
    ]
    pause, revision = uuid.uuid4().hex, c.requests.intent().revision
    c.command(pause, "pause", revision)
    assert (await completed_control(b, pause, mode="paused", revision=revision + 1, ceiling=0))["local_hold_verified"]
    assert (await returned_to_app(b, expected))["normal_start_ready"]
    locals_seen = []
    for number in range(2):
        beginning, meter_start = fresh_daily_values(rig)
        local = await positive_flow(
            b,
            "daily",
            call_id=uuid.uuid4().hex,
            meter_start=meter_start,
            timestamp=beginning.isoformat().replace("+00:00", "Z"),
        )
        assert local not in locals_seen
        locals_seen.append(local)
        if not demand:
            rig.peer.permission_enabled, firmware.requested_current = False, 0
        await healthy(c)
        assert firmware.current == (expected if demand else 0)
        assert await witnessed_native_loss(b, expected, local) >= 3
        await close_daily(
            b,
            local,
            first_close,
            timestamp=(beginning + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            meter_stop=meter_start + 100,
        )
        assert b.session.journal.active() is None and not starts(rig)
        assert not any(row[2] == "RemoteStopTransaction" for row in rig.bench.device_calls)
    assert (
        b.session.journal.db.execute("SELECT COUNT(*) FROM transactions WHERE stop IS NOT NULL").fetchone()[0]
        == closed_before + 2
    )


# Identify only the real reserved station profile at an exact physical ceiling.
def station_limit(frame, limit):
    return (
        frame[2] == "SetChargingProfile"
        and frame[3]["csChargingProfiles"]["chargingProfileId"] == 7001
        and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == limit
    )
