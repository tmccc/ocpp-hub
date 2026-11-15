"""An intermediate positive station period cannot hide behind a correct final zero state."""

from types import SimpleNamespace

import pytest
from ocpp_hub_core import commands

from tests.recovery_barriers import require_zero_held


# Intent: physical draw, physical offer and any intermediate station period each independently violate a zero hold.
# Why: final-state-only checks and first-period-only checks miss brief or scheduled positive authority.
@pytest.mark.parametrize("problem", ["control", "offer", "draw", "first_period", "later_period"])
def test_zero_hold_oracle_checks_physical_state_and_every_wire_period(problem):
    profile = commands.set_charging_profile(0, "ChargePointMaxProfile", 0, 7001)
    periods = profile["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
    if problem == "first_period":
        periods[0]["limit"] = 6
    elif problem == "later_period":
        periods.append({"startPeriod": 60, "limit": 6})
    firmware = SimpleNamespace(
        effective_limit=lambda: 6 if problem == "offer" else 0, current=6 if problem == "draw" else 0
    )
    rig = SimpleNamespace(
        bench=SimpleNamespace(
            firmware=firmware,
            device_calls=[[2, "synthetic-zero-oracle", "SetChargingProfile", profile]],
        )
    )
    if problem == "control":
        require_zero_held(rig, 0)
    else:
        with pytest.raises(AssertionError):
            require_zero_held(rig, 0)
