"""Native meter-channel refusals retained after plain commissioning recovery retirement."""

import time
from typing import Any

import pytest
from ocpp_hub_core.prototype.security import SecurityError


# Intent: native idle checks inspect every phase and every power channel, including malformed extra/missing channels.
# Why: a zero first phase and stale zero total must not hide current flowing elsewhere during recovery or cleanup.
@pytest.mark.parametrize(
    "field,values",
    [
        ("cur", [0, 60, 0]),
        ("cur", [0, 0, 60]),
        ("power", [1440, 0, 0, 0]),
        ("power", [0, 1440, 0, 0]),
        ("power", [0, 0, 1440, 0]),
        ("cur", [0]),
        ("cur", [0, 0, 0, 0]),
        ("power", [0, 0, 0]),
        ("cur", [0, True, 0]),
        ("power", [0, float("nan"), 0, 0]),
    ],
)
def test_native_idle_requires_all_meter_channels(field: str, values: list[Any]) -> None:
    from ocpp_hub_core.prototype.trial_evidence import native_evidence

    from tests.test_trial_transport import native_values

    data = native_values()
    data["METER"][0]["meterData"][field] = values
    with pytest.raises(SecurityError):
        evidence = native_evidence(data["config"], data, time.time())
        evidence.require_unplugged("OFFLINE001")
