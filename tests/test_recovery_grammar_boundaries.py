"""Independently stated adjacent histories protect causal exclusions beyond aggregate case counts."""

from dataclasses import replace

import pytest

from tests_model.ha_cases import OutageCase, outage_exclusion
from tests_model.recovery_cases import StartRecoveryCase, start_exclusion


# Intent: replay and startup replacement require their specific predecessors; a timed test remains six amperes.
# Why: these exclusions encode distinct command receipt and fresh-startup obligations rather than a count target.
@pytest.mark.parametrize(
    "events,expected",
    [
        (("restore", "pause", "restart-ready", "observe", "replay"), None),
        (("restore", "replay", "restart-ready", "observe", "pause"), "receipt replay precedes its original Pause"),
        (
            ("restart-ready", "pause", "restore", "observe", "replay"),
            "successful replacement listener requires restored native startup audit",
        ),
    ],
)
def test_start_recovery_adjacent_boundaries(events, expected):
    case = StartRecoveryCase("start", 6, "preflight", False, "ledger", events)
    assert start_exclusion(case) == expected
    assert start_exclusion(replace(case, action="start_test", selected=16)) == "fixed timed test requires selected6A"


# Intent: a terminal entry lifetime cannot retry setup without reactivation, while a reload remains live.
# Why: identical aggregate admission counts can hide retry authority crossing the lifecycle boundary.
@pytest.mark.parametrize("lifecycle", ["reload", "unload", "remove", "disable"])
def test_ha_terminal_retry_boundary(lifecycle):
    before = OutageCase("first", lifecycle, False, ("retry", "lifecycle", "deadline", "stale"))
    after = replace(before, order=("lifecycle", "retry", "deadline", "stale"))
    assert outage_exclusion(before) is None
    assert outage_exclusion(after) == (
        None if lifecycle == "reload" else "setup retry follows terminal entry cleanup without reactivation"
    )
