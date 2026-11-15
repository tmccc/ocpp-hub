"""Independently stated adjacent histories protect causal exclusions beyond aggregate case counts."""

from dataclasses import replace
from itertools import permutations

import pytest

from tests_model.ha_cases import OutageCase, outage_exclusion
from tests_model.recovery_cases import StartRecoveryCase, start_exclusion
from tests_model.schedule_cases import SequenceCase, SourceCase, exclusion, source_exclusion


# Intent: each side of the source-loss boundary is independently named and classified with its causal reason.
# Why: reversing the comparison preserves sixty admitted orders and would pass denominator-only checks.
@pytest.mark.parametrize(
    "events,expected",
    [
        (("loss", "restore", "observe", "cutoff", "restart"), None),
        (("restore", "loss", "observe", "cutoff", "restart"), "source restoration precedes its loss"),
        (("observe", "cutoff", "loss", "restart", "restore"), None),
        (("observe", "cutoff", "restore", "restart", "loss"), "source restoration precedes its loss"),
    ],
)
def test_source_loss_adjacent_boundary(events, expected):
    assert source_exclusion(SourceCase(6, False, None, "ledger", events)) == expected


# Intent: an equal-count membership swap fails the named witness contract despite preserving the denominator.
# Why: this explicit negative control demonstrates why a correct aggregate count is insufficient evidence.
def test_source_boundary_contract_detects_equal_count_swap():
    before = ("restore", "loss", "observe", "cutoff", "restart")
    after = ("loss", "restore", "observe", "cutoff", "restart")
    histories = [SourceCase(6, False, None, "ledger", order) for order in permutations(before)]
    admitted = {case.events for case in histories if source_exclusion(case) is None}
    mutant = (admitted - {after}) | {before}
    assert len(mutant) == len(admitted) == 60
    assert after in admitted and before not in admitted
    assert not (after in mutant and before not in mutant)


# Intent: cutoff ordering and physical departure each retain their separately stated admission boundary.
# Why: a generic event permutation count cannot identify either causal exclusion getting inverted.
@pytest.mark.parametrize(
    "events,disruption,first_close,expected",
    [
        (
            ("cutoff", "native", "report", "observe", "cancel"),
            "restart",
            "native",
            "this universe requires the contested session to begin before its cutoff",
        ),
        (("native", "cutoff", "report", "observe", "cancel"), "restart", "native", None),
        (("report", "cutoff", "native", "observe", "cancel"), "restart", "ledger", None),
        (
            ("native", "cancel", "report", "observe", "cutoff"),
            "unplug",
            "ledger",
            "the physical departure has already established native-first closure",
        ),
        (("cancel", "native", "report", "observe", "cutoff"), "unplug", "ledger", None),
    ],
)
def test_schedule_adjacent_boundaries(events, disruption, first_close, expected):
    case = SequenceCase("requested", 6, "daily", False, disruption, first_close, events)
    assert exclusion(case) == expected


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
