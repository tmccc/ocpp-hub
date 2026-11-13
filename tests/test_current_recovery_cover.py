"""Independent denominator, literal transport and reduction-sensitivity controls."""

from dataclasses import replace
from itertools import product

import pytest

from tests_model.current_obligations import obligations, require_cover, require_irreducible, require_transport_summary
from tests_model.deep_cases import current_domain_candidates, current_domain_exclusion, current_transport_candidates


# Read actual admitted inputs; expected obligations remain separately declared in the independent checker.
def admitted():
    return tuple(case for case in current_domain_candidates() if current_domain_exclusion(case) is None)


# Intent: the reduced recovery driver retains exactly the declared784 obligations and all1458 literal API pairs.
# Why: counting selected rows alone permits a whole value, direction or physical context to disappear unnoticed.
def test_current_recovery_cover_and_literal_transport_are_complete():
    raw, cover, transport = current_domain_candidates(), admitted(), current_transport_candidates()
    assert len(raw) == 2916 and len(cover) == 524 and len(raw) - len(cover) == 2392
    require_cover(cover)
    require_irreducible(cover)
    assert len(obligations(cover)) == 784
    assert len(transport) == 1458 and len({case.identifier for case in transport}) == 1458
    assert all(type(case.initial) is int and type(case.target) is int for case in transport)
    assert {(case.initial, case.target, case.intent) for case in transport} == set(
        product(range(6, 33), range(6, 33), ("app", "paused"))
    )


# Intent: same-value, lower/upper literal edge and interior direction deletions are independently detected.
# Why: an endpoint-only or hand-picked cover cannot silently replace the explicitly admitted obligations.
@pytest.mark.parametrize("pair", [(15, 15), (15, 6), (32, 15), (7, 31), (31, 7)])
def test_current_recovery_cover_rejects_missing_class(pair):
    broken = tuple(case for case in admitted() if (case.initial, case.target) != pair)
    assert obligations(admitted()) - obligations(broken)
    with pytest.raises(AssertionError):
        require_cover(broken)


# Intent: reversing the sole interior increase cannot masquerade as preserving an interior-increase obligation.
# Why: coverage tokens must retain directed meaning, beyond value marginals and undirected pair counts.
def test_current_recovery_cover_detects_direction_reversal():
    broken = tuple(
        replace(case, initial=31, target=7) if (case.initial, case.target) == (7, 31) else case for case in admitted()
    )
    assert len(broken) == len(admitted())
    missing = obligations(admitted()) - obligations(broken)
    assert all(token[:4] == ("boundary-direction", "interior", "interior", "up") for token in missing)
    with pytest.raises(AssertionError):
        require_cover(broken)


# Intent: actual transport summary assertions reject wrong frames, stale old values and repeat effects.
# Why: preserving a literal validator product is insufficient if the observed frame or durable receipt is wrong.
@pytest.mark.parametrize("fault", ["wrong-target", "old-initial", "replay-write", "same-failed"])
def test_current_transport_oracle_is_sensitive(fault):
    old, target = (15, 15) if fault == "same-failed" else (6, 16)
    observed = {
        "reset_selected": old,
        "reset_profile": old,
        "receipt_state": "completed",
        "receipt_current": target,
        "selected": target,
        "profile": target,
        "new_profile_limits": [target],
        "replayed_receipt_equal": True,
        "replay_new_wires": 0,
        "replay_durable_equal": True,
    }
    require_transport_summary(old, target, "app", observed)
    if fault == "wrong-target":
        observed["new_profile_limits"] = [17]
    elif fault == "old-initial":
        observed.update(receipt_current=old, selected=old, profile=old, new_profile_limits=[old])
    elif fault == "replay-write":
        observed.update(replay_new_wires=1, replay_durable_equal=False)
    else:
        observed["receipt_state"] = "failed"
    with pytest.raises(AssertionError):
        require_transport_summary(old, target, "app", observed)
