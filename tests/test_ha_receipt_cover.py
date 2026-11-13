"""Independent denominator and admission checks for the HA receipt identity grammar."""

from dataclasses import replace
from itertools import product

import pytest

from tests_model.catalog import FAMILIES
from tests_model.ha_cases import receipt_candidates, receipt_exclusion


# Compare production inputs/admission with the declared policy rather than with its implementation's own counts.
def require_exact_cover(raw, admitted):
    expected = {
        (action, position, identity, terminal, 16 if action == "set_current" else None)
        for action, position, identity, terminal in product(
            ("pause", "app_control", "start", "start_test", "set_current"),
            range(4),
            ("matching", "different", "missing", "null", "malformed"),
            ("completed", "failed"),
        )
    }
    expected_admitted = {row for row in expected if row[2] == "matching" or row[3] == "completed"}

    # Include the exact current payload in equality; bools or None cannot represent the declared sixteen-amp input.
    def row(case):
        assert (
            case.current_a is None
            if case.action != "set_current"
            else type(case.current_a) is int and case.current_a == 16
        )
        return case.action, case.boundary, case.identity, case.terminal, case.current_a

    raw_rows, admitted_rows = [row(c) for c in raw], [row(c) for c in admitted]
    assert len(raw_rows) == len(set(raw_rows)) == 200 and set(raw_rows) == expected
    assert len(admitted_rows) == len(set(admitted_rows)) == 120 and set(admitted_rows) == expected_admitted


# Intent: all five actions, exact payloads, positions and shapes remain declared while both matching terminals run.
# Why: exclusion cannot quietly shrink a denominator or drop the matching failed-receipt safety check.
def test_ha_receipt_exact_cover_and_catalog_admission():
    raw = receipt_candidates()
    admitted = tuple(c for c in raw if receipt_exclusion(c) is None)
    require_exact_cover(raw, admitted)
    family = next(f for f in FAMILIES if f.name == "ha_receipt")
    assert family.inputs is receipt_candidates and family.excluding is receipt_exclusion
    assert family.raw == 200 and family.admitted == 120
    assert len([c for c in admitted if c.identity == "matching" and c.terminal == "failed"]) == 20
    for excluded in (c for c in raw if receipt_exclusion(c) is not None):
        assert excluded.identity != "matching" and excluded.terminal == "failed"
        assert replace(excluded, terminal="completed") in admitted


# Intent: missing input, missing selected case, count-preserving replacement and wrong current all fail the oracle.
# Why: a cover checker which accepts its own reduced input cannot establish that redundancy removal stayed bounded.
@pytest.mark.parametrize("damage", ["raw-missing", "admitted-missing", "swap-matching-failure", "wrong-current"])
def test_ha_receipt_cover_rejects_corrupted_denominators(damage):
    raw = list(receipt_candidates())
    admitted = [c for c in raw if receipt_exclusion(c) is None]
    if damage == "raw-missing":
        raw.pop()
    elif damage == "admitted-missing":
        admitted.pop()
    elif damage == "swap-matching-failure":
        victim = next(c for c in admitted if c.identity == "matching" and c.terminal == "failed")
        admitted[admitted.index(victim)] = next(c for c in raw if receipt_exclusion(c) is not None)
    else:
        index = next(i for i, c in enumerate(raw) if c.action == "set_current")
        raw[index] = replace(raw[index], current_a=None)
    with pytest.raises(AssertionError):
        require_exact_cover(raw, admitted)
