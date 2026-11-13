"""Coverage declarations retain real source links, exact denominators and distinct safety/recovery obligations."""

from pathlib import Path

from tests_model.catalog import FAMILIES
from tools.recovery_coverage import matrix

ROOT = Path(__file__).resolve().parents[1]


# Intent: every declared finite family and named focused risk must resolve to executable, source-pinned tests.
# Why: a matrix with dangling links, uncounted exclusions or a missing risk is not reviewable coverage evidence.
def test_coverage_registry_has_real_tests_and_exact_denominators():
    report = matrix()
    risks = {row["identifier"]: row for row in report["risks"]}
    assert len(risks) == len(report["risks"])
    for family in FAMILIES:
        path, function = family.test_function.split("::")
        assert "def " + function + "(" in (ROOT / path).read_text()
        assert set(family.risks) <= set(risks)
    for risk in risks.values():
        assert risk["safety"] and risk["recovery"] and risk["limits"] and risk["focused"]
        assert all((ROOT / path).is_file() for path in risk["focused"])
    assert report["execution_status"] == "declared only; no qualification inferred"
