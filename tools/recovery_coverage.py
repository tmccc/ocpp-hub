#!/usr/bin/env python3
"""Build the source-linked risk matrix, optionally attaching freshly re-audited complete qualification evidence."""

import argparse
import json
import subprocess
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests_model.catalog import FAMILIES, SCHEMA  # noqa: E402
from tests_model.coverage import RISKS  # noqa: E402


# Declared coverage never masquerades as execution; finite denominators and focused-only areas remain distinct.
def matrix():
    risks = {risk.identifier: asdict(risk) | {"families": []} for risk in RISKS}
    families = []
    for family in FAMILIES:
        raw, admitted, exclusions = 0, 0, Counter()
        for case in family.inputs():
            raw += 1
            reason = family.excluding(case)
            if reason:
                exclusions[reason] += 1
            else:
                admitted += 1
        if (raw, admitted) != (family.raw, family.admitted):
            raise ValueError("family declaration differs from generated inputs: " + family.name)
        families.append(
            {
                "name": family.name,
                "suites": family.suites,
                "test_function": family.test_function,
                "raw": raw,
                "admitted": admitted,
                "exclusions": dict(exclusions),
                "risks": family.risks,
            }
        )
        for identifier in family.risks:
            risks[identifier]["families"].append(family.name)
    for risk in risks.values():
        risk["coverage_kind"] = "finite histories plus focused tests" if risk["families"] else "focused tests"
    return {
        "schema": SCHEMA,
        "execution_status": "declared only; no qualification inferred",
        "families": families,
        "risks": list(risks.values()),
        "scope": "Finite declared inputs and named failure boundaries; not arbitrary product histories.",
    }


# Full claims require the normal validator to re-audit original raw artifacts, never a copied green flag.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--qualification-report", type=Path)
    args = parser.parse_args()
    report = matrix()
    if args.qualification_report:
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(ROOT / "tools/validate.py"),
                "--tests-only",
                "--qualification-report",
                str(args.qualification_report),
            ],
            cwd=ROOT,
            check=True,
        )
        bundle = json.loads(args.qualification_report.read_text())
        report["qualification"] = {
            "commit": bundle["commit"],
            "source_digest": bundle["source_digest"],
            "phases": {
                name: json.loads((Path(path) / "audit.json").read_text()) for name, path in bundle["phases"].items()
            },
        }
        report["execution_status"] = "all four complete20-worker suites freshly re-audited"
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
