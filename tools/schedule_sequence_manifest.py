#!/usr/bin/env python3
"""Write the complete declared input universe, exclusions and runnable sequence identifiers."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests_model.catalog import FAMILIES, SCHEMA, case_record  # noqa: E402


# Make the finite coverage denominator independently inspectable before or after execution.
def manifest(path: Path, suite: str = "engine-model") -> dict:
    accepted = 0
    excluded = Counter()
    families = Counter()
    identifiers = set()
    with path.open("w") as output:
        for family in FAMILIES:
            if suite not in family.suites:
                continue
            raw_count, admitted_count = 0, 0
            for case in family.inputs():
                raw_count += 1
                reason = family.excluding(case)
                assert case.identifier not in identifiers
                identifiers.add(case.identifier)
                row = case_record(family, case)
                output.write(json.dumps(row, sort_keys=True) + "\n")
                if reason:
                    excluded[reason] += 1
                else:
                    accepted += 1
                    families[family.name] += 1
                    admitted_count += 1
            if (raw_count, admitted_count) != (family.raw, family.admitted):
                raise ValueError("registered family denominator differs: " + family.name)
    return {
        "schema": SCHEMA,
        "suite": suite,
        "families": dict(families),
        "raw_candidates": len(identifiers),
        "runnable": accepted,
        "exclusions": dict(excluded),
        "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "scope": ("complete registered finite input families and declared exclusions; not arbitrary product histories"),
    }


# The caller chooses an authorized scratch output; running the model is a separate explicit offline-test command.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--suite", choices=("engine-model", "ha-proxy", "ha-proxy-legacy", "ha-legacy"), default="engine-model"
    )
    args = parser.parse_args()
    result = manifest(args.output, args.suite)
    args.output.with_suffix(".summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
