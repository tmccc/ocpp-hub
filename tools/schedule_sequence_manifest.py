#!/usr/bin/env python3
"""Write the complete declared input universe, exclusions and runnable sequence identifiers."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests_model.schedule_cases import candidates, exclusion, source_candidates, source_exclusion  # noqa: E402


# Make the finite coverage denominator independently inspectable before or after execution.
def manifest(path: Path) -> dict:
    accepted = 0
    excluded = Counter()
    families = Counter()
    identifiers = set()
    with path.open("w") as output:
        for family, inputs, excluding in (
            ("two_receipt", candidates(), exclusion),
            ("source_proof", source_candidates(), source_exclusion),
        ):
            for case in inputs:
                reason = excluding(case)
                assert case.identifier not in identifiers
                identifiers.add(case.identifier)
                row = {"id": case.identifier, "family": family, **asdict(case), "exclusion": reason}
                output.write(json.dumps(row, sort_keys=True) + "\n")
                if reason:
                    excluded[reason] += 1
                else:
                    accepted += 1
                    families[family] += 1
    return {
        "families": dict(families),
        "raw_candidates": len(identifiers),
        "runnable": accepted,
        "exclusions": dict(excluded),
        "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "scope": (
            "all five-event permutations and declared finite seeds; not arbitrary histories or all read interleavings"
        ),
    }


# The caller chooses an authorized scratch output; running the model is a separate explicit offline-test command.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = manifest(args.output)
    args.output.with_suffix(".summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
