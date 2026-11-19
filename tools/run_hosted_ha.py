#!/usr/bin/env python3
"""Run a complete hosted Home Assistant suite and verify that every collected test passed."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.qualification_collection import ROOTS  # noqa: E402
from tools.run_model_shard import hosted_collection, verify_execution  # noqa: E402
from tools.schedule_sequence_manifest import manifest  # noqa: E402


# Collect independently, preserve pytest's exit status, and verify every test's execution record.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=[name for name in ROOTS if name != "engine-model"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.output.is_absolute():
        parser.error("use an absolute output directory")
    args.output.mkdir(parents=True, exist_ok=False)
    plan = args.output / "collection.json"
    evidence = args.output / "results.jsonl"
    env = {**os.environ, "TMPDIR": str(args.output), "SCENARIO_RESULTS": str(evidence)}
    collected = subprocess.run(
        [
            sys.executable,
            "-B",
            "tools/qualification_collection.py",
            "--source",
            str(ROOT),
            "--suite",
            args.suite,
            "--output",
            str(plan),
        ],
        cwd=ROOT,
        env=env,
    )
    if collected.returncode:
        return collected.returncode
    selected = hosted_collection(plan, args.suite)
    cases = args.output / "sequence-manifest.jsonl"
    manifest(cases, args.suite)
    models = {
        row["node_id"]: row for line in cases.read_text().splitlines() if (row := json.loads(line))["exclusion"] is None
    }
    if not models.keys() <= set(selected):
        raise RuntimeError("hosted Home Assistant plan omitted registered test cases")

    command = [sys.executable, "-B", "tools/offline_ha_validate.py", "-p", "tools.qualification_evidence"]
    if args.suite == "ha-legacy":
        command.append("--legacy-ha")
    result = subprocess.run([*command, *ROOTS[args.suite]], cwd=ROOT, env=env)
    if result.returncode:
        return result.returncode
    rows = [json.loads(line) for line in evidence.read_text().splitlines()]
    verify_execution(rows, selected, models)
    print(f"Verified every phase for {len(selected)} Home Assistant tests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
