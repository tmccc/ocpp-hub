#!/usr/bin/env python3
"""Run one exact partition of the full model collection on an independent CI runner."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# CI runs twenty separate machines; each subprocess preserves pytest's true exit and raw phase evidence.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shards", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.shards != 20 or not 0 <= args.shard < args.shards or not args.output.is_absolute():
        parser.error("use shard0..19 of20 and an absolute output directory")
    if os.environ.get("MODEL_SMOKE") or os.environ.get("MODEL_CASE_LIMIT"):
        parser.error("CI model qualification never samples the declared universe")
    args.output.mkdir(parents=True, exist_ok=False)
    env = {
        **os.environ,
        "TMPDIR": str(args.output),
        "PYTHONDONTWRITEBYTECODE": "1",
        "MODEL_WORKER_INDEX": str(args.shard),
        "SCENARIO_RESULTS": str(args.output / "collection.jsonl"),
    }
    command = [sys.executable, "-B", "tools/offline_validate.py", "-p", "tools.qualification_evidence"]
    collected = subprocess.run(
        [*command, "tests_model", "--collect-only"], cwd=ROOT, env=env, capture_output=True, text=True
    )
    (args.output / "collection.log").write_text(collected.stdout + collected.stderr)
    if collected.returncode:
        return collected.returncode
    rows = [json.loads(line) for line in (args.output / "collection.jsonl").read_text().splitlines()]
    collections = [row["ids"] for row in rows if row.get("event") == "collection"]
    if len(collections) != 1 or len(collections[0]) != 7200 or len(set(collections[0])) != 7200:
        raise RuntimeError("full model collection differs from the declared7200 runnable cases")
    selected = collections[0][args.shard :: args.shards]
    (args.output / "assigned-ids.json").write_text(json.dumps(selected) + "\n")
    env["SCENARIO_RESULTS"] = str(args.output / "results.jsonl")
    result = subprocess.run([*command, *selected, "--junitxml=" + str(args.output / "junit.xml")], cwd=ROOT, env=env)
    if result.returncode:
        return result.returncode
    rows = [json.loads(line) for line in (args.output / "results.jsonl").read_text().splitlines()]
    phases = {
        identifier: [r for r in rows if r.get("event") == "report" and r["id"] == identifier] for identifier in selected
    }
    if any(
        len(reports) != 3
        or {r["phase"] for r in reports} != {"setup", "call", "teardown"}
        or any(r["outcome"] != "passed" for r in reports)
        for reports in phases.values()
    ):
        raise RuntimeError("model shard did not complete every assigned phase")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
