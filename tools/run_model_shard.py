#!/usr/bin/env python3
"""Run one exact partition of the hosted engine or model-only collection."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests_model.catalog import FAMILIES  # noqa: E402
from tools.audit_parallel_qualification import valid_collection_contract  # noqa: E402
from tools.qualification_collection import source_inventory  # noqa: E402


# Keep the full model root fail-closed while enrolling exact registered engine functions in other test roots.
def collection_targets() -> list[str]:
    return [
        "tests_model",
        *sorted(
            {
                family.test_function
                for family in FAMILIES
                if "engine-model" in family.suites and not family.test_function.startswith("tests_model/")
            }
        ),
    ]


# Verify the hosted suite's test list came from unfiltered discovery of this checkout.
def hosted_collection(path: Path, suite: str) -> list[str]:
    plan = json.loads(path.read_text())
    files, definitions = source_inventory(ROOT, suite)
    if (
        plan.get("suite") != suite
        or plan.get("errors") != []
        or plan.get("test_bodies_executed") is not False
        or plan.get("source_files") != files
        or plan.get("source_definitions") != definitions
        or not valid_collection_contract(plan)
        or not set(definitions) <= {identifier.split("[", 1)[0] for identifier in plan["ids"]}
    ):
        raise RuntimeError(f"hosted {suite} plan differs from this checkout's complete discovery")
    return plan["ids"]


# Check collection evidence before using it as a denominator or an execution witness.
def observed_collection(rows: list[dict]) -> list[str]:
    collections = [row for row in rows if row.get("event") == "collection"]
    if len(collections) != 1 or not valid_collection_contract(collections[0]):
        raise RuntimeError("shard lacks a complete, unfiltered collection record")
    return collections[0]["ids"]


# Partition a single ordered denominator, with every registered model included in it exactly once.
def assigned_ids(ids: list[str], manifest: list[dict], shard: int, count: int, *, full_engine: bool) -> list[str]:
    admitted = {row["node_id"] for row in manifest if row["exclusion"] is None}
    functions = {row["node_id"].split("[", 1)[0] for row in manifest}
    if full_engine:
        observed = {
            identifier
            for identifier in ids
            if identifier.startswith("tests_model/") or identifier.split("[", 1)[0] in functions
        }
    else:
        observed = set(ids)
    if not ids or len(ids) != len(set(ids)) or observed != admitted:
        raise RuntimeError("shard collection differs from the exact registered finite universe")
    return ids[shard::count]


# Reject extra or repeated executions as well as missing setup, call or teardown evidence.
def verify_execution(rows: list[dict], selected: list[str], expected: dict[str, dict]) -> None:
    observed = observed_collection(rows)
    starts = Counter(row["id"] for row in rows if row["event"] == "start")
    reports = defaultdict(list)
    for row in rows:
        if row["event"] == "report":
            reports[row["id"]].append(row)
    if Counter(observed) != Counter(selected) or starts != Counter(selected) or reports.keys() != set(selected):
        raise RuntimeError("hosted shard execution differs from its exact assignment")
    for identifier, phases in reports.items():
        if [(row["phase"], row["outcome"]) for row in phases] != [
            ("setup", "passed"),
            ("call", "passed"),
            ("teardown", "passed"),
        ]:
            raise RuntimeError("hosted shard did not complete every assigned phase: " + identifier)
        props = phases[1].get("properties", {})
        case = expected.get(identifier)
        if case is None:
            if "model_case" in props:
                raise RuntimeError("unregistered model in hosted shard: " + identifier)
            continue
        if any(props.get(key) is not True for key in case.get("required_evidence", [])):
            raise RuntimeError("model shard lacks required boundary evidence: " + identifier)
        if any(
            props.get(key) != value
            for key, value in {
                "model_case": case["id"],
                "model_family": case["family"],
                "model_input_sha256": case["input_sha256"],
                "completed_events": case["events"],
                "suffix_complete": True,
            }.items()
        ):
            raise RuntimeError("model shard lacks exact input, events or recovery suffix: " + identifier)


# CI runs twenty separate machines; each subprocess preserves pytest's true exit and raw phase evidence.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shards", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--engine-collection", type=Path, help="shared complete engine discovery artifact for CI")
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
    if args.engine_collection is None:
        collected = subprocess.run(
            [*command, *collection_targets(), "--collect-only"], cwd=ROOT, env=env, capture_output=True, text=True
        )
        (args.output / "collection.log").write_text(collected.stdout + collected.stderr)
        if collected.returncode:
            return collected.returncode
        ids = observed_collection(
            [json.loads(line) for line in (args.output / "collection.jsonl").read_text().splitlines()]
        )
    else:
        ids = hosted_collection(args.engine_collection, "engine-model")
    manifest_path = args.output / "sequence-manifest.jsonl"
    generated = subprocess.run(
        [sys.executable, "-B", "tools/schedule_sequence_manifest.py", str(manifest_path)],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    (args.output / "manifest.log").write_text(generated.stdout + generated.stderr)
    manifest = [json.loads(line) for line in manifest_path.read_text().splitlines()]
    selected = assigned_ids(ids, manifest, args.shard, args.shards, full_engine=args.engine_collection is not None)
    (args.output / "assigned-ids.json").write_text(json.dumps(selected) + "\n")
    env["SCENARIO_RESULTS"] = str(args.output / "results.jsonl")
    result = subprocess.run([*command, *selected, "--junitxml=" + str(args.output / "junit.xml")], cwd=ROOT, env=env)
    if result.returncode:
        return result.returncode
    rows = [json.loads(line) for line in (args.output / "results.jsonl").read_text().splitlines()]
    verify_execution(rows, selected, {row["node_id"]: row for row in manifest if row["exclusion"] is None})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
