"""CI model collection must enroll registered functions outside the model root without ordinary test leakage."""

import json
import os
import subprocess
import sys

import pytest

from tests.qualification_test_helpers import synthetic_collection_row
from tools.run_model_shard import ROOT, assigned_ids, collection_targets, hosted_collection, verify_execution


# Run genuine pytest collection with separate evidence so nested collection cannot contaminate qualification phases.
def collect(directory, targets):
    directory.mkdir()
    evidence = directory / "collection.jsonl"
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "tools/offline_validate.py",
            "-p",
            "tools.qualification_evidence",
            *targets,
            "--collect-only",
            "--basetemp=" + str(directory / "tmp"),
        ],
        cwd=ROOT,
        env={**os.environ, "TMPDIR": str(directory), "SCENARIO_RESULTS": str(evidence)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    collections = [
        row["ids"]
        for line in evidence.read_text().splitlines()
        if (row := json.loads(line)).get("event") == "collection"
    ]
    assert len(collections) == 1
    return collections[0]


# Intent: real CI collection exactly matches every canonical engine model, including registered process functions.
# Why: a whole-engine qualification can pass while a separate CI collector silently omits an out-of-root family.
def test_model_ci_enrolls_external_families_without_ordinary_cases(tmp_path):
    manifest = tmp_path / "manifest.jsonl"
    subprocess.run(
        [sys.executable, "-B", "tools/schedule_sequence_manifest.py", str(manifest)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    declared = {
        row["node_id"] for line in manifest.read_text().splitlines() if (row := json.loads(line))["exclusion"] is None
    }
    observed = collect(tmp_path / "actual", collection_targets())
    assert len(observed) == len(set(observed)) and set(observed) == declared
    process = {identifier for identifier in declared if identifier.startswith("tests/test_real_process_death.py::")}
    assert len(process) == 80 and process <= set(observed)
    assert not any("test_process_accepted_start_late_lifecycle" in identifier for identifier in observed)
    assert not any("test_process_return_commit_preserves_honest_receipt" in identifier for identifier in observed)
    old = set(collect(tmp_path / "old-negative-control", ["tests_model"]))
    assert declared - old == process and not old - declared
    partitions = [observed[index::20] for index in range(20)]
    flattened = [identifier for partition in partitions for identifier in partition]
    assert len(flattened) == len(set(flattened)) and set(flattened) == declared


# Intent: all hosted jobs share the exact complete engine plan, including both kinds of case in the process-death file.
# Why: separate serial and model jobs used to execute registered out-of-root model cases twice.
def test_hosted_engine_shards_partition_controlled_collection_once(tmp_path):
    plan = tmp_path / "collection.json"
    collected = subprocess.run(
        [
            sys.executable,
            "-B",
            "tools/qualification_collection.py",
            "--source",
            str(ROOT),
            "--suite",
            "engine-model",
            "--output",
            str(plan),
        ],
        cwd=ROOT,
        env={**os.environ, "TMPDIR": str(tmp_path), "PYTEST_ADDOPTS": "", "PYTEST_PLUGINS": ""},
        capture_output=True,
        check=False,
        timeout=60,
    )
    assert collected.returncode == 0, collected.stdout + collected.stderr
    manifest = tmp_path / "manifest.jsonl"
    subprocess.run(
        [sys.executable, "-B", "tools/schedule_sequence_manifest.py", str(manifest)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        timeout=30,
    )
    ids = hosted_collection(plan, "engine-model")
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    shards = [assigned_ids(ids, rows, index, 20, full_engine=True) for index in range(20)]
    assert all(shards)
    flattened = [identifier for shard in shards for identifier in shard]
    assert len(flattened) == len(set(flattened)) == len(ids) and set(flattened) == set(ids)
    process = {identifier for identifier in flattened if identifier.startswith("tests/test_real_process_death.py::")}
    models = {row["node_id"] for row in rows if row["exclusion"] is None}
    assert len(models & process) == 80
    assert any("test_process_return_commit_preserves_honest_receipt" in case for case in process - models)
    omitted = next(iter(models & process))
    with pytest.raises(RuntimeError, match="exact registered finite universe"):
        assigned_ids([case for case in ids if case != omitted], rows, 0, 20, full_engine=True)


# Intent: each shard must prove actual once-only execution of its assigned IDs.
# Why: a collector and all selected phases may look correct while an extra case also ran.
@pytest.mark.parametrize("fault", [None, "foreign", "duplicate", "missing", "skipped"])
def test_hosted_shard_execution_requires_exact_assignment(fault):
    assigned = ["tests/test_device.py::test_one"]
    rows = [synthetic_collection_row(assigned), {"event": "start", "id": assigned[0]}]
    for phase in ("setup", "call", "teardown"):
        rows.append({"event": "report", "id": assigned[0], "phase": phase, "outcome": "passed"})
    if fault == "foreign":
        rows.append({**rows[-1], "id": "tests/test_other.py::test_two"})
    elif fault == "duplicate":
        rows.append(rows[1])
    elif fault == "missing":
        rows.pop()
    elif fault == "skipped":
        rows[-2]["outcome"] = "skipped"
    if fault is None:
        verify_execution(rows, assigned, {})
    else:
        with pytest.raises(RuntimeError, match="exact assignment|every assigned phase"):
            verify_execution(rows, assigned, {})
