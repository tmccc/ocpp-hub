"""CI model collection must enroll registered functions outside the model root without ordinary test leakage."""

import json
import os
import subprocess
import sys

from tools.run_model_shard import ROOT, collection_targets


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
