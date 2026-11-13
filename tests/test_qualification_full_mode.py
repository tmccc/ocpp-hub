"""Full validation rejects successful partial audits using explicitly synthetic raw inputs."""

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_qualification_evidence import project, write_json
from tools import audit_parallel_qualification as audit
from tools.qualification_snapshot import freeze
from tools.schedule_sequence_manifest import manifest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ("parallel_worker.py", "qualification_evidence.py", "audit_parallel_qualification.py")


# Write deliberately synthetic phase records, never representing them as actual qualification execution.
def events(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(value) + "\n" for value in values))


# Build all four auditor inputs with actual unchanged verifier bytes and the complete real model denominator.
def qualification_input(output, mode, complete):
    output.mkdir()
    (output / "SYNTHETIC-NOT-QUALIFICATION.txt").write_text("Artificial verifier inputs; no workers or cases ran.\n")
    names = (*HARNESS, "validate.py", "qualification_snapshot.py", "schedule_sequence_manifest.py")
    files = {"tools/" + name: (ROOT / "tools" / name).read_text() for name in names}
    for name in ("requirements-offline.txt", "requirements-ha-proxy.txt", "requirements-ha-legacy.txt"):
        files[name] = (ROOT / name).read_text()
    for suite in ("tests", "tests_ha_proxy", "tests_ha"):
        files[suite + "/test_synthetic.py"] = "".join(f"def test_{i:02d}(): pass\n" for i in range(40))
    for path in (ROOT / "tests_model").glob("*.py"):
        files[str(path.relative_to(ROOT))] = path.read_text()
    candidate = project(output / "candidate", files)
    phases = {}
    for suite in ("engine-model", "ha-proxy", "ha-legacy", "ha-proxy-legacy"):
        test_root = "tests" if suite == "engine-model" else "tests_ha" if suite == "ha-legacy" else "tests_ha_proxy"
        requirements = (
            "requirements-offline.txt"
            if suite == "engine-model"
            else "requirements-ha-proxy.txt"
            if suite == "ha-proxy"
            else "requirements-ha-legacy.txt"
        )
        batch = output / suite
        batch.mkdir()
        # The summary is separately compared with a fresh generation by the frozen auditor.
        write_json(batch / "sequence-manifest.summary.json", manifest(batch / "sequence-manifest.jsonl", suite))
        models = {
            row["node_id"]: row
            for line in (batch / "sequence-manifest.jsonl").read_text().splitlines()
            if (row := json.loads(line))["exclusion"] is None
        }
        metadata = freeze(candidate, batch / "source")
        pins = dict(
            line.split("==", 1) for line in files[requirements].splitlines() if line and not line.startswith("#")
        )
        config = {
            "suite": suite,
            "workers": 20,
            "commit": metadata["commit"],
            "source_digest": metadata["source_digest"],
            "image": "sha256:" + "a" * 64,
            "expected_packages": pins,
            "runner_args": ["--legacy-ha"] if suite == "ha-legacy" else [],
            "runner": "tools/offline_validate.py" if suite == "engine-model" else "tools/offline_ha_validate.py",
            "specs": ["tests", "tests_model"] if suite == "engine-model" else [test_root],
            "harness_hashes": {name: hashlib.sha256(files["tools/" + name].encode()).hexdigest() for name in HARNESS},
        }
        if mode is not None:
            config["mode"] = mode
        for name in HARNESS:
            shutil.copyfile(ROOT / "tools" / name, batch / name)
        collected = [f"{test_root}/test_synthetic.py::test_{i:02d}" for i in range(40)]
        collected += list(models)
        selected = collected if complete else collected[:20]
        runtime = {
            "python": sys.version,
            "python_sha256": hashlib.sha256(Path(sys.executable).resolve().read_bytes()).hexdigest(),
            "packages": pins,
        }
        result = {
            "mode": mode,
            "commit": metadata["commit"],
            "source_digest": metadata["source_digest"],
            "source_digest_after": metadata["source_digest"],
            "complete": True,
            "abort_reason": None,
            "groups": [{"index": i, "rc": 0} for i in range(20)],
            "cpu_assignments": list(range(20)),
            "worker_network": "none",
            "container_image": config["image"],
            "runtime_digest_before": "c" * 64,
            "runtime_digest_after": "c" * 64,
        }
        for name, value in (
            ("config", config),
            ("snapshot", metadata),
            ("result", result),
            ("collected-ids", collected),
            ("selected-ids", selected),
            ("transport-result", {"returncode": 0}),
        ):
            write_json(batch / (name + ".json"), value)
        events(
            batch / "collection/results.jsonl",
            [{"event": "worker_namespace", "runtime": runtime}, {"event": "collection", "ids": collected}],
        )
        for index in range(20):
            assigned = selected[index::20]
            write_json(batch / str(index) / "assigned-ids.json", assigned)
            records = [
                {
                    "event": "worker_namespace",
                    "runtime": runtime,
                    "cpu_affinity": [index],
                    "network_namespace": f"net:[{index}]",
                },
                {"event": "collection", "ids": assigned},
            ]
            for identifier in assigned:
                records.append({"event": "start", "id": identifier})
                props = (
                    {}
                    if identifier not in models
                    else {
                        "model_case": models[identifier]["id"],
                        "model_family": models[identifier]["family"],
                        "model_input_sha256": models[identifier]["input_sha256"],
                        "completed_events": models[identifier]["events"],
                        "suffix_complete": True,
                        **{key: True for key in models[identifier].get("required_evidence", [])},
                    }
                )
                records += [
                    {
                        "event": "report",
                        "id": identifier,
                        "phase": phase,
                        "outcome": "passed",
                        "duration_s": 0.001,
                        "properties": props,
                    }
                    for phase in ("setup", "call", "teardown")
                ]
            records.append({"event": "session_finish", "exitstatus": 0, "runtime": runtime})
            events(batch / str(index) / "results.jsonl", records)
        phases[suite] = str(batch)
    bundle = output / "bundle.json"
    write_json(bundle, {"commit": metadata["commit"], "source_digest": metadata["source_digest"], "phases": phases})
    return candidate, bundle


# Intent: full validation requires the entire collected universe regardless of a partial attempt's success.
# Why: a successful targeted audit must never qualify a release; complete full evidence must still pass.
@pytest.mark.parametrize(
    "mode,complete,expected", [("full", True, 0), ("full", False, 1), ("targeted", False, 1), (None, False, 1)]
)
def test_full_validator_requires_complete_full_mode(tmp_path, mode, complete, expected):
    candidate, bundle = qualification_input(tmp_path / "synthetic", mode, complete)
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(candidate / "tools/validate.py"),
            "--tests-only",
            "--qualification-report",
            str(bundle),
        ],
        cwd=candidate,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert result.returncode == expected, result.stdout + result.stderr


# Intent: edited inputs, entire omitted families and incomplete recovery suffixes cannot qualify.
# Why: raw phase success alone does not prove the pinned finite universe or its eventual recovery was exercised.
def test_model_audit_regenerates_inputs_and_requires_exact_recovery_evidence(tmp_path):
    _, bundle_path = qualification_input(tmp_path / "synthetic-model-tamper", "full", True)
    bundle = json.loads(bundle_path.read_text())
    batch = Path(bundle["phases"]["engine-model"])
    arguments = [str(batch), "--expected-commit", bundle["commit"], "--expected-source-digest", bundle["source_digest"]]
    assert audit.main(arguments) == 0
    path = batch / "sequence-manifest.jsonl"
    original = path.read_text()
    rows = [json.loads(line) for line in original.splitlines()]
    excluded = next(i for i, row in enumerate(rows) if row["exclusion"] is not None)
    for change in ("family-omission", "excluded-input", "duplicate", "exclusion"):
        altered = json.loads(json.dumps(rows))
        if change == "family-omission":
            altered = [row for row in altered if row["family"] != "start_recovery"]
        elif change == "excluded-input":
            altered[excluded]["events"] = list(reversed(altered[excluded]["events"]))
        elif change == "duplicate":
            altered.append(altered[0])
        else:
            altered[excluded]["exclusion"] = None
        path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in altered))
        assert audit.main(arguments) == 1
        assert any("pinned registry" in error for error in json.loads((batch / "audit.json").read_text())["errors"])
    path.write_text(original)
    evidence_path = next(
        path
        for path in batch.glob("*/results.jsonl")
        if "/collection/" not in str(path) and '"model_family": "start_recovery"' in path.read_text()
    )
    original_evidence = evidence_path.read_text()
    for key, value in (("suffix_complete", False), ("model_input_sha256", "0" * 64), ("completed_events", [])):
        records = [json.loads(line) for line in original_evidence.splitlines()]
        target = next(
            row
            for row in records
            if row.get("phase") == "call" and row.get("properties", {}).get("model_family") == "start_recovery"
        )
        target["properties"][key] = value
        events(evidence_path, records)
        assert audit.main(arguments) == 1
        assert (
            "selected model inputs lack exact full event/suffix evidence"
            in json.loads((batch / "audit.json").read_text())["errors"]
        )
    evidence_path.write_text(original_evidence)
    assert audit.main(arguments) == 0
    boundary_path = next(
        path
        for path in batch.glob("*/results.jsonl")
        if "/collection/" not in str(path) and '"boundary_entered": true' in path.read_text()
    )
    original_boundary = boundary_path.read_text()
    records = [json.loads(line) for line in original_boundary.splitlines()]
    target = next(
        row
        for row in records
        if row.get("phase") == "call" and row.get("properties", {}).get("boundary_entered") is True
    )
    target["properties"]["boundary_entered"] = False
    events(boundary_path, records)
    assert audit.main(arguments) == 1
    boundary_path.write_text(original_boundary)
    # HA models live outside tests_model; their mandatory suffix must receive the same independent audit.
    ha_batch = Path(bundle["phases"]["ha-proxy"])
    ha_path = next(
        path
        for path in ha_batch.glob("*/results.jsonl")
        if "/collection/" not in str(path) and '"model_family": "ha_receipt"' in path.read_text()
    )
    ha_records = [json.loads(line) for line in ha_path.read_text().splitlines()]
    next(
        row
        for row in ha_records
        if row.get("phase") == "call" and row.get("properties", {}).get("model_family") == "ha_receipt"
    )["properties"]["suffix_complete"] = False
    events(ha_path, ha_records)
    assert audit.main([str(ha_batch), *arguments[1:]]) == 1
