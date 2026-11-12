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
    names = (*HARNESS, "validate.py", "qualification_snapshot.py")
    files = {"tools/" + name: (ROOT / "tools" / name).read_text() for name in names}
    for name in ("requirements-offline.txt", "requirements-ha-proxy.txt", "requirements-ha-legacy.txt"):
        files[name] = (ROOT / name).read_text()
    for suite in ("tests", "tests_ha_proxy", "tests_ha"):
        files[suite + "/test_synthetic.py"] = "".join(f"def test_{i:02d}(): pass\n" for i in range(40))
    for path in (ROOT / "tests_model").glob("*.py"):
        files[str(path.relative_to(ROOT))] = path.read_text()
    candidate = project(output / "candidate", files)
    universe = output / "universe.jsonl"
    manifest(universe)
    functions = {
        "two_receipt": "tests_model/test_schedule_sequences.py::test_two_receipt_event_sequences",
        "source_proof": "tests_model/test_source_sequences.py::test_source_proof_event_sequences",
    }
    models = {
        functions[row["family"]] + "[" + row["id"] + "]": row
        for line in universe.read_text().splitlines()
        if (row := json.loads(line))["exclusion"] is None
    }
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
        if suite == "engine-model":
            collected += list(models)
            shutil.copyfile(universe, batch / "sequence-manifest.jsonl")
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
                        "completed_events": models[identifier]["events"],
                        "suffix_complete": True,
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
