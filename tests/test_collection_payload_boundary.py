"""Fresh collector payloads cross the actual auditor boundary without replacing canonical_collection itself."""

import copy
import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from tests.qualification_test_helpers import synthetic_collection_row
from tools import audit_parallel_qualification as audit


# Intent: changed pins, filtering, missing definitions and executed bodies cannot be accepted as fresh collection.
# Why: older synthetic auditor fixtures replace this boundary and cannot detect missing payload validation here.
@pytest.mark.parametrize(
    "problem",
    [
        "control",
        "schema",
        "suite",
        "commit",
        "source_digest",
        "batch",
        "errors",
        "filter",
        "executed",
        "source_files",
        "missing_source_definitions",
        "raw_ids",
        "deselected",
        "collection_errors",
    ],
)
def test_actual_canonical_collection_checks_subprocess_payload(tmp_path, monkeypatch, problem):
    batch = tmp_path / "SYNTHETIC-NOT-QUALIFICATION"
    source = batch / "source"
    helper = source / "tools/qualification_collection.py"
    helper.parent.mkdir(parents=True)
    helper.write_text("# Synthetic collector payload boundary; never an actual qualification.\n")
    tests = source / "tests/test_one.py"
    tests.parent.mkdir()
    tests.write_text("def test_one(): raise AssertionError('body must not run')\n")
    commit, checksum = "a" * 40, "b" * 64
    files = {"tests/test_one.py": hashlib.sha256(tests.read_bytes()).hexdigest()}
    (batch / "snapshot.json").write_text(
        json.dumps({"files": [{"path": path, "sha256": value} for path, value in files.items()]})
    )
    payload = synthetic_collection_row(["tests/test_one.py::test_one"]) | {
        "schema": 1,
        "suite": "engine-model",
        "commit": commit,
        "source_digest": checksum,
        "batch": str(batch),
        "errors": [],
        "test_bodies_executed": False,
        "source_files": files,
        "missing_source_definitions": [],
    }
    mutations = {
        "schema": 2,
        "suite": "ha-proxy",
        "commit": "c" * 40,
        "source_digest": "d" * 64,
        "batch": "/different-synthetic-batch",
        "errors": ["synthetic failure"],
        "test_bodies_executed": True,
        "source_files": {},
        "missing_source_definitions": ["test_missing"],
        "raw_ids": [],
        "deselected": ["test_missing"],
        "collection_errors": ["synthetic import error"],
    }
    if problem == "filter":
        payload["effective_settings"]["filters"]["keyword"] = "test_one"
    elif problem != "control":
        key = "test_bodies_executed" if problem == "executed" else problem
        payload[key] = mutations[key]
    calls = []

    # Replace only the subprocess transport; the real canonical payload validator and source manifest run.
    def return_payload(command, **kwargs):
        calls.append(copy.deepcopy(command))
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(audit.subprocess, "run", return_payload)
    if problem == "control":
        assert (
            audit.canonical_collection(batch, {"suite": "engine-model", "specs": ["tests"]}, commit, checksum)
            == payload
        )
    else:
        with pytest.raises(ValueError):
            audit.canonical_collection(batch, {"suite": "engine-model", "specs": ["tests"]}, commit, checksum)
    assert len(calls) == 1 and Path(calls[0][2]) == helper
