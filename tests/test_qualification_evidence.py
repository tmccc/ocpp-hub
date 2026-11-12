"""Raw qualification evidence rejects incomplete, repeated, foreign-source and mismatched-runtime attempts."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tools import audit_parallel_qualification as audit
from tools.qualification_snapshot import freeze


# Write deterministic fixture artifacts without invoking Docker or generating fake production test results.
def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n")


# A disposable Git project proves clean committed-byte capture without borrowing the real working tree's identity.
def project(path, files):
    path.mkdir()
    for name, value in files.items():
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    subprocess.run(["git", "-C", str(path), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(path),
            "-c",
            "user.name=Offline Test",
            "-c",
            "user.email=offline@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    return path


# Intent: immutable capture verifies committed bytes and rejects both dirty files and tracked symlinks.
# Why: a passing report must never qualify different bytes from the candidate commit.
def test_snapshot_requires_clean_ordinary_committed_blobs(tmp_path):
    source = project(tmp_path / "repo", {"subject.py": "VALUE = 1\n"})
    captured = freeze(source, tmp_path / "snapshot")
    assert freeze(source)["source_digest"] == captured["source_digest"]
    assert (tmp_path / "snapshot/subject.py").read_bytes() == (source / "subject.py").read_bytes()
    (source / "subject.py").write_text("VALUE = 2\n")
    with pytest.raises(RuntimeError, match="clean"):
        freeze(source)
    (source / "subject.py").write_text("VALUE = 1\n")
    (source / "alias.py").symlink_to("subject.py")
    subprocess.run(["git", "-C", str(source), "add", "alias.py"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Offline Test",
            "-c",
            "user.email=offline@example.invalid",
            "commit",
            "-qm",
            "symlink",
        ],
        check=True,
    )
    with pytest.raises(RuntimeError, match="ordinary"):
        freeze(source)


# Construct a small synthetic HA-scope input for the auditor, explicitly not a real qualification receipt.
@pytest.fixture
def audit_fixture(tmp_path):
    harness = ("parallel_worker.py", "qualification_evidence.py", "audit_parallel_qualification.py")
    files = {"tools/" + name: "# synthetic auditor-input fixture\n" for name in harness}
    files["requirements-ha-proxy.txt"] = "homeassistant==2026.11.0\n"
    files["tests_ha_proxy/test_fixture.py"] = "def test_one(): pass\ndef test_two(): pass\n"
    source = project(tmp_path / "repo", files)
    batch = tmp_path / "batch"
    batch.mkdir()
    metadata = freeze(source, batch / "source")
    config = {
        "mode": "full",
        "suite": "ha-proxy",
        "runner": "tools/offline_ha_validate.py",
        "runner_args": [],
        "specs": ["tests_ha_proxy"],
        "workers": 2,
        "commit": metadata["commit"],
        "source_digest": metadata["source_digest"],
        "image": "sha256:" + "a" * 64,
        "expected_packages": {"homeassistant": "2026.11.0"},
        "harness_hashes": {name: hashlib.sha256(files["tools/" + name].encode()).hexdigest() for name in harness},
    }
    for name in harness:
        (batch / name).write_text(files["tools/" + name])
    ids = ["tests_ha_proxy/test_fixture.py::test_one", "tests_ha_proxy/test_fixture.py::test_two"]
    runtime = {"python": "3.14.10", "python_sha256": "b" * 64, "packages": config["expected_packages"]}
    result = {
        "mode": "full",
        "commit": metadata["commit"],
        "source_digest": metadata["source_digest"],
        "source_digest_after": metadata["source_digest"],
        "complete": True,
        "abort_reason": None,
        "groups": [{"index": i, "rc": 0} for i in range(2)],
        "cpu_assignments": [0, 1],
        "worker_network": "none",
        "container_image": config["image"],
        "runtime_digest_before": "c" * 64,
        "runtime_digest_after": "c" * 64,
    }
    for name, value in (
        ("config.json", config),
        ("snapshot.json", metadata),
        ("result.json", result),
        ("collected-ids.json", ids),
        ("selected-ids.json", ids),
        ("transport-result.json", {"returncode": 0}),
    ):
        write_json(batch / name, value)
    collection = [{"event": "worker_namespace", "runtime": runtime}, {"event": "collection", "ids": ids}]
    path = batch / "collection/results.jsonl"
    path.parent.mkdir()
    path.write_text("".join(json.dumps(row) + "\n" for row in collection))
    for index, identifier in enumerate(ids):
        write_json(batch / str(index) / "assigned-ids.json", [identifier])
        rows = [
            {
                "event": "worker_namespace",
                "runtime": runtime,
                "cpu_affinity": [index],
                "network_namespace": f"net:[{index}]",
            },
            {"event": "collection", "ids": [identifier]},
            {"event": "start", "id": identifier},
        ]
        rows += [
            {"event": "report", "id": identifier, "phase": phase, "outcome": "passed", "duration_s": 0.001}
            for phase in ("setup", "call", "teardown")
        ]
        rows += [{"event": "session_finish", "exitstatus": 0, "runtime": runtime}]
        (batch / str(index) / "results.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    return batch, metadata


# Intent: complete matching raw phase evidence is accepted by the structural audit.
# Why: negative audit controls need a passing fixture with no hidden default failures.
def test_auditor_accepts_complete_fixture(audit_fixture):
    batch, metadata = audit_fixture
    assert (
        audit.main(
            [str(batch), "--expected-commit", metadata["commit"], "--expected-source-digest", metadata["source_digest"]]
        )
        == 0
    )


# Intent: no individual missing phase, retry, skipped test, source drift or runtime mismatch can remain green.
# Why: a report's own success flag is not evidence that the exact assigned candidate completed.
@pytest.mark.parametrize(
    "problem",
    [
        "teardown",
        "retry",
        "skip",
        "exit",
        "runtime",
        "source",
        "collection",
        "harness",
        "cpu",
        "namespace",
        "image",
        "packages",
        "scope",
        "transport",
    ],
)
def test_auditor_rejects_corrupted_raw_evidence(audit_fixture, problem):
    batch, metadata = audit_fixture
    path = batch / "0/results.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if problem == "teardown":
        rows = [row for row in rows if row.get("phase") != "teardown"]
    elif problem == "retry":
        rows.append(next(row for row in rows if row.get("event") == "start"))
    elif problem == "skip":
        next(row for row in rows if row.get("phase") == "call")["outcome"] = "skipped"
    elif problem == "exit":
        rows[-1]["exitstatus"] = 7
    elif problem == "cpu":
        rows[0]["cpu_affinity"] = [17]
    elif problem == "namespace":
        rows[0]["network_namespace"] = "net:[1]"
    elif problem == "packages":
        rows[0]["runtime"]["packages"]["homeassistant"] = "2026.10.4"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    if problem in ("runtime", "image"):
        r = json.loads((batch / "result.json").read_text())
        r["runtime_digest_after" if problem == "runtime" else "container_image"] = "d" * 64
        write_json(batch / "result.json", r)
    elif problem == "source":
        changed = batch / "source/tests_ha_proxy/test_fixture.py"
        changed.chmod(0o644)
        changed.write_text("changed\n")
    elif problem == "collection":
        write_json(batch / "collected-ids.json", ["tests_ha_proxy/test_fixture.py::test_one"])
    elif problem == "harness":
        (batch / "parallel_worker.py").write_text("changed\n")
    elif problem == "scope":
        r = json.loads((batch / "config.json").read_text())
        r["specs"] = ["tests"]
        write_json(batch / "config.json", r)
    elif problem == "transport":
        write_json(batch / "transport-result.json", {"returncode": 255})
    assert (
        audit.main(
            [str(batch), "--expected-commit", metadata["commit"], "--expected-source-digest", metadata["source_digest"]]
        )
        == 1
    )


# Intent: the default validator refuses missing qualification instead of falling back to a serial full suite.
# Why: the release command needs raw complete evidence and must respect the20-worker execution requirement.
def test_default_validator_requires_raw_qualification():
    root = Path(__file__).resolve().parents[1]
    env = {key: value for key, value in os.environ.items() if key != "OCPP_QUALIFICATION_REPORT"}
    result = subprocess.run(
        [sys.executable, "-B", "tools/validate.py", "--tests-only"], cwd=root, env=env, capture_output=True, text=True
    )
    assert result.returncode == 1 and "requires --qualification-report" in result.stdout
