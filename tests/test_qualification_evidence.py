"""Raw qualification evidence rejects incomplete, repeated, foreign-source and mismatched-runtime attempts."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.qualification_test_helpers import synthetic_collection_row, synthetic_source_recollection
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
def audit_fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "canonical_collection", synthetic_source_recollection)
    harness = ("parallel_worker.py", "qualification_evidence.py", "audit_parallel_qualification.py")
    files = {"tools/" + name: "# synthetic auditor-input fixture\n" for name in harness}
    files["requirements-ha-proxy.txt"] = "homeassistant==2026.11.0\n"
    files["tests_ha_proxy/test_fixture.py"] = "def test_one(): pass\ndef test_two(): pass\n"
    source = project(tmp_path / "repo", files)
    batch = tmp_path / "batch"
    batch.mkdir()
    (batch / "SYNTHETIC-NOT-QUALIFICATION.txt").write_text("Synthetic unit inputs; no workers ran.\n")
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
    collection = [{"event": "worker_namespace", "runtime": runtime}, synthetic_collection_row(ids)]
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
            synthetic_collection_row([identifier]),
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


# Intent: dynamic execution may differ from the old round-robin partition but still needs exact owner evidence.
# Why: uneven assignment is expected when one worker is faster, while lost or fabricated grants must fail.
@pytest.mark.parametrize(
    "problem",
    [
        None,
        "duplicate",
        "wrong_worker",
        "grant_after_start",
        "premature_teardown",
        "call_first",
        "late_lookahead",
        "late_terminal_grant",
        "after_finish",
    ],
)
def test_auditor_checks_dynamic_grants(audit_fixture, problem):
    batch, metadata = audit_fixture
    ids = json.loads((batch / "selected-ids.json").read_text())
    config_path = batch / "config.json"
    config = json.loads(config_path.read_text())
    write_json(config_path, {**config, "scheduler": "pull-v1"})
    result_path = batch / "result.json"
    write_json(result_path, {**json.loads(result_path.read_text()), "scheduler": "pull-v1"})
    paths = [batch / str(index) / "results.jsonl" for index in range(2)]
    records = [[json.loads(line) for line in path.read_text().splitlines()] for path in paths]
    grants = [
        {"worker": 0, "sequence": 0, "id": ids[0]},
        {"worker": 0, "sequence": 1, "id": ids[1]},
        {"worker": 1, "sequence": 0, "id": None},
        {"worker": 0, "sequence": 2, "id": None},
    ]
    claims = [{"event": "grant", "sequence": row["sequence"], "id": row["id"]} for row in grants]
    first = [records[0][0], synthetic_collection_row(ids), *claims[:2], *records[0][2:-1]]
    first += [claims[3], *records[1][2:-1], records[0][-1]]
    second = [records[1][0], synthetic_collection_row(ids), claims[2], records[1][-1]]
    if problem == "duplicate":
        grants.insert(1, grants[0])
    elif problem == "wrong_worker":
        grants[1] = {**grants[1], "worker": 1, "sequence": 0}
    elif problem == "grant_after_start":
        first[2], first[4] = first[4], first[2]
    elif problem == "premature_teardown":
        first.insert(2, first.pop(7))
    elif problem == "call_first":
        first[5], first[6] = first[6], first[5]
    elif problem == "late_lookahead":
        first.insert(7, first.pop(3))
    elif problem == "late_terminal_grant":
        first.insert(-1, first.pop(8))
    elif problem == "after_finish":
        first.append({"event": "exception", "id": ids[1]})
    for path, assigned, rows in zip(paths, (ids, []), (first, second), strict=True):
        write_json(path.parent / "assigned-ids.json", assigned)
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    (batch / "dispatch.jsonl").write_text("".join(json.dumps(row) + "\n" for row in grants))
    assert audit.main(
        [str(batch), "--expected-commit", metadata["commit"], "--expected-source-digest", metadata["source_digest"]]
    ) == int(problem is not None)


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


# Intent: an unverified snapshot launches neither the manifest generator nor the independent collector.
# Why: the raw failure report must not execute code from rejected qualification inputs.
@pytest.mark.parametrize("changed", ["content", "commit", "directory"])
def test_rejected_snapshot_runs_no_source_helper(audit_fixture, monkeypatch, changed):
    batch, metadata = audit_fixture

    def forbidden(*args, **kwargs):
        pytest.fail("a source helper ran on a rejected snapshot")

    monkeypatch.setattr(audit, "canonical_manifest", forbidden)
    monkeypatch.setattr(audit, "canonical_collection", forbidden)
    if changed == "commit":
        write_json(batch / "snapshot.json", {**metadata, "commit": "0" * 40})
    elif changed == "directory":
        (batch / "source/duplicate").symlink_to(batch / "source/tools", target_is_directory=True)
    else:
        path = batch / "source/tests_ha_proxy/test_fixture.py"
        path.chmod(0o644)
        path.write_text("# changed bytes\n")
    assert (
        audit.main(
            [str(batch), "--expected-commit", metadata["commit"], "--expected-source-digest", metadata["source_digest"]]
        )
        == 1
    )


# Intent: contradictory validation modes fail argument parsing.
# Why: a release gate must never return success with zero checks.
def test_validator_rejects_conflicting_check_modes():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-B", "tools/validate.py", "--quick", "--tests-only"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
