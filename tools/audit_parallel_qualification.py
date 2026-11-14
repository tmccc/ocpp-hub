#!/usr/bin/env python3
"""Audit one immutable attempt; repetitions can never fill gaps in a full qualification."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path


# Load optional result artifacts without manufacturing completed execution evidence.
def read_json(path, default):
    return json.loads(path.read_text()) if path.exists() else default


# Regenerate every declared input from the pinned source rather than trusting a run's copied denominator.
def canonical_manifest(root, suite):
    source = root / "source"
    if not (source / "tests_model/catalog.py").is_file():
        if suite == "engine-model":
            raise ValueError("engine model registry is missing")
        return [], None
    with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as scratch:
        output = Path(scratch) / "manifest.jsonl"
        generated = subprocess.run(
            [sys.executable, "-B", str(source / "tools/schedule_sequence_manifest.py"), str(output), "--suite", suite],
            cwd=source,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if generated.returncode:
            raise ValueError(
                "pinned model registry could not generate its declared universe: " + generated.stderr[-500:]
            )
        return [json.loads(line) for line in output.read_text().splitlines()], read_json(
            output.with_suffix(".summary.json"), None
        )


# Raw item generation and effective arguments expose filters even when downstream selected/evidence lists agree.
def valid_collection_contract(row):
    settings = row.get("effective_settings", {})
    environment = settings.get("environment")
    filters = settings.get("filters")
    return (
        row.get("raw_ids") == row.get("ids")
        and row.get("deselected") == []
        and row.get("collection_errors") == []
        and environment == {"PYTEST_ADDOPTS": "", "PYTEST_PLUGINS": ""}
        and isinstance(filters, dict)
        and set(filters) == {"keyword", "markexpr", "deselect", "ignore", "ignore_glob", "pyargs", "noconftest"}
        and not any(filters.values())
        and settings.get("addopts") == []
        and settings.get("python_files") == ["test_*.py", "*_test.py"]
        and settings.get("python_functions") == ["test"]
        and settings.get("python_classes") == ["Test"]
        and isinstance(settings.get("arguments"), list)
        and bool(settings["arguments"])
    )


# Regenerate complete dynamic pytest IDs under an explicitly selected pinned local or SSH runtime, never a cached list.
def canonical_collection(root, config, commit, checksum, executors=None):
    source = root / "source"
    helper = source / "tools/qualification_collection.py"
    helper_sha = hashlib.sha256(helper.read_bytes()).hexdigest()
    arguments = ["--replay-batch", str(root), "--commit", commit, "--source-digest", checksum]
    expected_batch = str(root)
    command = [sys.executable, "-B", str(helper), *arguments]
    if executors is not None:
        selected = json.loads(executors.read_text())
        if selected.get("commit") != commit or selected.get("source_digest") != checksum:
            raise ValueError("explicit collection executors differ from caller source pins")
        executor = selected.get("executors", {}).get(config.get("suite"))
        if executor is None:
            raise ValueError("explicit collection executor missing suite")
        if executor.get("kind") == "ssh":
            host, python, batch = (executor.get(key, "") for key in ("host", "supervisor_python", "batch"))
            if (
                not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@-]*", host)
                or not Path(python).is_absolute()
                or not Path(batch).is_absolute()
            ):
                raise ValueError("invalid explicitly chosen collection executor")
            expected_batch = batch
            remote_helper = str(Path(batch) / "source/tools/qualification_collection.py")
            bootstrap = (
                "import hashlib,pathlib,runpy,sys; p=pathlib.Path(sys.argv[1]); "
                "assert hashlib.sha256(p.read_bytes()).hexdigest()==sys.argv[2], 'pinned collector helper changed'; "
                "sys.argv=[str(p),*sys.argv[3:]]; runpy.run_path(str(p),run_name='__main__')"
            )
            remote = [
                python,
                "-B",
                "-c",
                bootstrap,
                remote_helper,
                helper_sha,
                "--replay-batch",
                batch,
                "--commit",
                commit,
                "--source-digest",
                checksum,
            ]
            command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host, shlex.join(remote)]
        elif executor.get("kind") == "local" and executor.get("batch") != str(root):
            raise ValueError("explicit local collection executor batch differs")
        elif executor.get("kind") != "local":
            raise ValueError("unsupported explicitly chosen collection executor")
    completed = subprocess.run(command, capture_output=True, text=True, timeout=240, check=False)
    if completed.returncode:
        raise ValueError("fresh pinned-runtime collection unavailable/failed: " + completed.stderr[-1000:])
    payload = json.loads(completed.stdout)
    if (
        payload.get("schema") != 1
        or payload.get("suite") != config.get("suite")
        or payload.get("errors")
        or payload.get("commit") != commit
        or payload.get("source_digest") != checksum
        or payload.get("batch") != expected_batch
    ):
        raise ValueError("controlled collection did not establish complete source discovery")
    if not valid_collection_contract(payload) or payload.get("test_bodies_executed") is not False:
        raise ValueError("controlled recollection contains filtering or test execution")
    metadata = json.loads((root / "snapshot.json").read_text())
    expected_files = {
        row["path"]: row["sha256"]
        for row in metadata["files"]
        if any(row["path"].startswith(name + "/") for name in config["specs"])
        and any(fnmatch.fnmatchcase(Path(row["path"]).name, pattern) for pattern in ("test_*.py", "*_test.py"))
    }
    if payload.get("source_files") != expected_files or payload.get("missing_source_definitions") != []:
        raise ValueError("controlled recollection omits pinned source files or definitions")
    return payload


# Reconcile raw phase evidence against the exact collection, assignment, manifest and caller-pinned snapshot.
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-source-digest", required=True)
    parser.add_argument(
        "--collection-executors", type=Path, help="explicit trusted source-pinned local/SSH executor map"
    )
    args = parser.parse_args(argv)
    root = args.batch.resolve()
    config = read_json(root / "config.json", {})
    metadata = read_json(root / "snapshot.json", {})
    result = read_json(root / "result.json", {})
    collected = read_json(root / "collected-ids.json", [])
    selected = read_json(root / "selected-ids.json", [])
    errors = []
    suites = {
        "engine-model": ["tests", "tests_model"],
        "ha-proxy": ["tests_ha_proxy"],
        "ha-legacy": ["tests_ha"],
        "ha-proxy-legacy": ["tests_ha_proxy"],
    }
    if config.get("specs") != suites.get(config.get("suite")) or config.get("suite") not in suites:
        errors.append("suite scope differs from its declared complete roots")
    expected_runner = (
        "tools/offline_validate.py" if config.get("suite") == "engine-model" else "tools/offline_ha_validate.py"
    )
    expected_arguments = ["--legacy-ha"] if config.get("suite") == "ha-legacy" else []
    if config.get("runner") != expected_runner or config.get("runner_args") != expected_arguments:
        errors.append("suite runner differs from its complete declared test environment")
    requirements = (
        "requirements-offline.txt"
        if config.get("suite") == "engine-model"
        else "requirements-ha-proxy.txt"
        if config.get("suite") == "ha-proxy"
        else "requirements-ha-legacy.txt"
    )
    requirement_path = root / "source" / requirements
    pins = (
        dict(
            line.strip().split("==", 1)
            for line in requirement_path.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        if requirement_path.exists()
        else {}
    )
    if not pins or any(config.get("expected_packages", {}).get(name) != value for name, value in pins.items()):
        errors.append("runtime configuration differs from committed package pins")
    if re.fullmatch(r"sha256:[a-f0-9]{64}", config.get("image", "")) is None:
        errors.append("missing full immutable image identity")
    if result.get("container_image") != config.get("image"):
        errors.append("image identity differs from immutable configuration")
    if re.fullmatch(r"[a-f0-9]{64}", result.get("runtime_digest_before", "")) is None or result.get(
        "runtime_digest_before"
    ) != result.get("runtime_digest_after"):
        errors.append("mounted runtime identity missing or changed")
    for filename, expected in config.get("harness_hashes", {}).items():
        if not (root / filename).is_file() or hashlib.sha256((root / filename).read_bytes()).hexdigest() != expected:
            errors.append("executed harness bytes changed: " + filename)
        if (
            not (root / "source/tools" / filename).is_file()
            or hashlib.sha256((root / "source/tools" / filename).read_bytes()).hexdigest() != expected
        ):
            errors.append("executed harness differs from frozen source: " + filename)
    if set(config.get("harness_hashes", {})) != {
        "parallel_worker.py",
        "qualification_evidence.py",
        "audit_parallel_qualification.py",
    }:
        errors.append("incomplete executed-harness identity")
    collection_rows = []
    collection_path = root / "collection/results.jsonl"
    if collection_path.exists():
        collection_rows = [json.loads(line) for line in collection_path.read_text().splitlines()]
    collection_events = [row for row in collection_rows if row.get("event") == "collection"]
    actual_collections = [row["ids"] for row in collection_events]
    if len(collection_events) != 1 or not all(valid_collection_contract(row) for row in collection_events):
        errors.append("initial collection lacks unfiltered effective-argument/raw-item evidence")
    collection_runtime = [row["runtime"] for row in collection_rows if row.get("event") == "worker_namespace"]
    if actual_collections != [collected] or len(collection_runtime) != 1:
        errors.append("missing/mismatched independent collection evidence")
    elif collection_runtime[0].get("packages") != config.get("expected_packages"):
        errors.append("actual runtime package versions differ from configured pins")
    if collection_runtime and (
        re.fullmatch(r"[a-f0-9]{64}", collection_runtime[0].get("python_sha256", "")) is None
        or not collection_runtime[0].get("python", "").startswith("3.14.")
    ):
        errors.append("actual interpreter identity is missing or unsupported")
    if not collected or len(collected) != len(set(collected)):
        errors.append("missing/empty/duplicate collection")
    if not selected or len(selected) != len(set(selected)) or not set(selected) <= set(collected):
        errors.append("missing/empty/duplicate/foreign selection")
    if config.get("mode") == "full" and selected != collected:
        errors.append("full selection differs from entire ordered collection")
    if any(
        item.get("commit") != args.expected_commit or item.get("source_digest") != args.expected_source_digest
        for item in (config, metadata, result)
    ):
        errors.append("snapshot/commit identity differs from caller pins")
    digest = hashlib.sha256()
    snapshot_bad = []
    for row in sorted(metadata.get("files", []), key=lambda item: os.fsencode(item["path"])):
        path = root / "source" / row["path"]
        if not path.is_file() or path.is_symlink():
            snapshot_bad.append(row["path"])
            continue
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        if checksum != row["sha256"]:
            snapshot_bad.append(row["path"])
        digest.update(os.fsencode(row["path"]) + b"\0" + bytes.fromhex(checksum))
    if snapshot_bad or not metadata.get("files") or digest.hexdigest() != args.expected_source_digest:
        errors.append("actual snapshot bytes differ from original source digest")
    expected_files = {row["path"] for row in metadata.get("files", [])}
    actual_files = {str(path.relative_to(root / "source")) for path in (root / "source").rglob("*") if path.is_file()}
    if actual_files != expected_files:
        errors.append("snapshot contains missing or additional files")
    assignments = []
    evidence = defaultdict(list)
    starts = Counter()
    sessions = {}
    namespaces = {}
    worker_collections = {}
    runtime_ends = {}
    malformed = []
    for index in range(config.get("workers", 0)):
        directory = root / str(index)
        assigned = read_json(directory / "assigned-ids.json", [])
        assignments.extend(assigned)
        if assigned != selected[index :: config["workers"]]:
            errors.append("worker assignment differs from deterministic partition: " + str(index))
        path = directory / "results.jsonl"
        if not path.exists():
            continue
        for number, line in enumerate(path.read_text().splitlines(), 1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                malformed.append(f"{index}:{number}")
                continue
            identifier = row.get("id")
            if identifier is not None and identifier not in assigned:
                errors.append("worker evidence has an unassigned ID: " + str(identifier))
            if row.get("event") == "start":
                starts[identifier] += 1
            elif row.get("event") == "report":
                evidence[identifier].append({**row, "worker": index})
            elif row.get("event") == "worker_namespace":
                namespaces.setdefault(index, []).append(row)
            elif row.get("event") == "collection":
                worker_collections.setdefault(index, []).append(row["ids"])
                if not valid_collection_contract(row):
                    errors.append(
                        "worker collection filtered or lacks raw-item/effective-argument evidence: " + str(index)
                    )
            elif row.get("event") == "session_finish":
                sessions.setdefault(index, []).append(row["exitstatus"])
                runtime_ends.setdefault(index, []).append(row.get("runtime"))
    for index in range(config.get("workers", 0)):
        if worker_collections.get(index) != [selected[index :: config["workers"]]]:
            errors.append("worker actual collection differs from assignment: " + str(index))
        records = namespaces.get(index, [])
        if (
            len(records) != 1
            or not collection_runtime
            or records[0].get("runtime") != collection_runtime[0]
            or runtime_ends.get(index) != collection_runtime
        ):
            errors.append("worker actual runtime differs from collection or changed: " + str(index))
    if len(assignments) != len(set(assignments)) or set(assignments) != set(selected):
        errors.append("assignment is not an exact once-only partition")
    if malformed:
        errors.append("malformed/truncated raw evidence")
    statuses = {"passed": [], "failed": [], "skipped": [], "interrupted": [], "unrun": []}
    failure_durations = []
    for identifier in selected:
        rows = evidence.get(identifier, [])
        phases = Counter(row["phase"] for row in rows)
        if starts[identifier] > 1 or any(count > 1 for count in phases.values()):
            errors.append("duplicate attempt/phase: " + identifier)
        if any(row["outcome"] == "failed" for row in rows):
            state = "failed"
            failure_durations.append(
                {
                    "id": identifier,
                    "worker": rows[0]["worker"],
                    "total_reported_seconds": sum(row["duration_s"] for row in rows),
                    "phases": [{key: row[key] for key in ("phase", "outcome", "duration_s")} for row in rows],
                }
            )
        elif any(row["outcome"] == "skipped" for row in rows):
            state = "skipped"
        elif (
            starts[identifier] == 1
            and phases == Counter({"setup": 1, "call": 1, "teardown": 1})
            and all(row["outcome"] == "passed" for row in rows)
        ):
            state = "passed"
        elif starts[identifier] or rows:
            state = "interrupted"
        else:
            state = "unrun"
        statuses[state].append(identifier)
    unexpected = sorted((set(evidence) | set(starts)) - set(selected))
    if unexpected:
        errors.append("unexpected raw IDs")
    if not result.get("complete") or result.get("abort_reason"):
        errors.append("supervisor did not complete cleanly")
    groups = result.get("groups", [])
    if len(groups) != config.get("workers") or any(group.get("rc") != 0 for group in groups):
        errors.append("worker exits not all zero")
    if result.get("source_digest_after") != args.expected_source_digest:
        errors.append("missing/different post-run source digest")
    if any(sessions.get(index) != [0] for index in range(config.get("workers", 0))):
        errors.append("pytest sessions not exactly once and zero")
    if len(statuses["passed"]) != len(selected):
        errors.append("not all selected IDs passed all three phases")
    manifest = []
    manifest_path = root / "sequence-manifest.jsonl"
    if manifest_path.exists():
        manifest = [json.loads(line) for line in manifest_path.read_text().splitlines()]
    expected_model = {}
    try:
        canonical, summary = canonical_manifest(root, config.get("suite"))
        if summary is not None:
            if manifest != canonical or read_json(root / "sequence-manifest.summary.json", None) != summary:
                errors.append("manifest inputs/denominators differ from independently regenerated pinned registry")
            expected_model = {row["node_id"]: row for row in canonical if row["exclusion"] is None}
            functions = {row["node_id"].split("[", 1)[0] for row in canonical}
            collected_model = {
                identifier
                for identifier in collected
                if identifier.startswith("tests_model/") or identifier.split("[", 1)[0] in functions
            }
            if collected_model != set(expected_model):
                errors.append("full collection differs from exact registered model IDs")
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        errors.append("canonical model manifest failed: " + str(exc))
    independently_collected = None
    try:
        if snapshot_bad or digest.hexdigest() != args.expected_source_digest or actual_files != expected_files:
            raise ValueError("source snapshot rejected; no recollection helper executed")
        independently_collected = canonical_collection(
            root, config, args.expected_commit, args.expected_source_digest, args.collection_executors
        )
        if independently_collected["ids"] != collected:
            errors.append("full collection differs from independently regenerated pinned-source pytest denominator")
        if collection_runtime and independently_collected.get("runtime") != collection_runtime[0]:
            errors.append("controlled recollection runtime differs from original collection")
        if independently_collected.get("runtime_digest") != result.get("runtime_digest_after"):
            errors.append("controlled recollection mounted runtime differs from qualification")
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        errors.append("independent complete collection failed: " + str(exc))
    invalid_model = []
    model_completed = Counter()
    for identifier in selected:
        case = expected_model.get(identifier)
        calls = [row for row in evidence.get(identifier, []) if row["phase"] == "call"]
        properties = calls[0].get("properties", {}) if calls else {}
        if case is None and not identifier.startswith("tests_model/") and "model_case" not in properties:
            continue
        valid = case is not None and len(calls) == 1 and calls[0]["outcome"] == "passed"
        valid = (
            valid
            and properties.get("model_case") == case["id"]
            and properties.get("model_family") == case["family"]
            and properties.get("model_input_sha256") == case["input_sha256"]
            and properties.get("completed_events") == case["events"]
            and properties.get("suffix_complete") is True
            and all(properties.get(key) is True for key in case.get("required_evidence", []))
        )
        if valid:
            model_completed[case["family"]] += 1
        else:
            invalid_model.append(identifier)
    if invalid_model:
        errors.append("selected model inputs lack exact full event/suffix evidence")
    if not result.get("worker_network", "").startswith("none"):
        errors.append("worker network isolation was not established")
    else:
        identities = []
        for index in range(config.get("workers", 0)):
            records = namespaces.get(index, [])
            expected_cpu = result.get("cpu_assignments", [])[index : index + 1]
            if len(records) != 1 or records[0].get("cpu_affinity") != expected_cpu:
                errors.append("worker namespace/CPU evidence invalid: " + str(index))
            elif records[0].get("network_namespace"):
                identities.append(records[0]["network_namespace"])
        if len(identities) != config.get("workers") or len(set(identities)) != len(identities):
            errors.append("worker network namespaces are not distinct")
    transport = read_json(root / "transport-result.json", None)
    if transport is not None and transport.get("returncode", transport.get("ssh_rc")) != 0:
        errors.append("SSH/remote command returned nonzero")
    audit = {
        "attempt_passed": not errors,
        "full_qualification_passed": not errors and config.get("mode") == "full",
        "mode": config.get("mode"),
        "suite": config.get("suite"),
        "workers": config.get("workers"),
        "commit": args.expected_commit,
        "source_digest": args.expected_source_digest,
        "actual_source_digest": digest.hexdigest(),
        "collected_cases": len(collected),
        "independent_collection_cases": len(independently_collected["ids"]) if independently_collected else None,
        "selected_cases": len(selected),
        "status_counts": {key: len(value) for key, value in statuses.items()},
        "model_manifest_raw": len(manifest),
        "model_manifest_runnable": len(expected_model),
        "completed_model_families": dict(model_completed),
        "errors": sorted(set(errors)),
        "snapshot_bad": snapshot_bad,
        "invalid_model": invalid_model,
        "unexpected": unexpected,
        "malformed_evidence": malformed,
        "scope": "One complete suite with immutable source and raw phases; finite model grammars where applicable.",
    }
    if independently_collected is not None:
        (root / "independent-collection.json").write_text(json.dumps(independently_collected, indent=2) + "\n")
    (root / "execution-accounting.json").write_text(
        json.dumps({"statuses": statuses, "failure_durations": failure_durations}, indent=2) + "\n"
    )
    (root / "audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    print(json.dumps(audit), flush=True)
    return 0 if audit["attempt_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
