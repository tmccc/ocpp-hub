"""Derive the full pytest denominator from pinned source in a controlled, collect-only runtime."""

from __future__ import annotations

import argparse
import ast
import fnmatch
import hashlib
import importlib.metadata
import inspect
import json
import os
import secrets
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath

ROOTS = {
    "engine-model": ("tests", "tests_model"),
    "ha-proxy": ("tests_ha_proxy",),
    "ha-proxy-legacy": ("tests_ha_proxy",),
    "ha-legacy": ("tests_ha",),
}
FILTER_OPTIONS = ("keyword", "markexpr", "deselect", "ignore", "ignore_glob", "pyargs", "noconftest")
OMISSION_HOOKS = {
    "pytest_ignore_collect",
    "pytest_collect_file",
    "pytest_pycollect_makemodule",
    "pytest_pycollect_makeitem",
    "pytest_collection",
    "pytest_collection_modifyitems",
    "pytest_collection_finish",
    "pytest_deselected",
    "pytest_cmdline_parse",
    "pytest_load_initial_conftests",
}
RAW = []
DESELECTED = []
COLLECTION_ERRORS = []


# Every allowed suite starts from complete source roots rather than a run's supplied node IDs or file list.
def source_inventory(source, suite):
    files, definitions = {}, []
    for name in ROOTS[suite]:
        base = source / name
        if not base.is_dir():
            raise ValueError("required test root missing: " + name)
        for path in sorted(base.rglob("*.py")):
            if any(fnmatch.fnmatchcase(path.name, pattern) for pattern in ("test_*.py", "*_test.py")):
                relative = str(path.relative_to(source))
                files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
                tree = ast.parse(path.read_text(), filename=relative)
                definitions.extend(source_definitions(tree.body, relative))
    if not files or not definitions or len(definitions) != len(set(definitions)):
        raise ValueError("empty or duplicate source test definition inventory")
    # Project conftests may supply fixtures and parameter generation; omission hooks require an explicit policy change.
    for path in sorted(source.rglob("conftest.py")):
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in OMISSION_HOOKS:
                raise ValueError("project collection omission hook forbidden: " + node.name)
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                if any(
                    isinstance(target, ast.Name)
                    and target.id in ("collect_ignore", "collect_ignore_glob", "pytest_plugins")
                    for target in targets
                ):
                    raise ValueError("project collection omission/plugin setting forbidden")
    return files, definitions


# Test bodies are never executed; source definitions only anchor discovery while pytest expands real parameters.
def source_definitions(statements, prefix):
    result = []
    for node in statements:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test"):
            result.append(prefix + "::" + node.name)
        elif isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            result.extend(source_definitions(node.body, prefix + "::" + node.name))
        elif isinstance(node, (ast.If, ast.Try, ast.With)):
            result.extend(source_definitions(node.body, prefix))
            if hasattr(node, "orelse"):
                result.extend(source_definitions(node.orelse, prefix))
            if hasattr(node, "finalbody"):
                result.extend(source_definitions(node.finalbody, prefix))
            if isinstance(node, ast.Try):
                for handler in node.handlers:
                    result.extend(source_definitions(handler.body, prefix))
    return result


# Record effective filtering inputs and reject every setting which can silently reduce a qualification collection.
def effective_settings(config):
    import pytest

    source = Path(config.rootpath).resolve()
    trusted = {source / "tools/qualification_collection.py", source / "tools/qualification_evidence.py"}
    for hook in OMISSION_HOOKS:
        for implementation in getattr(config.pluginmanager.hook, hook).get_hookimpls():
            filename = inspect.getsourcefile(implementation.function)
            if filename and Path(filename).resolve().is_relative_to(source) and Path(filename).resolve() not in trusted:
                raise pytest.UsageError("project collection omission hook forbidden: " + hook)
    environment = {key: os.environ.get(key, "") for key in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS")}
    if any(environment.values()):
        raise pytest.UsageError("qualification forbids PYTEST_ADDOPTS and PYTEST_PLUGINS")
    filters = {name: getattr(config.option, name, None) for name in FILTER_OPTIONS}
    if any(filters.values()) or config.getini("addopts"):
        raise pytest.UsageError("qualification forbids test filters, ignores and inherited addopts")
    for key, expected in (
        ("python_files", ["test_*.py", "*_test.py"]),
        ("python_functions", ["test"]),
        ("python_classes", ["Test"]),
    ):
        if config.getini(key) != expected:
            raise pytest.UsageError("qualification discovery setting differs: " + key)
    return {
        "arguments": list(config.invocation_params.args),
        "environment": environment,
        "filters": filters,
        "addopts": config.getini("addopts"),
        "python_files": config.getini("python_files"),
        "python_functions": config.getini("python_functions"),
        "python_classes": config.getini("python_classes"),
        "inifile": str(config.inipath),
    }


# Pin the interpreter and explicit suite package versions in the denominator's own actual runtime.
def runtime_identity():
    expected = json.loads(os.environ.get("QUALIFICATION_EXPECTED_PACKAGES", "{}"))
    actual = {key: importlib.metadata.version(key) for key in expected}
    if actual != expected:
        raise ValueError("controlled collection runtime package pins differ")
    return {
        "python": sys.version,
        "python_sha256": hashlib.sha256(Path(sys.executable).resolve().read_bytes()).hexdigest(),
        "packages": actual,
    }


# Capture raw items before collection-modification hooks and before final selection can hide them.
def pytest_itemcollected(item):
    RAW.append(item.nodeid)


# Any actual deselection is retained and makes the complete-denominator collection fail.
def pytest_deselected(items):
    DESELECTED.extend(item.nodeid for item in items)


# Import/collection errors cannot be mistaken for a complete shorter denominator.
def pytest_collectreport(report):
    if report.failed:
        COLLECTION_ERRORS.append(report.nodeid)


# Save the independently generated parameter-expanded universe plus its source and effective collection contract.
def pytest_collection_finish(session):
    import pytest

    source = Path(os.environ["QUALIFICATION_COLLECTION_SOURCE"])
    suite = os.environ["QUALIFICATION_COLLECTION_SUITE"]
    files, definitions = source_inventory(source, suite)
    ids = [item.nodeid for item in session.items]
    errors = []
    if ids != RAW or len(ids) != len(set(ids)) or not ids:
        errors.append("raw/final collection mismatch, duplicate or empty IDs")
    bases = {identifier.split("[", 1)[0] for identifier in ids}
    missing = sorted(set(definitions) - bases)
    if missing:
        errors.append("source test definitions absent from collection")
    if DESELECTED or COLLECTION_ERRORS:
        errors.append("collection deselected or failed")
    payload = {
        "schema": 1,
        "suite": suite,
        "ids": ids,
        "raw_ids": RAW,
        "deselected": DESELECTED,
        "collection_errors": COLLECTION_ERRORS,
        "source_files": files,
        "source_definitions": definitions,
        "missing_source_definitions": missing,
        "effective_settings": effective_settings(session.config),
        "runtime": runtime_identity(),
        "errors": errors,
        "test_bodies_executed": False,
    }
    Path(os.environ["QUALIFICATION_COLLECTION_OUTPUT"]).write_text(json.dumps(payload, sort_keys=True) + "\n")
    if errors:
        pytest.exit("controlled full collection failed: " + ", ".join(errors), returncode=4)


# Run only pytest collection through the existing loopback audit wrapper with explicit complete source files.
def collect(source, suite, output):
    files, _ = source_inventory(source, suite)
    if os.environ.get("PYTEST_ADDOPTS") or os.environ.get("PYTEST_PLUGINS"):
        raise ValueError("qualification forbids PYTEST_ADDOPTS and PYTEST_PLUGINS")
    os.environ.update(
        QUALIFICATION_COLLECTION_SOURCE=str(source),
        QUALIFICATION_COLLECTION_SUITE=suite,
        QUALIFICATION_COLLECTION_OUTPUT=str(output),
    )
    runner = "offline_validate.py" if suite == "engine-model" else "offline_ha_validate.py"
    sys.argv = [
        str(source / "tools" / runner),
        "-c",
        str(source / "pyproject.toml"),
        "--rootdir=" + str(source),
        "--confcutdir=" + str(source),
        "-p",
        "qualification_collection",
        "--collect-only",
        "--basetemp=" + str(output.parent / "pytest-tmp"),
        *files,
    ]
    if suite == "ha-legacy":
        sys.argv.append("--legacy-ha")
    # Running the committed wrapper preserves actual HA compatibility initialization and the network audit hook.
    import runpy

    runpy.run_path(str(source / "tools" / runner), run_name="__main__")


# Verify every snapshot byte before/after import-based collection rather than trusting metadata's success flag.
def source_digest(batch):
    metadata = json.loads((batch / "snapshot.json").read_text())
    digest = hashlib.sha256()
    names = set()
    for row in sorted(metadata["files"], key=lambda item: os.fsencode(item["path"])):
        path = batch / "source" / row["path"]
        if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != row["sha256"]:
            raise ValueError("controlled collection snapshot differs: " + row["path"])
        names.add(row["path"])
        digest.update(os.fsencode(row["path"]) + b"\0" + bytes.fromhex(row["sha256"]))
    if names != {str(p.relative_to(batch / "source")) for p in (batch / "source").rglob("*") if p.is_file()}:
        raise ValueError("controlled collection snapshot file inventory differs")
    return metadata["commit"], digest.hexdigest()


# Match the existing worker's exact mounted virtual-environment digest, excluding volatile bytecode caches.
def runtime_digest(config):
    digest = hashlib.sha256()
    base = Path(config["venv"])
    if not base.is_dir():
        raise ValueError("pinned collection runtime unavailable on selected executor")
    for path in sorted(base.rglob("*")):
        if (not path.is_file() and not path.is_symlink()) or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        data = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
        digest.update(str(path.relative_to(base)).encode() + b"\0" + hashlib.sha256(data).digest())
    return digest.hexdigest()


# Container destinations are lexical POSIX paths; reject traversal rather than resolving against the wrong host/image.
def container_path(value):
    if not isinstance(value, str) or not value.startswith("/") or ".." in value.split("/") or "," in value:
        raise ValueError("qualification mount paths must be absolute canonical container paths")
    path = PurePosixPath(value)
    if str(path) != value or value.startswith("//"):
        raise ValueError("qualification mount paths must have one canonical spelling")
    return path


# An overlay of a path, an ancestor or a descendant can change the bytes seen inside the container.
def overlapping_paths(left, right):
    return left == right or left in right.parents or right in left.parents


# Auxiliary OpenSSL assets cannot hide pinned source, harness, runtime or writable evidence.
def validate_readonly_mounts(config, batch, scratch, *, check_sources=True):
    required = {"venv_target", "container_python", "runtime_mode", "readonly_mounts"}
    if (
        not isinstance(config, dict)
        or not required <= config.keys()
        or config["runtime_mode"] not in ("host", "image")
        or not isinstance(config["readonly_mounts"], list)
    ):
        raise ValueError("invalid qualification runtime mount configuration")
    batch_path, scratch_path = container_path(str(batch)), container_path(str(scratch))
    runtime = container_path(config["venv_target"])
    interpreter = container_path(config["container_python"])
    data = (batch_path, batch_path / "source", scratch_path)
    if any(overlapping_paths(runtime, path) for path in data):
        raise ValueError("configured runtime mount overlaps pinned source or evidence")
    protected = (*data, runtime, interpreter)
    system = tuple(PurePosixPath(path) for path in ("/usr", "/lib", "/lib64", "/bin", "/sbin", "/opt", "/etc"))
    seen = set()
    for mount in config["readonly_mounts"]:
        if not isinstance(mount, dict) or set(mount) != {"source", "target"}:
            raise ValueError("invalid qualification auxiliary mount shape")
        source = container_path(mount["source"])
        target = container_path(mount["target"])
        if target in seen:
            raise ValueError("duplicate qualification auxiliary mount target")
        seen.add(target)
        if any(overlapping_paths(target, path) for path in protected):
            raise ValueError("auxiliary mount overlaps pinned source, runtime or evidence")
        openssl = target == PurePosixPath("/usr/bin/openssl") and config["runtime_mode"] == "image"
        if any(overlapping_paths(target, path) for path in system) and not openssl:
            raise ValueError("auxiliary mount overlaps pinned system runtime code")
        if not openssl and target != PurePosixPath("/task/openssl-runtime"):
            raise ValueError("unsupported auxiliary mount destination; only isolated OpenSSL assets are supported")
        if check_sources:
            path = Path(str(source))
            if openssl and (path.is_symlink() or not path.is_file()):
                raise ValueError("image OpenSSL auxiliary mount requires an actual nonsymlink regular file")
            if not openssl and not path.is_dir():
                raise ValueError("OpenSSL runtime auxiliary mount requires its actual directory")


# A timed-out Docker client can leave its container alive; remove only the nonce-named container with our exact owner.
def cleanup_collection_container(name, owner):
    checked = subprocess.run(
        ["docker", "inspect", "--format", '{{index .Config.Labels "codex.collection.owner"}}', name],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if checked.returncode:
        if "no such object" in checked.stderr.lower() or "no such container" in checked.stderr.lower():
            return {"outcome": "already_removed", "owner_verified": False}
        raise ValueError("collection container cleanup identity unavailable")
    if checked.stdout.strip() != owner:
        raise ValueError("collection container cleanup ownership differs; no removal attempted")
    removed = subprocess.run(["docker", "rm", "--force", name], capture_output=True, text=True, timeout=10, check=False)
    if (
        removed.returncode
        and "no such container" not in removed.stderr.lower()
        and "no such object" not in removed.stderr.lower()
    ):
        raise ValueError("owned collection container cleanup failed")
    return {"outcome": "removed", "owner_verified": True}


# Launch one bounded read-only/network-none COLLECTION container; unavailable executors never use a saved-list fallback.
def replay(batch, commit, checksum):
    config = json.loads((batch / "config.json").read_text())
    validate_readonly_mounts(config, batch, batch / "collection-replay-pending")
    result = json.loads((batch / "result.json").read_text())
    if source_digest(batch) != (commit, checksum):
        raise ValueError("collection replay source identity differs from caller pins")
    runtime_before = runtime_digest(config)
    if runtime_before != result.get("runtime_digest_after"):
        raise ValueError("collection replay runtime changed since qualification")
    with tempfile.TemporaryDirectory(prefix="collection-replay-", dir=batch) as temp:
        scratch = Path(temp)
        validate_readonly_mounts(config, batch, scratch)
        output = scratch / "collection.json"
        name = "icsq-collection-" + secrets.token_hex(12)
        owner = str(batch) + ":" + name
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            name,
            "--label",
            "codex.collection.owner=" + owner,
            "--pull=never",
            "--network",
            "none",
            "--read-only",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--mount",
            f"type=bind,src={batch},dst={batch},readonly",
            "--mount",
            f"type=bind,src={scratch},dst={scratch}",
            "--mount",
            f"type=bind,src={config['venv']},dst={config['venv_target']},readonly",
        ]
        if config["runtime_mode"] == "host":
            command += [
                "--mount",
                "type=bind,src=/usr,dst=/usr,readonly",
                "--mount",
                "type=bind,src=/etc/ssl/openssl.cnf,dst=/etc/ssl/openssl.cnf,readonly",
            ]
        for row in config["readonly_mounts"]:
            command += ["--mount", f"type=bind,src={row['source']},dst={row['target']},readonly"]
        env = {
            "TMPDIR": str(scratch),
            "PYTHONPATH": str(batch / "source/tools") + ":" + str(batch / "source"),
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
            "XDG_CACHE_HOME": str(scratch / "cache"),
            "PYTHONHOME": "",
            "LD_LIBRARY_PATH": "",
            "QUALIFICATION_EXPECTED_PACKAGES": json.dumps(config["expected_packages"]),
        }
        for key, value in env.items():
            command += ["--env", key + "=" + value]
        command += [
            "--workdir",
            str(batch / "source"),
            "--entrypoint",
            config["container_python"],
            config["image"],
            "-B",
            str(batch / "source/tools/qualification_collection.py"),
            "--source",
            str(batch / "source"),
            "--suite",
            config["suite"],
            "--output",
            str(output),
        ]
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=180, check=False)
        finally:
            cleanup = cleanup_collection_container(name, owner)
        if completed.returncode or not output.is_file():
            raise ValueError(
                "controlled pinned-runtime collection failed: " + completed.stdout[-1000:] + completed.stderr[-500:]
            )
        payload = json.loads(output.read_text())
        payload.update(commit=commit, source_digest=checksum, batch=str(batch))
        payload["runtime_digest"] = runtime_before
        payload["executor_evidence"] = {
            "container_cleanup": cleanup,
            "container_argv": command,
            "returncode": completed.returncode,
            "stdout_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
        }
    if source_digest(batch) != (commit, checksum) or runtime_digest(config) != runtime_before:
        raise ValueError("source/runtime changed during collection replay")
    return payload


# Expose source collection to its pinned runtime and replay orchestration to the explicitly chosen executor.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--suite", choices=tuple(ROOTS))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--replay-batch", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--source-digest")
    args = parser.parse_args()
    if args.replay_batch:
        print(json.dumps(replay(args.replay_batch.resolve(), args.commit, args.source_digest), sort_keys=True))
    elif args.source and args.suite and args.output:
        collect(args.source.resolve(), args.suite, args.output.resolve())
    else:
        parser.error("select either replay-batch with caller pins or source/suite/output")


if __name__ == "__main__":
    main()
