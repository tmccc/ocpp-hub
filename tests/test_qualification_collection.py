"""Small real pytest collections prove complete dynamic discovery and reject silent omission inputs."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_qualification_evidence import project
from tools.qualification_collection import replay
from tools.qualification_snapshot import freeze

ROOT = Path(__file__).resolve().parents[1]


# Build source-only collector fixtures whose bodies raise, so successful collection cannot be mistaken for execution.
@pytest.fixture
def collector_project(tmp_path):
    root = tmp_path / "source"
    (root / "tests").mkdir(parents=True)
    (root / "tools").mkdir()
    (root / "tests_model").mkdir()
    for name in ("qualification_collection.py", "offline_validate.py"):
        shutil.copyfile(ROOT / "tools" / name, root / "tools" / name)
    (root / "pyproject.toml").write_text('[tool.pytest.ini_options]\nasyncio_mode="auto"\n')
    (root / "tests/test_one.py").write_text(
        'import pytest\n@pytest.mark.parametrize("value",[1,2],ids=["left","right"])\n'
        'def test_parameter(value): raise AssertionError("test body must not run")\n'
        'def test_dynamic(shape): raise AssertionError("test body must not run")\n'
        'def test_plain(): raise AssertionError("test body must not run")\n'
    )
    (root / "tests/test_hidden.py").write_text('def test_hidden(): raise AssertionError("test body must not run")\n')
    (root / "tests/conftest.py").write_text(
        "def pytest_generate_tests(metafunc):\n"
        ' if "shape" in metafunc.fixturenames: metafunc.parametrize("shape",["a","b","c"])\n'
    )
    return root


# Launch the actual controlled collector in a separate offline process with no inherited pytest selection settings.
def collect(root, tmp_path, extra_environment=None):
    output = tmp_path / "controlled.json"
    environment = {
        **os.environ,
        "TMPDIR": str(tmp_path),
        "PYTHONPATH": str(root / "tools") + ":" + str(root),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
    }
    for key in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS", "SCENARIO_RESULTS", "QUALIFICATION_EXPECTED_PACKAGES"):
        environment.pop(key, None)
    environment.update(extra_environment or {})
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(root / "tools/qualification_collection.py"),
            "--source",
            str(root),
            "--suite",
            "engine-model",
            "--output",
            str(output),
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return result, json.loads(output.read_text()) if output.exists() else None


# Intent: static definitions and runtime parameter generation yield exactly the seven real cases without running a body.
# Why: source scanning alone cannot establish the full ordinary parameter denominator.
def test_controlled_collection_expands_real_dynamic_parameters(collector_project, tmp_path):
    result, payload = collect(collector_project, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert payload["ids"] == [
        "tests/test_hidden.py::test_hidden",
        "tests/test_one.py::test_parameter[left]",
        "tests/test_one.py::test_parameter[right]",
        "tests/test_one.py::test_dynamic[a]",
        "tests/test_one.py::test_dynamic[b]",
        "tests/test_one.py::test_dynamic[c]",
        "tests/test_one.py::test_plain",
    ]
    assert payload["ids"] == payload["raw_ids"] and not payload["errors"] and not payload["deselected"]
    assert payload["test_bodies_executed"] is False and len(payload["source_definitions"]) == 4


# Intent: filters, ignores, altered discovery and omission hooks never produce a shorter green universe.
# Why: a consistent selected/evidence list cannot reveal tests which disappeared before ordinary collection accounting.
@pytest.mark.parametrize(
    "problem",
    ["env-filter", "env-ignore", "env-plugin", "ignore-file", "remove-hook", "config-file-pattern", "config-filter"],
)
def test_controlled_collection_rejects_omission_inputs(collector_project, tmp_path, problem):
    environment = {}
    if problem.startswith("env-"):
        key = "PYTEST_PLUGINS" if problem == "env-plugin" else "PYTEST_ADDOPTS"
        environment[key] = (
            "pytest_asyncio.plugin"
            if problem == "env-plugin"
            else ("--ignore=tests/test_hidden.py" if problem == "env-ignore" else "-k test_plain")
        )
    elif problem == "ignore-file":
        with (collector_project / "tests/conftest.py").open("a") as stream:
            stream.write('collect_ignore=["test_hidden.py"]\n')
    elif problem == "remove-hook":
        with (collector_project / "tests/conftest.py").open("a") as stream:
            stream.write("def pytest_collection_modifyitems(items): items.pop()\n")
    else:
        with (collector_project / "pyproject.toml").open("a") as stream:
            stream.write(
                'python_files=["test_missing.py"]\n'
                if problem == "config-file-pattern"
                else 'addopts="-k test_plain"\n'
            )
    result, payload = collect(collector_project, tmp_path, environment)
    assert result.returncode != 0, "omission input unexpectedly established a complete denominator"
    expected = (
        "forbids PYTEST"
        if problem.startswith("env-")
        else (
            "omission/plugin setting forbidden"
            if problem == "ignore-file"
            else "omission hook forbidden"
            if problem == "remove-hook"
            else "discovery setting differs"
            if problem == "config-file-pattern"
            else "forbids test filters"
        )
    )
    assert expected in result.stdout + result.stderr, result.stdout + result.stderr
    assert "test body must not run" not in result.stdout + result.stderr
    assert payload is None or payload["errors"]


# Intent: no cached selected-list result substitutes for a missing pinned runtime.
# Why: later audits must fail clearly when their explicitly chosen runtime cannot perform independent recollection.
def test_collection_replay_refuses_unavailable_runtime(tmp_path):
    source = project(tmp_path / "repo", {"tests/test_one.py": "def test_one(): pass\n"})
    batch = tmp_path / "batch"
    batch.mkdir()
    metadata = freeze(source, batch / "source")
    (batch / "snapshot.json").write_text(json.dumps(metadata))
    (batch / "config.json").write_text(
        json.dumps(
            {
                "venv": str(tmp_path / "absent-runtime"),
                "venv_target": "/venv",
                "container_python": "/venv/bin/python",
                "runtime_mode": "image",
                "readonly_mounts": [],
            }
        )
    )
    (batch / "result.json").write_text(json.dumps({"runtime_digest_after": "0" * 64}))
    with pytest.raises(ValueError, match="runtime unavailable"):
        replay(batch, metadata["commit"], metadata["source_digest"])


# Intent: a trusted executor map cannot redirect source pins, suites or local batches before collection starts.
# Why: executor availability must be explicit caller authority rather than executable instructions in raw evidence.
@pytest.mark.parametrize("problem", ["pins", "suite", "host", "local-batch", "kind"])
def test_collection_executor_map_rejects_unbound_choices(collector_project, tmp_path, monkeypatch, problem):
    from tools import audit_parallel_qualification as audit

    batch = tmp_path / "batch"
    shutil.copytree(collector_project, batch / "source")
    choice = {"kind": "local", "batch": str(batch)}
    config = {"suite": "engine-model"}
    descriptor = {"commit": "a" * 40, "source_digest": "b" * 64, "executors": {"engine-model": choice}}
    if problem == "pins":
        descriptor["commit"] = "c" * 40
    elif problem == "suite":
        descriptor["executors"] = {}
    elif problem == "host":
        choice.update(kind="ssh", host="-bad-host", supervisor_python="/usr/bin/python3")
    elif problem == "local-batch":
        choice["batch"] = str(tmp_path / "different-batch")
    else:
        choice["kind"] = "arbitrary-command"
    path = tmp_path / "chosen-executors.json"
    path.write_text(json.dumps(descriptor))

    # No subprocess, SSH or collection may be invoked for an invalid explicitly selected executor.
    def forbidden_launch(*args, **kwargs):
        raise AssertionError("invalid descriptor unexpectedly launched a subprocess")

    monkeypatch.setattr(audit.subprocess, "run", forbidden_launch)
    with pytest.raises(ValueError):
        audit.canonical_collection(batch, config, "a" * 40, "b" * 64, path)


# Intent: absent local runtime stays a hard failure even when raw evidence offers SSH settings and a cached ID list.
# Why: a later auditor must neither trust a stale denominator nor execute a host chosen by raw evidence.
def test_collection_does_not_use_raw_executor_or_cached_denominator(collector_project, tmp_path, monkeypatch):
    from tools import audit_parallel_qualification as audit

    batch = tmp_path / "batch"
    shutil.copytree(collector_project, batch / "source")
    (batch / "independent-collection.json").write_text(json.dumps({"ids": ["forged_cached_case"]}))
    config = {"suite": "engine-model", "worker_host": "untrusted-raw-host"}
    commands = []

    # Report the real control boundary's failure, without allowing the synthetic unit test to launch any process.
    def unavailable(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, "", "pinned runtime unavailable")

    monkeypatch.setattr(audit.subprocess, "run", unavailable)
    with pytest.raises(ValueError, match="fresh pinned-runtime collection unavailable/failed"):
        audit.canonical_collection(batch, config, "a" * 40, "b" * 64)
    assert len(commands) == 1 and commands[0][0] == sys.executable and "ssh" not in commands[0]


# Intent: full launch argument parsing rejects inherited pytest filter/plugin variables before any freeze or worker run.
# Why: Docker image overrides cannot protect a collection whose supervising launch itself silently narrows pytest.
@pytest.mark.parametrize("variable", ["PYTEST_ADDOPTS", "PYTEST_PLUGINS"])
def test_full_launcher_rejects_inherited_selection_environment(tmp_path, monkeypatch, variable):
    from tools import parallel_qualify

    monkeypatch.setenv(variable, "unwanted-selection")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "parallel_qualify",
            "--output",
            str(tmp_path / "output"),
            "--venv",
            str(tmp_path / "venv"),
            "--image",
            "sha256:" + "a" * 64,
        ],
    )
    with pytest.raises(SystemExit) as error:
        parallel_qualify.arguments()
    assert error.value.code == 2


# Intent: the coverage CLI passes the caller's explicit collection executor map to its fresh full validator.
# Why: dropping the new argument would either choose the wrong local runtime or prevent a later remote re-audit.
def test_coverage_cli_forwards_chosen_collection_executors(tmp_path, monkeypatch):
    from tools import recovery_coverage

    bundle = tmp_path / "SYNTHETIC-CLI-ONLY-bundle.json"
    bundle.write_text(json.dumps({"commit": "synthetic", "source_digest": "synthetic", "phases": {}}))
    chosen = tmp_path / "explicit-unit-executors.json"
    chosen.write_text("{}")
    output = tmp_path / "SYNTHETIC-CLI-ONLY-output.json"
    commands = []

    # Only CLI propagation is exercised here; no validator, worker, test body or qualification is launched.
    def capture(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(recovery_coverage.subprocess, "run", capture)
    monkeypatch.setattr(recovery_coverage, "matrix", lambda: {"SYNTHETIC_NOT_QUALIFICATION": True})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "recovery_coverage",
            "--output",
            str(output),
            "--qualification-report",
            str(bundle),
            "--collection-executors",
            str(chosen),
        ],
    )
    assert recovery_coverage.main() == 0
    assert len(commands) == 1 and commands[0][-2:] == ["--collection-executors", str(chosen)]


# Intent: timed-out collection cleanup removes only its exact nonce-owned container and never another owner's container.
# Why: killing a Docker CLI alone does not bound a surviving container; ownership must be verified before removal.
@pytest.mark.parametrize("owner", ["correct", "different", "already-removed", "already-removed-lowercase"])
def test_collection_cleanup_checks_actual_owner_before_removal(monkeypatch, owner):
    from tools import qualification_collection as collector

    commands = []

    # Retain the exact requested operation while representing Docker's identity/absence responses in this isolated unit.
    def response(command, **kwargs):
        commands.append(command)
        if command[1] == "inspect":
            if owner.startswith("already-removed"):
                message = (
                    "error: no such object: nonce" if owner.endswith("lowercase") else "Error: No such object: nonce"
                )
                return subprocess.CompletedProcess(command, 1, "", message)
            return subprocess.CompletedProcess(command, 0, "expected" if owner == "correct" else "someone-else", "")
        return subprocess.CompletedProcess(command, 0, "removed", "")

    monkeypatch.setattr(collector.subprocess, "run", response)
    if owner == "different":
        with pytest.raises(ValueError, match="ownership differs"):
            collector.cleanup_collection_container("nonce", "expected")
    else:
        collector.cleanup_collection_container("nonce", "expected")
    assert len(commands) == (2 if owner == "correct" else 1)
    assert all(command[-1] == "nonce" for command in commands)


# Intent: supported deployment checks preserve the caller's explicit executor choice when invoking full validation.
# Why: remote qualification needs the explicit chosen host at the unchanged full-validation gate.
def test_supported_deployment_checks_forward_collection_executor(tmp_path, monkeypatch):
    from tools import deploy_ha

    chosen = tmp_path / "explicit-unit-executors.json"
    commands = []

    # Capture only the validator boundary; this unit never contacts HA, a container or a remote deployment target.
    def capture(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(deploy_ha.subprocess, "run", capture)
    deploy_ha.run_checks(tmp_path, chosen)
    assert commands == [[sys.executable, str(tmp_path / "tools/validate.py"), "--collection-executors", str(chosen)]]
