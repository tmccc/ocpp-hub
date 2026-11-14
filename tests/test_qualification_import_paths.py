"""The evidence hook must load its exact source helper despite unrelated installed tools packages."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


# Intent: use actual runner/plugin loading across script, copied-harness and direct qualified pytest entrypoints.
# Why: a direct collector test never reached the HA runner's conflicting regular tools package.
@pytest.mark.parametrize(
    ("entrypoint", "foreign_tools"),
    [("script", True), ("script", False), ("copied", True), ("qualified", False)],
)
def test_evidence_resolves_source_helper_across_import_contexts(tmp_path, entrypoint, foreign_tools):
    source = tmp_path / "source"
    tools = source / "tools"
    tools.mkdir(parents=True)
    for name in ("offline_validate.py", "qualification_evidence.py", "qualification_collection.py"):
        shutil.copyfile(ROOT / "tools" / name, tools / name)
    (source / "pyproject.toml").write_text("[tool.pytest.ini_options]\n")
    witness = source / "test_never_run.py"
    witness.write_text("def test_only_collection():\n    raise AssertionError('test body must never run')\n")
    dependencies = tmp_path / "site-packages"
    dependencies.mkdir()
    if foreign_tools:
        (dependencies / "tools").mkdir()
        (dependencies / "tools/__init__.py").write_text("UNRELATED_INSTALLED_PACKAGE = True\n")
    result_path = tmp_path / "raw.jsonl"
    environment = {
        **os.environ,
        "TMPDIR": str(tmp_path),
        "SCENARIO_RESULTS": str(result_path),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_ADDOPTS": "",
        "PYTEST_PLUGINS": "",
        "QUALIFICATION_EXPECTED_PACKAGES": "{}",
        "PYTHONPATH": os.pathsep.join((str(tmp_path), str(source), str(dependencies))),
    }
    if entrypoint == "script":
        command = [sys.executable, "-B", str(tools / "offline_validate.py"), "-p", "qualification_evidence"]
    else:
        if entrypoint == "copied":
            shutil.copyfile(tools / "qualification_evidence.py", tmp_path / "qualification_evidence.py")
        plugin = "tools.qualification_evidence" if entrypoint == "qualified" else "qualification_evidence"
        command = [sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-p", plugin]
    command += [
        "-c",
        str(source / "pyproject.toml"),
        "--rootdir=" + str(source),
        "--confcutdir=" + str(source),
        "--collect-only",
        str(witness),
        "--basetemp=" + str(tmp_path / "pytest"),
    ]
    completed = subprocess.run(command, cwd=source, env=environment, capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    records = [json.loads(line) for line in result_path.read_text().splitlines()]
    collection = [row for row in records if row.get("event") == "collection"]
    assert len(collection) == 1 and collection[0]["ids"] == ["test_never_run.py::test_only_collection"]
    assert collection[0]["raw_ids"] == collection[0]["ids"]
    assert collection[0]["deselected"] == collection[0]["collection_errors"] == []
    assert collection[0]["effective_settings"]["environment"] == {"PYTEST_ADDOPTS": "", "PYTEST_PLUGINS": ""}
    assert not any(row.get("event") in ("start", "report") for row in records)
