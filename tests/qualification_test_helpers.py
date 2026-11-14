"""Explicitly synthetic auditor-unit inputs; never usable as actual qualification receipts."""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


# Synthetic raw records have a full unfiltered schema so negative controls reach their intended evidence boundary.
def synthetic_collection_row(ids):
    return {
        "event": "collection",
        "ids": ids,
        "raw_ids": ids,
        "deselected": [],
        "collection_errors": [],
        "effective_settings": {
            "arguments": ["SYNTHETIC AUDITOR UNIT FIXTURE; NO QUALIFICATION"],
            "environment": {"PYTEST_ADDOPTS": "", "PYTEST_PLUGINS": ""},
            "filters": {
                "keyword": "",
                "markexpr": "",
                "deselect": [],
                "ignore": [],
                "ignore_glob": [],
                "pyargs": False,
                "noconftest": False,
            },
            "addopts": [],
            "python_files": ["test_*.py", "*_test.py"],
            "python_functions": ["test"],
            "python_classes": ["Test"],
        },
    }


# Real pytest expands only toy ordinary source; model rows and runtime identities remain explicitly synthetic unit data.
def synthetic_source_recollection(root, config, commit, checksum, executors=None):
    from tools import audit_parallel_qualification as audit

    assert (root / "SYNTHETIC-NOT-QUALIFICATION.txt").exists() or (
        root.parent / "SYNTHETIC-NOT-QUALIFICATION.txt"
    ).exists()
    source = root / "source"
    test_root = (
        "tests"
        if config["suite"] == "engine-model"
        else "tests_ha"
        if config["suite"] == "ha-legacy"
        else "tests_ha_proxy"
    )
    files = sorted((source / test_root).glob("test_*.py"))
    with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as temporary:
        output = Path(temporary) / "ordinary.json"
        code = """import json,pytest,sys
from pathlib import Path
class Capture:
 def pytest_collection_finish(self,session):
  Path(sys.argv[1]).write_text(json.dumps([item.nodeid for item in session.items]))
args=['--collect-only','-q','-p','no:cacheprovider','--rootdir='+sys.argv[2],*sys.argv[3:]]
raise SystemExit(pytest.main(args,plugins=[Capture()]))
"""
        environment = {
            **os.environ,
            "TMPDIR": temporary,
            "PYTHONPATH": str(source),
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        environment.pop("SCENARIO_RESULTS", None)
        environment.pop("QUALIFICATION_EXPECTED_PACKAGES", None)
        result = subprocess.run(
            [sys.executable, "-B", "-c", code, str(output), str(source), *map(str, files)],
            cwd=source,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode:
            raise ValueError("synthetic source collection failed: " + result.stdout + result.stderr)
        ordinary = json.loads(output.read_text())
    model, _ = audit.canonical_manifest(root, config["suite"])
    models = [row["node_id"] for row in model if row["exclusion"] is None]
    collection = [json.loads(line) for line in (root / "collection/results.jsonl").read_text().splitlines()]
    runtime = next(row["runtime"] for row in collection if row["event"] == "worker_namespace")
    result = json.loads((root / "result.json").read_text())
    return {
        "schema": 1,
        "suite": config["suite"],
        "ids": ordinary + models,
        "errors": [],
        "runtime": runtime,
        "runtime_digest": result["runtime_digest_after"],
        "SYNTHETIC_NOT_QUALIFICATION": True,
    }
