"""Exercise the work queue with real pytest sessions and an intentionally blocked worker."""

import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

from tools.audit_parallel_qualification import dispatch_grants
from tools.parallel_worker import WorkQueue

ROOT = Path(__file__).resolve().parents[1]
SUITE = """\
import os
import time
from pathlib import Path

import pytest

worker = Path(os.environ["TMPDIR"])

@pytest.fixture(scope="session", autouse=True)
def shared_resource():
    with (worker / "fixture").open("a") as output:
        output.write("setup\\n")
    yield
    with (worker / "fixture").open("a") as output:
        output.write("teardown\\n")

def test_wait():
    (worker / "waiting").touch()
    deadline = time.monotonic() + 20
    while not (worker / "release").exists():
        assert time.monotonic() < deadline
        time.sleep(0.01)

@pytest.mark.parametrize("case", range(6))
def test_short(case):
    pass
"""


# Run the real offline runner and queue plugin against a small local synthetic test source.
def start_worker(source, directory, output):
    directory.mkdir()
    return subprocess.Popen(
        [
            sys.executable,
            "-B",
            str(source / "tools/offline_validate.py"),
            "-p",
            "tools.qualification_evidence",
            "-x",
            "--qualification-work-queue",
            "cases",
        ],
        cwd=source,
        env={
            **os.environ,
            "TMPDIR": str(directory),
            "SCENARIO_RESULTS": str(directory / "results.jsonl"),
            "QUALIFICATION_EXPECTED_PACKAGES": "{}",
            "MODEL_WORKER_INDEX": directory.name,
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
        },
        stdout=output,
        stderr=subprocess.STDOUT,
    )


# Pump the same supervisor used in qualification, with a short diagnostic deadline.
def run_until(condition, queue, workers):
    deadline = time.monotonic() + 15
    while not condition():
        assert time.monotonic() < deadline, "worker did not reach its expected marker"
        for index, process in enumerate(workers):
            assert process.poll() in (None, 0)
            if process.poll() is None:
                queue.dispatch(index)
        time.sleep(0.01)


# Intent: a free worker completes queued cases while another is still executing its first case.
# Why: fixed shards cannot help a blocked worker, and restarting pytest would recreate the session fixture.
def test_idle_worker_drains_queue_without_restarting_pytest(tmp_path):
    source = tmp_path / "source"
    (source / "tools").mkdir(parents=True)
    for name in ("offline_validate.py", "qualification_evidence.py", "qualification_collection.py"):
        shutil.copyfile(ROOT / "tools" / name, source / "tools" / name)
    (source / "pyproject.toml").write_text("[tool.pytest.ini_options]\n")
    (source / "cases").mkdir()
    (source / "cases/test_sample.py").write_text(SUITE)
    prefix = "cases/test_sample.py::"
    selected = [prefix + "test_wait", *(prefix + f"test_short[{i}]" for i in range(6))]
    workers = []
    with (tmp_path / "dispatch.jsonl").open("w") as ledger, (tmp_path / "pytest.log").open("w") as output:
        queue = WorkQueue(tmp_path, selected, ledger)
        try:
            workers.append(start_worker(source, tmp_path / "0", output))
            run_until((tmp_path / "0/waiting").exists, queue, workers)
            workers.append(start_worker(source, tmp_path / "1", output))
            run_until(lambda: workers[1].poll() is not None, queue, workers)
            assert workers[1].returncode == 0 and workers[0].poll() is None
            assert queue.assigned[1] == selected[2:]
            (tmp_path / "0/release").touch()
            run_until(lambda: workers[0].poll() is not None, queue, workers)
            assert workers[0].returncode == 0
        finally:
            for process in workers:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)

    grants, errors = dispatch_grants(tmp_path, selected, 2)
    assert not errors
    for index in range(2):
        directory = tmp_path / str(index)
        records = [json.loads(line) for line in (directory / "results.jsonl").read_text().splitlines()]
        assert [row["ids"] for row in records if row["event"] == "collection"] == [selected]
        assert [(row["sequence"], row["id"]) for row in records if row["event"] == "grant"] == grants[index]
        assert [row["id"] for row in records if row["event"] == "start"] == queue.assigned[index]
        reports = [row for row in records if row["event"] == "report"]
        assert Counter((row["id"], row["phase"], row["outcome"]) for row in reports) == Counter(
            (identifier, phase, "passed")
            for identifier in queue.assigned[index]
            for phase in ("setup", "call", "teardown")
        )
        assert (directory / "fixture").read_text() == "setup\nteardown\n"
