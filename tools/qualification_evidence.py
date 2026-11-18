"""Retain every pytest phase, wall duration and incomplete start without altering outcomes."""

import hashlib
import importlib.metadata
import json
import os
import runpy
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pytest

RAW_ITEMS = []
DESELECTED = []
COLLECTION_ERRORS = []
EFFECTIVE_SETTINGS = None
SCRATCH_ROOTS = None


# Only the supervisor opts its execution workers into dispatch. Collectors and static CI retain the normal loop.
def pytest_addoption(parser):
    parser.addoption("--qualification-work-queue", action="store_true")
    parser.addoption("--qualification-start-recovery-ram", action="store_true")


# Resolve the reviewed helper by source path, independent of installed tools packages or copied plugin locations.
def collection_settings(config):
    helper = Path(config.rootpath).resolve() / "tools/qualification_collection.py"
    effective = runpy.run_path(str(helper))["effective_settings"]
    return effective(config)


# Qualification instrumentation rejects effective filters before any selected test can run.
def pytest_configure(config):
    global EFFECTIVE_SETTINGS
    EFFECTIVE_SETTINGS = collection_settings(config)


# Record every generated item before pytest or a hook can remove it from session.items.
def pytest_itemcollected(item):
    RAW_ITEMS.append(item.nodeid)


# Deselected input remains explicit evidence rather than disappearing from the denominator.
def pytest_deselected(items):
    DESELECTED.extend(item.nodeid for item in items)


# A failed file import is never accepted as a shorter complete collection.
def pytest_collectreport(report):
    if report.failed:
        COLLECTION_ERRORS.append(report.nodeid)


# Record the actual interpreter and assert explicitly provisioned package versions inside each container.
def runtime_identity():
    expected = json.loads(os.environ.get("QUALIFICATION_EXPECTED_PACKAGES", "{}"))
    packages = {name: importlib.metadata.version(name) for name in expected}
    if packages != expected:
        raise pytest.UsageError("actual runtime package versions differ from qualification pins")
    return {
        "python": sys.version,
        "python_sha256": hashlib.sha256(Path(sys.executable).resolve().read_bytes()).hexdigest(),
        "packages": packages,
    }


# Capture counters within the worker's own namespace without exposing network endpoints.
def namespace_counters():
    result = {
        "pid": os.getpid(),
        "network_namespace": os.readlink("/proc/self/ns/net"),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
    }
    for name in ("sockstat", "sockstat6"):
        result[name] = (Path("/proc/net") / name).read_text().strip()
    states = {}
    for name in ("tcp", "tcp6"):
        for line in (Path("/proc/net") / name).read_text().splitlines()[1:]:
            state = line.split()[3]
            states[state] = states.get(state, 0) + 1
    result["tcp_state_counts"] = states
    return result


# Record the isolated namespace before the first fixture starts.
def pytest_sessionstart(session):
    global SCRATCH_ROOTS
    SCRATCH_ROOTS = None
    config = session.config
    if config.getoption("qualification_start_recovery_ram"):
        from tools.qualification_scratch import RAM_DIRECTORY, worker_scratch

        disk = Path(os.environ["TMPDIR"]).resolve(strict=True)
        if (
            config.option.collectonly
            or not config.getoption("--qualification-work-queue")
            or Path(tempfile.gettempdir()).resolve() != disk
            or config.option.basetemp is None
            or Path(config.option.basetemp).resolve() != disk / "tmp"
            or Path(os.environ["SCENARIO_RESULTS"]).resolve().parent != disk
        ):
            raise pytest.UsageError("mixed scratch requires the engine worker's ordinary disk temp and evidence paths")
        SCRATCH_ROOTS = worker_scratch(disk, disk / RAM_DIRECTORY)
    emit(
        {
            "event": "worker_namespace",
            "runtime": runtime_identity(),
            "scratch_roots": SCRATCH_ROOTS,
            **namespace_counters(),
        }
    )


# Machine-readable collection prevents text formatting or root-prefix parsing from silently omitting cases.
def pytest_collection_finish(session):
    effective = collection_settings(session.config)
    emit(
        {
            "event": "collection",
            "ids": [item.nodeid for item in session.items],
            "raw_ids": RAW_ITEMS,
            "deselected": DESELECTED,
            "collection_errors": COLLECTION_ERRORS,
            "effective_settings": effective,
        }
    )


# Append one process-local evidence row; every worker has its own file.
def emit(row):
    with Path(os.environ["SCENARIO_RESULTS"]).open("a") as output:
        output.write(json.dumps({"epoch": time.time(), **row}, default=str) + "\n")


# Request work through this worker's own scratch directory. The supervisor grants each ID at most once.
def next_grant(directory, sequence):
    request = directory / "request.json"
    temporary = request.with_suffix(".new")
    temporary.write_text(json.dumps(sequence) + "\n")
    temporary.replace(request)
    grant_path = directory / "grant.json"
    while True:
        if grant_path.exists():
            grant = json.loads(grant_path.read_text())
            if grant["sequence"] == sequence:
                emit({"event": "grant", **grant})
                return grant["id"]
        time.sleep(0.02)


# Use pytest's normal protocol and one real next item so session fixtures retain their normal lifetimes.
@pytest.hookimpl(tryfirst=True)
def pytest_runtestloop(session):
    if not session.config.getoption("--qualification-work-queue") or session.config.option.collectonly:
        return None
    if session.testsfailed:
        raise session.Interrupted("Worker collection failed")
    items = {item.nodeid: item for item in session.items}
    if not items or len(items) != len(session.items):
        raise session.Interrupted("Worker collection is empty or contains duplicate IDs")
    directory = Path(os.environ["SCENARIO_RESULTS"]).parent
    sequence = 0
    identifier = next_grant(directory, sequence)
    while identifier is not None:
        sequence += 1
        following = next_grant(directory, sequence)
        session.config.hook.pytest_runtest_protocol(
            item=items.pop(identifier), nextitem=items[following] if following is not None else None
        )
        if session.shouldfail:
            raise session.Failed(session.shouldfail)
        if session.shouldstop:
            raise session.Interrupted(session.shouldstop)
        identifier = following
    return True


# Record an attempted node before setup, so forced interruption differs from an unrun node.
def pytest_runtest_logstart(nodeid, location):
    emit({"event": "start", "id": nodeid})


# Preserve setup/call/teardown failures as well as successful call properties.
@pytest.hookimpl(trylast=True)
def pytest_runtest_logreport(report):
    emit(
        {
            "event": "report",
            "id": report.nodeid,
            "phase": report.when,
            "outcome": report.outcome,
            "duration_s": report.duration,
            "properties": dict(report.user_properties),
            "scratch": getattr(report, "qualification_scratch", None),
            "failure": report.longreprtext if report.failed else None,
        }
    )
    if report.failed:
        emit({"event": "failure_namespace", "id": report.nodeid, "phase": report.when, **namespace_counters()})


# Preserve pytest's own aggregate status separately from the supervising process result.
def pytest_sessionfinish(session, exitstatus):
    emit({"event": "session_finish", "exitstatus": int(exitstatus), "runtime": runtime_identity()})


# Capture the actual temp fixture before the body and the Start journal before cleanup.
def observe_case_scratch(item, report):
    from tools.qualification_scratch import path_evidence

    tmp = item.funcargs.get("tmp_path")
    path = tmp.resolve() if isinstance(tmp, Path) else None
    ram = Path(SCRATCH_ROOTS["ram"]["path"])
    if report.when == "setup" and report.passed and path is not None:
        report.qualification_scratch = path_evidence(path)
    if path is None or not path.is_relative_to(ram):
        return
    if report.when == "call" and report.passed and (path / "state.db").is_file():
        report.qualification_scratch = path_evidence(path / "state.db")
    if report.failed and path.is_dir():
        destination = Path(SCRATCH_ROOTS["disk"]["path"]) / "start-recovery-failures" / path.name
        try:
            destination.parent.mkdir(exist_ok=True)
            shutil.copytree(path, destination, symlinks=True)
        except OSError as error:
            emit({"event": "scratch_backup_failed", "id": item.nodeid, "detail": str(error)[:1000]})


# Preserve exceptions hidden by transport classification without changing the actual test result.
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if SCRATCH_ROOTS is not None:
        observe_case_scratch(item, report)
    if call.excinfo is not None and report.failed:
        current = call.excinfo.value
        chain = []
        seen = set()
        while current is not None and id(current) not in seen and len(chain) < 16:
            seen.add(id(current))
            stack = []
            frame = current.__traceback__
            while frame is not None:
                stack.append(
                    {
                        "file": frame.tb_frame.f_code.co_filename,
                        "function": frame.tb_frame.f_code.co_name,
                        "line": frame.tb_lineno,
                    }
                )
                frame = frame.tb_next
            chain.append(
                {
                    "type": type(current).__name__,
                    "errno": getattr(current, "errno", None),
                    "detail": str(current)[:1000],
                    "stack": stack,
                }
            )
            current = current.__cause__ or current.__context__
        emit({"event": "exception", "id": item.nodeid, "phase": report.when, "chain": chain})
