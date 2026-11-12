"""Retain every pytest phase, wall duration and incomplete start without altering outcomes."""

import hashlib
import importlib.metadata
import json
import os
import sys
import time
from pathlib import Path

import pytest


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
    emit({"event": "worker_namespace", "runtime": runtime_identity(), **namespace_counters()})


# Machine-readable collection prevents text formatting or root-prefix parsing from silently omitting cases.
def pytest_collection_finish(session):
    emit({"event": "collection", "ids": [item.nodeid for item in session.items]})


# Append one process-local evidence row; every worker has its own file.
def emit(row):
    with Path(os.environ["SCENARIO_RESULTS"]).open("a") as output:
        output.write(json.dumps({"epoch": time.time(), **row}, default=str) + "\n")


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
            "failure": report.longreprtext if report.failed else None,
        }
    )
    if report.failed:
        emit({"event": "failure_namespace", "id": report.nodeid, "phase": report.when, **namespace_counters()})


# Preserve pytest's own aggregate status separately from the supervising process result.
def pytest_sessionfinish(session, exitstatus):
    emit({"event": "session_finish", "exitstatus": int(exitstatus), "runtime": runtime_identity()})


# Preserve exceptions hidden by transport classification without changing the actual test result.
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
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
