"""Real pytest fixture discovery must apply the worker listener policy to sibling model tests."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ACTUAL_FILES = (
    "tests/__init__.py",
    "tests/conftest.py",
    "tests/worker_ports.py",
    "tests_model/__init__.py",
    "tests_model/conftest.py",
    "custom_components/ocpp_hub/core/__init__.py",
    "tools/offline_validate.py",
    "pyproject.toml",
)
PROBE = """import os
import socket
from contextlib import ExitStack


def test_real_model_fixture_discovery(unused_tcp_port, unused_tcp_port_factory):
    worker = int(os.environ["MODEL_WORKER_INDEX"])
    ports = [unused_tcp_port, unused_tcp_port_factory(), unused_tcp_port_factory()]
    lower = 14000 + 500 * worker
    assert len(set(ports)) == 3, ports
    assert all(lower <= port < lower + 500 for port in ports), (worker, ports)
    with ExitStack() as stack:
        listeners = [stack.enter_context(socket.socket()) for _ in ports]
        for listener, port in zip(listeners, ports, strict=True):
            listener.bind(("127.0.0.1", port))
            listener.listen()
        client = stack.enter_context(socket.socket())
        client.connect(("127.0.0.1", ports[0]))
        assert client.getsockname()[1] not in ports
"""


# Intent: both complete-engine and model-only pytest roots apply actual worker-pool fixtures to model cases.
# Why: importing a sibling conftest initializes its package but does not register its pytest fixture definitions.
@pytest.mark.parametrize("worker", [0, 1, 19])
@pytest.mark.parametrize("roots", [("tests_model",), ("tests", "tests_model")], ids=["model-only", "combined"])
def test_model_listener_fixtures_apply_under_actual_discovery(tmp_path, worker, roots):
    checkout = tmp_path / "fixture-checkout"
    for relative in ACTUAL_FILES:
        target = checkout / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    # This probe exists only in the temporary hierarchy, never in the canonical model universe.
    (checkout / "tests_model/test_fixture_probe.py").write_text(PROBE)
    environment = {
        **os.environ,
        "TMPDIR": str(tmp_path),
        "PYTHONPATH": str(checkout),
        "MODEL_WORKER_INDEX": str(worker),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    for key in ("SCENARIO_RESULTS", "PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        environment.pop(key, None)
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(checkout / "tools/offline_validate.py"),
            "-c",
            str(checkout / "pyproject.toml"),
            *roots,
            "--basetemp=" + str(tmp_path / "child-tmp"),
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed" in result.stdout, result.stdout + result.stderr
