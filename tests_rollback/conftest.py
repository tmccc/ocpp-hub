"""The cross-build rehearsal runs only when explicitly given two release snapshots."""

from pathlib import Path

import pytest

from tests.conftest import unused_tcp_port_factory as unused_tcp_port_factory
from tools.check_proxy_rollback import CURRENT, PREVIOUS, build_matches


# Keep cross-build inputs explicit because frozen qualification has no Git history.
def pytest_addoption(parser):
    parser.addoption("--rollback-current", type=Path)
    parser.addoption("--rollback-previous", type=Path)


# Supply the exact two source directories to each isolated simulator.
def verified_builds(current, previous):
    paths = (current, previous)
    if any(path is None or not path.is_absolute() or not path.is_dir() for path in paths):
        raise pytest.UsageError("supply two absolute archived source directories for the rollback rehearsal")
    current, previous = (path.resolve() for path in paths)
    if not build_matches(current, CURRENT) or not build_matches(previous, PREVIOUS):
        raise pytest.UsageError("the supplied source directories do not match the exact rollback pair")
    return current, previous


# Read the source directories for the focused rollback suite.
@pytest.fixture
def builds(pytestconfig):
    return verified_builds(pytestconfig.getoption("--rollback-current"), pytestconfig.getoption("--rollback-previous"))
