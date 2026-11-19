"""Use the same immutable builds as the standalone rollback rehearsal."""

from pathlib import Path

import pytest

from tests.conftest import unused_tcp_port_factory as unused_tcp_port_factory
from tests_rollback.conftest import verified_builds


# Keep the two build inputs explicit so the week can be rerun without a Git checkout.
def pytest_addoption(parser):
    parser.addoption("--week-current", type=Path)
    parser.addoption("--week-previous", type=Path)


# Confirm the two runtime sources before using the simulator.
@pytest.fixture
def builds(pytestconfig):
    return verified_builds(pytestconfig.getoption("--week-current"), pytestconfig.getoption("--week-previous"))
