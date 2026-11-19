"""Use the historical rehearsal pair or the exact builds selected by the release validator."""

import os
import sys
from pathlib import Path

import pytest

from tests.core_loader import load_test_core

INSTALLED_ROOT = os.environ.get("OCPP_WEEK_COMMISSIONING_ROOT")
if INSTALLED_ROOT:
    load_test_core(Path(INSTALLED_ROOT))

from tests.conftest import unused_tcp_port_factory as unused_tcp_port_factory  # noqa: E402
from tests_installation.migrated_layout import AUDITED_SCHEMA  # noqa: E402


# Keep the two build inputs explicit so the week can be rerun without a Git checkout.
def pytest_addoption(parser):
    parser.addoption("--week-current", type=Path)
    parser.addoption("--week-previous", type=Path)


# Confirm the two runtime sources before using the simulator.
@pytest.fixture
def builds(pytestconfig):
    current = pytestconfig.getoption("--week-current")
    previous = pytestconfig.getoption("--week-previous")
    if not INSTALLED_ROOT:
        from tests_rollback.conftest import verified_builds

        return (*verified_builds(current, previous), AUDITED_SCHEMA)
    if (
        current != Path(__file__).resolve().parents[1]
        or previous != Path(INSTALLED_ROOT)
        or Path(sys.modules["ocpp_hub_core"].__file__).resolve()
        != previous / "custom_components/ocpp_hub/core/__init__.py"
    ):
        raise pytest.UsageError(
            "the automatic week must commission with the installed build and test the frozen candidate"
        )
    return current, previous, None
