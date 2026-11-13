"""Model-only collection initializes the same HA-free engine package as ordinary offline tests."""

from tests import conftest as _engine  # noqa: F401 - register the standalone package before collecting model imports.
