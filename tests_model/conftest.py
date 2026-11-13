"""Model-only collection initializes the same HA-free engine package as ordinary offline tests."""

from tests import conftest as _engine  # noqa: F401 - register the standalone package before collecting model imports.

# Model tests are siblings of tests/, so expose the same actual pytest fixtures in this subtree.
from tests.conftest import unused_tcp_port as unused_tcp_port
from tests.conftest import unused_tcp_port_factory as unused_tcp_port_factory
