"""Explicit inputs for a bounded offline search or a saved event replay."""

from pathlib import Path

from tests.conftest import unused_tcp_port_factory as unused_tcp_port_factory


# Keep every output directory and optional saved trace explicit.
def pytest_addoption(parser):
    parser.addoption("--discovery-output", type=Path)
    parser.addoption("--discovery-seed", type=int, default=19)
    parser.addoption("--discovery-cases", type=int, default=20)
    parser.addoption("--discovery-replay", type=Path)
