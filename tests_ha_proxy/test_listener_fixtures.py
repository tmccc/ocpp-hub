"""Actual HA-root fixture discovery must retain the worker's planned-listener policy."""

import socket
from contextlib import ExitStack

import pytest


# Intent: HA-only collection obtains three real listeners from the declared worker pool through actual discovery.
# Why: importing a sibling conftest module alone leaves pytest's bind-and-release ephemeral fixtures in use.
@pytest.mark.parametrize("worker", [0, 1, 19])
def test_ha_proxy_listener_fixture_discovery(request, monkeypatch, worker):
    monkeypatch.setenv("MODEL_WORKER_INDEX", str(worker))
    allocate = request.getfixturevalue("unused_tcp_port_factory")
    ports = [request.getfixturevalue("unused_tcp_port"), allocate(), allocate()]
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
