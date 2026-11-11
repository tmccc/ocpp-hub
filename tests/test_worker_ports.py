"""Parallel listener reservations must avoid automatic client ports and other active listeners."""

import socket

import pytest

from tests.worker_ports import listener_port_factory


# Intent: two workers' planned listeners remain distinct and can really bind alongside ephemeral client allocation.
# Why: the full 20-worker qualification exposed a bind/release race in the default ephemeral port fixture.
@pytest.mark.parametrize("worker", [0, 15, 16, 19])
def test_worker_listener_ports_are_distinct_and_bindable(worker):
    allocate = listener_port_factory(worker)
    first, second = allocate(), allocate()
    assert first != second and 14000 + worker * 500 <= first < second < 14500 + worker * 500
    with socket.socket() as listener, socket.socket() as client:
        listener.bind(("127.0.0.1", first))
        listener.listen()
        client.connect(("127.0.0.1", first))
        assert client.getsockname()[1] not in (first, second)
        assert listener_port_factory(worker)() != first
        with socket.socket() as following:
            following.bind(("127.0.0.1", second))


# Intent: an unsupported worker cannot silently overlap another worker's listener pool.
# Why: accidental reuse would reintroduce concurrency failures despite a valid isolated test case.
@pytest.mark.parametrize("worker", [-1, 20])
def test_worker_listener_scope_is_bounded(worker):
    with pytest.raises(ValueError, match="worker index"):
        listener_port_factory(worker)
