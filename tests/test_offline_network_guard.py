"""Check guard decisions directly so no prohibited socket or DNS request is attempted."""

from __future__ import annotations

import socket
from types import SimpleNamespace
from typing import Any

import pytest

from tools.offline_validate import network_guard


# Intent: all supported DNS lookup paths reject public hostnames before resolution.
# Why: libc lookup APIs can perform DNS without a Python socket.connect event.
@pytest.mark.parametrize(
    "event,args",
    [
        ("socket.getaddrinfo", ("example.invalid", 443)),
        ("socket.gethostbyname", ("example.invalid",)),
        ("socket.gethostbyaddr", ("192.0.2.1",)),
        ("socket.getnameinfo", (("192.0.2.1", 443), 0)),
    ],
)
def test_offline_guard_rejects_dns(event: str, args: tuple[Any, ...]) -> None:
    with pytest.raises(PermissionError):
        network_guard(event, args)


# Intent: wildcard, non-loopback and Unix-service addresses are forbidden.
# Why: a local Unix socket could be a live control service rather than a test peer.
@pytest.mark.parametrize(
    "family,address",
    [
        (socket.AF_INET, ("0.0.0.0", 0)),
        (socket.AF_INET, ("192.0.2.1", 443)),
        (socket.AF_INET6, ("::", 0)),
        (socket.AF_UNIX, "/offline-forbidden-service.sock"),
    ],
)
def test_offline_guard_rejects_service_sockets(family: int, address: Any) -> None:
    with pytest.raises(PermissionError):
        network_guard("socket.connect", (SimpleNamespace(family=family), address))


# Intent: literal loopback peers remain available for real TLS engine tests.
# Why: the guard must permit a useful offline test bench, not silently skip networked tests.
@pytest.mark.parametrize(
    "family,address",
    [
        (socket.AF_INET, ("127.0.0.1", 0)),
        (socket.AF_INET6, ("::1", 0)),
    ],
)
def test_offline_guard_allows_loopback(family: int, address: Any) -> None:
    network_guard("socket.bind", (SimpleNamespace(family=family), address))
