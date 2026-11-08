#!/usr/bin/env python3
"""Run the prototype or engine tests with Python network operations limited to loopback.

The audit hook is a Python-level guard, not an operating-system network namespace.
Only reviewed local test commands are executed; no Home Assistant or charger setup is loaded.
"""

from __future__ import annotations

import ipaddress
import os
import socket
import sys
from pathlib import Path
from typing import Any


# Check a literal address without resolving hostnames or allowing wildcard interfaces.
def allowed_address(address: Any) -> bool:
    if not isinstance(address, tuple) or not address or not isinstance(address[0], str):
        return False
    try:
        return ipaddress.ip_address(address[0]).is_loopback
    except ValueError:
        return False


# Deny non-loopback socket use before pytest or plugins are imported.
def network_guard(event: str, args: tuple[Any, ...]) -> None:
    if event in ("socket.getaddrinfo", "socket.gethostbyname"):
        if not allowed_address((args[0], 0)):
            raise PermissionError("offline tests permit literal loopback addresses only")
    if event in ("socket.gethostbyaddr", "socket.getnameinfo"):
        raise PermissionError("offline tests do not perform reverse DNS lookups")
    if event in ("socket.bind", "socket.connect", "socket.sendto", "socket.sendmsg"):
        sock = args[0]
        if sock.family not in (socket.AF_INET, socket.AF_INET6) or not allowed_address(args[1]):
            raise PermissionError("offline tests permit loopback sockets only")


# Run tests with an explicit plugin list and without writing temporary data outside the task scratch directory.
def main() -> int:
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    sys.path.insert(0, str(root))
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    sys.dont_write_bytecode = True
    scratch = os.environ.get("TMPDIR")
    if scratch is None or not Path(scratch).is_absolute():
        raise SystemExit("set TMPDIR to the task's absolute scratch directory")
    sys.addaudithook(network_guard)
    import pytest

    args = sys.argv[1:] or ["tests/test_prototype.py", "tests/test_prototype_tls.py"]
    if not any(arg.startswith("--basetemp") for arg in args):
        args += ["--basetemp=" + str(Path(scratch) / "ocpp-prototype-pytest")]
    return pytest.main(
        [
            "-p",
            "pytest_asyncio.plugin",
            "-p",
            "no:cacheprovider",
            "-q",
            "-o",
            "asyncio_default_fixture_loop_scope=function",
            *args,
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
