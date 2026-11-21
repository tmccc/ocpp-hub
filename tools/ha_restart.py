"""Restart a selected Home Assistant instance and verify its new Core lifetime."""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


class RestartError(Exception):
    """The deployment helper cannot prove the requested restart."""


class ResponseError(RestartError):
    """Home Assistant explicitly returned an unsuccessful HTTP status."""


@dataclass(frozen=True)
class HomeAssistantAPI:
    connect: Callable[[], http.client.HTTPConnection]
    token: str = field(repr=False)
    prefix: str = "/api"

    # Keep each request on its checked connection and never follow a redirect.
    def request(self, path: str, *, post: bool = False) -> Any:
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        with closing(self.connect()) as connection:
            connection.request("POST" if post else "GET", self.prefix + path, b"{}" if post else None, headers)
            with connection.getresponse() as response:
                if not 200 <= response.status < 300:
                    raise ResponseError(f"Home Assistant rejected {path} (HTTP {response.status}).")
                return json.loads(response.read() or "{}")


# Configuration and health must come from the already selected API.
def require_running(api: HomeAssistantAPI) -> dict[str, Any]:
    config = api.request("/config")
    if not isinstance(config, dict) or config.get("state") != "RUNNING":
        raise RestartError("The selected Home Assistant API is not running.")
    return config


# Check that Core accepted a valid configuration, then prove a new lifetime is serving requests.
def restart(api: HomeAssistantAPI, before: object, lifetime: Callable[[], object], *, timeout: float = 300) -> None:
    check = api.request("/config/core/check_config", post=True)
    if not isinstance(check, dict) or check.get("result") != "valid":
        raise RestartError("The Home Assistant configuration check failed.")
    if lifetime() != before:
        raise RestartError("Home Assistant changed before the requested restart.")
    try:
        api.request("/services/homeassistant/restart", post=True)
    except OSError, http.client.HTTPException:
        pass  # Closing transport may race a successful request. Only lifetime evidence resolves it.

    deadline = time.monotonic() + timeout
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            if lifetime() != before:
                return
        except OSError, http.client.HTTPException, RestartError, ValueError:
            pass
        time.sleep(min(2, remaining))
    raise RestartError("The Home Assistant restart did not produce a verified new Core lifetime.")


# The optional SSH restart uses the installation's Home Assistant start-time sensor as its lifetime evidence.
def uptime(api: HomeAssistantAPI, entity: str, config_dir: str) -> datetime:
    if require_running(api).get("config_dir") != config_dir:
        raise RestartError("The Home Assistant API is using a different configuration directory.")
    state = api.request(f"/states/{entity}")
    if not isinstance(state, dict) or not isinstance(state.get("state"), str):
        raise RestartError("The Home Assistant uptime sensor is unavailable.")
    attributes = state.get("attributes")
    if not isinstance(attributes, dict) or attributes.get("device_class") not in ("uptime", "timestamp"):
        raise RestartError("The Home Assistant uptime sensor must report the Core start timestamp.")
    try:
        started = datetime.fromisoformat(state["state"])
    except ValueError:
        raise RestartError("The Home Assistant uptime sensor must report the Core start timestamp.") from None
    if started.tzinfo is None:
        raise RestartError("The Home Assistant uptime sensor must report a timezone-aware timestamp.")
    return started


# Locate the unique Core process inside the selected container and distinguish reuse of its PID.
def core_lifetime() -> tuple[int, str]:
    matches = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
            if (b"-m", b"homeassistant") in zip(args, args[1:]):
                ticks = (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]
                matches.append((int(proc.name), ticks))
        except FileNotFoundError, ProcessLookupError:
            continue
    if len(matches) != 1:
        raise RestartError("The selected container has no unique Home Assistant Core process.")
    return matches[0]


# /proc/net/tcp represents IPv4 addresses as native-endian hexadecimal words.
def tcp_address(endpoint: tuple[str, int], *, mapped: bool = False) -> str:
    host, port = endpoint
    packed = socket.inet_pton(socket.AF_INET6, f"::ffff:{host}") if mapped else socket.inet_aton(host)
    words = "".join(f"{int.from_bytes(packed[i : i + 4], sys.byteorder):08X}" for i in range(0, len(packed), 4))
    return f"{words}:{port:04X}"


# Identify the exact accepted TCP connection in the selected process's descriptor table.
def owns_connection(pid: int, client: socket.socket) -> bool:
    for table, mapped in (("tcp", False), ("tcp6", True)):
        local = tcp_address(client.getpeername(), mapped=mapped)
        remote = tcp_address(client.getsockname(), mapped=mapped)
        try:
            rows = (Path("/proc/net") / table).read_text().splitlines()[1:]
        except FileNotFoundError:
            continue  # IPv6 can be disabled.
        for line in rows:
            fields = line.split()
            if fields[1:4] == [local, remote, "01"] and fields[9] != "0":
                inode = f"socket:[{fields[9]}]"
                for fd in (Path("/proc") / str(pid) / "fd").iterdir():
                    try:
                        if os.readlink(fd) == inode:
                            return True
                    except FileNotFoundError:
                        continue
    return False


# Connect first, then send credentials only after proving that Core owns this same socket.
def connect_to_core(port: int, expected: tuple[int, str]) -> http.client.HTTPConnection:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.connect()
        client = connection.sock
        assert client is not None
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if owns_connection(expected[0], client) and core_lifetime() == expected:
                return connection
            time.sleep(0.05)
        raise RestartError("The local API socket does not belong to Home Assistant Core in the selected container.")
    except OSError, RestartError:
        connection.close()
        raise


# Authenticate to the API bound to this exact lifetime and verify Core uses the deployment directory.
def container_lifetime(port: int, token: str) -> tuple[int, str]:
    current = core_lifetime()
    api = HomeAssistantAPI(lambda: connect_to_core(port, current), token)
    if require_running(api).get("config_dir") != "/config":
        raise RestartError("Home Assistant Core is not using the selected container's /config directory.")
    return current


# Bind the install and restart to one original Core lifetime.
def prepare_container_restart(port: int, token: str) -> Callable[[], None]:
    try:
        before = container_lifetime(port, token)
    except RestartError as error:
        raise SystemExit(str(error)) from None
    except OSError, http.client.HTTPException, ValueError:
        raise SystemExit("The Home Assistant container's local API could not be verified.") from None

    # The installer keeps its lock until the same checked build has had time to load.
    def activate() -> None:
        api = HomeAssistantAPI(lambda: connect_to_core(port, before), token)
        try:
            restart(api, before, lambda: container_lifetime(port, token))
        except RestartError as error:
            raise SystemExit(str(error)) from None
        except OSError, http.client.HTTPException, ValueError:
            raise SystemExit("The Home Assistant container's local API could not be verified.") from None

    return activate


# Keep the standalone process-lifetime probe and restart interface for existing callers.
def container_command() -> None:
    port = int(sys.argv[1])
    token = sys.stdin.read()
    try:
        before = container_lifetime(port, token)
        if len(sys.argv) == 2:
            print(json.dumps(before))
            return
        if json.loads(sys.argv[2]) != list(before):
            raise RestartError("Home Assistant changed after the deployment preflight.")
        api = HomeAssistantAPI(lambda: connect_to_core(port, before), token)
        restart(api, before, lambda: container_lifetime(port, token))
    except RestartError as error:
        raise SystemExit(str(error)) from None
    except OSError, http.client.HTTPException, ValueError:
        raise SystemExit("The Home Assistant container's local API could not be verified.") from None


if __name__ == "__main__":
    container_command()
