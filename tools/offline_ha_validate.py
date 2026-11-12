#!/usr/bin/env python3
"""Run actual HA suites against an explicitly provisioned HA runtime with networking limited to loopback."""

from __future__ import annotations

import os
import socket
import sys
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
from offline_validate import network_guard as engine_guard  # noqa: E402

os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True


# HA may use local Unix sockets, while every IP operation remains restricted by the engine's loopback audit.
def network_guard(event, args):
    if (
        event in ("socket.bind", "socket.connect", "socket.sendto", "socket.sendmsg")
        and args[0].family == socket.AF_UNIX
    ):
        return
    engine_guard(event, args)


# HA11 installs its compatibility aliases before pytest imports optional HTTP plugins.
def main() -> int:
    sys.addaudithook(network_guard)
    import homeassistant  # noqa: F401
    import pytest

    args = sys.argv[1:]
    legacy = "--legacy-ha" in args
    if legacy:
        args.remove("--legacy-ha")
        requirements = (
            __import__("importlib.metadata", fromlist=["requires"]).requires("pytest-homeassistant-custom-component")
            or []
        )
        if f"homeassistant=={version('homeassistant')}" not in [r.replace(" ", "") for r in requirements]:
            raise SystemExit("legacy HA tests require a plugin pinned to this exact HA version")
    if not Path(os.environ.get("TMPDIR", "")).is_absolute():
        raise SystemExit("set an absolute task TMPDIR")
    os.chdir(ROOT)
    plugins = ["pytest_asyncio.plugin", "pytest_freezer", "pytest_socket", "pytest_aiohttp.plugin", "pytest_timeout"]
    if legacy:
        plugins.append("pytest_homeassistant_custom_component.plugins")
    options = [item for plugin in plugins for item in ("-p", plugin)]
    options += [
        "-p",
        "no:cacheprovider",
        "--asyncio-mode=auto",
        "--disable-socket",
        "--allow-unix-socket",
        "--allow-hosts=127.0.0.1,::1",
        "-q",
    ]
    print(
        f"Isolated actual HA={version('homeassistant')}; legacy plugin={'enabled' if legacy else 'disabled'}",
        flush=True,
    )
    return pytest.main(options + args)


if __name__ == "__main__":
    raise SystemExit(main())
