#!/usr/bin/env python3
"""Check a commissioning package offline; run/capture require explicit subcommands and are never deployed here."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


# Load the HA-free engine by path, without running the live integration entry point.
def load_core() -> None:
    name = "ocpp_hub_core"
    if name in sys.modules:
        return
    directory = ROOT / "custom_components/ocpp_hub/core"
    spec = importlib.util.spec_from_file_location(
        name, directory / "__init__.py", submodule_search_locations=[str(directory)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)


load_core()
from ocpp_hub_core.prototype.trial_config import TrialConfig  # noqa: E402
from ocpp_hub_core.prototype.trial_evidence import NativeObserver  # noqa: E402
from ocpp_hub_core.prototype.trial_resources import Baseline  # noqa: E402
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight, save_private  # noqa: E402


# Write only safe classified JSON; neither exception text nor configuration objects are printed.
def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


# Read explicit terminal commands without blocking the websocket event loop or opening a control network port.
async def interactive(session: Any, stopped: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=4096)
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    try:
        while not stopped.is_set():
            raw = await reader.readline()
            if not raw:
                return
            try:
                value = json.loads(raw)
                if value == {"command": "exit"}:
                    stopped.set()
                    return
                emit(await session.command(value))
            except Exception as error:
                # Operator input is untrusted and may contain secrets; keep the command loop alive without quoting it.
                emit({"command_failed": type(error).__name__})
    finally:
        transport.close()


# Bound a supervised run; shutdown keeps its zero hold and history unless rollback has independently completed.
async def run(config_path: Path, duration: int, terminal: bool) -> int:
    prepared = preflight(config_path)
    session = TrialSession(prepared)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stopped.set)
    tasks: list[asyncio.Task[None]] = []
    try:
        async with asyncio.timeout(duration):
            await session.start()
            emit({"trial_started": True, "maximum_duration_seconds": duration, "ceiling_a": 6})
            if terminal:
                tasks.append(asyncio.create_task(interactive(session, stopped)))
            while not stopped.is_set():
                status = await session.command({"command": "status"})
                save_private(prepared.config.journal.with_suffix(".status.json"), status)
                try:
                    await asyncio.wait_for(stopped.wait(), 1)
                except TimeoutError:
                    pass
    except TimeoutError:
        emit({"trial_duration_expired": True, "automatic_backend_restore": False})
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await session.close()
        finally:
            try:
                if session.final_status is not None:
                    save_private(prepared.config.journal.with_suffix(".status.json"), session.final_status)
                    emit({"trial_stopped": True, "shutdown_uncertain": session.final_status["shutdown_uncertain"]})
            finally:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    loop.remove_signal_handler(signum)
    return 2 if session.final_status is None or session.final_status["shutdown_uncertain"] else 0


# A separate explicit action captures a fresh read-only baseline and original configuration backup.
def capture(path: Path) -> int:
    config = TrialConfig.load(path)
    observer = NativeObserver(config)
    evidence = observer.snapshot()
    baseline = Baseline.capture(config.identity, evidence)
    save_private(config.baseline, baseline.value())
    save_private(config.baseline.with_suffix(".original-config.json"), observer.configuration_snapshot)
    emit(
        {
            "native_baseline_captured": True,
            "original_resources": len(baseline.resources),
            "charger_settings_changed": False,
        }
    )
    return 0


# Checking is the default; neither command-line passwords nor automatic service installation are supported.
def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", nargs="?", choices=("check", "capture-baseline", "run"), default="check")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=900)
    parser.add_argument("--interactive", action="store_true")
    args = parser.parse_args(argv)
    if not 60 <= args.duration <= 1800:
        parser.error("duration must be 60 to 1800 seconds")
    try:
        if args.action == "check":
            emit(preflight(args.config).summary())
            return 0
        if args.action == "capture-baseline":
            return capture(args.config)
        return asyncio.run(run(args.config, args.duration, args.interactive))
    except Exception as error:
        # This outer boundary must never turn a parser/TLS/network error into a credential-bearing traceback.
        emit({"trial_action_failed": type(error).__name__, "live_readiness_proven": False})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
