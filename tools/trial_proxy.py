#!/usr/bin/env python3
"""Check a commissioning package offline; run/capture require explicit subcommands and are never deployed here."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import Iterator
from contextlib import closing, contextmanager
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
from ocpp_hub_core.prototype.native_bounded import (  # noqa: E402
    NativeBoundedSpec,
    capture_native_bounded_baseline,
)
from ocpp_hub_core.prototype.security import SecurityError  # noqa: E402
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint  # noqa: E402
from ocpp_hub_core.prototype.trial_config import TrialConfig, private_json  # noqa: E402
from ocpp_hub_core.prototype.trial_evidence import NativeObserver  # noqa: E402
from ocpp_hub_core.prototype.trial_session import (  # noqa: E402
    PreparedNative,
    TrialSession,
    open_native_journal,
    preflight,
    save_private,
)


# Loading a native strategy is dormant and must match both the original baseline and the actual listener settings.
def native_spec(path: Path, prepared: PreparedNative) -> NativeBoundedSpec:
    prepared.config.check_paths(path)
    spec = NativeBoundedSpec.parse(private_json(path))
    config, route = prepared.config, spec.trial_route
    if (
        spec.hardware_identity != config.hardware_identity
        or spec.original_configuration["Id"] != config.identity
        or fingerprint(spec.original_configuration) != prepared.baseline.configuration_digest
        or route["sDomain"] != config.listener_name
        or route["sPort"] != str(config.listener_port)
        or route["sURL"].rstrip("/") + "/" + config.identity != config.charger_path
    ):
        raise SecurityError("native bounded strategy does not match the configured commissioning listener")
    return spec


# A stopped inspection runs normal recovery checks on a private copy, with the original owner lock held.
@contextmanager
def operator_journal(config: TrialConfig, spec: NativeBoundedSpec, *, inspect: bool) -> Iterator[Journal]:
    if not inspect:
        with closing(open_native_journal(config.journal, config.identity, spec)) as journal:
            yield journal
        return

    import fcntl

    original = config.journal
    with Path(str(original) + ".lock").open("rb") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RecoveryRequiredError("journal already has an active owner") from None
        with tempfile.TemporaryDirectory(prefix="ics-journal-inspect-") as temporary:
            copy = Path(temporary) / original.name
            shutil.copyfile(original, copy)
            # Copy committed WAL frames without opening SQLite on the original or rebuilding its -shm file.
            wal = Path(str(original) + "-wal")
            if wal.exists():
                shutil.copyfile(wal, Path(str(copy) + "-wal"))
            with closing(open_native_journal(copy, config.identity, spec)) as journal:
                yield journal


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
        discarding = False
        while not stopped.is_set():
            try:
                raw = await reader.readuntil(b"\n")
            except asyncio.LimitOverrunError as error:
                await reader.readexactly(error.consumed)
                if not discarding:
                    emit({"command_failed": "CommandTooLong"})
                discarding = True
                continue
            except asyncio.IncompleteReadError as error:
                raw = error.partial
            if not raw:
                stopped.set()
                return
            if discarding:
                discarding = False
                continue
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
async def run(
    config_path: Path,
    duration: int,
    terminal: bool,
    native_bounded_config: Path | None = None,
) -> int:
    if native_bounded_config is None:
        raise SecurityError("commissioning run requires --native-bounded-spec")
    prepared = preflight(config_path)
    bounded = native_spec(native_bounded_config, prepared)
    session = TrialSession(prepared, native_bounded=bounded)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    stop_signals = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
    for signum in stop_signals:
        loop.add_signal_handler(signum, stopped.set)
    tasks: list[asyncio.Task[None]] = []
    cleanup_completed = False
    try:
        async with asyncio.timeout(duration):
            await session.start()
            if session.started:
                emit({"trial_started": True, "maximum_duration_seconds": duration, "ceiling_a": 6})
            elif terminal and session.journal.phase.read().value == "rollback":
                emit({"trial_started": False, "cleanup_available": True})
            else:
                raise RecoveryRequiredError("commissioning requires an open charger listener")
            if terminal:
                tasks.append(asyncio.create_task(interactive(session, stopped)))
            while not stopped.is_set():
                status = await session.command({"command": "status"})
                save_private(prepared.config.journal.with_suffix(".status.json"), status)
                try:
                    await asyncio.wait_for(stopped.wait(), 1)
                except TimeoutError:
                    pass
            if not session.started:
                cleanup_completed = session.journal.phase.read().value == "backend_restore_pending"
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
                for signum in stop_signals:
                    loop.remove_signal_handler(signum)
    if session.final_status is None or session.final_status["shutdown_uncertain"]:
        return 2
    return 0 if session.started or cleanup_completed else 2


# A separate explicit action captures a fresh read-only baseline and original configuration backup.
def capture(path: Path) -> int:
    config = TrialConfig.load(path)
    observer = NativeObserver(config)
    native = asyncio.run(observer.observe_handover())
    baseline = capture_native_bounded_baseline(native, config.identity, config.hardware_identity)
    original = native.configuration
    save_private(config.baseline, baseline.value())
    save_private(config.baseline.with_suffix(".original-config.json"), original)
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
    parser.add_argument("action", nargs="?", choices=("check", "capture-bounded-baseline", "run"), default="check")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=900)
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--native-bounded-spec", type=Path)
    args = parser.parse_args(argv)
    if not 60 <= args.duration <= 1800:
        parser.error("duration must be 60 to 1800 seconds")
    if args.action == "run" and args.native_bounded_spec is None:
        parser.error("run requires --native-bounded-spec")
    try:
        if args.action == "check":
            prepared = preflight(args.config)
            if args.native_bounded_spec is not None:
                native_spec(args.native_bounded_spec, prepared)
            emit({**prepared.summary(), "native_bounded_strategy_configured": args.native_bounded_spec is not None})
            return 0
        if args.action == "capture-bounded-baseline":
            return capture(args.config)
        return asyncio.run(run(args.config, args.duration, args.interactive, args.native_bounded_spec))
    except Exception as error:
        # This outer boundary must never turn a parser/TLS/network error into a credential-bearing traceback.
        emit({"trial_action_failed": type(error).__name__, "live_readiness_proven": False})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
