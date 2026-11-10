#!/usr/bin/env python3
"""Persistent owner app/schedule proxy service; no local Start endpoint or automatic resource cleanup."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.trial_proxy import load_core  # noqa: E402

load_core()
from ocpp_hub_core.prototype.managed import ManagedController  # noqa: E402
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings  # noqa: E402
from ocpp_hub_core.prototype.managed_view import health_snapshot  # noqa: E402
from ocpp_hub_core.prototype.policy import DeniedError  # noqa: E402
from ocpp_hub_core.prototype.security import SecurityError  # noqa: E402
from ocpp_hub_core.prototype.trial_config import TrialConfig, private_json  # noqa: E402
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight, save_private  # noqa: E402

from tools.trial_proxy import native_spec  # noqa: E402


# The manager receives only short classified status; no protocol bodies, identities or credential paths are sent.
def notify(value: str) -> None:
    destination = os.environ.get("NOTIFY_SOCKET")
    if not destination:
        return
    address = "\0" + destination[1:] if destination.startswith("@") else destination
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as stream:
            stream.sendto(value.encode(), address)
    except OSError:
        pass  # Running outside a manager must not break the independent charger receiver.


# A responsive event loop stays supervised even during a bounded native read or ordinary vendor outage.
async def watchdog(stopped: asyncio.Event) -> None:
    while not stopped.is_set():
        notify("WATCHDOG=1")
        try:
            await asyncio.wait_for(stopped.wait(), 5)
        except TimeoutError:
            pass


# The extra mode input cannot alias any file the service writes or rotates.
def check_spec_path(config: TrialConfig, path: Path) -> None:
    outputs = (
        config.journal,
        Path(str(config.journal) + ".lock"),
        Path(str(config.journal) + "-wal"),
        Path(str(config.journal) + "-shm"),
        config.journal.with_suffix(".status.json"),
        config.journal.with_suffix(".events.jsonl"),
        config.journal.with_suffix(".events.jsonl.1"),
        config.baseline,
        config.baseline.with_suffix(".original-config.json"),
        config.evidence,
    )
    if path.resolve(strict=False) in {output.resolve(strict=False) for output in outputs}:
        raise SecurityError("managed specification must not alias service outputs")


# Keep bounded durable fault/state transitions beside the journal without storing private native bodies in logs.
def event(path: Path, value: dict[str, Any]) -> None:
    if path.exists() and path.stat().st_size > 2 * 1024 * 1024:
        path.replace(path.with_suffix(path.suffix + ".1"))
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


# Human/monitor checks read private status only: no connection, journal ownership or physical command is created.
def health(path: Path, now: float | None = None) -> dict[str, Any]:
    return health_snapshot(private_json(path), now)


# Start one journal owner and retain the same zero/recovery/event semantics through every supervised replacement.
async def serve(config_path: Path, spec_path: Path, management_path: Path | None = None) -> int:
    prepared = preflight(config_path)
    check_spec_path(prepared.config, spec_path)
    spec = native_spec(spec_path, prepared)
    expires = prepared.tls.certificate_expires_at
    management = None
    if management_path is not None:
        check_spec_path(prepared.config, management_path)
        management = ManagementSettings.load(management_path)
        check_spec_path(prepared.config, management.token_file)
        if management.token_file in (
            prepared.config.native_secret,
            prepared.config.incoming_secret,
            prepared.config.upstream_secret,
        ):
            raise SecurityError("management credential must be independent")
    session = TrialSession(prepared, native_bounded=spec, allow_local_start=False, allow_unplugged_native=True)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    controller: ManagedController | None = None

    # Signal synchronously retires pending positive authority before asynchronous session shutdown starts.
    def request_stop() -> None:
        stopped.set()
        if controller is not None:
            try:
                controller.request_stop()
            except DeniedError, sqlite3.Error, OSError:
                pass  # A completed/frozen cleanup already has no positive managed release authority.

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, request_stop)
    task = asyncio.create_task(watchdog(stopped))
    status_path = prepared.config.journal.with_suffix(".status.json")
    events_path = prepared.config.journal.with_suffix(".events.jsonl")
    started = time.time()
    last_state: tuple[Any, ...] | None = None
    published: dict[str, Any] = {"running": True, "state": "starting"}
    api: ManagementServer | None = None
    try:
        await session.start()
        controller = ManagedController(session)
        if management is not None:
            api = ManagementServer(management, controller, lambda: dict(published))
            await api.start()
        notify("READY=1\nSTATUS=Waiting for verified charger and owner control")
        while not stopped.is_set():
            status = await controller.tick()
            remaining = expires - time.time()
            status.update(pid=os.getpid(), started_at=started, certificate_remaining_seconds=remaining, running=True)
            if remaining <= 1800:
                status.update(state="degraded", fault="listener_certificate_expiring")
            published = status
            save_private(status_path, status)
            classification = status.get("state"), status.get("fault"), status.get("phase")
            if classification != last_state:
                event(events_path, {"at_epoch": time.time(), "state": classification[0], "fault": classification[1]})
                print(json.dumps({"state": classification[0], "fault": classification[1]}), flush=True)
                notify("STATUS=App/schedule control: " + str(classification[0]))
                last_state = classification
            try:
                await asyncio.wait_for(stopped.wait(), 5)
            except TimeoutError:
                pass
    finally:
        request_stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        notify("STOPPING=1\nSTATUS=Retaining charger hold and persistent records")
        if api is not None:
            await api.close()
        try:
            if controller is not None:
                await controller.tests.close()
                if controller.pause_task is not None:
                    await asyncio.gather(controller.pause_task, return_exceptions=True)
            await session.close()
        finally:
            save_private(
                status_path, {"running": False, "state": "stopped", "checked_at": time.time(), "pid": os.getpid()}
            )
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)
    return 0


# Explicit serve is the only networking action; check/health remain safe for deployment and monitoring tools.
def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "serve", "health"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--native-bounded-spec", type=Path)
    parser.add_argument("--management-config", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action == "health":
            config = TrialConfig.load(args.config)
            result = health(config.journal.with_suffix(".status.json"))
            print(json.dumps(result, sort_keys=True))
            return 0 if result["healthy"] else 1
        if args.native_bounded_spec is None:
            raise ValueError("an explicit native bounded specification is required")
        if args.action == "check":
            prepared = preflight(args.config)
            check_spec_path(prepared.config, args.native_bounded_spec)
            native_spec(args.native_bounded_spec, prepared)
            if args.management_config is not None:
                check_spec_path(prepared.config, args.management_config)
                management = ManagementSettings.load(args.management_config)
                check_spec_path(prepared.config, management.token_file)
            remaining = prepared.tls.certificate_expires_at - time.time()
            print(
                json.dumps(
                    {**prepared.summary(), "managed_mode": "app_schedule", "certificate_remaining_seconds": remaining}
                )
            )
            return 0
        return asyncio.run(serve(args.config, args.native_bounded_spec, args.management_config))
    except Exception as error:
        # The executable boundary exposes a classification only, never native responses, credentials or parser text.
        print(json.dumps({"managed_action_failed": type(error).__name__}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
