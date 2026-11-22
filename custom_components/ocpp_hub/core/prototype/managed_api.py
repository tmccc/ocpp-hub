"""Authenticated loopback management for guarded Start, Pause, Return and current selection."""

from __future__ import annotations

import asyncio
import hmac
import json
import re
import sqlite3
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from .command_processor import CommandPriority
from .ics import unique_object
from .managed import ManagedController
from .managed_view import refresh_for_read, session_history
from .policy import DeniedError
from .rpc import UnavailableError
from .security import TRANSPORT_LOG, SecurityError
from .store import RecoveryRequiredError
from .trial_config import private_json


@dataclass(frozen=True, repr=False)
class ManagementSettings:
    """The only configurable transport is a port on literal IPv4 loopback, with a separate private bearer file."""

    port: int
    token_file: Path
    token: str

    # Dormant parsing cannot broaden the listener or reuse an unrelated credential format.
    @classmethod
    def load(cls, path: Path) -> ManagementSettings:
        value = private_json(path)
        if set(value) != {"host", "port", "token_file"} or value["host"] != "127.0.0.1":
            raise SecurityError("management requires an explicit loopback-only listener")
        port = value["port"]
        if type(port) is not int or not 1024 <= port <= 65535 or not isinstance(value["token_file"], str):
            raise SecurityError("management port or credential path is invalid")
        token_file = Path(value["token_file"])
        secret = private_json(token_file)
        if (
            set(secret) != {"token"}
            or not isinstance(secret["token"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", secret["token"])
        ):
            raise SecurityError("management requires a private independent random bearer credential")
        return cls(port, token_file, secret["token"])


class ManagementServer:
    """Four local clients share ordinary work, with one short-lived reservation for Pause."""

    # A status provider is a private in-process snapshot, rather than a new native observation on every UI refresh.
    def __init__(
        self, settings: ManagementSettings, controller: ManagedController, status: Callable[[], dict[str, Any]]
    ) -> None:
        self.settings, self.controller, self.status = settings, controller, status
        self.server: Server | None = None
        self.clients: set[ServerConnection] = set()
        self.pause_client: ServerConnection | None = None
        self.idle: dict[ServerConnection, None] = {}
        self.recent: deque[float] = deque()
        self.recent_pauses: deque[float] = deque()

    # Reject browser origins, duplicate credentials, wrong paths/hosts and actual non-loopback peers before upgrade.
    def authorize(self, connection: ServerConnection, request: Request) -> Response | None:
        tokens = request.headers.get_all("Authorization")
        hosts = request.headers.get_all("Host")
        if (
            connection.remote_address is None
            or connection.remote_address[0] != "127.0.0.1"
            or request.path != "/management"
            or request.headers.get_all("Origin")
            or hosts != [f"127.0.0.1:{self.settings.port}"]
            or len(tokens) != 1
            or not hmac.compare_digest(tokens[0], "Bearer " + self.settings.token)
        ):
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        return None

    # Validation and rate bounds run before any intent, receipt or physical control can change.
    async def dispatch(self, text: str | bytes, *, pause_only: bool = False) -> dict[str, Any]:
        if not isinstance(text, str) or len(text.encode()) > 2048:
            self.limit_requests(self.recent)
            raise SecurityError("management requires a bounded text object")
        try:
            value = json.loads(text, object_pairs_hook=unique_object)
        except ValueError, RecursionError:
            self.limit_requests(self.recent)
            raise SecurityError("management JSON is malformed") from None
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("id"), str)
            or not re.fullmatch(r"[a-f0-9]{32}", value["id"])
        ):
            self.limit_requests(self.recent)
            raise SecurityError("management identifier is invalid")
        command = value.get("command")
        pause = (
            command == "pause"
            and set(value) == {"id", "command", "revision"}
            and type(value["revision"]) is int
            and 0 <= value["revision"] < 2**53
        )
        if pause_only and not pause:
            raise UnavailableError("management reservation accepts Pause only")
        self.limit_requests(self.recent_pauses if pause else self.recent)
        if command in ("status", "history") and set(value) == {"id", "command"}:
            return self.read_only(value["id"], command)
        if command == "receipt" and set(value) == {"id", "command", "request_id"}:
            identifier = value["request_id"]
            if not isinstance(identifier, str) or not re.fullmatch(r"[a-f0-9]{32}", identifier):
                raise SecurityError("management receipt identifier is invalid")
            return self.read_only(value["id"], command, request_id=identifier)
        fields = {"id", "command", "revision"}
        if command == "set_current":
            fields.add("current_a")
        elif command == "stop_session":
            fields.add("target")
            if not isinstance(value.get("target"), str) or not re.fullmatch(r"[a-f0-9]{64}", value["target"]):
                raise SecurityError("management Stop target is invalid")
        elif command not in ("pause", "app_control", "start", "start_test", "arm_cloud_test"):
            raise SecurityError("management command is not supported")
        if set(value) != fields:
            raise SecurityError("management command is not supported")
        try:
            result = await self.controller.commands.run(
                lambda: self.controller.command(
                    value["id"],
                    command,
                    value["revision"],
                    current_a=value.get("current_a"),
                    target=value.get("target"),
                ),
                priority=CommandPriority.RESTRICT if command in ("pause", "stop_session") else CommandPriority.NORMAL,
            )
        except sqlite3.Error:
            # An absent receipt outside a transaction proves this request was not admitted.
            if not self.controller.session.journal.db.in_transaction:
                try:
                    if self.controller.requests.receipt(value["id"])["state"] == "not_found":
                        return {"id": value["id"], "ok": False, "error": "not_ready"}
                except sqlite3.Error:
                    pass
            # A reply read can fail after admission commits. The client may look up the receipt.
            return {"id": value["id"], "ok": False, "error": "outcome_unknown"}
        binding = {"revision": value["revision"]}
        if command == "set_current":
            binding["current_a"] = value["current_a"]
        if command == "stop_session":
            binding["target"] = value["target"]
        return {"id": value["id"], "ok": True, "result": {**result, **binding}}

    # Reserve Pause capacity independently of dashboard requests and other controls.
    @staticmethod
    def limit_requests(recent: deque[float]) -> None:
        now = time.monotonic()
        while recent and recent[0] <= now - 1:
            recent.popleft()
        if len(recent) >= 30:
            raise UnavailableError("management request rate exceeded")
        recent.append(now)

    # A failed journal read returns no guessed state; sent controls retain their separate unknown-outcome handling.
    def read_only(self, identifier: str, command: str, *, request_id: str | None = None) -> dict[str, Any]:
        try:
            if command == "receipt":
                if request_id is None:
                    raise SecurityError("management receipt identifier is missing")
                result: Any = self.controller.requests.receipt(request_id, include_binding=True)
            elif command == "history":
                result = {"sessions": session_history(self.controller.session.journal)}
            elif command == "status":
                result = refresh_for_read(self.controller, self.status())
            else:
                raise SecurityError("management read is not supported")
        except sqlite3.Error:
            raise UnavailableError("management journal read is temporarily unavailable") from None
        return {"id": identifier, "ok": True, "result": result}

    # The network boundary reports fixed classifications only; peer text and bearer credentials are never logged.
    async def handle(self, connection: ServerConnection) -> None:
        displaced = None
        reserved = False
        if len(self.clients) >= 4:
            displaced = next(iter(self.idle), None)
            if displaced is None:
                if self.pause_client is not None:
                    await connection.close(1013, "Busy")
                    return
                self.pause_client = connection
                reserved = True
            else:
                self.idle.pop(displaced)
                self.clients.remove(displaced)
        self.clients.add(connection)
        try:
            if displaced is not None:
                await displaced.close(1013, "Idle connection replaced")
            while connection in self.clients:
                if not reserved:
                    self.idle[connection] = None
                try:
                    async with asyncio.timeout(2 if reserved else 30):
                        text = await connection.recv()
                except TimeoutError:
                    await connection.close(1000, "Idle")
                    return
                finally:
                    self.idle.pop(connection, None)
                if connection not in self.clients:
                    return  # A displaced reader cannot admit work after releasing its slot.
                try:
                    response = await self.dispatch(text, pause_only=reserved)
                except SecurityError, DeniedError:
                    response = {"ok": False, "error": "invalid_request"}
                except RecoveryRequiredError, UnavailableError:
                    response = {"ok": False, "error": "not_ready"}
                async with asyncio.timeout(5):
                    await connection.send(json.dumps(response, allow_nan=False))
                if reserved:
                    return
        except ConnectionClosed, TimeoutError:
            pass  # A client disappearing cannot cancel an accepted physical Stop or change requested ownership.
        finally:
            self.clients.discard(connection)
            if self.pause_client is connection:
                self.pause_client = None

    # Management owns a separate loopback socket and cannot replace or retire either OCPP connection.
    async def start(self) -> None:
        self.server = await serve(
            self.handle,
            "127.0.0.1",
            self.settings.port,
            process_request=self.authorize,
            subprotocols=["ics-proxy.v1"],
            compression=None,
            max_size=2048,
            max_queue=4,
            open_timeout=5,
            close_timeout=2,
            ping_interval=20,
            ping_timeout=20,
            logger=TRANSPORT_LOG,
        )

    # Closing the management listener never changes intent, sends Start or implicitly returns app control.
    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
