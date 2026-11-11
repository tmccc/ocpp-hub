"""Authenticated loopback-only management websocket; no Start, configuration or arbitrary charger RPC endpoint."""

from __future__ import annotations

import hmac
import json
import re
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

from .ics import unique_object
from .managed import ManagedController
from .managed_view import health_snapshot, session_history
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
    """Four bounded local clients share cached status and durable command acceptance, never journal ownership."""

    # A status provider is a private in-process snapshot, rather than a new native observation on every UI refresh.
    def __init__(
        self, settings: ManagementSettings, controller: ManagedController, status: Callable[[], dict[str, Any]]
    ) -> None:
        self.settings, self.controller, self.status = settings, controller, status
        self.server: Server | None = None
        self.clients: set[ServerConnection] = set()
        self.recent: deque[float] = deque()

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
    def dispatch(self, text: str | bytes) -> dict[str, Any]:
        now = time.monotonic()
        while self.recent and self.recent[0] <= now - 1:
            self.recent.popleft()
        if len(self.recent) >= 30:
            raise UnavailableError("management request rate exceeded")
        self.recent.append(now)
        if not isinstance(text, str) or len(text.encode()) > 2048:
            raise SecurityError("management requires a bounded text object")
        try:
            value = json.loads(text, object_pairs_hook=unique_object)
        except ValueError, RecursionError:
            raise SecurityError("management JSON is malformed") from None
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("id"), str)
            or not re.fullmatch(r"[a-f0-9]{32}", value["id"])
        ):
            raise SecurityError("management identifier is invalid")
        command = value.get("command")
        if command in ("status", "history") and set(value) == {"id", "command"}:
            if command == "history":
                result: Any = {"sessions": session_history(self.controller.session.journal)}
            else:
                result = dict(self.status())
                intent = self.controller.requests.intent()
                if (
                    result.get("desired_control", "app") != intent.mode
                    or result.get("control_revision", 0) != intent.revision
                ):
                    result.update(state="pausing", local_hold_verified=False, owner_control_enabled=False)
                result.update(desired_control=intent.mode, control_revision=intent.revision)
                result["api_version"] = 1
                result = health_snapshot(result)
                result["can_pause"] = not self.controller.stopping
                result["can_start_test"] = result["healthy"] and self.controller.tests.available(
                    self.controller.latest_native
                )
                result["can_start"] = result["local_healthy"] and self.controller.tests.available(
                    self.controller.latest_native, normal=True
                )
                result["requested_current_a"] = self.controller.current.selected()
                result["can_set_current"] = result["local_healthy"] and self.controller.current.available(
                    self.controller.latest_native
                )
                result["can_return"] = (
                    intent.mode == "paused"
                    and result["healthy"]
                    and result.get("local_hold_verified") is True
                    and self.controller.requests.pending_return() is None
                    and not self.controller.current.pending()
                    and self.controller.session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[
                        0
                    ]
                    < 4095
                )
                result["sessions"] = session_history(self.controller.session.journal)
            return {"id": value["id"], "ok": True, "result": result}
        if command == "receipt" and set(value) == {"id", "command", "request_id"}:
            identifier = value["request_id"]
            if not isinstance(identifier, str) or not re.fullmatch(r"[a-f0-9]{32}", identifier):
                raise SecurityError("management receipt identifier is invalid")
            return {"id": value["id"], "ok": True, "result": self.controller.requests.receipt(identifier)}
        if command == "temporary_schedule" and set(value) == {"id", "command", "revision", "start_epoch"}:
            result = self.controller.schedule.accept(value["id"], value["revision"], value["start_epoch"])
            return {"id": value["id"], "ok": True, "result": result}
        if command == "set_current" and set(value) == {"id", "command", "revision", "current_a"}:
            result = self.controller.command(value["id"], command, value["revision"], current_a=value["current_a"])
            return {"id": value["id"], "ok": True, "result": result}
        if command in ("pause", "app_control", "start", "start_test", "arm_cloud_test") and set(value) == {
            "id",
            "command",
            "revision",
        }:
            result = self.controller.command(value["id"], command, value["revision"])
            return {"id": value["id"], "ok": True, "result": result}
        raise SecurityError("management command is not supported")

    # The network boundary reports fixed classifications only; peer text and bearer credentials are never logged.
    async def handle(self, connection: ServerConnection) -> None:
        if len(self.clients) >= 4:
            await connection.close(1013, "Busy")
            return
        self.clients.add(connection)
        try:
            async for text in connection:
                try:
                    response = self.dispatch(text)
                except SecurityError, DeniedError:
                    response = {"ok": False, "error": "invalid_request"}
                except RecoveryRequiredError, UnavailableError:
                    response = {"ok": False, "error": "not_ready"}
                await connection.send(json.dumps(response, allow_nan=False))
        except ConnectionClosed:
            pass  # A client disappearing cannot cancel an accepted physical Stop or change requested ownership.
        finally:
            self.clients.discard(connection)

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
