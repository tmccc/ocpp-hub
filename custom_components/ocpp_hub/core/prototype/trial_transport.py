"""Separate fixed-endpoint commissioning transport; the offline adapters remain loopback-only."""

from __future__ import annotations

import hmac
import ipaddress
import ssl
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from http import HTTPStatus

from websockets.asyncio.client import ClientConnection
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.http11 import Request, Response

from .runtime import RuntimeSettings
from .security import TRANSPORT_LOG, FixedVendorConnect, SecurityError, require_verified_tls
from .trial_config import PreparedTLS, TrialConfig


class TrialTransport:
    """Dormant until an explicit trial start; no endpoint discovery or credential inference."""

    # Require already validated TLS and configuration; construction performs no DNS, bind or connection.
    def __init__(self, config: TrialConfig, tls: PreparedTLS, settings: RuntimeSettings | None = None) -> None:
        require_verified_tls(tls.client)
        if tls.server.protocol != ssl.PROTOCOL_TLS_SERVER:
            raise SecurityError("trial requires a listener server TLS context")
        if (config.incoming_auth == "basic") != (tls.incoming is not None) or (config.upstream_auth == "basic") != (
            tls.upstream is not None
        ):
            raise SecurityError("prepared transport credentials must match both explicit authentication modes")
        self.config, self.tls, self.settings = config, tls, settings or RuntimeSettings()

    # Admit one exact source/path/credential before a candidate can displace the current device connection.
    def admit(self, connection: ServerConnection, request: Request) -> Response | None:
        headers = request.headers.get_all("Authorization")
        remote = connection.remote_address
        peer = remote[0] if isinstance(remote, tuple) and remote else ""
        try:
            source_matches = ipaddress.ip_address(peer) == ipaddress.ip_address(self.config.charger_ip)
        except ValueError:
            source_matches = False
        credential_matches = (
            not headers
            if self.tls.incoming is None
            else len(headers) == 1 and hmac.compare_digest(headers[0].encode(), self.tls.incoming.encode())
        )
        if not source_matches or request.path != self.config.charger_path or not credential_matches:
            return connection.respond(HTTPStatus.UNAUTHORIZED, "trial admission refused\n")
        return None

    # Bind only the explicit local address, using TLS and bounded protocol-library buffers/timeouts.
    def listen(self, handler: Callable[[ServerConnection], Awaitable[None]]) -> Awaitable[Server]:
        s = self.settings
        listener: Awaitable[Server] = serve(
            handler,
            self.config.listener_host,
            self.config.listener_port,
            ssl=self.tls.server,
            subprotocols=["ocpp1.6"],
            process_request=self.admit,
            logger=TRANSPORT_LOG,
            max_size=131072,
            max_queue=16,
            compression=None,
            open_timeout=10,
            ping_interval=s.ping_interval,
            ping_timeout=s.ping_timeout,
            close_timeout=s.close_timeout,
        )
        return listener

    # Use the independently configured upstream Basic credential at this exact URL, without proxies or redirects.
    def connect(self) -> AbstractAsyncContextManager[ClientConnection]:
        s = self.settings
        return FixedVendorConnect(
            self.config.vendor_url,
            ssl=self.tls.client,
            subprotocols=["ocpp1.6"],
            additional_headers=[] if self.tls.upstream is None else [("Authorization", self.tls.upstream)],
            proxy=None,
            logger=TRANSPORT_LOG,
            max_size=131072,
            max_queue=16,
            compression=None,
            open_timeout=10,
            ping_interval=s.ping_interval,
            ping_timeout=s.ping_timeout,
            close_timeout=s.close_timeout,
        )
