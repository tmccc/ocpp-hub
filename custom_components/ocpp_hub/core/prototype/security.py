"""Strict, authenticated TLS adapters restricted to the offline loopback test bench."""

from __future__ import annotations

import base64
import binascii
import hmac
import ipaddress
import logging
import re
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from http import HTTPStatus
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response

from ..frames import join_websocket_url


class SecurityError(ValueError):
    """A transport would weaken identity, authentication or certificate checks."""


# The prototype never lets a synthetic credential reach a LAN or public endpoint.
def loopback(host: str | None) -> bool:
    try:
        return host is not None and ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


# Client TLS contexts must verify both trust and hostname, even with a test-only CA.
def require_verified_tls(context: ssl.SSLContext) -> None:
    if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
        raise SecurityError("vendor TLS requires certificate and hostname verification")


class _NoTransportBodies(logging.Filter):
    """Do not let dependency debug logs serialize credentials or proprietary messages."""

    # Protocol-library records may contain whole headers or payloads, so none are emitted.
    def filter(self, record: logging.LogRecord) -> bool:
        return False


TRANSPORT_LOG = logging.getLogger(__name__ + ".transport")
TRANSPORT_LOG.addFilter(_NoTransportBodies())


@dataclass(frozen=True)
class Admission:
    """Only synthetic credentials, carried in memory and excluded from representations."""

    identity: str
    authorization: str = field(repr=False)

    # Reject an empty or malformed configured credential before any listener exists.
    def __post_init__(self) -> None:
        if not isinstance(self.identity, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.identity) is None:
            raise SecurityError("invalid configured charger identity")
        if (
            not isinstance(self.authorization, str)
            or not self.authorization.startswith("Basic ")
            or len(self.authorization) > 1024
        ):
            raise SecurityError("a bounded Basic credential is required")
        try:
            decoded = base64.b64decode(self.authorization[6:], validate=True).decode("utf-8")
        except ValueError, binascii.Error:
            raise SecurityError("invalid configured Basic credential") from None
        user, separator, password = decoded.partition(":")
        if user != self.identity or not separator or not password or any(ord(char) < 32 for char in decoded):
            raise SecurityError("credential must match identity and contain a nonempty password")

    # Authenticate before the server handler can attach or replace any charger session.
    def process(self, connection: ServerConnection, request: Request) -> Response | None:
        values = request.headers.get_all("Authorization")
        identity = urlsplit(request.path).path.rstrip("/").rsplit("/", 1)[-1]
        allowed = (
            len(values) == 1
            and identity == self.identity
            and hmac.compare_digest(values[0].encode(), self.authorization.encode())
        )
        if not allowed:
            return connection.respond(HTTPStatus.UNAUTHORIZED, "authentication required\n")
        return None


class FixedVendorConnect(connect):  # type: ignore[misc]
    """A credential-bearing vendor connection never follows an HTTP redirect."""

    # Return the original error, never a replacement URI that could receive authentication.
    def process_redirect(self, exc: Exception) -> Exception | str:
        return exc


# Construct a dormant TLS listener; only the test bench explicitly starts it on loopback.
def offline_server(
    handler: Callable[[ServerConnection], Awaitable[None]],
    admission: Admission,
    context: ssl.SSLContext,
    host: str = "127.0.0.1",
    port: int = 0,
) -> serve:
    if not loopback(host) or context.protocol != ssl.PROTOCOL_TLS_SERVER:
        raise SecurityError("offline server requires loopback and a server TLS context")
    return serve(
        handler,
        host,
        port,
        ssl=context,
        subprotocols=["ocpp1.6"],
        process_request=admission.process,
        logger=TRANSPORT_LOG,
        max_size=131072,
    )


# Forward the admitted header only to one fixed, verified loopback vendor endpoint.
def offline_vendor(base_url: str, admission: Admission, context: ssl.SSLContext) -> FixedVendorConnect:
    parts = urlsplit(base_url)
    if (
        parts.scheme != "wss"
        or not loopback(parts.hostname)
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise SecurityError("offline vendor must be a fixed authenticated loopback WSS endpoint")
    require_verified_tls(context)
    return FixedVendorConnect(
        join_websocket_url(base_url, admission.identity),
        ssl=context,
        subprotocols=["ocpp1.6"],
        additional_headers=[("Authorization", admission.authorization)],
        logger=TRANSPORT_LOG,
        proxy=None,
        open_timeout=1,
    )
