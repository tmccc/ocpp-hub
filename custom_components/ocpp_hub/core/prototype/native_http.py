"""Bounded asynchronous HTTP for the existing iCS read-only LAN API; no proxies, redirects or worker threads."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from .ics import unique_object
from .security import SecurityError
from .store import encoded

MAXIMUM = 2 * 1024 * 1024


class NativeTransportUnavailableError(SecurityError):
    """An absent or undecodable native response provides no new validated installation evidence."""


# A stream ending partway through any framing line supplies no complete HTTP response.
async def framing_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line.endswith(b"\n"):
        raise asyncio.IncompleteReadError(line, None)
    return line


# Keep the framing type so a truncated connection-close body can be distinguished from a complete invalid document.
async def framed_body(reader: asyncio.StreamReader) -> tuple[bytes, bool]:
    status = await framing_line(reader)
    fields = status.split()
    if len(status) > 8192 or len(fields) < 2 or fields[0] not in (b"HTTP/1.0", b"HTTP/1.1"):
        raise SecurityError("native reader received an invalid HTTP status line")
    code = fields[1]
    if code in (b"408", b"429") or len(code) == 3 and code.startswith(b"5") and code.isdigit():
        raise ConnectionError("native HTTP temporarily unavailable")
    if code != b"200":
        raise SecurityError("native reader received a non-success HTTP response")
    headers: dict[bytes, bytes] = {}
    total = 0
    for _ in range(64):
        line = await framing_line(reader)
        total += len(line)
        if total > 65536:
            raise ValueError("native headers are incomplete or oversized")
        if line == b"\r\n":
            break
        key, value = line.split(b":", 1)
        key, value = key.strip().lower(), value.strip().lower()
        if key in headers and key in (b"content-length", b"transfer-encoding"):
            raise ValueError("duplicate native framing header")
        headers[key] = value
    else:
        raise ValueError("too many native headers")
    transfer = headers.get(b"transfer-encoding")
    length = headers.get(b"content-length")
    if transfer is not None:
        if transfer != b"chunked" or length is not None:
            raise ValueError("ambiguous native body framing")
        body = bytearray()
        while True:
            line = await framing_line(reader)
            if len(line) > 128:
                raise ValueError("native chunk header is oversized")
            raw_size = line.split(b";", 1)[0].strip()
            if re.fullmatch(rb"[0-9a-fA-F]+", raw_size) is None:
                raise ValueError("native chunk size is not hexadecimal wire syntax")
            size = int(raw_size, 16)
            if not 0 <= size <= MAXIMUM - len(body):
                raise ValueError("native body is oversized")
            if size == 0:
                for _ in range(32):
                    trailer = await framing_line(reader)
                    if trailer == b"\r\n":
                        return bytes(body), False
                    if len(trailer) > 8192:
                        raise ValueError("invalid native trailer")
                raise ValueError("too many native trailers")
            body.extend(await reader.readexactly(size))
            if await reader.readexactly(2) != b"\r\n":
                raise ValueError("invalid native chunk terminator")
    if length is not None:
        if not length.isdigit() or not 0 <= int(length) <= MAXIMUM:
            raise ValueError("native body length is invalid")
        return await reader.readexactly(int(length)), False
    body = bytearray()
    while chunk := await reader.read(min(8192, MAXIMUM + 1 - len(body))):
        body.extend(chunk)
        if len(body) > MAXIMUM:
            raise ValueError("native body is oversized")
    return bytes(body), True


# Return the bounded payload to callers that perform their own application decoding.
async def response_body(reader: asyncio.StreamReader) -> bytes:
    body, _ = await framed_body(reader)
    return body


# Bad JSON supplies no native facts; duplicate keys and nonfinite/invalid validated content remain strict errors.
def decoded_native_json(raw: bytes | str) -> Any:
    try:
        return json.loads(raw, object_pairs_hook=unique_object)
    except json.JSONDecodeError, UnicodeDecodeError:
        raise NativeTransportUnavailableError("native response JSON is unavailable or incomplete") from None


# The whole request has one deadline; closing the stream on cancellation does not leave a background reader running.
async def read_native(host: str, port: int, authorization: str, action: str, data: str | dict[str, Any] | None) -> Any:
    writer: asyncio.StreamWriter | None = None
    payload: dict[str, Any] = {"action": action}
    if data is not None:
        payload["data"] = data
    body = encoded(payload).encode()
    try:
        async with asyncio.timeout(5):
            reader, writer = await asyncio.open_connection(host, port, limit=65536)
            authority = "[" + host + "]" if ":" in host else host
            request = (
                "POST /cgi-bin/action.exe HTTP/1.0\r\nHost: " + authority + "\r\n"
                "Authorization: " + authorization + "\r\nContent-Type: application/json\r\n"
                "Accept-Encoding: identity\r\nConnection: close\r\nContent-Length: " + str(len(body)) + "\r\n\r\n"
            ).encode("ascii") + body
            writer.write(request)
            await writer.drain()
            raw, _ = await framed_body(reader)
            value = decoded_native_json(raw)
            encoded(value)
            if (
                action == "statistics"
                and data == "OCPP"
                and isinstance(value, dict)
                and type(value.get("code")) is int
                and value == {"code": 1004, "message": "NO FOUND /tmp/Ioc/OCPP.json", "data": None}
            ):
                # This exact firmware reply means it has not yet created its backend-status file after restart.
                return {"online": False}
            if (
                not isinstance(value, dict)
                or type(value.get("code")) is not int
                or value["code"] != 200
                or "data" not in value
            ):
                raise NativeTransportUnavailableError("native response envelope is unavailable or invalid")
            result = value["data"]
            if isinstance(result, str):
                result = decoded_native_json(result)
            encoded(result)
            return result
    except NativeTransportUnavailableError:
        raise  # This subclass is a ValueError too; application unavailability must survive the strict outer boundary.
    except OSError, asyncio.IncompleteReadError:
        raise NativeTransportUnavailableError("native HTTP observation was interrupted") from None
    except ValueError, KeyError, TypeError, RecursionError:
        raise SecurityError("native trial observation was unavailable or malformed") from None
    finally:
        if writer is not None:
            writer.close()
            try:
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            except OSError, TimeoutError:
                pass
