"""Bounded asynchronous HTTP for the existing iCS read-only LAN API; no proxies, redirects or worker threads."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from .ics import unique_object
from .security import SecurityError
from .store import encoded

MAXIMUM = 2 * 1024 * 1024


# Read only a bounded successful body, supporting ordinary length, chunked and connection-close framing.
async def response_body(reader: asyncio.StreamReader) -> bytes:
    status = await reader.readline()
    if len(status) > 8192 or len(status.split()) < 2 or status.split()[1] != b"200":
        raise SecurityError("native reader received a non-success HTTP response")
    headers: dict[bytes, bytes] = {}
    total = 0
    for _ in range(64):
        line = await reader.readline()
        total += len(line)
        if total > 65536 or not line:
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
            line = await reader.readline()
            if len(line) > 128:
                raise ValueError("native chunk header is oversized")
            size = int(line.split(b";", 1)[0].strip(), 16)
            if not 0 <= size <= MAXIMUM - len(body):
                raise ValueError("native body is oversized")
            if size == 0:
                for _ in range(32):
                    trailer = await reader.readline()
                    if trailer == b"\r\n":
                        return bytes(body)
                    if not trailer or len(trailer) > 8192:
                        raise ValueError("invalid native trailer")
                raise ValueError("too many native trailers")
            body.extend(await reader.readexactly(size))
            if await reader.readexactly(2) != b"\r\n":
                raise ValueError("invalid native chunk terminator")
    if length is not None:
        if not length.isdigit() or not 0 <= int(length) <= MAXIMUM:
            raise ValueError("native body length is invalid")
        return await reader.readexactly(int(length))
    body = bytearray()
    while chunk := await reader.read(min(8192, MAXIMUM + 1 - len(body))):
        body.extend(chunk)
        if len(body) > MAXIMUM:
            raise ValueError("native body is oversized")
    return bytes(body)


# The whole request has one deadline; closing the stream on cancellation does not leave a background reader running.
async def read_native(host: str, port: int, authorization: str, action: str, data: str | None) -> Any:
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
            raw = await response_body(reader)
            value = json.loads(raw, object_pairs_hook=unique_object)
            encoded(value)
            if not isinstance(value, dict) or type(value.get("code")) is not int or value["code"] != 200:
                raise ValueError("native envelope is invalid")
            result = value["data"]
            if isinstance(result, str):
                result = json.loads(result, object_pairs_hook=unique_object)
            encoded(result)
            return result
    except OSError, ValueError, KeyError, TypeError, RecursionError, asyncio.IncompleteReadError:
        raise SecurityError("native trial observation was unavailable or malformed") from None
    finally:
        if writer is not None:
            writer.close()
            try:
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            except OSError, TimeoutError:
                pass
