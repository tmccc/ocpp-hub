"""Independent bounded HTTP message grammars exercise parsing, arbitrary fragmentation and complete rejection."""

import asyncio
import json
from itertools import product

import pytest
from ocpp_hub_core.prototype.native_http import read_native, response_body
from ocpp_hub_core.prototype.security import SecurityError

BODY = json.dumps({"code": 200, "data": {"synthetic": "native-evidence"}}).encode()


# Build wire bytes from the HTTP grammar, independently of the native parser and engine's observation objects.
def wire(version=b"HTTP/1.1", framing="length", *, size=None, last=b"0", extra=b""):
    head = version + b" 200 OK\r\n"
    if framing == "length":
        return head + b"Content-Length: " + str(len(BODY)).encode() + b"\r\n" + extra + b"\r\n" + BODY
    if framing == "close":
        return head + extra + b"\r\n" + BODY
    size = size if size is not None else format(len(BODY), "x").encode()
    return (
        head
        + b"Transfer-Encoding: chunked\r\n"
        + extra
        + b"\r\n"
        + size
        + b"\r\n"
        + BODY
        + b"\r\n"
        + last
        + b"\r\n\r\n"
    )


# These malformed numeric spellings are accepted by Python int(), but are outside HTTP's1*HEXDIG grammar.
def invalid_wires():
    size = format(len(BODY), "x").encode()
    return (
        *(("version-" + version.decode(), wire(version)) for version in (b"NOTHTTP", b"HTTP/2.0", b"HTTP/9.9")),
        *(
            ("chunk-" + repr(value), wire(framing="chunked", size=value))
            for value in (b"+" + size, b"0x" + size, size[:1] + b"_" + size[1:])
        ),
        *(("last-" + repr(value), wire(framing="chunked", last=value)) for value in (b"+0", b"-0", b"0x0", b"0_0")),
    )


# Consume actual network reads in the independently controlled chunk boundaries rather than pre-decoding a result.
async def fragmented(raw, split):
    reader = asyncio.StreamReader()

    # A real asynchronous feeder permits parser reads to wait at every declared byte boundary.
    async def feed():
        for piece in (raw[:split], raw[split:]):
            if piece:
                reader.feed_data(piece)
            await asyncio.sleep(0)
        reader.feed_eof()

    feeder = asyncio.create_task(feed())
    try:
        return await response_body(reader)
    finally:
        await feeder


# Intent: every split point in valid length/chunked/close-delimited replies preserves exactly the same body bytes.
# Why: header, chunk and JSON boundaries need not coincide with a TCP packet or an event-loop turn.
@pytest.mark.parametrize("version,framing", tuple(product((b"HTTP/1.0", b"HTTP/1.1"), ("length", "chunked", "close"))))
async def test_valid_native_wire_is_fragmentation_invariant(version, framing):
    raw = wire(version, framing)
    for split in range(len(raw) + 1):
        assert await fragmented(raw, split) == BODY


# Intent: every incomplete prefix of a self-delimiting reply fails, including partial framing lines and last chunks.
# Why: a complete JSON prefix is insufficient evidence when the HTTP message itself was cut off.
@pytest.mark.parametrize("framing", ["length", "chunked"])
async def test_every_native_message_truncation_is_rejected(framing):
    raw = wire(framing=framing)
    for length in range(len(raw)):
        with pytest.raises((asyncio.IncompleteReadError, SecurityError, ValueError)):
            await fragmented(raw[:length], length // 2)


# Intent: invalid protocol versions and Python-only numeric chunk spellings never decode as successful evidence.
# Why: parser convenience conversions must not widen the wire protocol's declared grammar.
@pytest.mark.parametrize("name,raw", [pytest.param(name, raw, id=name) for name, raw in invalid_wires()])
async def test_invalid_native_wire_cannot_supply_evidence(name, raw):
    with pytest.raises((SecurityError, ValueError)):
        await fragmented(raw, len(raw) // 2)


# Intent: the complete native reader rejects invalid framing even when it surrounds a valid authoritative JSON body.
# Why: successful application decoding must not hide an invalid lower-level protocol response.
@pytest.mark.parametrize("name,raw", [pytest.param(name, raw, id=name) for name, raw in invalid_wires()])
async def test_invalid_native_reply_is_classified_over_real_http(name, raw):
    # Only this independent loopback peer supplies response bytes; the actual native reader runs unmodified.
    async def peer(reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        length = next(
            int(line.split(b":", 1)[1]) for line in head.split(b"\r\n") if line.lower().startswith(b"content-length:")
        )
        await reader.readexactly(length)
        writer.write(raw)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    try:
        with pytest.raises(SecurityError):
            await read_native("127.0.0.1", server.sockets[0].getsockname()[1], "Basic synthetic", "getConfig", None)
    finally:
        server.close()
        await server.wait_closed()
