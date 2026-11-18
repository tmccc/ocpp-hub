"""Synthetic token and real management WebSocket round trips."""

from __future__ import annotations

import json
import uuid
from typing import Any

TOKEN = "a" * 64  # Synthetic bearer used only by loopback test peers.


# Send one bounded protocol message and correlate its real socket response.
async def call(socket: Any, command: str, **fields: Any) -> dict[str, Any]:
    identifier = fields.pop("id", uuid.uuid4().hex)
    await socket.send(json.dumps({"id": identifier, "command": command, **fields}))
    value = json.loads(await socket.recv())
    assert value.get("id") == identifier or value.get("ok") is False
    return value
