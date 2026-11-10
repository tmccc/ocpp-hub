"""Small local management client; it imports no OCPP engine and never retries a mutating command."""

from __future__ import annotations

import asyncio
import re
import uuid
from typing import Any

import aiohttp


class ProxyError(Exception):
    """A fixed safe error, without credentials, URLs or raw peer bodies."""


class ProxyClient:
    """Home Assistant shares the host network with the proxy; every connection is literal loopback only."""

    # A caller supplies HA's shared session without creating another event-loop or connection lifecycle owner.
    def __init__(self, session: aiohttp.ClientSession, port: int, token: str) -> None:
        if (
            type(port) is not int
            or not 1024 <= port <= 65535
            or not isinstance(token, str)
            or not re.fullmatch(r"[a-f0-9]{64}", token)
        ):
            raise ProxyError("The local management settings are invalid")
        self.session, self.port, self.token = session, port, token

    # One bounded websocket exchange has no reconnect or retry path and authenticates outside the message body.
    async def request(self, command: str, **fields: Any) -> dict[str, Any]:
        identifier = fields.pop("id", uuid.uuid4().hex)
        try:
            async with asyncio.timeout(10):
                async with self.session.ws_connect(
                    f"http://127.0.0.1:{self.port}/management",
                    headers={"Authorization": "Bearer " + self.token},
                    protocols=["ics-proxy.v1"],
                    max_msg_size=65536,
                ) as connection:
                    await connection.send_json({"id": identifier, "command": command, **fields})
                    response = await connection.receive_json()
        except aiohttp.ClientError, TimeoutError, ValueError, TypeError:
            raise ProxyError("The local charger proxy could not be reached or authenticated") from None
        if (
            not isinstance(response, dict)
            or response.get("ok") is not True
            or response.get("id") != identifier
            or not isinstance(response.get("result"), dict)
        ):
            raise ProxyError("The local charger proxy could not confirm this request")
        result = response["result"]
        if command == "status" and (
            type(result.get("api_version")) is not int
            or result.get("api_version") != 1
            or type(result.get("control_revision")) is not int
            or result.get("desired_control") not in ("app", "paused")
            or not isinstance(result.get("sessions"), list)
        ):
            raise ProxyError("The local charger proxy returned an unsupported status")
        return result

    # Acceptance is separate from actual zero/release confirmation; a lost response never repeats the physical write.
    async def command(self, action: str, revision: int) -> None:
        receipt = await self.request(action, revision=revision)
        if not isinstance(receipt.get("id"), str) or not re.fullmatch(r"[a-f0-9]{32}", receipt["id"]):
            raise ProxyError("The local charger proxy returned an invalid command receipt")
        identifier = receipt["id"]
        try:
            async with asyncio.timeout(25):
                while receipt.get("state") in ("accepted", "running"):
                    await asyncio.sleep(1)
                    receipt = await self.request("receipt", request_id=identifier)
        except TimeoutError:
            raise ProxyError("The request was recorded; physical confirmation is still pending") from None
        if receipt.get("state") != "completed":
            raise ProxyError("The charger could not confirm the requested control state")
