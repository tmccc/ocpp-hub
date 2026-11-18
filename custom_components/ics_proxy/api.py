"""Small local management client; it imports no OCPP engine and never retries a mutating command."""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import UTC, datetime
from typing import Any

import aiohttp

_UNKNOWN_OUTCOME = "The request may have been sent; its outcome is unknown. Check the charger status"


# Validate the timestamp shape without rewriting the charger's recorded clock values.
def _history_time(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else None
    except ValueError, OverflowError:
        return None


# Publish only bounded, public summaries. A missing latest row must not promote an older energy reading.
def _history(value: object, *, proxy: bool = False) -> tuple[list[dict[str, Any]], bool]:
    if not isinstance(value, list):
        return [], False
    rows = []
    for row in value[:20]:
        if not isinstance(row, dict):
            continue
        start = _history_time(row.get("started_at"))
        if start is None or not {"stopped_at", "energy_kwh"} <= row.keys():
            continue
        stopped, energy = row["stopped_at"], row["energy_kwh"]
        if stopped is None:
            if energy is not None:
                continue
        else:
            end = _history_time(stopped)
            if end is None or type(energy) not in (float, int) or not -1e12 <= energy <= 1e12:
                continue
            if not proxy and (end < start or not 0 <= energy <= 1e9):
                continue
        item = {"started_at": row["started_at"], "stopped_at": stopped, "energy_kwh": energy}
        if proxy and type(row.get("cloud_acknowledged")) is bool:
            item["cloud_acknowledged"] = row["cloud_acknowledged"]
        rows.append(item)
    return rows, len(rows) == len(value)


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
        sending = False
        try:
            async with asyncio.timeout(10):
                async with self.session.ws_connect(
                    f"http://127.0.0.1:{self.port}/management",
                    headers={"Authorization": "Bearer " + self.token},
                    protocols=["ics-proxy.v1"],
                    max_msg_size=65536,
                ) as connection:
                    sending = True
                    await connection.send_json({"id": identifier, "command": command, **fields})
                    response = await connection.receive_json()
        except aiohttp.ClientError, TimeoutError, ValueError, TypeError, RecursionError:
            if sending and command not in ("status", "history", "receipt"):
                raise ProxyError(_UNKNOWN_OUTCOME) from None
            raise ProxyError("The local charger proxy could not be reached or authenticated") from None
        if response == {"id": identifier, "ok": False, "error": "outcome_unknown"}:
            raise ProxyError(_UNKNOWN_OUTCOME)
        if (
            not isinstance(response, dict)
            or response.get("ok") is not True
            or response.get("id") != identifier
            or not isinstance(response.get("result"), dict)
        ):
            raise ProxyError("The local charger proxy could not confirm this request")
        result: dict[str, Any] = response["result"]
        if command == "status" and (
            type(result.get("api_version")) is not int
            or result.get("api_version") != 1
            or type(result.get("control_revision")) is not int
            or result.get("desired_control") not in ("app", "paused")
            or not isinstance(result.get("sessions"), list)
        ):
            raise ProxyError("The local charger proxy returned an unsupported status")
        if command in ("status", "history"):
            result["sessions"], _ = _history(result.get("sessions"), proxy=True)
        if command == "status":
            result["charger_sessions"], complete = _history(result.get("charger_sessions", []))
            result["charger_history_complete"] = complete and result.get("charger_history_complete", True) is True
        return result

    # Acceptance is separate from actual zero/release confirmation; a lost response never repeats the physical write.
    async def command(self, action: str, revision: int, **fields: Any) -> None:
        identifier = fields.pop("id", uuid.uuid4().hex)
        receipt = await self.request(action, id=identifier, revision=revision, **fields)
        if (
            not isinstance(receipt.get("id"), str)
            or not re.fullmatch(r"[a-f0-9]{32}", receipt["id"])
            or receipt["id"] != identifier
        ):
            raise ProxyError("The local charger proxy returned an invalid command receipt")
        try:
            async with asyncio.timeout(25):
                while receipt.get("state") in ("accepted", "running"):
                    await asyncio.sleep(1)
                    receipt = await self.request("receipt", request_id=identifier)
                    if receipt.get("id") != identifier:
                        raise ProxyError("The local charger proxy returned another command's receipt")
        except TimeoutError, ProxyError:
            raise ProxyError("The request was recorded; physical confirmation is still pending") from None
        if receipt.get("state") != "completed":
            raise ProxyError("The charger could not confirm the requested control state")
