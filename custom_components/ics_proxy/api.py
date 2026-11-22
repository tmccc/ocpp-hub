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


class UnknownCommandOutcomeError(ProxyError):
    """The command may have reached the proxy, so only its saved result can settle it."""


class ReceiptUnavailableError(ProxyError):
    """A temporary receipt read failure can be retried without issuing another command."""


class ReceiptMismatchError(ProxyError):
    """The response cannot confirm the original request or its actual successor."""


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
                    message = await connection.receive()
                    if command == "receipt" and message.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.CLOSED,
                    ):
                        raise ReceiptUnavailableError("The saved command result is temporarily unavailable")
                    # The server uses these close reasons before a client can enter command admission.
                    if (
                        message.type is aiohttp.WSMsgType.CLOSE
                        and message.data == 1013
                        and message.extra in ("Busy", "Idle connection replaced")
                    ):
                        raise ProxyError("The local charger proxy was too busy to accept this request")
                    if message.type is not aiohttp.WSMsgType.TEXT:
                        raise TypeError("The local charger proxy returned a non-text response")
                    response = message.json()
        except (aiohttp.ClientError, TimeoutError, ValueError, TypeError, RecursionError) as error:
            if command == "receipt" and isinstance(error, (aiohttp.ClientConnectionError, TimeoutError)):
                raise ReceiptUnavailableError("The saved command result is temporarily unavailable") from None
            if sending and command not in ("status", "history", "receipt"):
                raise UnknownCommandOutcomeError(_UNKNOWN_OUTCOME) from None
            raise ProxyError("The local charger proxy could not be reached or authenticated") from None
        if response == {"id": identifier, "ok": False, "error": "outcome_unknown"}:
            raise UnknownCommandOutcomeError(_UNKNOWN_OUTCOME)
        if command == "receipt" and response == {"ok": False, "error": "not_ready"}:
            raise ReceiptUnavailableError("The saved command result is temporarily unavailable")
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
            latest_known = result.get("charger_latest_completion_known", result.get("charger_history_complete", True))
            result["charger_latest_completion_known"] = complete and latest_known is True
            result["charger_history_complete"] = complete and result.get("charger_history_complete", True) is True
            if "last_session_energy_kwh" in result:
                energy = result["last_session_energy_kwh"]
                result["last_session_energy_kwh"] = (
                    energy if type(energy) in (int, float) and 0 <= energy <= 1e9 else None
                )
            elif result["charger_latest_completion_known"]:
                result["last_session_energy_kwh"] = next(
                    (row["energy_kwh"] for row in result["charger_sessions"] if row["stopped_at"] is not None), None
                )
        return result

    # Retry only temporary read failures within the command caller's existing deadline.
    async def _receipt(self, identifier: str) -> dict[str, Any]:
        while True:
            try:
                return await self.request("receipt", request_id=identifier)
            except ReceiptUnavailableError:
                await asyncio.sleep(1)

    # Send the control once and recover a lost first reply by reading the original request's receipt.
    async def command(self, action: str, revision: int, **fields: Any) -> None:
        supplied_id = "id" in fields
        identifier = fields.pop("id", uuid.uuid4().hex)

        # Match each reply to this command and require full binding when looking up a caller-supplied ID.
        def matches(receipt: dict[str, Any], *, lookup: bool = False) -> bool:
            returned_id = receipt.get("id")
            if not isinstance(returned_id, str) or not re.fullmatch(r"[a-f0-9]{32}", returned_id):
                return False
            returned_action = receipt.get("action") if lookup else receipt.get("action", action)
            if returned_id != identifier or returned_action != action:
                return False
            if (
                lookup
                and supplied_id
                and ("revision" not in receipt or (action == "set_current" and "current_a" not in receipt))
            ):
                return False
            observed_revision = receipt.get("revision", revision)
            if type(observed_revision) is not int or observed_revision != revision:
                return False
            if action == "set_current" and "current_a" in receipt:
                return type(receipt["current_a"]) is int and receipt["current_a"] == fields.get("current_a")
            if action == "stop_session":
                return isinstance(receipt.get("target"), str) and receipt["target"] == fields.get("target")
            return True

        try:
            receipt: dict[str, Any] | None = await self.request(action, id=identifier, revision=revision, **fields)
        except UnknownCommandOutcomeError:
            receipt = None
        if receipt is not None and not matches(receipt):
            raise ProxyError("The local charger proxy returned an invalid command receipt")
        receipt_id = identifier
        followed: set[str] = set()
        try:
            # Both local Start commands can spend up to 60 seconds waiting for physical confirmation.
            async with asyncio.timeout(70 if action in ("start", "start_test") else 25):
                if receipt is None:
                    saved = await self._receipt(identifier)
                    if not matches(saved, lookup=True) or saved.get("state") not in (
                        "accepted",
                        "running",
                        "completed",
                        "failed",
                        "uncertain",
                    ):
                        raise UnknownCommandOutcomeError(_UNKNOWN_OUTCOME)
                    receipt = saved
                while True:
                    if receipt.get("state") in ("accepted", "running"):
                        await asyncio.sleep(1)
                    elif action == "pause" and (newer := _newer_pause(receipt)) is not None:
                        if newer == identifier or newer in followed:
                            raise ReceiptMismatchError("A newer Pause could not be confirmed.")
                        followed.add(newer)
                        receipt_id = newer
                    else:
                        break
                    receipt = await self._receipt(receipt_id)
                    if receipt_id == identifier:
                        matched = matches(receipt, lookup=True)
                    else:
                        matched = receipt.get("id") == receipt_id and receipt.get("action") == "pause"
                    if not matched:
                        raise ReceiptMismatchError("The local charger proxy returned another command's receipt")
        except ReceiptMismatchError:
            raise
        except TimeoutError, ProxyError:
            if receipt is None:
                raise UnknownCommandOutcomeError(_UNKNOWN_OUTCOME) from None
            raise ProxyError("The request was recorded; physical confirmation is still pending") from None
        if receipt.get("state") == "uncertain":
            raise ProxyError(
                "The proxy recorded the request. The charger outcome is uncertain. Check the charger status."
            )
        result = receipt.get("result")
        if receipt.get("state") == "failed" and isinstance(result, dict):
            if action == "app_control" and result.get("classification") == "return_not_sent":
                raise ProxyError("Return to app / schedule stopped before sending the release command.")
        if receipt.get("state") != "completed":
            raise ProxyError("The charger could not confirm the requested control state")


# A matched replacement points to another Pause receipt to read, without repeating either command.
def _newer_pause(receipt: dict[str, Any]) -> str | None:
    result = receipt.get("result")
    if (
        receipt.get("state") != "failed"
        or not isinstance(result, dict)
        or result.get("classification") != "superseded_by_pause"
    ):
        return None
    identifier = result.get("superseded_by")
    return identifier if isinstance(identifier, str) and re.fullmatch(r"[a-f0-9]{32}", identifier) else None
