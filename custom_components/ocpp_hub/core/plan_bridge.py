"""The bridge between the planning of the Raedian app and the plan of the hub: recognise what the app asks, answer it.

The app keeps no planning of its own: it reads the charger's. The hub sits in between, so it can answer for the charger
from the plan it holds (and take over what the app writes), instead of passing the app's planning on to the charger.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from . import raedian
from .raedian import PlanWindow
from .schedule_plan import SECONDS_PER_DAY, Block


class PlanHandler(Protocol):
    """What the hub does with the planning: the plan it holds, what it does when the app writes, what it learns."""

    def plan(self, cp_id: str) -> list[PlanWindow] | None: ...

    async def app_wrote(
        self, cp_id: str, windows: list[PlanWindow], *, before_send: Callable[[], None] | None = None
    ) -> bool: ...

    def imported(self, cp_id: str, windows: list[PlanWindow]) -> None: ...

    async def planning_switched(self, cp_id: str, on: bool) -> None: ...


@dataclass(frozen=True, slots=True)
class PlanWrite:
    """The app writes a planning: these windows, in this session."""

    windows: list[PlanWindow]
    session: bytes


@dataclass(frozen=True, slots=True)
class PlanQuery:
    """The app asks for the planning, in this session."""

    session: bytes


@dataclass(frozen=True, slots=True)
class PlanUnreadable:
    """A planning message of the app that the hub does not understand completely; it is not passed on."""

    reason: str


# What a DataTransfer of the cloud is, as far as the planning goes: a write, a query, a message the hub cannot read, or
# None when it is not about the planning (it is passed on as any other).
def classify(payload: dict[str, Any]) -> PlanWrite | PlanQuery | PlanUnreadable | None:
    if payload.get("vendorId") != "READIAN" or payload.get("messageId") != "RemoteConfiguration":
        return None
    try:
        body = json.loads(payload.get("data") or "")
        original = body["originalData"]
        instruct = body.get("instructType")
    except TypeError, ValueError, KeyError, AttributeError:
        return None
    try:
        frame = raedian.parse_frame(original)
    except raedian.FrameError as error:
        # A message that says it is about the planning but does not parse is not passed on untouched.
        if str(instruct).lower() == f"{raedian.COMMAND_PLAN:02x}":
            return PlanUnreadable(str(error))
        return None
    if frame.command != raedian.COMMAND_PLAN or frame.head != raedian.HEAD_CLOUD:
        return None
    if frame.payload == b"\x00":
        return PlanQuery(frame.session)
    if frame.payload[:1] != b"\x01":
        return PlanUnreadable("a planning write starts with 01")
    try:
        return PlanWrite(raedian.decode_plan(frame.payload), frame.session)
    except raedian.FrameError as error:
        return PlanUnreadable(str(error))


# Whether a DataTransfer of the cloud switches the app's planning on (True) or off (False), or None when it does not.
def planning_switch(payload: dict[str, Any]) -> bool | None:
    if payload.get("vendorId") != "READIAN" or payload.get("messageId") != "RemoteConfiguration":
        return None
    try:
        frame = raedian.parse_frame(json.loads(payload.get("data") or "")["originalData"])
    except TypeError, ValueError, KeyError, AttributeError, raedian.FrameError:
        return None
    if frame.command != raedian.COMMAND_PLANNING_SWITCH or frame.head != raedian.HEAD_CLOUD:
        return None
    return {bytes([raedian.PLANNING_ON]): True, bytes([raedian.PLANNING_OFF]): False}.get(frame.payload)


# The DataTransfer answer that carries one protocol frame, in the form the charger uses.
def _answer(frame_hex: str) -> dict[str, Any]:
    body = {
        "messageId": "RemoteConfiguration",
        "type": "RemoteConfiguration",
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "instructType": "TF",
        "originalData": frame_hex.upper(),
    }
    return {"status": "Accepted", "data": json.dumps(body)}


# The answer that says a planning was written, in the session of the message.
def write_acknowledged(session: bytes) -> dict[str, Any]:
    return _answer(raedian.build_frame(raedian.HEAD_CHARGER, raedian.COMMAND_PLAN, session, b"\x00"))


# The answer to a query for the planning: the windows of the plan, in the same layout the app writes them in.
def plan_answer(session: bytes, windows: Sequence[PlanWindow]) -> dict[str, Any]:
    payload = raedian.encode_plan(list(windows))
    return _answer(raedian.build_frame(raedian.HEAD_CHARGER, raedian.COMMAND_PLAN, session, payload))


# The windows as blocks per weekday (0 is Monday), as the schedule planner wants them. A window that runs overnight is
# cut at midnight: its first part ends the day and the rest starts the next one (Sunday's overnight runs into Monday).
def windows_to_blocks(windows: Sequence[PlanWindow]) -> dict[int, list[Block]]:
    blocks: dict[int, list[Block]] = {}
    for window in windows:
        if not window.enabled:
            continue
        day = window.weekday - 1
        if window.overnight:
            blocks.setdefault(day, []).append(Block(window.start_s, SECONDS_PER_DAY))
            if window.end_s > 0:
                blocks.setdefault((day + 1) % 7, []).append(Block(0, window.end_s))
        else:
            blocks.setdefault(day, []).append(Block(window.start_s, window.end_s))
    return blocks
