"""The binary planning protocol of the Raedian app, carried in DataTransfer (READIAN / RemoteConfiguration).

It was worked out from captured exchanges, not from documentation, so only what was seen is decoded:

    frame   = head, command, 00 00, length (2 bytes, little endian), 08, check, session (8 bytes), payload
    check   = the XOR of every other byte of the frame
    head    = FE from the cloud, AA from the charger
    command A7 = the planning. Written with payload 01, count, then count windows of 12 bytes:
        enabled (01; 00 for a window that is switched off), overnight flag, weekday mask (bit 0 = Monday ... bit 6 =
        Sunday), start (4 bytes, seconds after midnight), end (same), 20.
        Read with payload 00; an empty plan answers 00 00. A write is acknowledged with the single byte 00.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass

COMMAND_PLAN = 0xA7
# The switch of the planning in the app: the cloud sends one byte, 02 for on and 01 for off. Seen on the wire (two
# switches each way) but not documented by the vendor.
COMMAND_PLANNING_SWITCH = 0xB3
PLANNING_ON = 2
PLANNING_OFF = 1
HEAD_CLOUD = 0xFE
HEAD_CHARGER = 0xAA
WINDOW_SIZE = 12
SECONDS_PER_DAY = 86400


class FrameError(ValueError):
    """The bytes are not a frame of this protocol."""


@dataclass(frozen=True, slots=True)
class RaedianFrame:
    """One packet: who sent it, which command, the session it belongs to, and its payload."""

    head: int
    command: int
    session: bytes
    payload: bytes


@dataclass(frozen=True, slots=True)
class PlanWindow:
    """One window of the planning: a weekday (1 is Monday), the seconds after midnight it starts and ends, and whether
    the app has it switched on. A switched-off window is kept so the app finds it again, but the charger ignores it."""

    weekday: int
    start_s: int
    end_s: int
    enabled: bool = True

    @property
    def overnight(self) -> bool:
        """True when the window ends before it starts, so it runs into the next day."""
        return self.end_s <= self.start_s


# The check byte of a frame: the XOR of all its other bytes. Proven on every captured frame of both directions.
def check_byte(frame_without_check: bytes) -> int:
    return functools.reduce(lambda a, b: a ^ b, frame_without_check, 0)


# Read a frame from its hex text. A frame whose length, size or check byte does not agree is refused.
def parse_frame(text: object) -> RaedianFrame:
    if not isinstance(text, str):
        raise FrameError("frame contents must be text")
    try:
        data = bytes.fromhex(text)
    except ValueError as error:
        raise FrameError("not hexadecimal") from error
    if len(data) < 16 or data[2:4] != b"\x00\x00" or data[6] != 0x08:
        raise FrameError("not a frame of this protocol")
    if len(data) != 16 + int.from_bytes(data[4:6], "little"):
        raise FrameError("the length does not match")
    if check_byte(data[:7] + data[8:]) != data[7]:
        raise FrameError("the check byte does not match")
    return RaedianFrame(data[0], data[1], data[8:16], data[16:])


# Build a frame as hex text, with its length and check byte.
def build_frame(head: int, command: int, session: bytes, payload: bytes) -> str:
    body = bytes([head, command, 0, 0]) + len(payload).to_bytes(2, "little") + b"\x08"
    return (body + bytes([check_byte(body + session + payload)]) + session + payload).hex()


# The windows of a planning payload (01, count, windows). Anything that is not exactly that is refused, because a
# payload is only trusted when every byte of it is accounted for.
def decode_plan(payload: bytes) -> list[PlanWindow]:
    if len(payload) < 2 or payload[0] not in (0, 1) or len(payload) != 2 + payload[1] * WINDOW_SIZE:
        raise FrameError("not a planning payload")
    windows: list[PlanWindow] = []
    for offset in range(2, len(payload), WINDOW_SIZE):
        enabled, overnight, day_mask, start, end, tail = (
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            int.from_bytes(payload[offset + 3 : offset + 7], "little"),
            int.from_bytes(payload[offset + 7 : offset + 11], "little"),
            payload[offset + 11],
        )
        if enabled not in (0, 1) or tail != 0x20 or not 1 <= day_mask <= 0x7F or max(start, end) > SECONDS_PER_DAY:
            raise FrameError("a window is not one the hub understands")
        # The app marks the days with a mask (Wednesday is 4, not 3); a window for several days is one per day here.
        days = [PlanWindow(day, start, end, bool(enabled)) for day in range(1, 8) if day_mask >> (day - 1) & 1]
        if bool(overnight) != days[0].overnight:
            raise FrameError("the overnight flag does not agree with the times")
        windows.extend(days)
    return windows


# The planning payload for some windows, in the form the app writes it.
def encode_plan(windows: list[PlanWindow]) -> bytes:
    payload = bytes([1, len(windows)])
    for window in windows:
        payload += bytes([int(window.enabled), int(window.overnight), 1 << (window.weekday - 1)])
        payload += window.start_s.to_bytes(4, "little") + window.end_s.to_bytes(4, "little") + b"\x20"
    return payload


# What an exchange of the cloud and the charger says about the planning, or None when it says nothing about it: a write
# the charger acknowledged gives the plan that was written, a read gives the plan that the charger answered. The windows
# are only believed when the whole exchange parses and every byte is accounted for.
def plan_from_exchange(request_hex: str, answer_hex: str) -> list[PlanWindow] | None:
    try:
        request, answer = parse_frame(request_hex), parse_frame(answer_hex)
        if request.command != COMMAND_PLAN or answer.command != COMMAND_PLAN or request.head != HEAD_CLOUD:
            return None
        if len(request.payload) > 1:
            return decode_plan(request.payload) if answer.payload == b"\x00" else None
        if request.payload == b"\x00":
            return decode_plan(answer.payload)
    except FrameError:
        return None
    return None
