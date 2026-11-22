"""Frame helpers: parsing, building and URL handling."""

from __future__ import annotations

import json

from ocpp_hub_core.frames import (
    ERROR_NOT_IMPLEMENTED,
    basic_auth_header,
    build_call,
    build_call_error,
    build_call_result,
    call_action,
    join_websocket_url,
    parse_frame,
)


# Intent: only well-formed OCPP-J arrays are parsed; anything else yields None.
# Why: a charger can send garbage and the hub must keep the connection alive instead of crashing on it.
def test_parse_frame_accepts_ocpp_frames_and_rejects_everything_else() -> None:
    assert parse_frame('[2,"id","Heartbeat",{}]') == [2, "id", "Heartbeat", {}]
    assert parse_frame("not json") is None
    assert parse_frame('{"a": 1}') is None
    assert parse_frame('[2,"id"]') is None
    assert parse_frame(b"\x00\x01") is None
    assert parse_frame('[2,"id","MeterValues",{"reading":' + "9" * 5000 + "}]") is None


# Intent: only CALL frames have an action name.
# Why: results and errors are matched by unique id, never by action.
def test_call_action_only_names_calls() -> None:
    assert call_action([2, "id", "BootNotification", {}]) == "BootNotification"
    assert call_action([3, "id", {"status": "Accepted"}]) is None
    assert call_action([2, "id", "Heartbeat"]) is None


# Intent: built frames are compact JSON that parses back to the same arrays.
# Why: the wire format is the contract with the charger; a spaced or reordered frame is a different frame.
def test_builders_produce_compact_frames_that_round_trip() -> None:
    call = build_call("u1", "Heartbeat", {})
    result = build_call_result("u1", {"currentTime": "2026-10-20T18:00:00Z"})
    error = build_call_error("u1", ERROR_NOT_IMPLEMENTED, "no handler")

    assert call == '[2,"u1","Heartbeat",{}]'
    assert parse_frame(result) == [3, "u1", {"currentTime": "2026-10-20T18:00:00Z"}]
    assert json.loads(error) == [4, "u1", "NotImplemented", "no handler", {}]


# Intent: joining a path segment never produces a double slash.
# Why: the backend derives the charger identity from the last URL path segment.
def test_join_websocket_url_never_doubles_slashes() -> None:
    assert join_websocket_url("ws://host:9000", "CP1") == "ws://host:9000/CP1"
    assert join_websocket_url("ws://host:9000/ocpp/", "/CP1") == "ws://host:9000/ocpp/CP1"


# Intent: the Basic header carries base64 of user:password.
# Why: this is what a vendor backend checks on the websocket upgrade request.
def test_basic_auth_header_encodes_user_and_password() -> None:
    assert basic_auth_header("relay", "s3cret") == ("Authorization", "Basic cmVsYXk6czNjcmV0")
