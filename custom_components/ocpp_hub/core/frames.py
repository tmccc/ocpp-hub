"""OCPP-J frame helpers: parsing, building and websocket URL handling."""

from __future__ import annotations

import base64
import json
from typing import Any
from urllib.parse import urlsplit, urlunsplit

Frame = list[Any]

# OCPP-J message type ids: [2, id, action, payload], [3, id, payload], [4, id, code, description, details].
MSG_CALL = 2
MSG_CALL_RESULT = 3
MSG_CALL_ERROR = 4

# The CALLERROR codes (OCPP 1.6 section 4.2.3) this hub sends itself.
ERROR_NOT_IMPLEMENTED = "NotImplemented"
ERROR_NOT_SUPPORTED = "NotSupported"
ERROR_FORMATION_VIOLATION = "FormationViolation"
ERROR_INTERNAL = "InternalError"


# Decode a text frame into the OCPP-J array, or None when it is not a well-formed frame.
# A charger can send anything; the caller decides what to do with garbage instead of crashing on it.
def parse_frame(raw: str | bytes) -> Frame | None:
    # OCPP-J is text only; a binary frame is never interpreted.
    if isinstance(raw, bytes):
        return None
    try:
        frame = json.loads(raw)
    except ValueError, RecursionError:
        return None
    if not isinstance(frame, list) or len(frame) < 3 or not isinstance(frame[0], int):
        return None
    return frame


# Return the action name of a CALL frame (for example "BootNotification"), or None for any other frame.
# Only CALLs carry an action; results and errors are matched to a call by their unique id.
def call_action(frame: Frame) -> str | None:
    if frame[0] != MSG_CALL or len(frame) < 4 or not isinstance(frame[2], str):
        return None
    return frame[2]


# Serialise a frame the way OCPP-J expects, without spaces.
# Chargers with small buffers are known to choke on pretty-printed JSON, and compact output is easier to compare.
def _dump(frame: Frame) -> str:
    return json.dumps(frame, separators=(",", ":"))


# Build a CALL frame (a request from either side).
# One place decides the frame layout, so no caller assembles raw arrays.
def build_call(unique_id: str, action: str, payload: dict[str, Any]) -> str:
    return _dump([MSG_CALL, unique_id, action, payload])


# Build a CALLRESULT frame (the successful answer to a CALL).
# The unique id ties the answer to the request; the payload shape is defined per action.
def build_call_result(unique_id: str, payload: dict[str, Any]) -> str:
    return _dump([MSG_CALL_RESULT, unique_id, payload])


# Build a CALLERROR frame (the failed answer to a CALL).
# A charger waits for an answer to every request, so an unsupported or malformed one still needs a reply.
def build_call_error(unique_id: str, code: str, description: str = "", details: dict[str, Any] | None = None) -> str:
    return _dump([MSG_CALL_ERROR, unique_id, code, description, details or {}])


# Append a path segment to a websocket base URL without doubling slashes.
# The charger identity is the last path segment of the URL a backend sees.
def join_websocket_url(base_url: str, path_segment: str) -> str:
    parts = urlsplit(base_url)
    path = f"{parts.path.rstrip('/')}/{path_segment.lstrip('/')}"
    return urlunsplit(parts._replace(path=path))


# Build the HTTP Basic Authorization header used for a websocket upgrade request.
# Vendor backends authenticate the charger identity with Basic auth on the upgrade request.
def basic_auth_header(user: str, password: str) -> tuple[str, str]:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return ("Authorization", f"Basic {token}")
