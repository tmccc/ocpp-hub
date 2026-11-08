"""List the Raedian protocol frames in a Home Assistant log, so an unknown command can be read and implemented later.

The hub logs every vendor DataTransfer in full (request and the charger's answer) and every planning message it
refused. This tool pulls the protocol frames out of such a log, pairs each request of the cloud with the answer of the
charger, and prints one block per command: the command byte, the payloads in hex, the readable text inside them and
whether the hub decodes the command already. Commands the hub does not know yet are marked NEW.

    python tools/raedian_frames.py home-assistant.log
    cat home-assistant.log | python tools/raedian_frames.py --unknown-only
    python tools/raedian_frames.py home-assistant.log --json

Standard library plus the engine; it only reads text and never talks to a charger.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_component_path = str(ROOT / "custom_components" / "ocpp_hub")
sys.path.insert(0, _component_path)
try:
    from core import raedian
finally:
    # Do not leave calendar.py and other integration modules shadowing the standard library for callers.
    sys.path.remove(_component_path)

# The commands the hub understands, by command byte. Everything else is reported as NEW.
KNOWN_COMMANDS = {raedian.COMMAND_PLAN: "planning (decoded in core/raedian.py)"}

_FRAME = re.compile(r"originalData\W+([0-9A-Fa-f]{16,})")
_STAMP = re.compile(r"^\S*?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)")
_TEXT_RUN = re.compile(rb"[\x20-\x7e]{4,}")


@dataclass(frozen=True, slots=True)
class Seen:
    """One frame found in the log: when, which line it was on, and the parsed frame."""

    stamp: str
    line: int
    frame: raedian.RaedianFrame


@dataclass(frozen=True, slots=True)
class Exchange:
    """A request of the cloud with the answer of the charger, or either one alone."""

    command: int
    request: Seen | None
    answer: Seen | None


# Every frame in the log text that parses, in the order of the log lines. Text that is not a frame is skipped.
def find_frames(text: str) -> list[Seen]:
    seen: list[Seen] = []
    for number, line in enumerate(text.splitlines()):
        match = _FRAME.search(line)
        if match is None:
            continue
        try:
            frame = raedian.parse_frame(match.group(1).lower())
        except raedian.FrameError:
            continue
        stamp = _STAMP.match(line)
        seen.append(Seen(stamp.group(1) if stamp else "", number, frame))
    return seen


# Pair each request of the cloud with the answer of the charger that has the same command and session. The log may be
# oldest-first or newest-first, so the pairing goes by time stamp with the request first on a tie.
def pair_exchanges(seen: list[Seen]) -> list[Exchange]:
    ordered = sorted(seen, key=lambda item: (item.stamp, item.frame.head != raedian.HEAD_CLOUD, item.line))
    exchanges: list[Exchange] = []
    open_requests: dict[tuple[int, bytes], Seen] = {}
    for item in ordered:
        key = (item.frame.command, item.frame.session)
        if item.frame.head == raedian.HEAD_CLOUD:
            if key in open_requests:
                exchanges.append(Exchange(item.frame.command, open_requests[key], None))
            open_requests[key] = item
        elif key in open_requests:
            exchanges.append(Exchange(item.frame.command, open_requests.pop(key), item))
        else:
            exchanges.append(Exchange(item.frame.command, None, item))
    exchanges.extend(Exchange(request.frame.command, request, None) for request in open_requests.values())
    return sorted(exchanges, key=lambda exchange: (exchange.command, _first_stamp(exchange)))


# The earliest time stamp of an exchange, for ordering.
def _first_stamp(exchange: Exchange) -> str:
    return min(item.stamp for item in (exchange.request, exchange.answer) if item is not None)


# The readable text runs in a payload, for example a charger id or a firmware string.
def text_in(payload: bytes) -> list[str]:
    return [run.decode("ascii") for run in _TEXT_RUN.findall(payload)]


# One exchange as a plain dictionary, for the JSON output and for the text report.
def describe(exchange: Exchange) -> dict[str, object]:
    request = exchange.request.frame.payload if exchange.request else None
    answer = exchange.answer.frame.payload if exchange.answer else None
    return {
        "command": f"{exchange.command:02x}",
        "known": KNOWN_COMMANDS.get(exchange.command),
        "request": request.hex() if request is not None else None,
        "answer": answer.hex() if answer is not None else None,
        "text": text_in((request or b"") + b"\x00" + (answer or b"")),
        "time": _first_stamp(exchange),
    }


# The report: commands in order, the unknown ones first marked NEW, with the same payload shown once.
def report(exchanges: list[Exchange], unknown_only: bool) -> str:
    lines: list[str] = []
    shown: set[tuple[object, ...]] = set()
    for exchange in exchanges:
        info = describe(exchange)
        if unknown_only and info["known"]:
            continue
        key = (info["command"], info["request"], info["answer"])
        if key in shown:
            continue
        shown.add(key)
        label = info["known"] or "NEW"
        lines.append(f"command {info['command']}  [{label}]  {info['time']}")
        lines.append(f"  request: {info['request']}")
        lines.append(f"  answer:  {info['answer']}")
        if info["text"]:
            lines.append(f"  text:    {info['text']}")
    return "\n".join(lines) if lines else "No frames found."


# Read the log from a file or standard input and print the report.
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("log", nargs="?", help="log file; standard input when left out")
    parser.add_argument("--unknown-only", action="store_true", help="leave out the commands the hub decodes")
    parser.add_argument("--json", action="store_true", help="print JSON instead of text")
    args = parser.parse_args()
    text = Path(args.log).read_text() if args.log else sys.stdin.read()
    exchanges = pair_exchanges(find_frames(text))
    if args.json:
        print(
            json.dumps([describe(e) for e in exchanges if not (args.unknown_only and describe(e)["known"])], indent=1)
        )
    else:
        print(report(exchanges, args.unknown_only))


if __name__ == "__main__":
    main()
