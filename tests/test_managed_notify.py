"""Supervisor notification backpressure cannot delay the actual asyncio watchdog or other deadlines."""

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROBE = r'''
import asyncio, json, os, socket, sys, time
name = sys.argv[1]
address = "\0" + name

# This child has one synthetic local destination and no permitted IP or DNS operations.
def guard(event, args):
    if event in ("socket.getaddrinfo", "socket.gethostbyname", "socket.gethostbyaddr", "socket.getnameinfo"):
        raise PermissionError("notification probe permits no name lookups")
    if event in ("socket.bind", "socket.connect", "socket.sendto", "socket.sendmsg"):
        if args[0].family != socket.AF_UNIX or os.fsdecode(args[1]) != address:
            raise PermissionError("notification probe permits its synthetic Unix destination only")

sys.addaudithook(guard)
os.environ["NOTIFY_SOCKET"] = "@" + name
from tools.managed_proxy import watchdog

# Real socket backpressure must not prevent an unrelated loop deadline from firing.
async def run():
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as receiver, \
         socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as filler:
        receiver.bind(address)
        filler.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        filler.setblocking(False)
        filled = 0
        if sys.argv[2] == "full":
            while filled < 10000:
                try:
                    filler.sendto(b"synthetic prior notification", address)
                    filled += 1
                except BlockingIOError:
                    break
            assert 0 < filled < 10000
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as check:
                check.setblocking(False)
                try:
                    check.sendto(b"independent capacity check", address)
                except BlockingIOError:
                    pass
                else:
                    raise AssertionError("receiver queue was not full")
        stopped, deadline = asyncio.Event(), asyncio.Event()
        loop = asyncio.get_running_loop()
        start = time.monotonic()
        loop.call_later(0.1, deadline.set)
        task = asyncio.create_task(watchdog(stopped))
        await asyncio.wait_for(deadline.wait(), 1)
        elapsed = time.monotonic() - start
        stopped.set()
        await task
        receiver.setblocking(False)
        packets = []
        while True:
            try:
                packets.append(receiver.recv(65536))
            except BlockingIOError:
                break
        assert packets == ([b"synthetic prior notification"] * filled if filled else [b"WATCHDOG=1"])
        print(json.dumps({"elapsed": elapsed, "filled": filled, "packets": len(packets)}))

asyncio.run(run())
'''


# Intent: full and empty real supervisor queues both leave the actual watchdog's event loop responsive.
# Why: a synchronous notification must be best effort, including when its receiver remains open but stops draining.
@pytest.mark.skipif(sys.platform != "linux", reason="Linux abstract Unix datagram namespace")
@pytest.mark.parametrize("filled", [False, True])
def test_notification_receiver_backpressure_does_not_block_deadlines(filled):
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROBE, "ics-notify-" + uuid.uuid4().hex, "full" if filled else "empty"],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    evidence = json.loads(result.stdout)
    assert evidence["elapsed"] < 0.8
    assert bool(evidence["filled"]) is filled
