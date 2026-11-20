"""Local process and reply controls for the simulated owner workflow."""

import asyncio
import json
import sys

from aiohttp import WSMsgType, web
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


class HomeAssistantProcess:
    """Run new Home Assistant processes over one saved configuration."""

    # Give each process its own transcript while leaving its config directory in place.
    def __init__(self, directory, port):
        self.directory, self.port = directory, port
        self.generation = 0
        self.process = None

    # Start an actual HA process and wait for its first settled entity states.
    async def start(self):
        self.generation += 1
        self.log = self.directory / f"ha-{self.generation}.log"
        with self.log.open("w") as log:
            self.process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-B",
                "-m",
                "tests_ha_proxy.owner_workflow_child",
                str(self.directory / "home-assistant"),
                str(self.port),
                str(self.generation),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=log,
            )
        return await self.read()

    # Every test request consumes the corresponding reply from the actual HA child.
    async def read(self):
        async with asyncio.timeout(35):
            raw = await self.process.stdout.readline()
        assert raw, self.log.read_text()[-6000:]
        return json.loads(raw)

    # Ask the actual child to refresh or to press a registered Home Assistant button.
    async def request(self, command):
        self.process.stdin.write(command.encode() + b"\n")
        await self.process.stdin.drain()
        return await self.read()

    # Let HA finish saving its configuration before the same directory is reopened by a new process.
    async def stop(self):
        if self.process is not None and self.process.returncode is None:
            self.process.stdin.close()
            try:
                async with asyncio.timeout(15):
                    await self.process.wait()
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
            assert self.process.returncode == 0, self.log.read_text()[-6000:]


class LostCommandReply:
    """Forward local HA exchanges to the real proxy and withhold one command reply."""

    # Capture owner commands without changing their IDs, revisions or response bodies.
    def __init__(self, child, command, *, commit_gate=None):
        self.child, self.command, self.commit_gate = child, command, commit_gate
        self.control_requests = []
        self.receipt_requests = []
        self.command_ready = asyncio.Event()
        self.reply_ready = asyncio.Event()
        self.drop_reply = asyncio.Event()
        self.command_reply = None

    # Pass one request and its reply unchanged, except for the selected command response.
    async def endpoint(self, request):
        socket = web.WebSocketResponse(protocols=["ics-proxy.v1"])
        await socket.prepare(request)
        message = await socket.receive()
        if message.type != WSMsgType.TEXT:
            return socket
        value = message.json()
        command = value["command"]
        selected = command == self.command and not self.command_ready.is_set()
        if command == "receipt":
            self.receipt_requests.append(value)
        elif command not in ("status", "history"):
            self.control_requests.append(value)
        try:
            async with connect(
                f"ws://127.0.0.1:{self.child.port}/management",
                additional_headers={"Authorization": request.headers["Authorization"]},
                subprotocols=["ics-proxy.v1"],
                proxy=None,
            ) as peer:
                if selected and self.commit_gate is not None:
                    await self.child.arm(self.commit_gate, value["id"])
                await peer.send(message.data)
                if selected:
                    self.command_ready.set()
                reply = await peer.recv()
        except OSError, ConnectionClosed:
            return socket
        if selected:
            self.command_reply = json.loads(reply)
            self.reply_ready.set()
            await self.drop_reply.wait()
            return socket
        await socket.send_str(reply)
        return socket

    # Bind only the temporary loopback port passed to this test's actual Home Assistant config.
    async def start(self):
        app = web.Application()
        app.router.add_get("/management", self.endpoint)
        self.runner = web.AppRunner(app, shutdown_timeout=1)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        return self.runner.addresses[0][1]

    # Release a held reply before closing the fixture in case a check failed early.
    async def close(self):
        self.drop_reply.set()
        await self.runner.cleanup()
