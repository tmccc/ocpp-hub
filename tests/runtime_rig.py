"""Real connection runtime peers, short transport timings and asynchronous observation."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.policy import Policy
from ocpp_hub_core.prototype.runtime import OfflineRuntime, RuntimeSettings
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection

from tests.prototype_firmware import Firmware
from tests.prototype_rig import BOOT
from tests.tls_support import accept_cap

NOW = "2030-01-01T00:00:00Z"


FAST = RuntimeSettings(
    retry_min=0.02, retry_max=0.1, pump_interval=0.01, ping_interval=0.2, ping_timeout=0.2, close_timeout=0.2
)


# Wait for an observable result with a single aggregate deadline; no test changes the engine under it.
async def until(check: Callable[[], bool], timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not check():
            await asyncio.sleep(0.01)


class Bench:
    """One independent fake cloud and the actual supervisor, with configurable real peer replies."""

    # Allocate private real journal state; the only connection addresses are generated literal loopback ports.
    def __init__(
        self,
        path: Path,
        tls_pair: tuple[ssl.SSLContext, ssl.SSLContext, Path],
        credential: Admission,
        policy: Policy | None = None,
    ) -> None:
        self.server_tls, self.client_tls, _ = tls_pair
        self.admission = credential
        self.store = Journal(path, credential.identity)
        self.gateway = Gateway(self.store, policy, timeout=0.15)
        self.calls: list[list[Any]] = []
        self.connections: list[ServerConnection] = []
        self.boot_status = "Accepted"
        self.boot_interval = 1
        self.drop_start = False
        self.firmware = Firmware()
        self.runtime: OfflineRuntime | None = None

    # The fake backend is a real websocket endpoint and never shares the gateway's correlation state.
    async def vendor(self, connection: ServerConnection) -> None:
        self.connections.append(connection)
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            if frame[2] == "BootNotification":
                answer = {"status": self.boot_status, "interval": self.boot_interval, "currentTime": NOW}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": NOW}
            elif frame[2] == "StartTransaction":
                if self.drop_start:
                    continue
                answer = {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))

    # Start the real runtime only after the fake cloud's ephemeral port exists.
    async def start(self, port: int, settings: RuntimeSettings = FAST) -> OfflineRuntime:
        self.runtime = OfflineRuntime(
            self.gateway,
            self.admission,
            f"wss://127.0.0.1:{port}/ocpp",
            self.server_tls,
            self.client_tls,
            settings,
        )
        return await self.runtime.start()

    # Open an authenticated fake charger without starting any background work on the actual household.
    def charger(self) -> Any:
        assert self.runtime is not None
        return connect(
            f"wss://127.0.0.1:{self.runtime.port}/charger/{self.admission.identity}",
            ssl=self.client_tls,
            additional_headers=[("Authorization", self.admission.authorization)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        )

    # Supply a synthetic introduction; no actual Boot was available in the recorded device export.
    async def boot(self, charger: Any) -> None:
        await charger.send(build_call("boot", "BootNotification", BOOT))
        assert decode(await charger.recv())[2]["status"] == "Accepted"
        cap = decode(await charger.recv())
        assert cap[2] == "SetChargingProfile"
        await accept_cap(charger, cap, self.firmware)
        await until(lambda: self.gateway.controls.ready)

    # Keep runtime teardown before journal close so cancelled tasks never touch a closed database.
    async def close(self) -> None:
        if self.runtime is not None:
            await self.runtime.close()
        self.store.close()
