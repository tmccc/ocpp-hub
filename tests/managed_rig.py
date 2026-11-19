"""Managed service rigs and real lifecycle helpers used by the control and recovery suites."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

from ocpp_hub_core.frames import build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.trial_session import TrialSession
from websockets.asyncio.server import ServerConnection

from tests.connected_rig import ConnectedBench, connected_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.prototype_rig import BOOT
from tests.runtime_rig import FAST, until
from tests.trial_peer import Bench


class OwnerBench(Bench):
    """The independent cloud may issue owner commands over its actual TLS connection."""

    # Retain only the synthetic cloud socket while the ordinary peer independently answers proxy requests.
    async def vendor(self, connection: ServerConnection) -> None:
        self.cloud = connection
        await super().vendor(connection)

    # Correlate an actual TLS command with the reply observed by the independently running cloud receiver.
    async def cloud_call(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        await self.cloud.send(build_call(token, action, payload))
        await until(lambda: any(frame[0] in (3, 4) and frame[1] == token for frame in self.calls))
        return next(frame for frame in self.calls if frame[0] in (3, 4) and frame[1] == token)


class CloudReplyPeer:
    """Only the independent cloud's Stop response changes; engine, journal and TLS transport stay real."""

    # Record actions by actual correlated RPC identity before the peer sends its response.
    def __init__(self, connection, bench):
        self.connection, self.bench, self.actions = connection, bench, {}

    # Keep the websocket interface and its ordinary independent vendor behavior intact.
    def __getattr__(self, name):
        return getattr(self.connection, name)

    # Observe actual Stop transmissions without replacing a gateway or RPC method.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2:
                self.actions[frame[1]] = frame[2]
                if frame[2] == "StopTransaction":
                    self.bench.stop_wires = getattr(self.bench, "stop_wires", []) + [frame]
            yield raw

    # A retry still needs a real response on this socket; operator proof cannot manufacture that response.
    async def send(self, raw):
        frame = decode(raw)
        if frame[0] == 3 and self.actions.get(frame[1]) == "StopTransaction":
            gate = getattr(self.bench, "stop_gate", None)
            if gate is not None and not gate.is_set():
                waits = getattr(self.bench, "held_stop_replies", [])
                self.bench.held_stop_replies = [*waits, frame[1]]
                await gate.wait()
            info = getattr(self.bench, "stop_info", None)
            if getattr(self.bench, "stop_error", False):
                raw = build_call_error(frame[1], "InternalError", "synthetic cloud refusal")
            elif info is not None:
                raw = build_call_result(frame[1], {"idTagInfo": info})
        await self.connection.send(raw)


class ReplyBench(OwnerBench):
    """Use the same independent cloud implementation behind its transparent response wrapper."""

    # Keep ordinary cloud Boot/Authorize/Start behavior, changing only the final Stop reply at the peer.
    async def vendor(self, connection):
        await super().vendor(CloudReplyPeer(connection, self))


# Share the connected native bench while approving one synthetic timer identity for the owner-control tests.
@asynccontextmanager
async def managed_bench(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
    time_zone: str | None = None,
    initial_history: list[dict[str, Any]] | None = None,
    cli_route: bool = False,
) -> AsyncIterator[ConnectedBench]:
    async with connected_bench(
        directory,
        contexts,
        port,
        initial_history=initial_history,
        time_zone=time_zone,
        native_peer=native_peer,
        bench_type=bench_type,
    ) as rig:
        # Synthetic schedule identity is approved without changing private input files or production policy.
        rig.prepared.config = replace(rig.prepared.config, local_tags=frozenset({"NATIVE_TIMER"}))
        if cli_route:
            value = rig.spec.value()
            value["trial_route"]["sURL"] = rig.prepared.config.charger_path.rsplit("/", 1)[0] + "/"
            rig.spec = type(rig.spec).parse(value)
        yield rig


# Register a real bounded session for teardown with the assistant Start path disabled from construction.
def managed_session(rig: ConnectedBench) -> TrialSession:
    session = TrialSession(
        rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False, allow_unplugged_native=True
    )
    rig.sessions.append(session)
    return session


# Wait for genuine device Boot and cloud acceptance before inspecting introduction or release outcomes.
async def introduced(rig: ConnectedBench, session: TrialSession, charger: Any, token: str) -> None:
    assert (await rig.bench.call(charger, token, "BootNotification", BOOT))[0] == 3
    await until(lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted")


# Allow ordinary correlated work to settle without replacing fresh native evidence or manufacturing acknowledgements.
async def healthy(controller: ManagedController) -> dict[str, Any]:
    async with asyncio.timeout(5):
        while True:
            status = await controller.tick()
            if status["state"] == "healthy":
                return status
            assert status["state"] not in ("attention", "stopping"), status
            await asyncio.sleep(0.02)


# Wait for a permitted physical offer while independently retaining the unhealthy unbound cloud-Start obligation.
async def pending_cloud_offer(controller: ManagedController, limit: int) -> dict[str, Any]:
    async with asyncio.timeout(5):
        while True:
            status = await controller.tick()
            assert status["cloud_start_pending"] is True and status.get("local_healthy") is not True
            assert status["state"] not in ("attention", "stopping"), status
            if status.get("station_ceiling_a") == limit and status.get("owner_control_enabled") is True:
                assert status["state"] == "degraded" and status["phase"] == "active"
                return status
            await asyncio.sleep(0.02)


# Count the physical peer's actual Start requests independently from the supervisor's saved state.
def starts(rig: ConnectedBench) -> list[list[Any]]:
    return [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStartTransaction"]
