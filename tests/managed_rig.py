"""Managed service rigs and real lifecycle helpers used by the control and recovery suites."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

from ocpp_hub_core.frames import build_call
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
