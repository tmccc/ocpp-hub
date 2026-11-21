"""TLS and native peers for a real managed charging lifecycle."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype import managed_charging as charge_tests
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.connected_rig import ConnectedBench
from tests.handover_helpers import NativeVehiclePeer
from tests.managed_rig import OwnerBench, healthy, introduced, managed_bench, managed_session, starts
from tests.prototype_rig import START
from tests.runtime_rig import until


@dataclass
class ChargeRig:
    """One real managed service and independent charger/cloud/native peers retain their own state."""

    rig: ConnectedBench
    session: TrialSession
    controller: ManagedController
    charger: Any


# Keep test durations short while using the real protocol, native observer, ledger and physical profile checks.
@asynccontextmanager
async def ready(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
    duration: float = 0.15,
    startup: float = 3,
    owner_tag: str | None = None,
    cli_route: bool = False,
    initial_history: list[dict[str, Any]] | None = None,
) -> AsyncIterator[ChargeRig]:
    monkeypatch.setattr(charge_tests, "DURATION", duration)
    monkeypatch.setattr(charge_tests, "STARTUP_BUDGET", startup)
    async with managed_bench(
        directory,
        contexts,
        port,
        cli_route=cli_route,
        native_peer=native_peer,
        bench_type=bench_type,
        initial_history=initial_history,
    ) as rig:
        session = managed_session(rig)
        if owner_tag is not None:
            gateway = session.gateway
            gateway.policy = replace(gateway.policy, allowed_tags=frozenset({owner_tag, *gateway.policy.allowed_tags}))
            gateway.controls.policy = gateway.policy
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "test-charge-boot")
            await healthy(controller)
            try:
                yield ChargeRig(rig, session, controller, charger)
            finally:
                await controller.charging.close()
                if controller.pause_task is not None:
                    await asyncio.gather(controller.pause_task, return_exceptions=True)
                await session.close()


# Create an actual owner-cloud Start when requested; arming alone never issues that command.
async def owner_start(test: ChargeRig) -> None:
    assert isinstance(test.rig.bench, OwnerBench)
    reply = await test.rig.bench.cloud_call(
        "cloud-two-minute-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
    )
    assert reply[0] == 3 and reply[2]["status"] == "Accepted"


# Send a real charger lifecycle and let the independent native peer report genuine bounded power.
async def positive_flow(
    test: ChargeRig,
    source: str = "local",
    *,
    call_id: str = "actual-test-start",
    meter_start: int | None = None,
    timestamp: str | None = None,
) -> int:
    if source == "daily":
        tag = "NATIVE_TIMER"
    elif source == "local":
        tag = starts(test.rig)[-1][3]["idTag"]
    else:
        tag = "CLOUD_OWNER"
    payload = START | {"idTag": tag} | ({} if meter_start is None else {"meterStart": meter_start})
    if timestamp is not None:
        payload["timestamp"] = timestamp
    answer = await test.rig.bench.call(test.charger, call_id, "StartTransaction", payload)
    assert answer[0] == 3, answer
    assert answer[2]["idTagInfo"]["status"] == "Accepted"
    local = answer[2]["transactionId"]
    test.rig.peer.plan_session = (local, 9 if source == "daily" else 987654)
    await until(
        lambda: test.session.journal.vendor_id(local) is not None and test.session.journal.next_delivery() is None
    )
    test.rig.peer.paused, test.rig.peer.permission_enabled = False, True
    test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
    test.rig.bench.firmware.fire_plan(9)
    return local


# A real StopTransaction closes the independent peer only after the physical Stop command has arrived.
async def report_actual_stop(test: ChargeRig, local: int) -> None:
    await asyncio.wait_for(test.rig.bench.remote_stop.wait(), 3)
    assert test.rig.bench.firmware.current == 0
    reply = await test.rig.bench.call(
        test.charger,
        "actual-test-stop",
        "StopTransaction",
        {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
    )
    assert reply[0] == 3
    test.rig.peer.permission_enabled = False
    await until(lambda: test.session.journal.active() is None and test.session.journal.next_delivery() is None)


# Use the ordinary healthy barrier for tests that have no latched fault to clear.
async def actual_stop(test: ChargeRig, local: int) -> dict[str, Any]:
    await report_actual_stop(test, local)
    return await healthy(test.controller)


# Ordinary managed Start uses this explicitly approved owner identity.
owner_rig = partial(ready, owner_tag="APP_OWNER")
