"""Run the actual managed service and observe its own periodic publications through real local peers."""

import asyncio
import json
import signal
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.trial_session import TrialSession, save_private
from websockets.asyncio.client import connect

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_api import TOKEN, call
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, until
from tools import managed_proxy


@dataclass
class RunningService:
    """Only the fixture inputs and observations are exposed; tests never drive a controller tick."""

    charge: ChargeRig
    api: Any
    task: asyncio.Task[int]
    status_path: Path

    # Wait for a genuinely newer publication, failing immediately if the actual service exits.
    async def published(self, previous=0, *, state=None, desired=None, power=False):
        async with asyncio.timeout(18):
            while True:
                if self.task.done():
                    await self.task
                    raise AssertionError("service exited before the expected publication")
                if self.status_path.exists():
                    value = json.loads(self.status_path.read_text())
                    if (
                        value.get("checked_at", 0) > previous
                        and (state is None or value.get("state") == state)
                        and (desired is None or value.get("desired_control") == desired)
                        and (not power or value.get("power_w", 0) > 0)
                    ):
                        return value
                await asyncio.sleep(0.02)

    # Close the independently generated lifecycle with its real report and wait for its real cloud ACK.
    async def close_daily(self, local):
        b = self.charge
        response = await b.rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        assert response[0] == 3
        b.rig.peer.permission_enabled = False
        await until(lambda: b.session.journal.active() is None and b.session.journal.next_delivery() is None)


# Supply only fixture endpoints and capture actual signal callbacks; all service, API and worker code stays real.
@asynccontextmanager
async def running_service(tmp_path, tls, ports, monkeypatch, current):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls, ports(), monkeypatch, current, cli_route=True) as original:
        await original.controller.current.close()
        await original.controller.tests.close()
        await crash(original.session)
        rig, sessions, controllers, callbacks = original.rig, [], [], {}
        loop = asyncio.get_running_loop()

        # Construct the real service-owned session with the existing fast loopback transport timings.
        def create_session(prepared, **kwargs):
            session = TrialSession(prepared, FAST, **kwargs)
            sessions.append(session)
            rig.sessions.append(session)
            return session

        # Retain the real controller only to inspect durable receipts and observe fault-injection boundaries.
        def create_controller(session):
            controller = ManagedController(session)
            controllers.append(controller)
            return controller

        monkeypatch.setattr(managed_proxy, "preflight", lambda _: rig.prepared)
        monkeypatch.setattr(managed_proxy, "TrialSession", create_session)
        monkeypatch.setattr(managed_proxy, "ManagedController", create_controller)
        monkeypatch.setattr(managed_proxy, "notify", lambda _: None)
        monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: callbacks.update({sig: callback}))
        monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)
        spec, settings, secret = (
            tmp_path / name for name in ("serve-native.json", "serve-api.json", "serve-token.json")
        )
        save_private(spec, rig.spec.value())
        save_private(secret, {"token": TOKEN})
        port = ports()
        save_private(settings, {"host": "127.0.0.1", "port": port, "token_file": str(secret)})
        task = asyncio.create_task(managed_proxy.serve(tmp_path / "trial.json", spec, settings))
        try:
            await until(lambda: sessions and sessions[0].started and controllers)
            session, controller = sessions[0], controllers[0]
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                async with connect(
                    f"ws://127.0.0.1:{port}/management",
                    additional_headers={"Authorization": "Bearer " + TOKEN},
                    subprotocols=["ics-proxy.v1"],
                    proxy=None,
                ) as api:
                    service = RunningService(
                        ChargeRig(rig, session, controller, charger),
                        api,
                        task,
                        rig.prepared.config.journal.with_suffix(".status.json"),
                    )
                    first = await service.published(state="healthy")
                    if first["desired_control"] == "paused":
                        response = await call(
                            api, "app_control", id=uuid.uuid4().hex, revision=first["control_revision"]
                        )
                        assert response["ok"]
                        first = await service.published(first["checked_at"], state="healthy", desired="app")
                    assert first["station_ceiling_a"] == current
                    try:
                        yield service
                    finally:
                        rig.peer.unavailable = False
                        local = session.journal.active()
                        if local is not None and not task.done():
                            await service.close_daily(local)
        finally:
            rig.peer.unavailable = False
            if not task.done():
                callbacks[signal.SIGTERM]()
                result = await asyncio.wait_for(task, 10)
                assert result == 0
