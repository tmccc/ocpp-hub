"""Follow a Home Assistant timed Start through a lost reply, replacement and exact physical Stop."""

import asyncio
import time
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import managed_charging
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.security import SecurityError
from websockets.asyncio.client import connect

from tests.charge_rig import positive_flow, report_actual_stop
from tests.current_rig import commissioned
from tests.managed_rig import healthy, starts
from tests.management_peer import TOKEN, call
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls
from tests_ha_proxy.owner_workflow_support import LostCommandReply
from tests_ha_proxy.test_client import setup
from tests_model.home_interactions import Exchange
from tests_model.system_interaction import System, system_edges
from tests_model.timed_interactions import Active


# Take an independently modelled event and keep the expected command and physical evidence separate.
def step(state, event):
    (decision,) = (edge for edge in system_edges(state) if edge.event == event)
    return decision.state


# Intent: replay both HA replacement orders against explicit Pause and automatic timed Stop.
# Why: losing a dashboard reply must leave control with the proxy and the real charger session it started.
@pytest.mark.parametrize("stop_by", ["pause", "deadline"])
@pytest.mark.parametrize("reload_first", [False, True])
async def test_ha_timed_start_stop_and_replacement(
    tmp_path, tls, unused_tcp_port_factory, actual_hass, monkeypatch, stop_by, reload_first
):
    async with commissioned(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, 6) as rig:
        offset = 0.0
        monkeypatch.setattr(managed_charging, "DURATION", 60)
        monkeypatch.setattr(
            managed_charging, "time", SimpleNamespace(monotonic=lambda: time.monotonic() + offset, time=time.time)
        )
        controller, journal = rig.controller, rig.session.journal
        status, model = await healthy(controller), step(System(), "current installation becomes usable")
        with pytest.raises(SecurityError, match="managed current capability"):
            await rig.session.command({"command": "native-bounded-handover"})
        port = unused_tcp_port_factory()
        server = ManagementServer(
            ManagementSettings(port, tmp_path / "synthetic-token", TOKEN),
            controller,
            lambda: dict(status, running=True, certificate_remaining_seconds=86400),
        )
        relay = LostCommandReply(SimpleNamespace(port=port), "start_test")
        await server.start()
        press = pause = returned = selected = None

        # Call the exported HA button service through the actual config entry.
        async def button(action):
            await actual_hass.services.async_call(
                "button",
                "press",
                {"entity_id": f"button.ics_proxy_{action}"},
                blocking=True,
            )

        # Reload only Home Assistant's config entry; the ongoing test belongs to the original proxy.
        async def reload_entry(entry):
            nonlocal model
            prior = entry.runtime_data
            await actual_hass.config_entries.async_reload(entry.entry_id)
            model = step(model, "entry unloaded or disabled")
            model = step(step(model, "fresh entry setup begins"), "fresh exact full status received")
            assert entry.runtime_data is not prior
            assert len(starts(rig.rig)) == 1

        # Exercise the proxy's own admission checks independently of buttons being hidden in HA.
        async def management(action, **fields):
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
                proxy=None,
            ) as socket:
                return await call(socket, action, revision=controller.requests.intent().revision, **fields)

        try:
            entry = await setup(actual_hass, {"port": await relay.start()})
            model = step(step(model, "fresh exact full status received"), "owner presses start_test")
            press = asyncio.create_task(button("start_test"))
            await asyncio.wait_for(relay.reply_ready.wait(), 5)
            (request,) = relay.control_requests
            assert relay.command_reply["result"]["state"] == "accepted"
            model = step(step(model, "local test accepted"), "local Start sent")
            await controller.charging.settle()
            model = step(model, "matching Start accepted")
            assert len(starts(rig.rig)) == 1 and not press.done()
            assert controller.requests.receipt(request["id"])["state"] == "accepted"

            local = await positive_flow(rig)
            model = step(model, "exact test lifecycle reported")
            assert model.timed.active is Active.TEST and journal.active() == local
            status = await healthy(controller)
            model = step(step(model, "matching power first observed"), "original power proof saved")
            assert status["power_w"] > 10 and model.timed.flow_saved
            assert controller.requests.receipt(request["id"])["state"] == "completed"
            for command, fields in (("start", {}), ("set_current", {"current_a": 16}), ("arm_cloud_test", {})):
                assert (await management(command, **fields))["ok"] is False
            assert (await management("app_control"))["ok"] is False
            assert controller.current.selected() == 6
            if reload_first:
                await reload_entry(entry)
                assert not rig.rig.bench.remote_stop.is_set()

            if stop_by == "pause":
                pause = asyncio.create_task(button("pause"))
                await until(lambda: controller.requests.intent().mode == "paused")
                model = step(model, "new Pause committed")
            else:
                # No client request or supervision tick is needed to start the timed Stop.
                lease = controller.charging.lease
                assert lease is not None
                offset = lease.deadline - time.monotonic() + 0.01
                controller.charging.changed.set()
                await asyncio.wait_for(rig.rig.bench.remote_stop.wait(), 4)
                model = step(model, "nominal duration elapsed")
            assert model.timed.paused and controller.requests.intent().mode == "paused"
            await report_actual_stop(rig, local)
            model = step(model, "timed Stop sent to matching session")
            model = step(step(model, "matching ledger Stop"), "matching native closure")
            status = await healthy(controller)
            model = step(step(model, "fresh station zero"), "exact timed settlement saved")
            assert model.timed.zero and status["local_hold_verified"] and status["power_w"] == 0
            assert [
                frame[3]["transactionId"] for frame in rig.rig.bench.device_calls if frame[2] == "RemoteStopTransaction"
            ] == [local]
            if pause is not None:
                await asyncio.wait_for(pause, 5)
            if not reload_first:
                await reload_entry(entry)

            relay.drop_reply.set()
            await asyncio.wait_for(press, 5)
            model = step(step(model, "first reply lost"), "exact completion read")
            assert model.home.exchange is Exchange.OK
            assert any(row["request_id"] == request["id"] for row in relay.receipt_requests)
            assert sum(row["command"] == "start_test" for row in relay.control_requests) == 1
            await entry.runtime_data.async_refresh()
            assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"

            returned = asyncio.create_task(button("app_control"))
            await until(lambda: controller.requests.pending_return() is not None)
            status = await healthy(controller)
            await asyncio.wait_for(returned, 5)
            model = step(model, "verified Return completed")
            assert not model.timed.paused and status["station_ceiling_a"] == 6
            await entry.runtime_data.async_refresh()
            selected = asyncio.create_task(
                actual_hass.services.async_call(
                    "number",
                    "set_value",
                    {"entity_id": "number.ics_proxy_charge_current_a", "value": 16},
                    blocking=True,
                )
            )
            await until(lambda: controller.current.selected() == 16 or controller.current.pending())
            await controller.current.settle()
            status = await healthy(controller)
            await asyncio.wait_for(selected, 5)
            model = step(model, "selected current changed")
            assert not model.timed.six_amps and status["station_ceiling_a"] == 16
            assert (await management("start_test"))["ok"] is False

            following = await positive_flow(
                rig,
                "daily",
                call_id="next-scheduled-start",
                meter_start=2000,
                timestamp="2026-01-01T00:02:00Z",
            )
            assert following != local and (await healthy(controller))["current_a"] == 16
            response = await rig.rig.bench.call(
                rig.charger,
                uuid.uuid4().hex,
                "StopTransaction",
                {"transactionId": following, "timestamp": START["timestamp"], "meterStop": 2100},
            )
            assert response[0] == 3
            await until(lambda: journal.active() is None and journal.next_delivery() is None)
            assert len(starts(rig.rig)) == 1
        finally:
            tasks = [task for task in (press, pause, returned, selected) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if relay.runner is not None:
                await relay.close()
            await server.close()
