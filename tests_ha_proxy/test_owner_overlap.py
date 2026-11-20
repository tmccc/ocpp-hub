"""Actual Home Assistant commands at the proxy's owner-control boundaries."""

import asyncio
from contextlib import AsyncExitStack

import pytest
from homeassistant.exceptions import HomeAssistantError

from tests.managed_rig import starts
from tests.model_tls_support import model_tls as model_tls
from tests.model_tls_support import tls_for_case
from tests.native_peer_failures import InterruptedNativePeer
from tests.process_helpers import process_rig
from tests.recovery_barriers import require_zero_held
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests_ha_proxy.conftest import actual_core
from tests_ha_proxy.owner_workflow_child import press, view
from tests_ha_proxy.owner_workflow_support import LostCommandReply
from tests_ha_proxy.test_client import setup

CURRENT = "number.ics_proxy_charge_current_a"


class PausedCurrentPeer(OneNativeResponseGate, InterruptedNativePeer):
    """Hold one native response after the intended current request is saved."""

    pending = None

    # Ignore reads before the real management command commits its pending record.
    async def handle(self, reader, writer):
        if self.pending is not None and self.pending():
            self.pending = None
            self.arm = True
        await super().handle(reader, writer)


# Use the exact requested UUID to distinguish these controls from commissioning.
def saved_request(proxy, identifier):
    return next((row for row in proxy.durable()["requests"] if row["id"] == identifier), None)


# Invoke the same registered number service an owner uses.
async def select_current(hass, value):
    await hass.services.async_call("number", "set_value", {"entity_id": CURRENT, "value": value}, blocking=True)


# Intent: HA recovers a 16 A command whose initial reply disappears after the real proxy applies it.
# Why: the saved result must settle the original UI action without a second charger write.
async def test_owner_current_recovers_reply_after_charger_applies_limit(
    tmp_path, model_tls, unused_tcp_port_factory, monkeypatch
):
    async with (
        process_rig(
            tmp_path,
            tls_for_case(tmp_path, model_tls),
            unused_tcp_port_factory(),
            unused_tcp_port_factory(),
            monkeypatch,
            32,
            timing="fast-poll",
        ) as proxy,
        actual_core(tmp_path / "ha") as hass,
        AsyncExitStack() as cleanup,
    ):
        await proxy.start()
        await proxy.status(lambda data: data["can_set_current"] and data["station_ceiling_a"] == 32)
        relay = LostCommandReply(proxy, "set_current")
        port = await relay.start()
        cleanup.push_async_callback(relay.close)
        entry = await setup(hass, {"port": port})
        baseline = len(proxy.rig.bench.device_calls)
        selection = asyncio.create_task(select_current(hass, 16))
        try:
            await asyncio.wait_for(relay.reply_ready.wait(), 5)
            (command,) = relay.control_requests
            assert relay.command_reply["result"]["id"] == command["id"]
            assert relay.command_reply["result"]["state"] == "accepted"
            receipt = await proxy.completed_control(command["id"])
            assert receipt["result"] == {"current_a": 16, "charging_started": False}
            await proxy.status(lambda data: data["can_set_current"] and data["station_ceiling_a"] == 16)
            assert proxy.rig.bench.firmware.effective_limit() == 16 and not selection.done()
            relay.drop_reply.set()
            await selection
            await entry.runtime_data.async_refresh()
            assert float(hass.states.get(CURRENT).state) == 16
            (lookup,) = relay.receipt_requests
            assert lookup["request_id"] == command["id"] and lookup["id"] != command["id"]
            assert relay.control_requests == [command]
            assert [
                frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                for frame in proxy.rig.bench.device_calls[baseline:]
                if frame[2] == "SetChargingProfile"
            ] == [16]
            assert not starts(proxy.rig)
        finally:
            if not selection.done():
                selection.cancel()
            await asyncio.gather(selection, return_exceptions=True)


# Wait for fresh Resume and number availability before trusting the display.
async def verify_pause(proxy, hass, entry, selected):
    await proxy.status(lambda data: data["local_hold_verified"] and data["can_return"] and data["can_set_current"])
    assert await view(hass, entry) == {"entry_id": entry.entry_id, "mode": "Paused locally", "return_available": True}
    assert float(hass.states.get(CURRENT).state) == selected
    assert proxy.rig.bench.firmware.effective_limit() == 0


# Intent: a current selection and a delayed Return can overlap a newer Pause and a proxy replacement.
# Why: the late Return must fail, and the still unverified setting must never become permission to charge.
async def test_owner_current_and_delayed_return_yield_to_pause(
    tmp_path, model_tls, unused_tcp_port_factory, monkeypatch
):
    async with (
        process_rig(
            tmp_path,
            tls_for_case(tmp_path, model_tls),
            unused_tcp_port_factory(),
            unused_tcp_port_factory(),
            monkeypatch,
            16,
            native_peer=PausedCurrentPeer,
            timing="fast-poll",
        ) as proxy,
        actual_core(tmp_path / "ha") as hass,
    ):
        await proxy.start()
        await proxy.status()
        entry = await setup(hass, {"port": proxy.port})
        assert await press(hass, "pause") == {"ok": True}
        await verify_pause(proxy, hass, entry, 16)
        zero_mark = len(proxy.rig.bench.device_calls)
        return_waiting, forward_return = asyncio.Event(), asyncio.Event()
        pause_waiting = asyncio.Event()
        peer = proxy.rig.peer
        client = entry.runtime_data.client
        ordinary_request = client.request
        commands = []

        # Delay the older Return before sending it and observe the exact newer Pause commit.
        async def forward(command, **fields):
            if command in ("app_control", "set_current", "pause"):
                commands.append((command, fields))
            if command == "app_control" and not return_waiting.is_set():
                return_waiting.set()
                await forward_return.wait()
            elif command == "set_current":
                # The held reply is from the independent native peer after this exact commit.
                def pending():
                    change = proxy.durable()["metadata"]["managed_current_change"]
                    return change["id"] == fields["id"] and change["state"] == "pending"

                peer.pending = pending
            elif command == "pause":
                await proxy.arm("after-pause-request-commit", fields["id"])
                pause_waiting.set()
            return await ordinary_request(command, **fields)

        monkeypatch.setattr(client, "request", forward)
        async with asyncio.TaskGroup() as tasks:
            stale_return = tasks.create_task(press(hass, "app_control"))
            try:
                await asyncio.wait_for(return_waiting.wait(), 5)

                # The procedure loses the proxy after acceptance and while physical confirmation is pending.
                async def interrupted_selection():
                    message = "request was recorded; physical confirmation is still pending"
                    with pytest.raises(HomeAssistantError, match=message):
                        await select_current(hass, 32)

                selection = tasks.create_task(interrupted_selection())
                await asyncio.wait_for(peer.entered.wait(), 5)
                assert peer.gated_request["action"] == "getConfig"
                (_, old_return), (_, current) = commands
                assert current["revision"] == old_return["revision"]
                assert saved_request(proxy, current["id"])["state"] == "accepted"
                forward_return.set()
                assert "could not confirm this request" in (await stale_return)["error"]
                assert saved_request(proxy, old_return["id"]) is None
                assert (await view(hass, entry))["return_available"] is False
                assert hass.states.get(CURRENT).state == "unavailable"
                require_zero_held(proxy.rig, zero_mark)

                newer_pause = tasks.create_task(press(hass, "pause"))
                await asyncio.wait_for(pause_waiting.wait(), 5)
                _, pause = commands[2]
                await proxy.gate("after-pause-request-commit", pause["id"])
                saved = proxy.durable()
                intent = saved["metadata"]["managed_control_intent"]
                claim = saved["metadata"]["managed_owner_pause"]
                assert intent == {"mode": "paused", "revision": pause["revision"] + 1}
                assert claim["revision"] == intent["revision"]
                assert saved["metadata"]["managed_current_change"]["state"] == "pending"
                assert saved_request(proxy, pause["id"])["state"] == "accepted"
                await proxy.kill()
                assert "outcome is unknown" in (await newer_pause)["error"]
                await selection
            finally:
                forward_return.set()
                peer.release.set()

        await proxy.start()
        await proxy.completed_control(pause["id"])
        await verify_pause(proxy, hass, entry, 16)
        after = proxy.durable()["metadata"]
        assert after["managed_owner_pause"] == claim and after["managed_control_intent"] == intent
        assert after["managed_current_change"]["state"] == "cancelled"
        receipt = (await proxy.request("receipt", request_id=current["id"]))["result"]
        assert receipt["state"] == "failed" and receipt["result"] == {"classification": "process_replaced"}
        require_zero_held(proxy.rig, zero_mark)
        assert await press(hass, "app_control") == {"ok": True}
        assert [action for action, _ in commands] == ["app_control", "set_current", "pause", "app_control"]
        assert saved_request(proxy, old_return["id"]) is None
        assert saved_request(proxy, commands[-1][1]["id"])["state"] == "completed"
        assert len(await proxy.scheduled_sessions(16, True)) == 2
        assert not starts(proxy.rig)


# Intent: compare the two sides of Return's saved success after actual Home Assistant current selection.
# Why: a physical positive ceiling already exists at both cuts, but only the committed Return owns it after restart.
@pytest.mark.parametrize("committed", [False, True], ids=["before-commit", "after-commit"])
async def test_owner_return_commit_survives_lost_confirmation(
    tmp_path, model_tls, unused_tcp_port_factory, monkeypatch, committed
):
    async with (
        process_rig(
            tmp_path,
            tls_for_case(tmp_path, model_tls),
            unused_tcp_port_factory(),
            unused_tcp_port_factory(),
            monkeypatch,
            native_peer=InterruptedNativePeer,
            timing="fast-poll",
        ) as proxy,
        actual_core(tmp_path / "ha") as hass,
    ):
        await proxy.start()
        await proxy.status()
        entry = await setup(hass, {"port": proxy.port})
        assert await press(hass, "pause") == {"ok": True}
        await verify_pause(proxy, hass, entry, 6)
        await select_current(hass, 16)
        await verify_pause(proxy, hass, entry, 16)

        phase = "after-return-success-commit" if committed else "before-return-success-commit"
        client = entry.runtime_data.client
        ordinary_request = client.request
        requests = []
        return_waiting = asyncio.Event()

        # Observe the exact UUID made by HA and arm before forwarding its first actual Return.
        async def forward(command, **fields):
            if command == "app_control":
                if not requests:
                    await proxy.arm(phase, fields["id"])
                requests.append(fields["id"])
                return_waiting.set()
            return await ordinary_request(command, **fields)

        with monkeypatch.context() as patch:
            patch.setattr(client, "request", forward)
            async with asyncio.TaskGroup() as tasks:
                returning = tasks.create_task(press(hass, "app_control"))
                await asyncio.wait_for(return_waiting.wait(), 5)
                (identifier,) = requests
                gate = await proxy.gate(phase, identifier)
                assert gate["transaction_open"] is not committed
                assert saved_request(proxy, identifier)["state"] == ("completed" if committed else "running")
                intent = proxy.durable()["metadata"]["managed_control_intent"]
                assert intent["mode"] == ("app" if committed else "paused")
                assert proxy.rig.bench.firmware.effective_limit() == 16
                assert proxy.rig.bench.firmware.current == 0 and not starts(proxy.rig)
                await proxy.kill()
                assert "request was recorded; physical confirmation is still pending" in (await returning)["error"]
                assert requests == [identifier]

        assert (await view(hass, entry))["mode"] == "unavailable"
        await proxy.start()
        await proxy.status(lambda data: data["charger_connected"])
        assert saved_request(proxy, identifier)["state"] == ("completed" if committed else "uncertain")
        if not committed:
            await verify_pause(proxy, hass, entry, 16)
            assert await press(hass, "pause") == {"ok": True}
            assert saved_request(proxy, identifier)["state"] == "failed"
            await verify_pause(proxy, hass, entry, 16)
            assert await press(hass, "app_control") == {"ok": True}
        await proxy.status(lambda data: data["owner_control_enabled"] and data["station_ceiling_a"] == 16)
        assert (await view(hass, entry))["mode"] == "App / schedule"
        assert len(await proxy.scheduled_sessions(16, True)) == 2
        assert saved_request(proxy, identifier)["state"] == ("completed" if committed else "failed")
        assert not starts(proxy.rig)
