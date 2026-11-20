"""Home Assistant and the real proxy agree about a saved Pause after either restarts."""

import asyncio
from contextlib import AsyncExitStack

import pytest

from tests.managed_rig import starts
from tests.model_tls_support import model_tls as model_tls
from tests.model_tls_support import tls_for_case
from tests.native_peer_failures import InterruptedNativePeer
from tests.process_helpers import process_rig
from tests.recovery_barriers import require_zero_held
from tests_ha_proxy.owner_workflow_support import HomeAssistantProcess, LostCommandReply


# Keep commissioning's saved requests outside the count of the owner's test presses.
def added_requests(snapshot, baseline):
    return [row for row in snapshot["requests"] if row["id"] not in baseline]


# Agree on the exact saved Pause and fresh physical zero before relying on Home Assistant's display.
async def verify_pause(proxy, hass, saved, claim, entry_id):
    receipt = await proxy.completed_control(saved["id"])
    revision = saved["revision"] + 1
    await proxy.status(lambda state: state.get("local_hold_verified") is True and state.get("can_return") is True)
    current = proxy.durable()
    assert current["metadata"]["managed_control_intent"] == {"mode": "paused", "revision": revision}
    assert current["metadata"]["managed_owner_pause"] == claim
    assert await hass.request("view") == {"entry_id": entry_id, "mode": "Paused locally", "return_available": True}
    require_zero_held(proxy.rig, len(proxy.rig.bench.device_calls))
    assert not starts(proxy.rig)
    return receipt


# Intent: the owner's Pause survives a missing reply and replacement of both processes in either order.
# Why: the app's displayed state, the saved request and the charger's behavior can disagree across those boundaries.
@pytest.mark.parametrize("proxy_first", [True, False], ids=["proxy-first", "ha-first"])
async def test_owner_pause_with_missing_reply_and_restarts(
    tmp_path, model_tls, unused_tcp_port_factory, monkeypatch, proxy_first
):
    ports = unused_tcp_port_factory
    async with (
        process_rig(
            tmp_path,
            tls_for_case(tmp_path, model_tls),
            ports(),
            ports(),
            monkeypatch,
            native_peer=InterruptedNativePeer,
            timing="fast-poll",
        ) as proxy,
        AsyncExitStack() as cleanup,
    ):
        await proxy.start()
        proxy_pid = proxy.process.pid
        await proxy.status()
        baseline = {row["id"] for row in proxy.durable()["requests"]}
        relay = LostCommandReply(proxy, "pause", commit_gate="after-pause-request-commit" if proxy_first else None)
        hass = HomeAssistantProcess(tmp_path, await relay.start())
        cleanup.push_async_callback(relay.close)
        cleanup.push_async_callback(hass.stop)
        first_view = await hass.start()
        hass_pid = hass.process.pid
        entry_id = first_view["entry_id"]
        assert first_view["mode"] == "App / schedule" and first_view["return_available"] is False

        # Home Assistant does not complete its press while the saved Pause is being observed.
        press = asyncio.create_task(hass.request("pause"))
        try:
            await asyncio.wait_for(relay.command_ready.wait(), 5)
            (pause,) = relay.control_requests
            if proxy_first:
                gate = await proxy.gate("after-pause-request-commit", pause["id"])
                assert gate["transaction_open"] is False and not relay.reply_ready.is_set()
            else:
                await asyncio.wait_for(relay.reply_ready.wait(), 5)
                assert relay.command_reply["ok"] is True
                assert relay.command_reply["id"] == relay.command_reply["result"]["id"] == pause["id"]
                assert relay.command_reply["result"]["state"] == "accepted"
            assert not press.done()
            snapshot = proxy.durable()
            (saved,) = added_requests(snapshot, baseline)
            assert saved["id"] == pause["id"] and saved["action"] == "pause" and saved["revision"] == pause["revision"]
            assert saved["state"] in (("accepted",) if proxy_first else ("accepted", "completed"))
            revision = pause["revision"] + 1
            assert snapshot["metadata"]["managed_control_intent"] == {"mode": "paused", "revision": revision}
            claim = snapshot["metadata"]["managed_owner_pause"]
            assert claim == {"revision": revision, "ticket": snapshot["metadata"]["control"]["ticket"]}

            if proxy_first:
                await proxy.kill()
            else:
                relay.drop_reply.set()
            result = await press
            if proxy_first:
                assert "outcome is unknown" in result.get("error", ""), result
            else:
                assert result == {"ok": True}
            assert relay.receipt_requests
            assert {read["request_id"] for read in relay.receipt_requests} == {pause["id"]}
        finally:
            relay.drop_reply.set()
            if not press.done():
                press.cancel()
            await asyncio.gather(press, return_exceptions=True)

        if proxy_first:
            assert await hass.request("view") == {
                "entry_id": entry_id,
                "mode": "unavailable",
                "return_available": False,
            }
            await proxy.start()
        else:
            await hass.stop()
            assert (await hass.start())["entry_id"] == entry_id
        original_receipt = await verify_pause(proxy, hass, saved, claim, entry_id)
        mark = len(proxy.rig.bench.device_calls)

        if proxy_first:
            await hass.stop()
            assert (await hass.start())["entry_id"] == entry_id
        else:
            await proxy.kill()
            await proxy.start()
        assert hass.process.pid != hass_pid and proxy.process.pid != proxy_pid
        assert await verify_pause(proxy, hass, saved, claim, entry_id) == original_receipt
        require_zero_held(proxy.rig, mark)
        (retained,) = added_requests(proxy.durable(), baseline)
        assert retained["id"] == pause["id"] and retained["revision"] == pause["revision"]
        assert retained["state"] == "completed" and relay.control_requests == [pause]

        assert await hass.request("app_control") == {"ok": True}
        pause_again, returned = relay.control_requests
        assert pause_again == pause and returned["command"] == "app_control" and returned["revision"] == revision
        (return_row,) = added_requests(proxy.durable(), baseline | {pause["id"]})
        assert return_row["id"] == returned["id"] and return_row["state"] == "completed"
        assert await hass.request("view") == {"entry_id": entry_id, "mode": "App / schedule", "return_available": False}
        assert len(await proxy.scheduled_sessions(6, True)) == 2
        assert await proxy.completed_control(pause["id"]) == original_receipt
        assert {row["id"] for row in added_requests(proxy.durable(), baseline)} == {pause["id"], returned["id"]}
        assert relay.control_requests == [pause, returned] and not starts(proxy.rig)
