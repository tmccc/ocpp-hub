"""Running Return retains exclusive admission across actual native HTTP waits."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

from tests import test_native_bounded_session as native_bench
from tests.test_managed_api import TOKEN, call
from tests.test_managed_current import adjustable
from tests.test_managed_current_tick_race import OneNativeResponseGate, chain
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import ready
from tests.test_prototype_tls import tls as tls


# Intent: accepted and running Return refuse Start/current while a newer Pause can still retain zero.
# Why: execution selects accepted work only, but admission must remain exclusive throughout its actual I/O.
@pytest.mark.parametrize("action", ["start", "set_current", "pause"])
async def test_running_return_keeps_positive_admission_closed(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, action
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    context = (
        adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch)
        if action == "set_current"
        else ready(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, owner_tag="APP_OWNER")
    )
    async with context as b:
        c, peer = b.controller, b.rig.peer
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-secret", TOKEN), c, lambda: published)
        await api.start()
        tick = None
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
                proxy=None,
            ) as socket:
                revision, returning = c.requests.intent().revision, uuid.uuid4().hex
                assert (await call(socket, "app_control", id=returning, revision=revision))["ok"]
                for running in (False, True):
                    if running:
                        peer.arm = True
                        tick = asyncio.create_task(c.tick())
                        await asyncio.wait_for(peer.entered.wait(), 2)
                        assert "return_requested" in chain(tick)
                        assert peer.gated_request["action"] == "getConfig"
                        assert c.requests.receipt(returning)["state"] == "running"
                    status = (await call(socket, "status"))["result"]
                    assert not any(status[k] for k in ("can_start", "can_start_test", "can_set_current", "can_return"))
                    assert (await call(socket, "app_control", revision=revision)).get("ok") is False
                    if action != "pause":
                        fields = {"current_a": 16} if action == "set_current" else {}
                        assert (await call(socket, action, revision=revision, **fields)).get("ok") is False
                    assert not starts(b.rig) and b.rig.bench.firmware.effective_limit() == 0
                if action == "pause":
                    assert (await call(socket, "pause", revision=revision))["ok"]
                peer.release.set()
                await asyncio.wait_for(tick, 5)
                published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
                if action == "pause":
                    assert published["local_hold_verified"] and b.rig.bench.firmware.effective_limit() == 0
                    assert c.requests.receipt(returning)["state"] != "completed"
                else:
                    assert c.requests.receipt(returning)["state"] == "completed"
                    assert published["owner_control_enabled"] and b.rig.bench.firmware.effective_limit() == 6
                    fields = {"current_a": 16} if action == "set_current" else {}
                    assert (await call(socket, action, revision=c.requests.intent().revision, **fields))["ok"]
                    if action == "start":
                        await c.tests.settle()
                        assert len(starts(b.rig)) == 1 and b.rig.bench.firmware.effective_limit() == 6
                    else:
                        await c.current.settle()
                        assert c.current.selected() == 16 and b.rig.bench.firmware.effective_limit() == 16
                        assert not starts(b.rig)
                assert not b.session.journal.get("managed_control_fault")
        finally:
            peer.release.set()
            if tick is not None:
                await asyncio.gather(tick, return_exceptions=True)
            await api.close()
