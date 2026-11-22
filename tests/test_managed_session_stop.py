"""Use real management, charger and cloud sockets for a session-only Stop."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_session_stop import ManagedSessionStop
from ocpp_hub_core.prototype.managed_view import refresh_for_read
from websockets.asyncio.client import connect

from tests.charge_rig import owner_rig, positive_flow
from tests.current_rig import adjustable, locally_ready, select
from tests.managed_rig import healthy
from tests.management_peer import TOKEN, call
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.test_managed_boot_supersession import delay_reply
from tests.test_managed_normal_start import natural_stop, zero_power_session
from tests.tls_support import tls as tls


# Intent: a zero-watt session can be stopped immediately after its Start completes on the management socket.
# Why: a charger reply or 0 W cannot prove closure, and this Stop must retain the app/schedule ceiling.
async def test_management_start_then_session_stop_at_zero_watts(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as t:
        c, j, bench = t.controller, t.session.journal, t.rig.bench
        port = unused_tcp_port_factory()
        published = await healthy(c)

        # Mirror the service's outer status fields without manufacturing native evidence.
        def status():
            return {**published, "running": True, "certificate_remaining_seconds": 86400}

        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-secret", TOKEN), c, status)
        await api.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as socket:
                started = await call(socket, "start", revision=c.requests.intent().revision)
                await c.charging.settle()
                local = await zero_power_session(t)
                published = await healthy(c)
                view = (await call(socket, "status"))["result"]
                assert view["charging_session_open"] is True and view["power_w"] == 0
                assert (await call(socket, "receipt", request_id=started["id"]))["result"]["state"] == "completed"
                target, revision = view["stop_target"], view["control_revision"]
                assert isinstance(target, str)
                mark, limit = len(bench.device_calls), bench.firmware.effective_limit()
                reply = await call(socket, "stop_session", revision=revision, target="b" * 64)
                assert reply["ok"] is False and not bench.remote_stop.is_set()

                stop = uuid.uuid4().hex
                accepted = await call(socket, "stop_session", id=stop, revision=revision, target=target)
                assert accepted["result"]["state"] == "accepted"
                await asyncio.wait_for(bench.remote_stop.wait(), 3)
                await until(lambda: j.outcomes.accepted_stop(local))
                assert (await call(socket, "receipt", request_id=stop))["result"]["state"] != "completed"
                assert bench.firmware.effective_limit() == limit == 6
                assert (await call(socket, "stop_session", id=stop, revision=revision, target="b" * 64))["ok"] is False
                await natural_stop(t, local)
                published = await healthy(c)

                receipt = (await call(socket, "receipt", request_id=stop))["result"]
                assert receipt["state"] == "completed" and receipt["target"] == target
                assert c.requests.intent().mode == "app" and c.requests.intent().revision == revision
                assert published["charging_session_open"] is False and published["station_ceiling_a"] == limit
                assert (await call(socket, "stop_session", id=stop, revision=revision, target=target))["ok"]
                sent = [frame for frame in bench.device_calls[mark:] if frame[2] != "GetCompositeSchedule"]
                assert [(frame[2], frame[3]) for frame in sent] == [("RemoteStopTransaction", {"transactionId": local})]
        finally:
            await api.close()


# Intent: local and vendor Stop both reuse one physical Stop with a positive station ceiling.
# Why: the session identity is shared even though neither request intends to Pause scheduled charging.
@pytest.mark.parametrize("first", ["local", "vendor"])
async def test_session_stop_shares_the_vendor_wire_in_either_order(tmp_path, tls, unused_tcp_port, monkeypatch, first):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as t:
        c, j, bench = t.controller, t.session.journal, t.rig.bench
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.charging.settle()
        local = await positive_flow(t)
        status = await healthy(c)
        view = refresh_for_read(c, {**status, "running": True, "certificate_remaining_seconds": 86400})
        target = view["stop_target"]
        assert isinstance(target, str)
        stop, revision, vendor = uuid.uuid4().hex, view["control_revision"], j.vendor_id(local)
        assert vendor is not None

        # Send an independently correlated vendor Stop for the same current cloud mapping.
        async def vendor_stop():
            result = await bench.cloud_call(uuid.uuid4().hex, "RemoteStopTransaction", {"transactionId": vendor})
            assert result[0] == 3 and result[2] == {"status": "Accepted"}

        if first == "vendor":
            await vendor_stop()
        assert c.command(stop, "stop_session", revision, target=target)["state"] == "accepted"
        if first == "local":
            await until(lambda: j.outcomes.accepted_stop(local))
            await vendor_stop()
        async with asyncio.timeout(3):
            while j.outcomes.session(local) is not None and not bench.remote_stop.is_set():
                await asyncio.sleep(0.01)
        await natural_stop(t, local)
        await healthy(c)
        assert c.requests.receipt(stop)["state"] == "completed"
        assert sum(frame[2] == "RemoteStopTransaction" for frame in bench.device_calls) == 1
        assert bench.firmware.effective_limit() == 6 and c.requests.intent().mode == "app"


# Intent: an unmapped local session can physically close before its real Stop report arrives.
# Why: this is a temporary disagreement about the exact stopped session, not proof of a new identity fault.
async def test_unmapped_session_stop_waits_for_report_after_native_closure(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as t:
        c, j, bench = t.controller, t.session.journal, t.rig.bench
        await select(t, 16)
        await healthy(c)
        await until(lambda: j.next_delivery() is None and not t.session.gateway.vendor._pending)
        await t.session.runtime.stop_vendor()
        await locally_ready(c)
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.charging.settle()
        reply = await bench.call(
            t.charger, "unmapped-local-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
        )
        assert reply[2]["idTagInfo"]["status"] == "Accepted"
        local = reply[2]["transactionId"]
        t.rig.peer.plan_session = (local, 987654)
        await locally_ready(c)
        assert j.vendor_id(local) is None
        target, revision = c.session_stop.target(), c.requests.intent().revision
        assert isinstance(target, str)
        stop = uuid.uuid4().hex
        assert c.command(stop, "stop_session", revision, target=target)["state"] == "accepted"
        await until(lambda: j.outcomes.accepted_stop(local))

        bench.firmware.active = None
        t.rig.peer.permission_enabled = False
        for _ in range(4):
            assert (await c.tick())["state"] != "attention"
        assert c.requests.receipt(stop)["state"] != "completed" and j.active() == local
        reply = await bench.call(
            t.charger,
            "unmapped-local-stop",
            "StopTransaction",
            {
                "transactionId": local,
                "timestamp": START["timestamp"],
                "meterStop": 1100,
            },
        )
        assert reply[0] == 3
        final = await locally_ready(c)
        assert final["charging_session_open"] is False and c.requests.receipt(stop)["state"] == "completed"
        assert c.requests.intent().mode == "app" and bench.firmware.effective_limit() == 16
        assert sum(frame[2] == "RemoteStopTransaction" for frame in bench.device_calls) == 1


# Intent: a lost Stop reply leaves the exact session owned without a new Stop after replacement or a competing request.
# Why: the real closure, and not how the prior connection ended, settles a possibly sent session Stop.
@pytest.mark.parametrize("first", ["local", "vendor"])
async def test_session_stop_reuses_a_wire_with_a_lost_reply(tmp_path, tls, unused_tcp_port, monkeypatch, first):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as t:
        c, j, bench = t.controller, t.session.journal, t.rig.bench
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.charging.settle()
        local = await zero_power_session(t)
        await healthy(c)
        target, revision = c.session_stop.target(), c.requests.intent().revision
        stop, vendor = uuid.uuid4().hex, j.vendor_id(local)
        assert isinstance(target, str) and vendor is not None
        _, _, entered = delay_reply(monkeypatch, t.rig, t.charger, "RemoteStopTransaction")
        monkeypatch.setattr(t.session.gateway.charger, "timeout", 0.25)
        if first == "vendor":
            cloud = asyncio.create_task(
                bench.cloud_call("lost-cloud-stop", "RemoteStopTransaction", {"transactionId": vendor})
            )
            await asyncio.wait_for(entered.wait(), 3)
        assert c.command(stop, "stop_session", revision, target=target)["state"] == "accepted"
        await asyncio.wait_for(entered.wait(), 3)
        assert c.session_stop.worker is not None
        await asyncio.wait_for(c.session_stop.worker, 3)
        if first == "vendor":
            assert (await cloud)[0] == 4
        else:
            c.session_stop = ManagedSessionStop(c)
            assert c.command(stop, "stop_session", revision, target=target)["state"] == "uncertain"
            cloud = await bench.cloud_call("after-local-loss", "RemoteStopTransaction", {"transactionId": vendor})
            assert cloud[0] == 4
        assert sum(frame[2] == "RemoteStopTransaction" for frame in bench.device_calls) == 1
        assert c.requests.receipt(stop)["state"] != "completed"
        await natural_stop(t, local)
        if first == "vendor":
            status = await c.tick()
            assert status["operator_reconciliation_required"] is True
        else:
            await healthy(c)
        assert c.requests.receipt(stop)["state"] == "completed"
