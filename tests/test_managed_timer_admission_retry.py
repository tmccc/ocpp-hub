"""Accepted native timers survive a genuine pending ordinary cloud acknowledgement."""

import asyncio
import json
import uuid
from datetime import UTC, datetime

from ocpp_hub_core.prototype.managed_schedule import KEY

from tests.test_managed_proxy import healthy
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


# Intent: an accepted unsent timer waits for an ordinary report ACK without losing its immutable request.
# Why: a busy publication is not owner revocation, and only a later fully guarded send may install the timer.
async def test_requested_timer_waits_for_real_status_ack(tmp_path, tls, unused_tcp_port, monkeypatch):
    seen, release = asyncio.Event(), asyncio.Event()
    targets = set()
    timestamp = ""
    vendor = Bench.vendor

    class GateConnection:
        # Retain the actual independent cloud connection and its normal request handling.
        def __init__(self, connection):
            self.connection = connection

        # Observe only the new ordinary report; Available would instead prove vehicle departure.
        def __aiter__(self):
            # Forward every genuine request to the ordinary fake backend without changing its body.
            async def frames():
                async for raw in self.connection:
                    frame = json.loads(raw)
                    if (
                        frame[0] == 2
                        and frame[2] == "StatusNotification"
                        and frame[3].get("status") == "Preparing"
                        and frame[3].get("timestamp") == timestamp
                    ):
                        targets.add(frame[1])
                        seen.set()
                    yield raw

            return frames()

        # Withhold only the actual corresponding reply after the backend received its request.
        async def send(self, raw):
            frame = json.loads(raw)
            if frame[0] == 3 and frame[1] in targets:
                async with asyncio.timeout(5):
                    await release.wait()
            await self.connection.send(raw)

    # Put a transport gate around the existing backend rather than replacing engine decisions or queue rows.
    async def gated_vendor(bench, connection):
        await vendor(bench, GateConnection(connection))

    monkeypatch.setattr(Bench, "vendor", gated_vendor)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        j, g = b.session.journal, b.session.gateway
        g.vendor.timeout = 5
        identifier = uuid.uuid4().hex
        b.request(identifier)
        accepted = j.get(KEY)
        receipt = b.c.requests.receipt(identifier)
        assert receipt["state"] == "accepted"
        before_calls = len(b.rig.bench.device_calls)
        timestamp = datetime.fromtimestamp(b.clock.now + 1, UTC).isoformat()
        try:
            reply = await b.rig.bench.call(
                b.sock,
                uuid.uuid4().hex,
                "StatusNotification",
                {"connectorId": 1, "errorCode": "NoError", "status": "Preparing", "timestamp": timestamp},
            )
            assert reply[0] == 3
            await asyncio.wait_for(seen.wait(), 3)
            assert j.next_delivery() is not None and g.vendor._pending
            session = g._vendor_epoch
            faults = b.session.runtime.status.vendor_fault.count
            assert b.rig.peer.configuration["AmpLmt"] == "32"
            assert b.c.requests.intent().mode == "app"
            await b.c.tick()
            assert j.next_delivery() is not None and g.vendor._pending
            assert g._vendor_epoch == session and g.vendor_registration == "Accepted"
            assert b.session.runtime.status.vendor_connected
            assert b.session.runtime.status.vendor_fault.count == faults
            assert b.c.requests.receipt(identifier) == receipt
            current = j.get(KEY)
            assert current["state"] == "requested", current
            for key in ("id", "revision", "epoch", "ticket", "start_epoch", "end_epoch", "body"):
                assert current[key] == accepted[key]
            assert not j.get("managed_control_fault")
            assert not any(
                frame[2] == "DataTransfer" and frame[3].get("messageId") == "setPlan"
                for frame in b.rig.bench.device_calls[before_calls:]
            )
        finally:
            release.set()
        await until(lambda: j.next_delivery() is None and not g.vendor._pending)
        for _ in range(2):
            await healthy(b.c)
        assert b.c.schedule.summary()["state"] == "armed"
        assert j.get(KEY)["id"] == identifier
        assert b.c.requests.receipt(identifier) == receipt
        installs = [
            frame
            for frame in b.rig.bench.device_calls[before_calls:]
            if frame[2] == "DataTransfer" and frame[3].get("messageId") == "setPlan"
        ]
        assert len(installs) == 1
        assert b.device.plans[b.timer] == json.loads(accepted["install"]["data"])
