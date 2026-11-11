"""Production Home Assistant client and management server with a real loopback charger session."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime

import aiohttp

from custom_components.ics_proxy.api import ProxyClient
from tests import conftest as _engine  # noqa: F401 - register the standalone engine package.
from tests.test_managed_api import TOKEN, ManagementServer, ManagementSettings
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype_tls import tls as tls


# Intent: HA Pause closes an otherwise open, full scheduled session; Return restores ordinary app/schedule operation.
# Why: a simulated management receipt cannot show that the real client waits for measured closure or original readback.
async def test_full_vehicle_pause_return(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start
        local = await b.fire(b.timer)
        assert (await healthy(b.c))["power_w"] > 0
        b.rig.peer.permission_enabled = False
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        b.device.requested_current = 0
        published = await healthy(b.c)
        assert published["power_w"] == 0 and b.session.journal.active() == local
        port = unused_tcp_port_factory()
        server = ManagementServer(
            ManagementSettings(port, tmp_path / "unused", TOKEN),
            b.c,
            lambda: dict(published, running=True, certificate_remaining_seconds=86400),
        )
        await server.start()

        # Use the same supervised tick and publication lifecycle as the service, independent of client polling.
        async def supervise():
            nonlocal published
            while True:
                published = await b.c.tick()
                await asyncio.sleep(0.02)

        supervisor = asyncio.create_task(supervise())
        try:
            async with aiohttp.ClientSession() as session:
                client = ProxyClient(session, port, TOKEN)
                initial = await client.request("status")
                pause = asyncio.create_task(client.command("pause", initial["control_revision"]))
                try:
                    async with asyncio.timeout(4):
                        while not b.stops():
                            await asyncio.sleep(0.01)
                    assert b.stops() == [local] and not pause.done()
                    assert b.timer in b.device.plans and b.session.journal.active() == local
                    await b.close_transaction(local)
                    await pause
                finally:
                    if not pause.done():
                        pause.cancel()
                        with suppress(asyncio.CancelledError):
                            await pause
                paused = await client.request("status")
                assert paused["local_hold_verified"] and paused["station_ceiling_a"] == 0
                await client.command("app_control", paused["control_revision"])
                shared = await client.request("status")
                assert shared["desired_control"] == "app" and shared["station_ceiling_a"] == 6
                assert set(b.device.plans) == {9} and not starts(b.rig)
                native = await b.c.native.read()
                assert native.saved_schedule_digest == b.rig.spec.saved_schedule_digest
                b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
                daily = await b.fire(9)
                assert daily != local
                b.clock.now += 21600
                await b.close_transaction(daily)
        finally:
            supervisor.cancel()
            with suppress(asyncio.CancelledError):
                await supervisor
            await server.close()


# Intent: the HA client can observe a scheduled full car reach the cutoff and recover its normal daily window.
# Why: zero watts and an accepted Stop do not complete the schedule receipt or establish closure.
async def test_full_vehicle_unattended_deadline(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        published = await healthy(b.c)
        port = unused_tcp_port_factory()
        server = ManagementServer(
            ManagementSettings(port, tmp_path / "unused", TOKEN),
            b.c,
            lambda: dict(published, running=True, certificate_remaining_seconds=86400),
        )
        await server.start()
        try:
            async with aiohttp.ClientSession() as session:
                client = ProxyClient(session, port, TOKEN)
                initial = await client.request("status")
                receipt = await client.request(
                    "temporary_schedule", revision=initial["control_revision"], start_epoch=b.start
                )
                b.timer = b.session.journal.get("managed_temporary_schedule")["body"]["planId"]
                for _ in range(2):
                    published = await healthy(b.c)
                assert (await client.request("status"))["temporary_schedule"]["state"] == "armed"
                local = await b.advance_idle_to(b.start)
                assert local is not None
                b.device.requested_current = 0
                b.rig.peer.permission_enabled = False
                b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
                published = await healthy(b.c)
                assert (await client.request("status"))["power_w"] == 0
                b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
                published = await b.c.tick()
                assert b.stops() == [local] and b.device.effective_limit() == 0
                b.clock.now += 61
                published = await b.c.tick()
                alarm = await client.request("status")
                assert alarm["fault"] == "temporary_schedule_stop_unconfirmed" and not alarm["healthy"]
                assert (await client.request("receipt", request_id=receipt["id"]))["state"] == "accepted"
                await b.close_transaction(local)
                published = await healthy(b.c)
                restored = await client.request("status")
                assert restored["healthy"] and restored["desired_control"] == "app"
                assert restored["station_ceiling_a"] == 6 and restored["temporary_schedule"]["state"] == "complete"
                assert (await client.request("receipt", request_id=receipt["id"]))["state"] == "completed"
                assert set(b.device.plans) == {9} and not starts(b.rig)
                daily = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
                published = await healthy(b.c)
                assert daily is not None and daily != local and (await client.request("status"))["power_w"] > 0
                await b.advance_running_plan(9, daily, b.clock.now + 21600)
                assert (await healthy(b.c))["power_w"] == 0 and daily not in b.stops()
        finally:
            await server.close()
