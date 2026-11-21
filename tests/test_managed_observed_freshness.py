"""Expiry at a real final Pause recheck stays inside managed supervision."""

import asyncio
import copy
import json
import sys
import time
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import trial_evidence
from ocpp_hub_core.prototype.managed_view import refresh_for_read
from ocpp_hub_core.prototype.security import StaleEvidenceError

from tests.charge_rig import actual_stop, owner_start, positive_flow, ready
from tests.managed_rig import healthy, starts
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls
from tests.trial_peer import Bench


# Intent: a new charger lifecycle report retires the earlier dashboard measurement before the next poll.
# Why: the viewer cannot use power or session state taken before a genuine Start or Stop.
@pytest.mark.parametrize("event", ["start", "stop"])
async def test_charger_lifecycle_change_retires_older_live_measurement(
    tmp_path, tls, unused_tcp_port, monkeypatch, event
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        observed = await healthy(c)
        local = await positive_flow(b, "daily")
        if event == "stop":
            observed = await healthy(c)
            assert observed["charging_session_open"] is True and observed["power_w"] == 1440
            reply = await b.rig.bench.call(
                b.charger,
                "daily-natural-stop",
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            assert reply[0] == 3 and j.active() is None
            b.rig.peer.permission_enabled = False
            b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        else:
            assert observed["charging_session_open"] is False and j.active() == local
        stale = refresh_for_read(c, dict(observed, running=True, certificate_remaining_seconds=86400))
        assert stale["charging_session_open"] is None and stale["power_w"] is None
        assert not stale["can_start"] and not stale["can_set_current"]
        fresh = await healthy(c)
        assert fresh["charging_session_open"] is (event == "start")
        if event == "start":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            await actual_stop(b, local)


# Intent: the same actual native sample can expire at observed's final Pause check without ending supervision.
# Why: moving the check must keep genuine pending Stop debt and withhold healthy publication of stale evidence.
@pytest.mark.parametrize("source", ["local", "cloud"])
async def test_observed_recheck_expires_inside_supervision(tmp_path, tls, unused_tcp_port, monkeypatch, source):
    stop_seen, release_ack = asyncio.Event(), asyncio.Event()
    targets = set()
    vendor = Bench.vendor

    class GateConnection:
        # Delegate all non-gated transport details to the real websocket.
        def __init__(self, connection):
            self.connection = connection

        # Observe only real Stop frames received by the independent cloud.
        def __aiter__(self):
            # Keep the original wire stream intact while identifying the one held Stop reply.
            async def frames():
                async for raw in self.connection:
                    frame = json.loads(raw)
                    if frame[0] == 2 and frame[2] == "StopTransaction":
                        targets.add(frame[1])
                        stop_seen.set()
                    yield raw

            return frames()

        # Withhold only its actual correlated successful acknowledgement.
        async def send(self, raw):
            frame = json.loads(raw)
            if frame[0] == 3 and frame[1] in targets:
                async with asyncio.timeout(8):
                    await release_ack.wait()
            await self.connection.send(raw)

    # Keep the original independent cloud responsible for every reply body.
    async def gated_vendor(bench, connection):
        await vendor(bench, GateConnection(connection))

    monkeypatch.setattr(Bench, "vendor", gated_vendor)
    try:
        async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, duration=60, startup=60) as b:
            c, j, g = b.controller, b.session.journal, b.session.gateway
            # Increase only the fake transport budget, BEFORE the Stop call is allocated.
            old_timeout = g.vendor.timeout
            g.vendor.timeout = 6
            try:
                identifier = uuid.uuid4().hex
                c.command(identifier, "start_test" if source == "local" else "arm_cloud_test", 0)
                if source == "local":
                    await c.charging.settle()
                else:
                    await owner_start(b)
                local = await positive_flow(b, source)
                await healthy(c)
                lease = c.charging.lease
                assert lease is not None and not lease.normal
                assert c.charging.state()["state"] == "charging"
                # Existing live-deadline pattern: the real independent task owns Pause/zero/Stop.
                lease.deadline = time.monotonic() - 1
                c.charging.changed.set()
                await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 3)
                assert c.charging.state()["state"] == "stopping"
                assert c.requests.intent().mode == "paused" and c.charging.lease is lease
                reply = await b.rig.bench.call(
                    b.charger,
                    uuid.uuid4().hex,
                    "StopTransaction",
                    {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
                )
                assert reply[0] == 3
                b.rig.peer.permission_enabled = False
                await asyncio.wait_for(stop_seen.wait(), 3)
                if c.pause_task is not None:
                    await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
                await until(lambda: not g.charger._pending and not g.controls._operations)
                pending = j.next_delivery()
                assert j.active() is None and pending is not None and pending.action == "StopTransaction"
                assert pending.state not in ("ambiguous", "rejected") and g.vendor._pending
                assert targets and not release_ack.is_set() and not j.get("managed_control_fault")
                assert c.charging.lease is lease and c.charging.state()["state"] == "stopping"
                assert b.rig.bench.firmware.active is None and b.rig.bench.firmware.effective_limit() == 0
                # Do NOT call healthy()/tick()/actual_stop() between this prefix and the target tick.
                intent, receipt = c.requests.intent(), copy.deepcopy(c.requests.receipt(identifier))
                latest, unavailable = c.latest_native, c.native_unavailable
                assert unavailable == 0
                start_count = len(starts(b.rig))
                wire = len(b.rig.bench.device_calls)
                seen_tick, seen_observed, mismatches = [], [], []
                offset = 0.0
                real_now = time.time
                fresh_code = trial_evidence.Evidence.require_fresh.__code__
                pause_code = type(c).require_paused.__code__
                tick_code = type(c)._tick.__code__
                supervise_code = type(c)._supervise.__code__
                observed_code = type(c.charging).observed.__code__

                # Supply wall time only; inspect real call/sample identity without replacing a validator.
                def scoped_time():
                    nonlocal offset
                    now = real_now()
                    fresh = sys._getframe(1)
                    node = pause = caller = None
                    try:
                        if fresh.f_code is not fresh_code or fresh.f_locals.get("max_age") != 5:
                            return now + offset
                        node = fresh
                        has_tick = False
                        while node is not None:
                            if node.f_code is tick_code and node.f_locals.get("self") is c:
                                has_tick = True
                            if node.f_code is pause_code and node.f_locals.get("self") is c:
                                pause = node
                            node = node.f_back
                        if pause is None or not has_tick:
                            return now + offset
                        caller = pause.f_back
                        native = pause.f_locals.get("native")
                        physical = fresh.f_locals.get("self")
                        if native is None or native.physical is not physical:
                            return now + offset
                        sample = (id(native), id(physical), physical.observed_at)
                        if caller is not None and caller.f_code is supervise_code and caller.f_locals.get("self") is c:
                            seen_tick.append((sample, now + offset - physical.observed_at))
                        elif (
                            caller is not None
                            and caller.f_code is observed_code
                            and caller.f_locals.get("self") is c.charging
                        ):
                            matches = (
                                len(seen_tick) == 1
                                and sample == seen_tick[0][0]
                                and c.charging.lease is lease
                                and not seen_observed
                            )
                            if matches:
                                offset = 6.0
                            else:
                                mismatches.append(sample)
                            seen_observed.append((sample, now + offset - physical.observed_at))
                        return now + offset
                    finally:
                        # Never retain frame objects in the witness data.
                        del fresh, node, pause, caller

                escaped = status = None
                # Scope only this module NAME; never patch the shared time.time function.
                with monkeypatch.context() as clock_patch:
                    clock_patch.setattr(trial_evidence, "time", SimpleNamespace(time=scoped_time))
                    try:
                        status = await c.tick()
                    except StaleEvidenceError as error:
                        escaped = error
                # The clock is real again BEFORE any further awaits/new native observations.
                assert len(seen_tick) == len(seen_observed) == 1 and not mismatches
                assert seen_tick[0][0] == seen_observed[0][0]
                assert 0 <= seen_tick[0][1] <= 5 < seen_observed[0][1] and offset == 6
                assert c.charging.lease is lease and c.charging.state()["state"] == "stopping"
                assert c.requests.intent() == intent and c.requests.receipt(identifier) == receipt
                assert not release_ack.is_set() and j.next_delivery().action == "StopTransaction"
                assert b.rig.bench.firmware.effective_limit() == b.rig.bench.firmware.current == 0
                assert len(starts(b.rig)) == start_count
                assert not any(
                    f[2] in ("RemoteStartTransaction", "RemoteStopTransaction") for f in b.rig.bench.device_calls[wire:]
                )
                # This exact assertion is RED on old code, AFTER proving the target boundary fired.
                assert escaped is None, "real observed StaleEvidenceError escaped the controller tick"
                assert status["state"] == "degraded" and status["fault"] == "native_observation_unavailable"
                assert status["owner_control_enabled"] is False
                assert c.native_unavailable == unavailable + 1 and c.latest_native is latest
                assert j.get("managed_native_failure")["type"] == "StaleEvidenceError"
                release_ack.set()
                await until(lambda: j.next_delivery() is None and not g.vendor._pending)
                assert (await healthy(c))["local_hold_verified"]
                assert c.charging.state()["state"] == "completed" and c.charging.lease is None
                assert c.requests.receipt(identifier) == receipt and c.requests.intent() == intent
                assert b.rig.bench.firmware.effective_limit() == 0 and len(starts(b.rig)) == start_count
            finally:
                # Also runs on old/red failure, before ready() closes its real peers.
                release_ack.set()
                g.vendor.timeout = old_timeout
    finally:
        release_ack.set()
