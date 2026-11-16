"""A healthy socket must not retain a failed read-only Boot probe forever."""

import asyncio
import json
import uuid
from dataclasses import replace

import pytest
from ocpp_hub_core.frames import build_call_result

from tests.test_managed_current import adjustable, select
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


class RetryBootPeer:
    """An independent charger loses one probe exchange before responding normally."""

    # Keep the selected wire failure and observed probe tokens outside the engine.
    def __init__(self, connection, failure):
        self.connection = connection
        self.failure = failure
        self.probes = []

    # Record real TriggerMessage requests before forwarding them to the ordinary fake charger.
    def __aiter__(self):
        # The peer still answers every unrelated profile, configuration and lifecycle request normally.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2 and frame[2] == "TriggerMessage":
                    self.probes.append(frame[1])
                yield raw

        return frames()

    # Lose the first Boot exchange, then forward a real Accepted reply and genuine Boot on the same socket.
    async def send(self, raw):
        frame = json.loads(raw)
        first = len(self.probes) == 1 or self.failure == "silent"
        if frame[0] == 3 and frame[1] in self.probes:
            if first and self.failure in ("lost_ack", "silent"):
                return
            status = "Rejected" if first and self.failure == "rejected" else "Accepted"
            await self.connection.send(build_call_result(frame[1], {"status": status}))
            return
        if frame[0] == 2 and frame[2] == "BootNotification" and self.probes and first and self.failure != "normal":
            return
        await self.connection.send(raw)


# Install a transport-only failure without replacing any engine readiness or introduction decision.
def observe_probe_peers(monkeypatch, policy):
    peers = []
    actual = Bench.read_charger

    # Each replacement socket gets its own independent request history and the then-current peer policy.
    async def read(bench, connection):
        peer = RetryBootPeer(connection, policy["failure"])
        peers.append(peer)
        await actual(bench, peer)

    monkeypatch.setattr(Bench, "read_charger", read)
    return peers


# Intent: a failed first probe is retried on the same real socket and only genuine Boot completes introduction.
# Why: rejection, a lost reply and Accepted-without-Boot must not strand otherwise healthy connections.
@pytest.mark.parametrize("failure", ["rejected", "lost_ack", "accepted_without_boot"])
async def test_read_only_boot_probe_retries_same_connection(tmp_path, tls, unused_tcp_port, monkeypatch, failure):
    peers = observe_probe_peers(monkeypatch, {"failure": failure})
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        session, controller, rig = b.session, b.controller, b.rig
        await select(b, 32)
        controller.command(uuid.uuid4().hex, "pause", controller.requests.intent().revision)
        assert (await healthy(controller))["local_hold_verified"]
        await b.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        session.runtime.settings = replace(
            session.runtime.settings, boot_probe_after=0.03, retry_min=0.01, retry_max=0.04
        )
        session.gateway.charger.timeout = 0.06
        rig.bench.boot_on_probe = True
        initial_limits = len(rig.bench.received_limits)
        async with rig.charger(session):
            epoch = session.gateway._charger_epoch
            await until(lambda: controller.native.introduced_epoch == epoch and session.gateway.controls.ready)
            peer = peers[-1]
            assert len(peer.probes) >= 2
            assert session.gateway._charger_epoch == epoch
            assert (await healthy(controller))["local_hold_verified"]
            count = len(peer.probes)
            await asyncio.sleep(0.12)
            assert len(peer.probes) == count
            assert rig.bench.received_limits[initial_limits:] and set(rig.bench.received_limits[initial_limits:]) == {0}
            assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)


# Intent: retiring a silent socket cancels its probe loop before a replacement connection is introduced.
# Why: an old timer must not keep issuing requests or establish authority on a newer charger epoch.
async def test_boot_probe_retry_is_cancelled_on_detach(tmp_path, tls, unused_tcp_port, monkeypatch):
    policy = {"failure": "silent"}
    peers = observe_probe_peers(monkeypatch, policy)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        session, controller, rig = b.session, b.controller, b.rig
        controller.command(uuid.uuid4().hex, "pause", controller.requests.intent().revision)
        assert (await healthy(controller))["local_hold_verified"]
        await b.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        session.runtime.settings = replace(
            session.runtime.settings, boot_probe_after=0.03, retry_min=0.01, retry_max=0.04
        )
        session.gateway.charger.timeout = 0.06
        rig.bench.boot_on_probe = True
        async with rig.charger(session):
            await until(lambda: len(peers[-1].probes) >= 2)
            retired, epoch = peers[-1], session.gateway._charger_epoch
        await until(lambda: not session.runtime.status.charger_connected)
        count = len(retired.probes)
        await asyncio.sleep(0.12)
        assert len(retired.probes) == count and not session.gateway.charger._pending
        policy["failure"] = "normal"
        async with rig.charger(session):
            await until(lambda: session.gateway.controls.ready and controller.native.introduced_epoch != epoch)
            assert (await healthy(controller))["local_hold_verified"]
            assert len(retired.probes) == count and not starts(rig)
