"""A late genuine Boot can finish introduction after the one read-only probe failed."""

import asyncio
import json
import uuid
from dataclasses import replace

import pytest

from tests.test_managed_current import adjustable, select
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


class SilentProbePeer:
    """Lose only the read-only TriggerMessage acknowledgement on the independent charger transport."""

    # Keep all ordinary peer configuration, composite and mutation replies intact.
    def __init__(self, connection):
        self.connection = connection
        self.probes = set()

    # Observe the actual probe correlation ID without reading gateway state.
    def __aiter__(self):
        # Forward every real received request to the unchanged fake charger implementation.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2 and frame[2] == "TriggerMessage":
                    self.probes.add(frame[1])
                yield raw

        return frames()

    # Drop the selected response while leaving the physical connection alive for a genuine late Boot.
    async def send(self, raw):
        frame = json.loads(raw)
        if frame[0] == 3 and frame[1] in self.probes:
            return
        await self.connection.send(raw)


# Intent: a current-epoch real Boot after refusal completes zero introduction and the same explicit Return.
# Why: an optional probe failure must not require another transport reconnect or replay any mutating command.
@pytest.mark.parametrize("case", ["prompt", "late", "lost_ack", "replacement", "conflict"])
async def test_genuine_boot_after_probe_failure(tmp_path, tls, unused_tcp_port, monkeypatch, case):
    if case == "lost_ack":
        actual = Bench.read_charger

        # Intercept only the independent peer transport, never introduction or RPC methods.
        async def silent_probe(bench, connection):
            await actual(bench, SilentProbePeer(connection))

        monkeypatch.setattr(Bench, "read_charger", silent_probe)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        s, c, rig = b.session, b.controller, b.rig
        await select(b, 32)
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        await b.charger.close()
        await until(lambda: not s.runtime.status.charger_connected)
        s.runtime.settings = replace(s.runtime.settings, boot_probe_after=0.05)
        if case == "lost_ack":
            s.gateway.charger.timeout = 0.1
        if case == "replacement":
            async with rig.charger(s):
                await until(lambda: s.runtime.status.last_failure == "BootOrCeilingUnavailable")
            await until(lambda: not s.runtime.status.charger_connected)
        async with rig.charger(s) as charger:
            epoch = s.gateway._charger_epoch
            if case != "prompt":
                await until(lambda: s.runtime.status.last_failure == "BootOrCeilingUnavailable")
                await asyncio.sleep(0.1)
            if case == "conflict":
                rig.peer.configuration["untouched"] = "independent-conflict"
            reply = await rig.bench.call(charger, "actual-late-boot", "BootNotification", BOOT)
            assert reply[0] == 3 and reply[2]["status"] == "Accepted"
            if case == "conflict":
                await until(lambda: s.journal.get("trial_phase") == "blocked")
                assert c.native.introduced_epoch != epoch
                assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)
                return
            await until(lambda: c.native.introduced_epoch == epoch and s.gateway.controls.ready)
            assert (await healthy(c))["local_hold_verified"]
            request = uuid.uuid4().hex
            c.command(request, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 32
            assert c.requests.receipt(request)["state"] == "completed"
            assert rig.bench.firmware.effective_limit() == 32 and not starts(rig)
