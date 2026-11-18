"""A pre-effect native-read SQL refusal must not terminate the current Boot's introduction probe."""

import asyncio
import sqlite3
import uuid

import pytest

from tests.current_rig import selected_ready
from tests.managed_rig import healthy, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import BOOT
from tests.runtime_rig import until
from tests.tls_support import tls as tls


class GatedIntroductionPeer(InterruptedNativePeer):
    """Hold one real native HTTP response so the test can place an actual storage refusal."""

    # Admission and ordinary polls remain unchanged until the test arms its next response boundary.
    def __init__(self, bench):
        super().__init__(bench)
        self.gate_next = False
        self.arrived, self.release = asyncio.Event(), asyncio.Event()

    # Only the independent HTTP response is delayed; the engine's complete introduction still runs.
    async def handle(self, reader, writer):
        if self.gate_next:
            self.gate_next = False
            self.arrived.set()
            await self.release.wait()
        await super().handle(reader, writer)


# Identify the actual live probe for this runtime and socket generation without changing its behavior.
def current_probe(runtime, epoch):
    for task in asyncio.all_tasks():
        coroutine = task.get_coro()
        frame = getattr(coroutine, "cr_frame", None)
        if getattr(coroutine, "__qualname__", "").endswith("._boot_probe") and frame is not None:
            if frame.f_locals.get("self") is runtime and frame.f_locals.get("epoch") == epoch:
                return task
    return None


# Intent: two real initial-read refusals retry and complete the same paused connection without another Boot.
# Why: ordinary fresh polls and a new Pause cannot revive a probe that already exited on SQLite failure.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("storage_fault", [False, True])
async def test_paused_introduction_read_storage_recovers_on_same_boot(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, storage_fault
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=GatedIntroductionPeer
    ) as b:
        c, j, g, peer = b.controller, b.session.journal, b.session.gateway, b.rig.peer
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        intent = c.requests.intent()
        await b.charger.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        peer.gate_next = True
        async with b.rig.charger(b.session) as charger:
            assert (await b.rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(peer.arrived.wait(), 2)
            epoch, boot = g._charger_epoch, g.boot_observation
            probe, refusals = current_probe(b.session.runtime, epoch), []
            assert probe is not None and not probe.done()

            # Refuse only real resource-table reads; no native/probe method or result is substituted.
            def authorizer(action, table, column, database, trigger):
                if action == sqlite3.SQLITE_READ and table == "trial_resources":
                    refusals.append(column)
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            if storage_fault:
                j.db.set_authorizer(authorizer)
            try:
                peer.release.set()
                if storage_fault:
                    await until(lambda: len(refusals) >= 2, 4)
                    assert not probe.done() and c.native.introduced_epoch != epoch
                    assert b.rig.bench.firmware.effective_limit() == 0
            finally:
                j.db.set_authorizer(None)
            await asyncio.wait_for(asyncio.shield(probe), 5)
            assert probe.exception() is None
            assert c.native.introduced_epoch == g._charger_epoch == epoch and g.boot_observation == boot
            status = await healthy(c)
            assert status["local_hold_verified"] and c.requests.intent() == intent
            assert c.requests.receipt(pause)["state"] == "completed" and not starts(b.rig)
            assert not [f for f in b.rig.bench.device_calls if f[2] == "RemoteStopTransaction"]
            assert b.rig.bench.firmware.effective_limit() == 0
            await b.session.close()
