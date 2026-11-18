"""Actual post-read SQLite refusals cannot retire the current connection's introduction probe."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from websockets.asyncio.server import serve

from tests import test_native_bounded_session as native_bench
from tests.test_introduction_storage_retry import GatedIntroductionPeer, current_probe
from tests.test_managed_introduction_retry import selected_ready
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import Bench as RuntimeBench
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls


# Intent: actual post-read metadata refusals retry on the same Boot, retaining zero and a newer owner Pause.
# Why: the existing SQL-read witness never reaches the unguarded binding/hold/recover/introduced write boundary.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("newer_pause", [False, True])
@pytest.mark.parametrize("cut", ["recover", "introduced"])
async def test_postread_introduction_write_recovers_on_same_boot(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, newer_pause, cut
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", GatedIntroductionPeer)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as test:
        c, j, g, peer = test.controller, test.session.journal, test.session.gateway, test.rig.peer
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        await test.charger.close()
        await until(lambda: not test.session.runtime.status.charger_connected)
        peer.gate_next = True
        async with test.rig.charger(test.session) as charger:
            assert (await test.rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(peer.arrived.wait(), 2)
            epoch, boot = g._charger_epoch, g.boot_observation
            probe, refusals = current_probe(test.session.runtime, epoch), []
            assert probe is not None and not probe.done()

            # The trigger records the real failing write without replacing an owner or a native/protocol result.
            def refused():
                refusals.append(cut)
                return 1

            j.db.create_function("record_intro_refusal", 0, refused)
            key, value = ("trial_phase", '"recovering"') if cut == "recover" else ("trial_starting", "false")
            j.db.execute(
                f"CREATE TEMP TRIGGER refuse_intro BEFORE INSERT ON meta WHEN NEW.key='{key}' AND NEW.value='{value}' "
                "BEGIN SELECT record_intro_refusal(); SELECT RAISE(ABORT,'temporary introduction write refusal'); END"
            )
            try:
                peer.release.set()
                await until(lambda: len(refusals) >= 2, 4)
                assert not probe.done() and c.native.introduced_epoch != epoch
                assert test.rig.bench.firmware.effective_limit() == 0 and not starts(test.rig)
            finally:
                j.db.execute("DROP TRIGGER refuse_intro")
            if newer_pause:
                pause = uuid.uuid4().hex
                c.command(pause, "pause", c.requests.intent().revision)
            intent = c.requests.intent()
            await asyncio.wait_for(asyncio.shield(probe), 5)
            assert probe.exception() is None and c.native.introduced_epoch == epoch
            assert g._charger_epoch == epoch and g.boot_observation == boot
            status = await healthy(c)
            assert status["local_hold_verified"] and c.requests.intent() == intent
            assert status["requested_current_a"] == current and test.rig.bench.firmware.effective_limit() == 0
            assert c.requests.receipt(pause)["state"] == "completed" and not starts(test.rig)
            assert not [frame for frame in test.rig.bench.device_calls if frame[2] == "RemoteStopTransaction"]
            returning = uuid.uuid4().hex
            c.command(returning, "app_control", intent.revision)
            async with asyncio.timeout(5):
                while c.requests.receipt(returning)["state"] != "completed":
                    await c.tick()
                    await asyncio.sleep(0.02)
            status = await healthy(c)
            assert status["owner_control_enabled"] and status["station_ceiling_a"] == current
            assert test.rig.bench.firmware.effective_limit() == current and not starts(test.rig)


# Intent: a later introduction storage failure is recorded once without retrying its effect-capable callback.
# Why: only the explicitly deferred prefix is replay-safe; the outer runtime catch must remain terminal.
async def test_terminal_introduction_sql_failure_is_recorded_without_replay(tmp_path, tls, admission):
    bench = RuntimeBench(tmp_path / "terminal.db", tls, admission)
    bench.store.db.execute("CREATE TEMP TABLE terminal_write (id INTEGER PRIMARY KEY)")
    bench.store.db.execute("INSERT INTO terminal_write VALUES (1)")
    entered, release, invocations = asyncio.Event(), asyncio.Event(), []

    # A configured collaborator reaches a real SQLite failure after its non-replayable operation boundary.
    async def before_ceiling():
        invocations.append(1)
        entered.set()
        await release.wait()
        bench.store.db.execute("INSERT INTO terminal_write VALUES (1)")

    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        runtime.before_ceiling = before_ceiling
        try:
            async with bench.charger() as charger:
                await charger.send(build_call("terminal-boot", "BootNotification", BOOT))
                assert decode(await charger.recv())[2]["status"] == "Accepted"
                await asyncio.wait_for(entered.wait(), 2)
                epoch = bench.gateway._charger_epoch
                probe = current_probe(runtime, epoch)
                assert probe is not None and not probe.done()
                release.set()
                await asyncio.wait_for(asyncio.shield(probe), 2)
                assert probe.exception() is None and invocations == [1]
                assert runtime.status.last_failure == "BootOrCeilingUnavailable"
                assert runtime.status.charger_fault.classification == "IntegrityError"
                assert runtime.status.charger_fault.stage == "boot_or_ceiling"
                await until(lambda: not runtime.introduction_pending)
                await charger.send(build_call("terminal-heartbeat", "Heartbeat", {}))
                assert decode(await charger.recv())[:2] == [3, "terminal-heartbeat"]
                assert invocations == [1] and bench.gateway._charger_epoch == epoch
        finally:
            release.set()
            await bench.close()
