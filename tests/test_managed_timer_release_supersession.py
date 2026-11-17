"""An actual timer deadline supersedes an unsent automatic release without losing zero ownership."""

import asyncio
import copy
from contextlib import AsyncExitStack
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_cutoffs import idle_cutoff_pending, supervised_timer
from ocpp_hub_core.prototype.managed_occurrences import KEY
from ocpp_hub_core.prototype.native_bounded_handover import NativeBoundedHandover
from ocpp_hub_core.prototype.rpc import Peer

from tests import test_managed_schedule_cutoff as cutoff_bench
from tests.handover_helpers import crash
from tests.native_fault_support import FinalReleaseHttpPeer, coroutine_locals
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_idle_cutoff import END, early_cleanup
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: a real independent cutoff supersedes an entirely unsent automatic release without a permanent false fault.
# Why: the newer pending zero owns the gate; an older withheld release must not erase its uncertainty or block it.
async def test_idle_timer_deadline_supersedes_unsent_release(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(cutoff_bench, "SchedulePeer", FinalReleaseHttpPeer)
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b,
        AsyncExitStack() as replacements,
    ):
        await b.arm()
        identifier = b.session.journal.get(KEY)["id"]
        await early_cleanup(b)
        saved = copy.deepcopy(b.session.journal.get(KEY))
        assert saved["id"] == identifier and saved["state"] == "complete"
        assert saved["result"] == "no_scheduled_session" and idle_cutoff_pending(saved)
        assert supervised_timer(saved) and b.session.journal.active() is None
        plan, configuration = copy.deepcopy(b.device.plans[9]), copy.deepcopy(b.rig.peer.configuration)
        # This is a fresh actual introduction before the separate original06:00 cutoff, not a forged timer state.
        b.clock.now = END - 1
        await b.c.tests.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        b.sock = await replacements.enter_async_context(b.rig.charger(b.session))
        await introduced(b.rig, b.session, b.sock, "cf05-real-idle-reintroduction")
        c, g, j, peer = b.c, b.session.gateway, b.session.journal, b.rig.peer
        assert isinstance(peer, FinalReleaseHttpPeer)
        intent, selected = c.requests.intent(), c.current.selected()
        epoch, boot, revision = g._charger_epoch, g.boot_observation, g.controls._revision
        assert intent.mode == "app" and boot is not None and c.released_epoch != epoch
        assert g.controls.ready and g.controls.desired() == b.device.effective_limit() == 0
        assert j.get(KEY)["id"] == identifier and idle_cutoff_pending(j.get(KEY))
        assert not j.get(KEY).get("idle_cutoff_hold") and not j.get("managed_control_fault")
        wire = len(b.rig.bench.device_calls)

        # Select this genuine release's separately running positive native validator on its actual HTTP request.
        def select_idle_release():
            lease = j.get(KEY, {})
            ticket = g.controls._release_ticket
            if (
                lease.get("id") != identifier
                or lease.get("state") != "complete"
                or lease.get("result") != "no_scheduled_session"
                or not idle_cutoff_pending(lease)
                or lease.get("idle_cutoff_hold")
                or c.requests.intent() != intent
                or c.requests.pending_return() is not None
                or c.ordinary.active()
                or j.get("trial_phase") != "active"
                or j.active() is not None
                or ticket is None
                or j.get("control", {}).get("ticket") != ticket
                or b.session.book.before_positive_release is None
                or g._charger_epoch != epoch
                or g.boot_observation != boot
                or g.controls._revision != revision
            ):
                return None
            for task in tuple(g.controls._operations):
                release = coroutine_locals(task, LocalControl._release.__code__)
                call = coroutine_locals(task, Peer._call_locked.__code__)
                validation = coroutine_locals(task, Peer._validate.__code__)
                validator = validation.get("validation")
                if not isinstance(validator, asyncio.Task):
                    continue
                native_call = coroutine_locals(validator, NativeBoundedHandover.validate_send.__code__)
                native_read = coroutine_locals(validator, NativeBoundedHandover._read.__code__)
                actual = call.get("actual")
                if (
                    release.get("self") is not g.controls
                    or release.get("ticket") != ticket
                    or release.get("epoch") != epoch
                    or call.get("self") is not g.charger
                    or call.get("action") != "SetChargingProfile"
                    or validation.get("self") is not g.charger
                    or validation.get("action") != "SetChargingProfile"
                    or native_call.get("self") is not c.native
                    or native_call.get("payload") is not actual
                    or native_read.get("self") is not c.native
                    or not isinstance(actual, dict)
                ):
                    continue
                profile = actual.get("csChargingProfiles", {})
                periods = profile.get("chargingSchedule", {}).get("chargingSchedulePeriod", [])
                if profile.get("chargingProfileId") == 7001 and periods and periods[0].get("limit") == selected:
                    return {"ticket": ticket, "token": call["unique_id"], "lease": copy.deepcopy(lease)}
            return None

        peer.select = select_idle_release
        zero_seen, release_zero = asyncio.Event(), asyncio.Event()
        previous_hook = b.rig.bench.before_reply

        # Hold the newer actual zero acknowledgement after the independent firmware applied its command.
        async def hold_zero_ack(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    zero_seen.set()
                    async with asyncio.timeout(5):
                        await release_zero.wait()
            if previous_hook is not None:
                await previous_hook(action, payload)

        driving = asyncio.create_task(c.tick())
        watcher = None
        try:
            await asyncio.wait_for(peer.entered.wait(), 5)
            witness = peer.witness
            assert witness is not None and witness["http_action"] == "getConfig"
            assert witness["lease"]["id"] == identifier and not driving.done()
            assert not any(frame[1] == witness["token"] for frame in b.rig.bench.device_calls)
            assert not g.controls.ready and b.device.effective_limit() == 0
            assert c.requests.intent() == intent and c.current.selected() == selected
            b.rig.bench.before_reply = hold_zero_ack
            # Real elapsed scheduling crosses the genuine deadline while the positive HTTP audit stays held.
            loop = asyncio.get_running_loop()
            boundary = loop.time() + 0.2
            with monkeypatch.context() as clock_patch:
                clock_patch.setattr(
                    managed_schedule, "time", SimpleNamespace(time=lambda: END + loop.time() - boundary)
                )
                watcher = asyncio.create_task(c.schedule.watch_deadline())
                c.schedule.changed.set()
                await asyncio.wait_for(driving, 3)
                # Prove the actual watchdog/occurrence boundary before the intended old-source failure assertion.
                assert j.get(KEY)["id"] == identifier and j.get(KEY)["idle_cutoff_hold"] is True
                assert j.get(KEY)["cutoff_reached"] is True
                assert not any(frame[1] == witness["token"] for frame in b.rig.bench.device_calls)
                assert all(
                    frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                    for frame in b.rig.bench.device_calls[wire:]
                    if frame[2] == "SetChargingProfile"
                )
                # The baseline must fail HERE with its genuine owner_release_unconfirmed, not at a generic wait.
                assert not j.get("managed_control_fault"), j.get("managed_control_fault")
                await asyncio.wait_for(zero_seen.wait(), 2)
                assert not release_zero.is_set() and g.controls._operations
                assert j.get("control")["ticket"] != witness["ticket"]
                assert j.get("control")["owner"] == "local" and j.get("control")["inhibit"] is True
                assert j.get("control")["uncertain"] is True and not g.controls.ready
                assert c.released_epoch != epoch and c.requests.intent() == intent
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert b.device.effective_limit() == 0 and not b.stops() and not starts(b.rig)
                release_zero.set()
                await until(lambda: g.controls.ready and not g.controls._operations and not g.charger._pending)
                b.clock.now = END + 1
            assert (await healthy(c))["station_ceiling_a"] == 0
            assert c.requests.intent() == intent and c.current.selected() == selected
            assert b.device.plans == {9: plan} and peer.configuration == configuration
            assert not j.get("managed_control_fault") and not b.stops() and not starts(b.rig)
        finally:
            release_zero.set()
            peer.release.set()
            peer.select = None
            b.rig.bench.before_reply = previous_hook
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            if not driving.done():
                driving.cancel()
            await asyncio.gather(driving, return_exceptions=True)
            # A shielded release must retire before fixture teardown closes its durable journal.
            if g.controls._operations:
                await b.session.runtime.close()
                await asyncio.gather(*tuple(g.controls._operations), return_exceptions=True)
