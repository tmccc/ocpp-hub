"""A closed temporary session retries an unsent automatic release after a genuine final native HTTP failure."""

import asyncio
import copy
import uuid
from datetime import datetime

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.managed_cutoffs import supervised_timer
from ocpp_hub_core.prototype.managed_occurrences import KEY, occurrences
from ocpp_hub_core.prototype.native_bounded_handover import NativeBoundedHandover
from ocpp_hub_core.prototype.rpc import Peer

from tests import test_managed_schedule_cutoff as cutoff_bench
from tests.native_fault_support import FinalReleaseHttpPeer as FinalReleaseHttpPeer
from tests.native_fault_support import coroutine_locals as coroutine_locals
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: actual temporary closure survives one final pre-dispatch HTTP failure and restores the same app selection.
# Why: a completed occurrence must not turn a known unsent automatic release into a fault requiring owner intervention.
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("cancel_outer", [False, True])
async def test_closed_timer_restoration_retries_final_native_http503(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, cancel_outer
):
    monkeypatch.setattr(cutoff_bench, "SchedulePeer", FinalReleaseHttpPeer)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j, peer = b.c, b.session.gateway, b.session.journal, b.rig.peer
        assert isinstance(peer, FinalReleaseHttpPeer)
        if selected != c.current.selected():
            c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=selected)
            await c.current.settle()
            await healthy(c)
        timer_request = uuid.uuid4().hex
        await b.arm(timer_request)
        original_plan = copy.deepcopy(b.device.plans[9])
        original_configuration = copy.deepcopy(peer.configuration)
        intent = c.requests.intent()
        b.clock.now = b.start
        temporary = await b.fire(b.timer)
        assert (await healthy(c))["power_w"] > 0
        assert j.active() == temporary and j.get(KEY)["bound_local"] == temporary
        b.clock.now += 60
        await b.close_transaction(temporary)
        await until(lambda: j.next_delivery() is None and not g.vendor._pending)
        assert j.active() is None and b.device.active is None
        epoch, boot, metadata_revision = g._charger_epoch, g.boot_observation, g.boot_metadata_revision
        control_revision = g.controls._revision
        assert boot is not None
        call_mark = len(b.rig.bench.device_calls)
        observations = []

        # Select only a real owned release whose separately running native validator is auditing this positive call.
        def select_final_release():
            lease = j.get(KEY, {})
            ticket = g.controls._release_ticket
            if (
                lease.get("id") != timer_request
                or lease.get("state") != "complete"
                or lease.get("result") != "session_closed"
                or any(supervised_timer(value) for value in occurrences(j).values())
                or c.requests.intent() != intent
                or c.requests.pending_return() is not None
                or c.ordinary.active()
                or j.get("trial_phase") != "active"
                or ticket is None
                or j.get("control", {}).get("ticket") != ticket
                or b.session.book.before_positive_release is None
                or g._charger_epoch != epoch
                or g.boot_observation != boot
                or g.boot_metadata_revision != metadata_revision
                or g.controls._revision != control_revision
            ):
                return None
            candidates = []
            for task in tuple(g.controls._operations):
                release = coroutine_locals(task, LocalControl._release.__code__)
                call = coroutine_locals(task, Peer._call_locked.__code__)
                validation = coroutine_locals(task, Peer._validate.__code__)
                validator_task = validation.get("validation")
                if not isinstance(validator_task, asyncio.Task):
                    continue
                native_validation = coroutine_locals(validator_task, NativeBoundedHandover.validate_send.__code__)
                native_read = coroutine_locals(validator_task, NativeBoundedHandover._read.__code__)
                actual = call.get("actual")
                if (
                    release.get("self") is not g.controls
                    or release.get("ticket") != ticket
                    or release.get("epoch") != epoch
                    or call.get("self") is not g.charger
                    or call.get("action") != "SetChargingProfile"
                    or validation.get("self") is not g.charger
                    or validation.get("action") != "SetChargingProfile"
                    or native_validation.get("self") is not c.native
                    or native_validation.get("action") != "SetChargingProfile"
                    or native_validation.get("payload") is not actual
                    or native_read.get("self") is not c.native
                    or not isinstance(actual, dict)
                ):
                    continue
                profile = actual.get("csChargingProfiles", {})
                periods = profile.get("chargingSchedule", {}).get("chargingSchedulePeriod", [])
                if profile.get("chargingProfileId") != 7001 or not periods or periods[0].get("limit") != selected:
                    continue
                candidates.append(
                    {
                        "ticket": ticket,
                        "epoch": epoch,
                        "boot": boot,
                        "metadata_revision": metadata_revision,
                        "control_revision": control_revision,
                        "token": call["unique_id"],
                        "payload": copy.deepcopy(actual),
                        "native_sequence": native_read["sequence"],
                        "lease": copy.deepcopy(lease),
                        "timer_receipt": copy.deepcopy(c.requests.receipt(timer_request)),
                    }
                )
            assert len(candidates) <= 1, candidates
            return candidates[0] if candidates else None

        peer.select = select_final_release

        # Let normal ticks finish real selective removal and reach automatic restoration without changing journal state.
        async def drive_until_http_failure():
            for _ in range(10):
                status = await c.tick()
                observations.append(status)
                if peer.responded:
                    return status
                await asyncio.sleep(0.01)
            raise AssertionError({"no_final_http_failure": observations, "lease": j.get(KEY)})

        driving = asyncio.create_task(drive_until_http_failure())
        try:
            await asyncio.wait_for(peer.entered.wait(), 8)
            witness = peer.witness
            assert witness is not None and witness["http_action"] == "getConfig"
            assert witness["lease"]["state"] == "complete" and witness["lease"]["result"] == "session_closed"
            assert witness["timer_receipt"]["state"] == "completed"
            assert not any(supervised_timer(value) for value in occurrences(j).values())
            assert j.active() is None and b.device.active is None and b.device.effective_limit() == 0
            assert not g.controls.ready and not driving.done()
            assert c.requests.intent() == intent and c.current.selected() == selected
            assert not b.stops() and not starts(b.rig)
            assert not any(frame[1] == witness["token"] for frame in b.rig.bench.device_calls)
            assert all(
                frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                for frame in b.rig.bench.device_calls[call_mark:]
                if frame[2] == "SetChargingProfile"
            )
            attempt = g.controls.release_attempt
            guard = b.session.book.before_positive_release
            assert attempt is not None and guard is attempt.positive_guard
            if cancel_outer:
                driving.cancel()
                await asyncio.gather(driving, return_exceptions=True)
                assert driving.cancelled()
                assert g.controls.release_attempt is attempt and not attempt.finished
                assert b.session.book.before_positive_release is guard and not attempt.known_unsent()
            peer.release.set()
            if not cancel_outer:
                await asyncio.wait_for(driving, 8)
            await until(lambda: not g.controls._operations and not g.charger._pending)
            assert attempt.finished and attempt.known_unsent() and attempt.zero_confirmed
            assert b.session.book.before_positive_release is None
            assert peer.responded and peer.witness is witness
            assert not any(frame[1] == witness["token"] for frame in b.rig.bench.device_calls)
            assert all(
                frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                for frame in b.rig.bench.device_calls[call_mark:]
                if frame[2] == "SetChargingProfile"
            )
            assert not j.get("managed_control_fault"), {"witness": witness, "statuses": observations}
            assert j.get("control")["uncertain"] is False
            assert g.controls.ready and g.controls.desired() == 0 and b.device.effective_limit() == 0
            assert c.released_epoch != epoch
            assert c.requests.intent() == intent and c.current.selected() == selected
            assert c.requests.receipt(timer_request) == witness["timer_receipt"]
            assert j.get(KEY)["result"] == "session_closed" and set(b.device.plans) == {9}
            assert b.device.plans[9] == original_plan and peer.configuration == original_configuration
            assert not b.stops() and not starts(b.rig)
        finally:
            peer.release.set()
            peer.select = None
            if not driving.done():
                driving.cancel()
            await asyncio.gather(driving, return_exceptions=True)
            # Runtime retirement wakes any shielded child before fixture teardown closes its journal.
            if g.controls._operations:
                await b.session.runtime.close()
                await asyncio.gather(*tuple(g.controls._operations), return_exceptions=True)

        assert (await healthy(c))["station_ceiling_a"] == selected
        assert c.requests.intent() == intent and c.current.selected() == selected
        assert c.requests.receipt(timer_request) == witness["timer_receipt"]
        assert j.get(KEY)["result"] == "session_closed" and not j.get("managed_control_fault")
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (None, temporary) and j.active() == following
        status = await healthy(c)
        assert status["current_a"] == selected and status["power_w"] > 0
        assert b.device.plans == {9: original_plan} and peer.configuration == original_configuration
        assert not b.stops() and not starts(b.rig)
        await b.advance_running_plan(9, following, int(datetime(2026, 11, 12, 6, tzinfo=ZONE).timestamp()))
        assert (await healthy(c))["power_w"] == 0
        assert c.requests.receipt(timer_request) == witness["timer_receipt"]
        assert not b.stops() and not starts(b.rig)
