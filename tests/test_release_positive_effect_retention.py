"""Whole-release effects survive a later transport failure and genuine zero rollback."""

import asyncio
import copy
import json
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_cutoffs import idle_cutoff_pending, supervised_timer
from ocpp_hub_core.prototype.managed_occurrences import KEY
from ocpp_hub_core.prototype.recovery_contracts import FailureKind, WireReply

from tests.handover_helpers import crash
from tests.test_managed_proxy import introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_idle_cutoff import END, early_cleanup
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: a positive profile applied before an unsent composite remains a possible effect after verified rollback.
# Why: whole-attempt dispatch facts cannot be replaced by the last RPC's transport failure or a later zero result.
async def test_applied_positive_then_unsent_composite_keeps_release_fault(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b,
        AsyncExitStack() as replacements,
    ):
        await b.arm()
        identifier = b.session.journal.get(KEY)["id"]
        await early_cleanup(b)
        lease = copy.deepcopy(b.session.journal.get(KEY))
        assert lease["state"] == "complete" and lease["result"] == "no_scheduled_session"
        assert idle_cutoff_pending(lease) and supervised_timer(lease)
        assert b.session.journal.active() is None
        plan, configuration = copy.deepcopy(b.device.plans[9]), copy.deepcopy(b.rig.peer.configuration)
        # A real new introduction before the remaining cutoff reaches the ordinary automatic idle restoration.
        b.clock.now = END - 1
        await b.c.tests.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        b.sock = await replacements.enter_async_context(b.rig.charger(b.session))
        await introduced(b.rig, b.session, b.sock, "cf05-positive-composite-counter")
        c, g, j = b.c, b.session.gateway, b.session.journal
        intent, selected = c.requests.intent(), c.current.selected()
        epoch, boot, revision = g._charger_epoch, g.boot_observation, g.controls._revision
        assert intent.mode == "app" and boot is not None and c.released_epoch != epoch
        assert g.controls.ready and g.controls.desired() == b.device.effective_limit() == 0
        assert j.get(KEY)["id"] == identifier and idle_cutoff_pending(j.get(KEY))
        assert not j.get(KEY).get("idle_cutoff_hold") and not j.get("managed_control_fault")
        wire = len(b.rig.bench.device_calls)
        original_sender, original_hook = g.charger.sender, b.rig.bench.before_reply
        assert original_sender is not None
        witness = {}
        zero_seen, release_zero = asyncio.Event(), asyncio.Event()

        # Fail only the real socket sender for the first post-positive composite, before forwarding any bytes.
        async def composite_transport_failure(text):
            frame = json.loads(text)
            attempt = g.controls.release_attempt
            if (
                not witness
                and frame[0] == 2
                and frame[2] == "GetCompositeSchedule"
                and attempt is not None
                and attempt.purpose == "automatic"
                and attempt.positive_may_have_dispatched
                and attempt.ticket == g.controls._release_ticket == j.get("control", {}).get("ticket")
            ):
                assert not attempt.finished and not attempt.cancelled and not attempt.positive_confirmed
                assert attempt.intent == intent and not attempt.known_unsent()
                assert j.get(KEY)["id"] == identifier and j.get(KEY)["result"] == "no_scheduled_session"
                assert c.requests.intent() == intent and c.requests.pending_return() is None
                assert j.active() is None and not c.ordinary.active()
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert g.controls._revision == revision and b.session.book.before_positive_release is not None
                positives = [
                    item
                    for item in b.rig.bench.device_calls[wire:]
                    if item[2] == "SetChargingProfile"
                    and item[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
                ]
                assert len(positives) == 1
                assert (
                    positives[0][3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                    == selected
                )
                assert b.device.effective_limit() == selected
                # Reaching this actual composite sender also proves _cap consumed the genuine Accepted Set reply.
                witness.update(attempt=attempt, composite_token=frame[1], positive=copy.deepcopy(positives[0]))
                raise OSError("independent test transport failed before forwarding this composite frame")
            await original_sender(text)

        # Observe the independent firmware's actual rollback zero before allowing its genuine acknowledgement.
        async def hold_rollback_zero(action, payload):
            if action == "SetChargingProfile" and witness:
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    zero_seen.set()
                    async with asyncio.timeout(5):
                        await release_zero.wait()
            if original_hook is not None:
                await original_hook(action, payload)

        g.charger.sender = composite_transport_failure
        b.rig.bench.before_reply = hold_rollback_zero
        driving = asyncio.create_task(c.tick())
        try:
            await asyncio.wait_for(zero_seen.wait(), 5)
            attempt = witness["attempt"]
            assert g.controls.release_attempt is attempt and not attempt.finished
            assert attempt.positive_may_have_dispatched and not attempt.positive_confirmed
            assert attempt.zero_may_have_dispatched and not attempt.zero_confirmed
            assert not attempt.known_unsent() and not driving.done()
            assert b.device.effective_limit() == 0 and not release_zero.is_set()
            assert not any(item[1] == witness["composite_token"] for item in b.rig.bench.device_calls)
            assert j.get("control")["ticket"] == attempt.ticket
            assert j.get("control")["inhibit"] is True and j.get("control")["uncertain"] is True
            assert not g.controls.ready and c.released_epoch != epoch
            release_zero.set()
            status = await asyncio.wait_for(driving, 5)
            await until(lambda: not g.controls._operations and not g.charger._pending)
            assert attempt.finished and not attempt.cancelled and g.controls.release_attempt is None
            assert attempt.positive_may_have_dispatched and not attempt.positive_confirmed
            assert attempt.zero_may_have_dispatched and attempt.zero_confirmed
            result = attempt.result()
            assert not attempt.known_unsent() and result.pending is None and not result.published
            assert result.failure_kind is FailureKind.TRANSPORT and result.effects.positive_reply is WireReply.ACCEPTED
            assert result.effects.positive_dispatched and result.effects.zero_verified
            assert not any(item[1] == witness["composite_token"] for item in b.rig.bench.device_calls)
            sent = b.rig.bench.device_calls[wire:]
            assert sum(item[2] == "SetChargingProfile" for item in sent) == 2
            assert any(item[2] == "GetCompositeSchedule" for item in sent)
            assert b.device.effective_limit() == 0
            fault = copy.deepcopy(j.get("managed_control_fault"))
            assert fault["classification"] == "owner_release_unconfirmed", status
            assert j.get("control")["inhibit"] is True and j.get("control")["uncertain"] is True
            assert c.released_epoch != epoch and c.requests.intent() == intent and c.current.selected() == selected
            assert b.device.plans == {9: plan} and b.rig.peer.configuration == configuration
            assert not b.stops() and not starts(b.rig)
            # A subsequent real observation may verify the hold, but cannot retire this possible-positive fault.
            await c.tick()
            assert j.get("managed_control_fault") == fault
            assert attempt.positive_may_have_dispatched and attempt.zero_confirmed and not attempt.known_unsent()
            assert b.device.effective_limit() == 0 and c.released_epoch != epoch
            assert c.requests.intent() == intent and c.current.selected() == selected
            assert b.device.plans == {9: plan} and b.rig.peer.configuration == configuration
            assert not b.stops() and not starts(b.rig)
        finally:
            release_zero.set()
            b.rig.bench.before_reply = original_hook
            if not driving.done():
                driving.cancel()
            await asyncio.gather(driving, return_exceptions=True)
            # Restore the same live sender only after an owned child has left its transport sequence.
            if g.controls._operations:
                await b.session.runtime.close()
                await asyncio.gather(*tuple(g.controls._operations), return_exceptions=True)
            if g.charger.sender is composite_transport_failure:
                g.charger.sender = original_sender
