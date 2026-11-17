"""An audited open-session introduction hands an interrupted zero to the runtime's restrictive retry."""

import asyncio
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, managed_session, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_start_overlap import expire, owner_tag
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: lost zero/composite replies after an audited daily recovery retry zero without repeating introduction or Stop.
# Why: an unrelated expired normal Start retains debt, but must not strand this genuine session at starting forever.
@pytest.mark.parametrize("lost_action", ["SetChargingProfile", "GetCompositeSchedule"])
async def test_open_introduction_retries_only_zero(tmp_path, tls, unused_tcp_port, monkeypatch, lost_action):
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b,
        AsyncExitStack() as replacements,
    ):
        owner_tag(b)
        request = uuid.uuid4().hex
        b.c.command(request, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        daily = await b.start_plan(9)
        await healthy(b.c)
        receipt = b.session.journal.get("local_start_receipt")
        intent = b.c.requests.intent()
        assert receipt["stage"] == "accepted" and receipt["local"] is None
        await expire(b)
        await b.c.charging.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        owner_tag(b)
        g, j = b.session.gateway, b.session.journal
        g.charger.timeout = 0.3
        assert b.c.requests.intent() == intent and not j.get("trial_starting")
        assert j.get("control")["start_pending"] and j.active() == daily
        first, second, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        zeroes = 0
        reads = []
        interrupted = False
        before = len(b.rig.bench.device_calls)
        previous_hook = b.rig.bench.before_reply

        # Delay an actual peer reply beyond its wire budget, then hold the retry's real zero acknowledgement.
        async def before_reply(action, payload):
            nonlocal zeroes, interrupted
            if action == "SetChargingProfile":
                assert payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                zeroes += 1
                if zeroes == 2:
                    reads.append(b.c.native.read_sequence)
                    second.set()
                    async with asyncio.timeout(5):
                        await release.wait()
            if action == lost_action and not interrupted:
                interrupted = True
                reads.append(b.c.native.read_sequence)
                first.set()
                await asyncio.sleep(g.charger.timeout + 0.1)

        b.rig.bench.before_reply = before_reply
        try:
            b.sock = await replacements.enter_async_context(b.rig.charger(b.session))
            assert (await b.rig.bench.call(b.sock, "retry-open-zero", "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(first.wait(), 2)
            epoch, boot = g._charger_epoch, g.boot_observation
            await asyncio.wait_for(second.wait(), 2)
            assert zeroes == 2 and reads[0] == reads[1]
            assert b.c.native.introduced_revision == g.controls._revision
            assert not g.controls.ready and g.controls.desired() == 0
            assert g._charger_epoch == epoch and g.boot_observation == boot
            assert j.get("trial_phase") == "recovering" and j.get("control")["uncertain"]
            assert b.device.effective_limit() == 0
            assert j.get("local_start_receipt") == receipt and j.get("control")["start_pending"]
            assert b.c.requests.intent() == intent and not b.stops()
        finally:
            release.set()
            b.rig.bench.before_reply = previous_hook
        await until(lambda: g.controls.ready and g.vendor_registration == "Accepted")
        status = await healthy(b.c)
        assert status["station_ceiling_a"] == 6 and b.c.charging.current_daily(daily)
        assert j.get("local_start_receipt") == receipt and j.get("control")["start_pending"]
        assert b.c.requests.receipt(request)["state"] == "uncertain"
        assert len(starts(b.rig)) == 1
        assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls[before:])
        await b.close_transaction(daily)
