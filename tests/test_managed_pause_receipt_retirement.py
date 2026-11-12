"""A later authorized Start gives an unfinished Pause a truthful terminal receipt."""

import copy
import uuid
from dataclasses import replace

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_managed_test_charge import ChargeRig, actual_stop, positive_flow
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: Start retires the earlier Pause without breaking UUID replay or the physical effect of a new Pause.
# Why: a command receipt cannot remain pending forever after another authorized operation supersedes its hold.
async def test_normal_start_retires_unconfirmed_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        rig.prepared.config = replace(rig.prepared.config, local_tags=frozenset({"APP_OWNER", "NATIVE_TIMER"}))
        session = managed_session(rig)
        await session.start()
        from ocpp_hub_core.prototype.managed import ManagedController

        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "pause-receipt-boot")
            await healthy(c)
            old, revision = uuid.uuid4().hex, c.requests.intent().revision
            c.command(old, "pause", revision)
            await until(lambda: c.pause_task is not None and c.pause_task.done())
            await c.pause_task
            native = await rig.prepared.observer.observe_handover()
            assert all(v == 0 for v in native.currents_a + native.powers_w)
            assert rig.bench.firmware.effective_limit() == 0 and c.requests.receipt(old)["state"] == "accepted"
            start = uuid.uuid4().hex
            c.command(start, "start", c.requests.intent().revision)
            await c.tests.settle()
            receipt = c.requests.receipt(old)
            assert receipt["state"] == "failed" and receipt["result"] == {
                "classification": "superseded_by_start",
                "superseded_by": start,
            }
            before, intent = copy.deepcopy(rig.bench.device_calls), c.requests.intent()
            assert c.command(old, "pause", revision) == receipt
            assert rig.bench.device_calls == before and c.requests.intent() == intent
            assert len(starts(rig)) == 1 and rig.bench.firmware.effective_limit() == 6
            test = ChargeRig(rig, session, c, charger)
            local = await positive_flow(test)
            assert (await healthy(c))["current_a"] == 6 and c.requests.receipt(old) == receipt
            fresh = uuid.uuid4().hex
            c.command(fresh, "pause", c.requests.intent().revision)
            assert (await actual_stop(test, local))["local_hold_verified"]
            assert c.requests.receipt(fresh)["state"] == "completed" and c.requests.receipt(old) == receipt
            assert rig.bench.firmware.effective_limit() == 0
        await session.close()
