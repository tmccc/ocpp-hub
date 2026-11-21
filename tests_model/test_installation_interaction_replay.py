"""Check all orders of exact cleanup proof on the real TLS and native loopback paths."""

import asyncio
import copy
import uuid
from itertools import permutations

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.managed_rig import ReplyBench
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.test_native_bounded_handover import native_ready, restore_original_route, verify_original_backend
from tests.tls_support import tls as tls

from .installation_interactions import Installation, Phase, install_edges

CLOSURES = ("native session closed", "exact Stop report", "exact remaining delivery settled")


# Follow the same legal transitions used by the interactive installation model.
def step(state, event):
    (selected,) = (edge for edge in install_edges(state) if edge.event == event)
    return selected.state


# A cloud reply can only follow its real Stop. Native closure can arrive on either side of both.
def closure_orders():
    return tuple(order for order in permutations(CLOSURES) if order.index(CLOSURES[1]) < order.index(CLOSURES[2]))


# Intent: replay all legal native closure, OCPP report and cloud delivery orders before restoring the original route.
# Why: accepting RemoteStop or independently observing one closure cannot permit clearing an owned station profile.
@pytest.mark.parametrize("order", closure_orders(), ids="-".join)
async def test_installation_replay(tmp_path, tls, unused_tcp_port, order):
    async with native_ready(tmp_path, tls, unused_tcp_port, bench_type=ReplyBench) as (rig, session, charger):
        bench, journal = rig.bench, session.journal
        original_plan = copy.deepcopy(bench.firmware.plans)
        await session.command({"command": "release"})
        profile = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 100001, stack_level=2)
        assert (await session.gateway.charger.call("SetChargingProfile", profile))[2]["status"] == "Accepted"
        started = await bench.call(charger, "rollback-start", "StartTransaction", START)
        local = started[2]["transactionId"]
        rig.peer.paused, rig.peer.permission_enabled = False, True
        bench.cp, bench.connection = "C", "Charging"
        bench.firmware.requested_current = 6
        await until(lambda: journal.next_delivery() is None)
        bench.stop_gate = asyncio.Event()
        stopped = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 2000}
        model = Installation(phase=Phase.ACTIVE, matching_boot=True, latest_proof=True, profiles=2)
        model = step(step(model, "exact charger session opens"), "cleanup requested")
        cleanup = asyncio.create_task(session.command({"command": "native-bounded-handover"}))
        try:
            await asyncio.wait_for(bench.remote_stop.wait(), 3)
            for event in order:
                model = step(model, event)
                if event == CLOSURES[0]:
                    bench.firmware.active = None
                    bench.firmware.requested_current = 0
                    bench.cp, bench.connection = "B", "Plug"
                    rig.peer.permission_enabled = False
                    for row in bench.history:
                        if row["transactionId"] == local:
                            row["stop"] = copy.deepcopy(stopped)
                elif event == CLOSURES[1]:
                    # Send only the independent ledger report. The native reader sees its own physical state.
                    token = uuid.uuid4().hex
                    completion = asyncio.get_running_loop().create_future()
                    bench.replies[token] = completion
                    await charger.send(build_call(token, "StopTransaction", stopped))
                    assert (await asyncio.wait_for(completion, 2))[0] == 3
                    await until(lambda: bool(getattr(bench, "held_stop_replies", [])))
                else:
                    bench.stop_gate.set()
                    await until(lambda: journal.next_delivery() is None)

                if model.native_open or model.ledger_open or model.cloud_pending:
                    # Allow actual cleanup audits to run before observing that protection is still in place.
                    await asyncio.sleep(0.24)
                    assert not cleanup.done() and journal.get("trial_phase") in ("blocked", "recovering")
                    assert journal.get("native_bounded_handover")["stage"] == "closing"
                    assert bench.firmware.effective_limit() == 0
                    assert not any(frame[2] == "ClearChargingProfile" for frame in bench.device_calls)
                    with pytest.raises((DeniedError, RecoveryRequiredError)):
                        await session.command({"command": "release"})
                assert (journal.active() == local) is model.ledger_open
                assert (bench.firmware.active == local) is model.native_open
            model = step(step(model, "fresh independent idle verified for cleanup"), "cleanup commit")
            assert (await asyncio.wait_for(cleanup, 4))["profiles_restored"]
            for _ in range(model.profiles):
                model = step(step(model, "owned clear issued"), "owned profile independently absent")
            model = step(model, "profiles absent, restoration pending")
            assert [frame[3] for frame in bench.device_calls if frame[2] == "ClearChargingProfile"] == [
                {"id": 100001},
                {"id": 7001},
            ]
            assert [frame[3] for frame in bench.device_calls if frame[2] == "RemoteStopTransaction"] == [
                {"transactionId": local},
            ]
            assert journal.get("trial_phase") == model.phase.value and not bench.firmware.profiles
            await restore_original_route(rig, session, charger)
            model = step(step(model, "route restoration intent saved"), "proxy detached and offline verified")
            model = step(model, "original route independently observed")
            await verify_original_backend(rig, session)
            model = step(step(model, "original backend offline observed"), "later original backend online verified")

            # The synthetic backend applies the one expected setting after the original route is proven.
            async def restore(values):
                assert values == {"AmpLmt": "32"}
                rig.peer.configuration.update(values)
                rig.peer.runtime_user = 320

            assert (await session.native_bounded.restore_settings(restore))["complete"]
            model = step(
                step(model, "fresh idle and online proof recorded for settings"),
                "original settings and history verified",
            )
            assert journal.get("trial_phase") == step(model, "restoration completed").phase.value
            assert any(row.get("stop") == stopped for row in bench.history)
            assert bench.firmware.plans == original_plan
        finally:
            bench.stop_gate.set()
            if not cleanup.done():
                cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)
