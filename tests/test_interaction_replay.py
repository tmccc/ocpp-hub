"""Check the reference model's Start/Pause/report boundary against the live loopback engine."""

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from itertools import permutations

import pytest

from tests.managed_rig import starts
from tests.native_schedule_support import timer_bench
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls
from tests_model.interaction_model import Control, Mode, Start, decisions

EVENTS = ("daily Start report", "Pause commits", "matching accepted captured")


# Use the same rules as the reachability test to advance the independent expected state.
def apply(state, name):
    (edge,) = (edge for edge in decisions(state) if edge.event == name)
    return edge.state


# Intent: check the daily report, owner Pause and actual owner Start Accepted in all six orders.
# Why: the same Accepted belongs to its original command even when a different session or request intervenes.
@pytest.mark.parametrize("order", tuple(permutations(EVENTS)), ids="-".join)
async def test_start_pause_daily_replay(tmp_path, tls, unused_tcp_port, monkeypatch, order):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        gateway, journal = b.session.gateway, b.session.journal
        gateway.policy = replace(gateway.policy, allowed_tags=frozenset({"APP_OWNER", *gateway.policy.allowed_tags}))
        gateway.controls.policy = gateway.policy
        start_id, pause_id = uuid.uuid4().hex, uuid.uuid4().hex
        start_reply = asyncio.get_running_loop().create_future()
        send = b.sock.send

        # The simulated device still executes Start. Delay only the original correlated reply.
        async def hold_start_reply(raw):
            frame = json.loads(raw)
            issued = starts(b.rig)
            if not start_reply.done() and issued and frame[0] == 3 and frame[1] == issued[0][1]:
                assert frame[2] == {"status": "Accepted"}
                start_reply.set_result(raw)
                return
            await send(raw)

        monkeypatch.setattr(b.sock, "send", hold_start_reply)
        b.c.command(start_id, "start", b.c.requests.intent().revision)
        raw = await asyncio.wait_for(start_reply, 3)
        state = apply(apply(Control(), "owner Start prepared"), "owner Start dispatched")
        daily = None
        for event in order:
            if event == "daily Start report":
                if state.mode is Mode.APP:
                    state = apply(state, "daily report starts ceiling check")
                    state = apply(state, "daily Start admitted")
                    daily = await b.start_plan(9, demand=False)
                else:
                    state = apply(state, "daily Start refused under Pause")
                    response = await b.rig.bench.call(
                        b.sock,
                        uuid.uuid4().hex,
                        "StartTransaction",
                        START
                        | {
                            "idTag": b.device.plans[9]["idTag"],
                            "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
                        },
                    )
                    assert response[0] == 4, response
            elif event == "Pause commits":
                state = apply(state, event)
                b.c.command(pause_id, "pause", b.c.requests.intent().revision)
            else:
                state = apply(state, event)
                await send(raw)
                await until(lambda: journal.get("local_start_receipt", {}).get("stage") == "accepted")
                state = apply(state, "matching Accepted saved")
            assert b.c.requests.intent().mode == ("app" if state.mode is Mode.APP else "paused")
            assert journal.active() == (daily if state.daily else None)
            receipt = journal.get("local_start_receipt")
            assert receipt["request_id"] == start_id and receipt["local"] is None
            assert receipt["stage"] == ("accepted" if state.start is Start.ACCEPTED else "issued")
            assert len(starts(b.rig)) == 1

        await b.c.charging.settle()
        await b.c.pause_task
        assert b.c.requests.receipt(pause_id)["state"] == "accepted"
        assert b.device.effective_limit() == 0
        if daily is not None:
            await b.close_transaction(daily)
            state = apply(state, "daily Stop report")
            assert state.start is Start.ACCEPTED
            await b.c.tick()
            assert b.c.requests.receipt(pause_id)["state"] == "accepted"
            assert journal.get("local_start_receipt")["request_id"] == start_id
            assert b.device.effective_limit() == 0
        await b.c.charging.close()
