"""A real cloud allocates before its reply while closure, Pause and process replacement can proceed independently."""

import asyncio
import uuid
from contextlib import AsyncExitStack

import pytest

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_delivery_recovery import map_unknown_retry
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: lifecycle closure and owner revocation before the Start reply never lose real mapping or later recovery.
# Why: permutations that begin after mapping cannot establish the effect of a reply arriving after these events.
@pytest.mark.parametrize("reply", ["Accepted", "Blocked"])
@pytest.mark.parametrize("source", ["daily", "local"])
@pytest.mark.parametrize("cut", ["close-before-reply", "pause-before-reply", "restart-before-reply"])
async def test_cloud_start_reply_ordering(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, reply, source, cut):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        stack, gate = AsyncExitStack(), asyncio.Event()
        try:
            s, c, sock = await stack.enter_async_context(running(rig, "before-start-reply", owner_tag="APP_OWNER"))
            await mp.healthy(c)
            tag = "NATIVE_TIMER"
            if source == "local":
                c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
                await c.tests.settle()
                tag = mp.starts(rig)[-1][3]["idTag"]
            original_starts = len(mp.starts(rig))
            rig.bench.start_replies, rig.bench.start_reply_gate = [reply], gate
            opened = await rig.bench.call(sock, uuid.uuid4().hex, "StartTransaction", START | {"idTag": tag})
            assert opened[0] == 3
            local = opened[2]["transactionId"]
            await until(lambda: len(rig.bench.backend_starts) == 1)
            independent = rig.bench.backend_starts[0]
            assert s.journal.vendor_id(local) is None and independent["answer"]["idTagInfo"]["status"] == reply
            old_pause = await pause(c) if cut == "pause-before-reply" else None
            assert (
                await rig.bench.call(
                    sock,
                    uuid.uuid4().hex,
                    "StopTransaction",
                    {
                        "transactionId": local,
                        "meterStop": 1100,
                        "timestamp": "2026-01-01T00:01:00Z",
                    },
                )
            )[0] == 3
            if cut == "restart-before-reply":
                await pause(c)
                await stack.aclose()
                stack = AsyncExitStack()
                gate.set()
                rig.bench.start_reply_gate = None
                s, c, sock = await stack.enter_async_context(running(rig, "unknown-held-reply"))
                await until(lambda: s.runtime.status.delivery_blocked)
                await c.tick()
                await pause(c)
                assert s.journal.next_delivery().state == "ambiguous"
                await stack.aclose()
                stack = AsyncExitStack()
                await map_unknown_retry(rig, tmp_path, capsys)
                s, c, sock = await stack.enter_async_context(running(rig, "mapped-held-reply"))
            else:
                gate.set()
                rig.bench.start_reply_gate = None
            await until(lambda: s.journal.next_delivery() is None)
            assert s.journal.vendor_id(local) == independent["answer"]["transactionId"]
            assert (
                independent["closed"]
                and independent["stop"]["body"]["transactionId"] == independent["answer"]["transactionId"]
            )
            if old_pause is not None and reply == "Blocked":
                c.command(old_pause[0], "pause", old_pause[1])
                assert s.journal.get("managed_control_fault") is not None
            await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert s.journal.get("managed_control_fault") is None
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=6, original_starts=original_starts)
            assert len(rig.bench.backend_starts) == 3 and all(entry["closed"] for entry in rig.bench.backend_starts)
        finally:
            gate.set()
            await stack.aclose()
