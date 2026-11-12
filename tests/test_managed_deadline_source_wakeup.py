"""Source publication wakes deadline supervision after a released timer duty reopens."""

import asyncio
import uuid

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_unnumbered import unnumbered
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Inspect one actual coroutine's locals without wrapping it or changing any engine await point.
def task_locals(task, function):
    item = task.get_coro()
    for _ in range(30):
        if item is None:
            break
        code = getattr(item, "cr_code", None) or getattr(item, "gi_code", None)
        if code is not None and code.co_name == function:
            frame = getattr(item, "cr_frame", None) or getattr(item, "gi_frame", None)
            return frame.f_locals if frame is not None else {}
        item = getattr(item, "cr_await", None) or getattr(item, "gi_yieldfrom", None)
    return {}


# Intent: a valid native audit wakes the existing idle watchdog when it reopens a released timer obligation.
# Why: foreground ticks and test helpers that set changed cannot prove the independent watcher observes the transition.
async def test_native_source_wakes_parked_deadline(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        _, attempt = unnumbered(b, b.start, "latent-old-temporary.json")
        b.rig.bench.history.remove(attempt)
        b.device.requested_current = 0
        b.rig.peer.plan_session = None
        b.clock.now = b.start + 601
        for _ in range(5):
            await b.c.tick()
        lease = b.session.journal.get("managed_temporary_schedule")
        assert lease["state"] == "complete" and lease["result"] != "session_closed"
        assert set(b.device.plans) == {9}
        b.clock.now = lease["end_epoch"]
        await healthy(b.c)
        b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
        await healthy(b.c)
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        await healthy(b.c)
        lease = b.session.journal.get("managed_temporary_schedule")
        assert lease["idle_cutoff_released"] == "owner_return"
        assert b.device.effective_limit() == 6
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            # Yield only to let the actual watcher reach its existing Event.wait; inspect the concrete wait state.
            for _ in range(10):
                await asyncio.sleep(0)
                context = task_locals(watcher, "supervise_deadline")
                waiters = list(b.c.schedule.changed._waiters)
                if context and context.get("delay") is None and waiters:
                    break
            else:
                raise AssertionError("watcher did not enter its idle event wait")
            assert not b.c.schedule.changed.is_set() and len(waiters) == 1 and not waiters[0].done()
            idle_waiter = waiters[0]
            b.clock.now += 1
            b.rig.bench.history.append(attempt)
            b.device.active = 987654
            b.rig.peer.plan_session = (987654, b.timer)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = True
            b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
            b.device.requested_current = 32
            # The real vendor request drives native HTTP validation and is refused before any physical Start.
            result = await b.rig.bench.cloud_call(
                "observe-late-source", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
            )
            assert result[0] == 4 and not starts(b.rig)
            lease = b.session.journal.get("managed_temporary_schedule")
            assert lease["state"] == "clearing" and lease["retire_reason"] == "late_temporary_occurrence"
            assert lease["unreported_native"] is True
            await until(lambda: b.device.effective_limit() == 0)
            assert idle_waiter.done()
            assert b.device.current == 0 and b.stops() == []
            assert b.session.journal.active() is None and b.device.active == 987654
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
