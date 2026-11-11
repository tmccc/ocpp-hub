"""Independent temporary receipts retain their original identity through late older lifecycle reports."""

from __future__ import annotations

import uuid
from contextlib import nullcontext
from datetime import datetime

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_occurrences import ARCHIVE, HEAD, occurrences
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_schedule_unnumbered import unnumbered
from tests.test_prototype_tls import tls as tls


# Intent: late old native reports cannot acquire the newer receipt's plan, window, lifecycle or Stop authority.
# Why: both one-offs use the same native tag, and the old occurrence may first become visible after replacement.
@pytest.mark.parametrize("newer_state", ["requested", "armed", "complete"])
@pytest.mark.parametrize("restart", [False, True])
async def test_older_occurrence_after_newer_receipt(tmp_path, tls, unused_tcp_port, monkeypatch, newer_state, restart):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        daily = dict(b.device.plans[9])
        await b.arm()
        old = b.session.journal.get(managed_schedule.KEY)
        body, row = unnumbered(b, b.start, "old-temporary.json")
        b.rig.bench.history.remove(row)
        b.rig.peer.plan_session = None
        b.device.requested_current = 0
        b.clock.now = b.start + 601
        for _ in range(5):
            await b.c.tick()
        await healthy(b.c)
        assert set(b.device.plans) == {9}
        await b.wait_for_admission()
        b.start = (int(b.clock.now) // 60 + 4) * 60
        b.request()
        newer = b.session.journal.get(managed_schedule.KEY)
        assert newer["body"]["planId"] != old["body"]["planId"]
        if newer_state != "requested":
            for _ in range(2):
                await healthy(b.c)
        if newer_state == "complete":
            b.clock.now = b.start + 601
            for _ in range(5):
                await b.c.tick()
            await healthy(b.c)
        newer = b.session.journal.get(managed_schedule.KEY)
        assert newer["state"] == newer_state
        newer_receipt = b.c.requests.receipt(newer["id"])
        native_timetable = dict(b.device.plans)
        # A requested timer is deliberately retained in the same process; restart cancels only that unsent request.
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "occurrence-restart")
                if newer_state == "requested":
                    newer_receipt = b.c.requests.receipt(newer["id"])
                    newer = b.session.journal.get(managed_schedule.KEY)
            b.rig.bench.history.append(row)
            b.device.active = 987654
            b.rig.peer.plan_session = (987654, old["body"]["planId"])
            await b.c.tick()
            assert b.device.effective_limit() == 0 and not b.stops()
            response = await b.rig.bench.call(b.sock, "late-older", "StartTransaction", body)
            assert response[0] == 3, response
            local = response[2]["transactionId"]
            assert b.device.effective_limit() == 0 and not b.stops()
            # A provisional native number is never a Stop target, even for an identified old temporary plan.
            await b.c.tick()
            assert not b.stops()
            if newer_state == "armed":
                b.rig.peer.plan_session = (local, newer["body"]["planId"])
                for _ in range(2):
                    await b.c.tick()
                assert b.device.effective_limit() == 0 and not b.stops()
            b.rig.peer.plan_session = (local, old["body"]["planId"])
            for _ in range(4):
                await b.c.tick()
            assert b.stops() == [local] and b.device.effective_limit() == 0
            assert b.device.plans == native_timetable
            records = occurrences(b.session.journal)
            for lease in (old, newer):
                saved = records[lease["id"]]
                for key in ("id", "body", "install", "start_epoch", "end_epoch", "original_digest", "augmented_digest"):
                    assert saved[key] == lease[key]
            assert b.c.requests.receipt(newer["id"]) == newer_receipt
            await b.close_transaction(local)
            for _ in range(6):
                await b.c.tick()
            assert b.c.requests.receipt(newer["id"]) == newer_receipt
            assert b.session.journal.get(HEAD) == newer["id"]
            assert b.device.plans[9] == daily
            if newer_state == "armed" or newer_state == "requested" and not restart:
                assert b.c.schedule.summary()["state"] == "armed"
                b.clock.now = b.start
                following = await b.start_plan(newer["body"]["planId"])
                for _ in range(4):
                    await b.c.tick()
                assert b.device.effective_limit() == 6 and b.stops() == [local]
                await b.close_transaction(following)
            b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            for _ in range(6):
                await b.c.tick()
            assert set(b.device.plans) == {9}
            # The newer receipt that also expired unused carries its own idle cutoff.
            assert (await healthy(b.c))["station_ceiling_a"] == (0 if newer_state == "complete" else 6)
            midnight = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
            daily_local = await b.advance_idle_to(midnight)
            assert daily_local != local and (await healthy(b.c))["current_a"] == 6 and b.stops() == [local]


# Intent: an older pre-upgrade lease is archived before the next receipt, and old IDs cannot name a new window.
# Why: migration can recover the most recent surviving lease, while command retries must remain immutable.
async def test_legacy_occurrence_archive_and_reserved_id(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        old = b.session.journal.get(managed_schedule.KEY)
        b.session.journal.db.execute("DELETE FROM meta WHERE key IN (?,?)", (ARCHIVE, HEAD))
        b.session.journal.put(
            managed_schedule.KEY, {k: v for k, v in old.items() if k not in ("end_epoch", "accepted_epoch")}
        )
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "pre-archive-lease")
            assert occurrences(b.session.journal)[old["id"]]["end_epoch"] == old["end_epoch"]
            b.clock.now = b.start + 601
            for _ in range(5):
                await b.c.tick()
            await healthy(b.c)
            await b.wait_for_admission()
            b.start = (int(b.clock.now) // 60 + 4) * 60
            with pytest.raises(SecurityError, match="another time"):
                b.c.schedule.accept(old["id"], old["revision"], b.start)
            collision = old["id"][:8] + uuid.uuid4().hex[8:]
            with pytest.raises(RecoveryRequiredError, match="identifier already exists"):
                b.c.schedule.accept(collision, b.c.requests.intent().revision, b.start)
            b.request()
            assert occurrences(b.session.journal)[old["id"]]["body"] == old["body"]


# Intent: positive proof of a newer temporary or daily session can release an older occurrence's physical zero.
# Why: an old unreported attempt must not spend the newer receipt or permanently disable the original timetable.
@pytest.mark.parametrize("daily", [False, True])
@pytest.mark.parametrize("demand", [False, True])
async def test_newer_source_during_older_hold(tmp_path, tls, unused_tcp_port, monkeypatch, daily, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        old = b.session.journal.get(managed_schedule.KEY)
        body, row = unnumbered(b, b.start, "older-unreported.json")
        b.rig.bench.history.remove(row)
        b.rig.peer.plan_session = None
        b.device.requested_current = 0
        b.clock.now = b.start + 601
        for _ in range(5):
            await b.c.tick()
        await healthy(b.c)
        await b.wait_for_admission()
        b.start = (int(b.clock.now) // 60 + 4) * 60
        await b.arm()
        newer = b.session.journal.get(managed_schedule.KEY)
        newer_receipt = b.c.requests.receipt(newer["id"])
        b.rig.bench.history.append(row)
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, old["body"]["planId"])
        for _ in range(3):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        assert b.session.journal.get(managed_schedule.KEY)["id"] == old["id"]
        plan = 9 if daily else newer["body"]["planId"]
        fresh, _ = unnumbered(b, b.start, "newer-real-start.json")
        b.device.active = 987655
        b.rig.peer.plan_session = (987655, plan)
        for _ in range(2):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        reply = await b.rig.bench.call(b.sock, "newer-during-old-hold", "StartTransaction", fresh)
        assert reply[0] == 3, reply
        local = reply[2]["transactionId"]
        b.rig.peer.plan_session = (local, plan)
        b.rig.peer.permission_enabled = demand
        b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
        b.device.requested_current = 32 if demand else 0
        for _ in range(4):
            await b.c.tick()
        assert b.device.effective_limit() == 6 and b.device.current == (6 if demand else 0) and not b.stops()
        assert b.c.requests.receipt(newer["id"]) == newer_receipt
        await b.close_transaction(local)
        for _ in range(5):
            await b.c.tick()
        # The independent closure does not retire the older, still-unreported occurrence.
        b.device.active = 987656
        b.rig.peer.plan_session = (987656, old["body"]["planId"])
        for _ in range(2):
            await b.c.tick()
        reply = await b.rig.bench.call(b.sock, "older-after-independent-newer", "StartTransaction", body)
        assert reply[0] == 3, reply
        delayed = reply[2]["transactionId"]
        assert not b.stops()
        b.rig.peer.plan_session = (delayed, old["body"]["planId"])
        for _ in range(4):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and b.stops() == [delayed]
        await b.close_transaction(delayed)
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        for _ in range(6):
            await b.c.tick()
        assert set(b.device.plans) == {9}
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (local, delayed, None) and (await healthy(b.c))["current_a"] == 6
        assert b.stops() == [delayed]
