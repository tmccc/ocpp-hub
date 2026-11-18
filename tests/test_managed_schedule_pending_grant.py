"""A later genuine daily occurrence and an older owner's outstanding Start retain separate lifecycles."""

import asyncio
import copy
import sqlite3
import uuid
from contextlib import closing, nullcontext
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.recovery_contracts import GateDecision

from tests.handover_helpers import crash
from tests.managed_rig import healthy, introduced, managed_session, pending_cloud_offer, starts
from tests.native_schedule_support import ZONE, timer_bench
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Reach the existing ordinary native-outage hold through actual failed loopback HTTP reads.
async def native_outage_at_zero(b):
    port = b.session.prepared.observer.port

    # Close the independent endpoint after a real request, leaving both OCPP peers connected.
    async def unavailable(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.close()
        await writer.wait_closed()

    async with await asyncio.start_server(unavailable, "127.0.0.1", 0) as server:
        b.session.prepared.observer.port = server.sockets[0].getsockname()[1]
        try:
            for _ in range(3):
                await b.c.tick()
        finally:
            b.session.prepared.observer.port = port
    assert b.device.effective_limit() == 0 and not b.stops()


# Intent: let successive genuine daily occurrences run while retaining an unrelated acknowledged cloud Start.
# Why: the older grant may never report, or its report may immediately follow a daily Stop before the next tick.
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("keep_old_native", [False, True])
async def test_daily_and_unbound_owner(tmp_path, tls, unused_tcp_port, monkeypatch, demand, keep_old_native):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        await b.acknowledge_older_owner_start()
        j = b.session.journal
        grant = j.get("vendor_start_grant")
        assert grant.get("local") is None
        for day in (12, 13):
            b.clock.now = int(datetime(2026, 11, day, tzinfo=ZONE).timestamp())
            local = await b.start_plan(9, demand=demand)
            assert b.device.effective_limit() == 6 and j.get("vendor_start_grant") == grant
            assert (await pending_cloud_offer(b.c, 6))["station_ceiling_a"] == 6
            assert b.device.current == (6 if demand else 0) and not b.stops()
            b.clock.now += 60
            await b.close_transaction(local)
            assert j.get("vendor_start_grant") == grant
        if keep_old_native:
            b.device.active = local
            b.rig.peer.plan_session = (local, 9)
        result = await b.rig.bench.call(
            b.sock,
            "delayed-owner",
            "StartTransaction",
            START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert result[0] == 3 and b.device.effective_limit() == 6
        owner = result[2]["transactionId"]
        assert owner != local and j.get("vendor_start_grant") == {**grant, "local": owner}
        if keep_old_native:
            # Real outage zero stays owned while the independent native channel still names a closed daily.
            b.device.active = local
            await native_outage_at_zero(b)
            assert (await b.c.native.read()).physical.native_transaction_id == local
            for _ in range(3):
                await b.c.tick()
            assert b.device.effective_limit() == 0 and not b.stops()
        b.device.active = owner
        b.rig.peer.plan_session = (owner, 987655)
        b.rig.peer.paused = False
        b.rig.peer.permission_enabled = demand
        b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
        b.device.requested_current = 32 if demand else 0
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert b.device.current == (6 if demand else 0) and not b.stops()
        b.clock.now += 60
        await b.close_transaction(owner)
        await healthy(b.c)
        assert not j.get("vendor_start_grant") and set(b.device.plans) == {9}
        assert len(starts(b.rig)) == 1 and not b.stops()
        next_daily = await b.advance_idle_to(int(datetime(2026, 11, 14, tzinfo=ZONE).timestamp()))
        assert next_daily is not None and (await healthy(b.c))["power_w"] > 0


# Intent: ordinary replacement retains zero for a real known daily while an unrelated cloud Start remains unbound.
# Why: a fresh Boot and daily closure cannot consume that accepted command or authorize replay or positive release.
@pytest.mark.parametrize("demand", [False, True])
async def test_restart_retains_unbound_owner_at_zero(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        await b.acknowledge_older_owner_start()
        grant = b.session.journal.get("vendor_start_grant")
        daily = await b.start_plan(9, demand=demand)
        await pending_cloud_offer(b.c, 6)
        await until(lambda: b.session.journal.next_delivery() is None and not b.session.gateway.vendor._pending)
        assert b.session.journal.vendor_id(daily) is not None
        original = copy.deepcopy(b.device.plans[9])
        await b.c.charging.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "unbound-owner-known-daily-restart")
            for _ in range(4):
                status = await b.c.tick()
            native = await b.c.native.read()
            assert native.physical.native_transaction_id == daily and native.session_plan_id == 9
            assert b.session.journal.active() == daily and b.session.journal.get("vendor_start_grant") == grant
            assert b.c.requests.intent().mode == "app" and status["cloud_start_pending"]
            assert status["owner_control_enabled"] is False and b.device.effective_limit() == 0
            assert not b.stops() and len(starts(b.rig)) == 1
            assert not b.session.journal.get("managed_control_fault")
            await b.close_transaction(daily)
            for _ in range(4):
                await b.c.tick()
            assert b.session.journal.active() is None and b.session.journal.get("vendor_start_grant") == grant
            assert b.device.effective_limit() == 0 and b.device.plans == {9: original}
            assert not b.stops() and len(starts(b.rig)) == 1


# Intent: an original daily lifecycle retains a distinct previously acknowledged local owner's Start.
# Why: both a successful Start report and the positive profile restoration used to clear the one shared pending flag.
async def test_independent_daily_preserves_local_owner(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        g, j = b.session.gateway, b.session.journal
        g.policy = replace(g.policy, allowed_tags=frozenset({"APP_OWNER", *g.policy.allowed_tags}))
        g.controls.policy = g.policy
        b.c.command("e1dec715d70a414d82a57fec3bd160bb", "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        owner_tag = starts(b.rig)[0][3]["idTag"]
        assert owner_tag.casefold() != b.device.plans[9]["idTag"].casefold()
        assert j.get("control")["start_pending"] and b.c.charging.state()["start_rpc_accepted"]
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        for _ in range(4):
            await b.c.tick()
        assert set(b.device.plans) == {9}
        b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        local = await b.start_plan(9)
        assert j.get("control")["start_pending"] and b.device.effective_limit() == 6
        await healthy(b.c)
        assert b.device.current == 6 and j.get("control")["start_pending"]
        b.clock.now += 60
        await b.close_transaction(local)
        result = await b.rig.bench.call(
            b.sock,
            "delayed-local",
            "StartTransaction",
            START | {"idTag": owner_tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert result[0] == 3 and b.device.effective_limit() == 6 and not j.get("control")["start_pending"]
        owner_local = result[2]["transactionId"]
        b.rig.peer.plan_session = (owner_local, 132456)
        await healthy(b.c)
        assert b.device.effective_limit() == 6 and not b.stops()
        await b.close_transaction(owner_local)
        await healthy(b.c)
        await b.c.charging.close()


# Intent: an incomplete cloud grant cannot bind or be consumed by an independently permitted native daily.
# Why: ordinary app permission allows the original daily; only the real cloud owner's lifecycle spends its grant.
@pytest.mark.parametrize("changed", ["mutation", "digest"])
async def test_unproven_pending_grant_does_not_bind_daily(tmp_path, tls, unused_tcp_port, monkeypatch, changed):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        await b.acknowledge_older_owner_start()
        j = b.session.journal
        original = j.get("vendor_start_grant")
        grant = {**original, changed: ""}
        j.put("vendor_start_grant", grant)
        b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        daily = await b.start_plan(9)
        assert b.device.effective_limit() == 6 and j.active() == daily
        await b.c.native.read()
        assert j.get("vendor_start_grant") == grant and grant.get("local") is None
        assert not b.stops() and len(starts(b.rig)) == 1
        j.put("vendor_start_grant", original)
        await pending_cloud_offer(b.c, 6)
        assert j.get("vendor_start_grant") == original
        await b.close_transaction(daily)
        b.clock.now += 60
        reply = await b.rig.bench.call(
            b.sock,
            "actual-owner-after-repair",
            "StartTransaction",
            START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert reply[0] == 3
        owner = reply[2]["transactionId"]
        assert owner != daily and j.get("vendor_start_grant") == {**original, "local": owner}
        b.rig.peer.plan_session = (owner, 987655)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        await b.close_transaction(owner)
        await healthy(b.c)
        assert not j.get("vendor_start_grant") and not b.stops() and len(starts(b.rig)) == 1


# Intent: an unrelated daily Start cannot consume the local owner's command before or after its acknowledgement.
# Why: the ordinary Start path lacks a timer lease, including when an intervening Pause or restart precedes the report.
@pytest.mark.parametrize("before_ack", [False, True])
@pytest.mark.parametrize("restart", ["none", "restart", "upgrade"])
@pytest.mark.parametrize("demand", [False, True])
async def test_normal_start_debt_before_hold(tmp_path, tls, unused_tcp_port, monkeypatch, before_ack, restart, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        selected = 32 if before_ack else 6
        if selected != 6:
            b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=selected)
            await b.c.current.settle()
            await healthy(b.c)
        g, j = b.session.gateway, b.session.journal
        g.policy = replace(g.policy, allowed_tags=frozenset({"APP_OWNER", *g.policy.allowed_tags}))
        g.controls.policy = g.policy
        daily_job = None

        # The independent daily plan can report while the charger still holds the local Start response.
        async def incoming(action, payload):
            nonlocal daily_job
            if action == "RemoteStartTransaction" and before_ack:
                daily_job = asyncio.create_task(b.start_plan(9, demand=demand))
                async with asyncio.timeout(3):
                    while j.active() is None:
                        await asyncio.sleep(0.01)
                assert j.get("control")["start_pending"] and j.get("local_start_receipt")["stage"] == "issued"

        b.rig.bench.before_reply = incoming
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        b.rig.bench.before_reply = None
        daily = await daily_job if daily_job is not None else await b.start_plan(9, demand=demand)
        assert j.get("local_start_receipt")["stage"] == "accepted" and j.get("control")["start_pending"]
        assert b.c.charging.state().get("local_transaction") is None
        pause = uuid.uuid4().hex
        b.c.command(pause, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        await b.close_transaction(daily)
        for _ in range(3):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and b.c.requests.receipt(pause)["state"] == "accepted"
        if restart == "upgrade":
            # The deployed gateway consumed the flag on the different daily tag and had no dispatch receipt.
            j.put("local_start_receipt", {})
            j.put("control", {**j.get("control"), "start_pending": False})
        if restart != "none":
            await b.c.charging.close()
            await crash(b.session)
            b.session = managed_session(b.rig)
            b.session.gateway.policy = g.policy
            b.session.gateway.controls.policy = g.policy
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart != "none" else nullcontext(b.sock) as b.sock:
            if restart != "none":
                await introduced(b.rig, b.session, b.sock, "local-debt-restart")
            j = b.session.journal
            assert j.get("control")["start_pending"]
            returning = uuid.uuid4().hex
            b.c.command(returning, "app_control", b.c.requests.intent().revision)
            await b.c.tick()
            assert b.c.requests.receipt(returning)["state"] == "accepted" and b.device.effective_limit() == 0
            b.clock.now += 60
            tag = starts(b.rig)[0][3]["idTag"]
            assert tag == "APP_OWNER"
            body = START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()}
            actual = await b.rig.bench.call(b.sock, "genuine-delayed-owner", "StartTransaction", body)
            assert actual[0] == 3 and b.device.effective_limit() == 0
            local = actual[2]["transactionId"]
            assert j.get("local_start_receipt")["local"] == local and not j.get("control")["start_pending"]
            b.rig.peer.plan_session = (local, 987654)
            for _ in range(3):
                await b.c.tick()
            assert local in b.stops() and b.c.requests.receipt(pause)["state"] != "completed"
            await b.close_transaction(local)
            for _ in range(5):
                await b.c.tick()
            assert b.c.requests.receipt(pause)["state"] == "completed"
            await healthy(b.c)
            assert b.c.requests.receipt(returning)["state"] == "completed" and b.device.effective_limit() == selected
            await b.c.charging.close()
            next_day = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
            assert next_day not in (None, local, daily) and (await healthy(b.c))["power_w"] > 0
            assert len(starts(b.rig)) == 1 and next_day not in b.stops()


# Intent: a Pause that takes the control ticket during dispatch still retains a later physical Start acknowledgement.
# Why: the owner's real lifecycle can arrive after revocation or process replacement.
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("close_between_ticks", [False, True])
async def test_ack_after_owner_pause(tmp_path, tls, unused_tcp_port, monkeypatch, restart, close_between_ticks):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        j = b.session.journal
        pause = uuid.uuid4().hex

        # The worker has sent its Start. The owner pauses before the station returns its actual wire result.
        async def pause_before_reply(action, payload):
            if action == "RemoteStartTransaction":
                assert j.get("local_start_receipt")["stage"] == "issued"
                b.c.command(pause, "pause", b.c.requests.intent().revision)

        b.rig.bench.before_reply = pause_before_reply
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        b.rig.bench.before_reply = None
        await b.c.pause_task
        assert j.get("local_start_receipt")["stage"] == "accepted" and j.get("control")["start_pending"]
        assert b.c.requests.intent().mode == "paused" and b.device.effective_limit() == 0
        if restart:
            await b.c.charging.close()
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "pause-before-ack-restart")
            j = b.session.journal
            body = START | {
                "idTag": starts(b.rig)[0][3]["idTag"],
                "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
            }
            actual = await b.rig.bench.call(b.sock, "paused-owner", "StartTransaction", body)
            assert actual[0] == 3 and b.device.effective_limit() == 0
            local = actual[2]["transactionId"]
            assert j.get("control")["start_pending"]
            b.rig.peer.plan_session = (local, 987655)
            await b.c.native.read()
            assert j.get("local_start_receipt")["local"] == local and not j.get("control")["start_pending"]
            if not close_between_ticks:
                for _ in range(3):
                    await b.c.tick()
                assert local in b.stops() and b.device.effective_limit() == 0
            await b.close_transaction(local)
            for _ in range(4):
                await b.c.tick()
            assert b.c.requests.receipt(pause)["state"] == "completed"
            assert (await healthy(b.c))["local_hold_verified"]
            await b.c.charging.close()
            b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            next_day = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
            assert next_day != local and (await healthy(b.c))["power_w"] > 0


# Intent: replacement cannot invent a pending Start from an already settled or ambiguous historical local request.
# Why: prior releases had a tag and receipt but no command-time allocation floor to disambiguate reused tags.
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_no_reopened_legacy_debt(tmp_path, tls, unused_tcp_port, monkeypatch, ambiguous):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        j = b.session.journal

        # The actual owner uses the same tag as the daily timer; its independently numbered source distinguishes it.
        async def report_owner():
            body = START | {
                "idTag": starts(b.rig)[-1][3]["idTag"],
                "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
            }
            reply = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", body)
            assert reply[0] == 3
            owner = reply[2]["transactionId"]
            b.rig.peer.plan_session = (owner, 987655)
            await b.c.native.read()
            return owner

        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        local = await report_owner()
        await healthy(b.c)
        b.clock.now += 20
        await b.close_transaction(local)
        await healthy(b.c)
        old = j.get("managed_test_charge")
        assert old["state"] == "completed" and old["local_transaction"] == local
        if ambiguous:
            j.put("managed_test_charge", {**old, "local_transaction": None, "state": "awaiting_power"})
        j.put("local_start_receipt", {})
        assert not j.get("control")["start_pending"]
        await b.c.charging.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "closed-legacy-start")
            j = b.session.journal
            assert not j.get("control")["start_pending"] and not j.get("local_start_receipt")
            await healthy(b.c)
            if b.c.requests.intent().mode == "paused":
                b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
                await healthy(b.c)
            b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
            await b.c.charging.settle()
            b.clock.now += 20
            later = await report_owner()
            assert later != local and j.get("local_start_receipt")["local"] == later
            await b.close_transaction(later)
            await healthy(b.c)
            assert not j.get("control")["start_pending"]
            await b.c.charging.close()


# Intent: a late or rejected local RPC cannot fabricate a new positive grant, even across replacement.
# Why: a missing reply can have a real lifecycle under zero; a positively rejected command cannot.
@pytest.mark.parametrize("outcome", ["late", "rejected"])
async def test_late_or_rejected_local_start(tmp_path, tls, unused_tcp_port, monkeypatch, outcome):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        g = b.session.gateway
        g.charger.timeout = 0.4
        answer = b.device.answer

        # Return an actual rejection or deliver the physical acceptance after the RPC was retired.
        def refuse(action, payload):
            if action == "RemoteStartTransaction" and outcome == "rejected":
                return {"status": "Rejected"}
            return answer(action, payload)

        # The independent peer keeps the request's real side effects even when its acceptance is lost.
        async def late(action, payload):
            if action == "RemoteStartTransaction" and outcome == "late":
                await asyncio.sleep(0.6)

        b.device.answer = refuse
        b.rig.bench.before_reply = late
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        b.rig.bench.before_reply = None
        await b.c.pause_task
        assert b.session.journal.get("control")["start_pending"] is (outcome == "late")
        assert b.device.effective_limit() == 0 and len(starts(b.rig)) == 1
        await b.c.charging.close()
        await crash(b.session)
        if outcome == "late":
            b.device.active = 987654
            b.rig.peer.plan_session = (987654, 987659)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "unconfirmed-start-restart")
            j = b.session.journal
            assert j.active() is None and b.device.effective_limit() == 0 and not b.stops()
            result = await b.rig.bench.call(
                b.sock,
                "late-owner",
                "StartTransaction",
                START
                | {
                    "idTag": starts(b.rig)[0][3]["idTag"],
                    "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
                },
            )
            assert b.device.effective_limit() == 0 and len(starts(b.rig)) == 1
            if outcome == "late":
                assert result[0] == 3 and j.get("local_start_receipt")["stage"] == "issued"
                local = result[2]["transactionId"]
                assert j.get("control")["start_pending"]
                b.rig.peer.plan_session = (local, 987659)
                await b.c.native.read()
                assert j.get("local_start_receipt")["local"] == local
                for _ in range(3):
                    await b.c.tick()
                assert local in b.stops()
                await b.close_transaction(local)
            else:
                assert result[0] == 4 and j.active() is None and not b.stops()
            assert not j.get("control")["start_pending"] and (await healthy(b.c))["local_hold_verified"]


# Intent: a crash during native Start validation retains the normal physical Pause and no impossible report debt.
# Why: the exclusive replacement process can distinguish prepared intent from the final transport dispatch receipt.
async def test_replacement_after_prepared_local_start(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        j = b.session.journal
        old_path = j.path
        entered, proceed = asyncio.Event(), asyncio.Event()
        real = b.session.gateway.charger.validate_send

        # Capture the real durable journal and the independent device's actual profiles before its Start send.
        async def delay(action, payload) -> GateDecision | None:
            if action == "RemoteStartTransaction":
                entered.set()
                await proceed.wait()
            return await real(action, payload)

        b.session.gateway.charger.validate_send = delay
        with closing(sqlite3.connect(":memory:")) as snapshot:
            b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
            try:
                await asyncio.wait_for(entered.wait(), 3)
                assert j.get("control")["start_pending"] and j.get("local_start_receipt")["stage"] == "prepared"
                profiles = copy.deepcopy(b.device.profiles)
                j.db.backup(snapshot)
                b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
            finally:
                proceed.set()
            await b.c.charging.settle()
            await b.c.pause_task
            await b.c.charging.close()
            await crash(b.session)
            with closing(sqlite3.connect(old_path)) as restored:
                snapshot.backup(restored)
        b.device.profiles = profiles
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "prepared-start-restart")
            paused = await healthy(b.c)
            assert paused["local_hold_verified"] and b.device.effective_limit() == 0
            assert b.session.journal.get("local_start_receipt")["stage"] == "unsent"
            assert not b.session.journal.get("control")["start_pending"] and not starts(b.rig)
            b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
            assert (await healthy(b.c))["station_ceiling_a"] == 6 and not starts(b.rig)
            daily = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
            assert daily is not None and (await healthy(b.c))["power_w"] > 0


# Intent: finish the original Return after a timed-out owner command eventually starts and closes.
# Why: native and protocol closure can arrive in either order while the requested Return is waiting.
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_return_after_automatic_start_timeout_waits_for_real_owner_closure(
    tmp_path, tls, unused_tcp_port, monkeypatch, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        b.c.charging.lease.deadline = asyncio.get_running_loop().time() - 1
        b.c.charging.changed.set()
        async with asyncio.timeout(3):
            while b.c.requests.intent().mode != "paused":
                await asyncio.sleep(0.01)
        await b.c.pause_task
        returning = uuid.uuid4().hex
        b.c.command(returning, "app_control", b.c.requests.intent().revision)
        await b.c.tick()
        assert b.c.requests.receipt(returning)["state"] == "accepted"
        b.clock.now += 60
        actual = await b.rig.bench.call(
            b.sock,
            "late-owner-without-explicit-pause",
            "StartTransaction",
            START
            | {
                "idTag": starts(b.rig)[0][3]["idTag"],
                "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
            },
        )
        assert actual[0] == 3
        owner = actual[2]["transactionId"]
        b.rig.peer.plan_session = (owner, 987654)
        for _ in range(3):
            await b.c.tick()
        assert owner in b.stops()
        during = b.c.requests.receipt(returning)
        if first == "native":
            b.device.active = None
        else:
            await b.close_transaction(owner)
            b.device.active = owner
        await b.c.tick()
        assert b.device.effective_limit() == 0 and b.c.requests.intent().mode == "paused"
        if first == "native":
            await b.close_transaction(owner)
        else:
            b.device.active = None
        assert during["state"] == "accepted"
        for _ in range(6):
            await b.c.tick()
        assert b.c.requests.receipt(returning)["state"] == "completed"
        assert b.c.requests.intent().mode == "app" and b.device.effective_limit() == 6
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (None, owner) and (await healthy(b.c))["current_a"] == 6
        assert set(b.stops()) == {owner}
