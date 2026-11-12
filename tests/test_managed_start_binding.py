"""Preserve a pending command when a genuine native timer shares its charging tag."""

import asyncio
import uuid
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: exercise a scheduled source on either side of the local ACK, then the still-distinct owner lifecycle.
# Why: debt consumption must depend on command provenance even when the configured tags are equal.
@pytest.mark.parametrize("before_ack", [True, False])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("distinct", [False, True])
async def test_same_tag_recovery(tmp_path, tls, unused_tcp_port, monkeypatch, before_ack, demand, distinct):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        g, j = b.session.gateway, b.session.journal
        if distinct:
            g.policy = replace(g.policy, allowed_tags=frozenset({"APP_OWNER", *g.policy.allowed_tags}))
            g.controls.policy = g.policy
        b.clock.now = int(datetime(2026, 11, 11, 0, tzinfo=ZONE).timestamp())
        daily_job = None

        # Deliver the independent daily on either side of the owner acknowledgement.
        async def incoming(action, payload):
            nonlocal daily_job
            if action == "RemoteStartTransaction" and before_ack:
                daily_job = asyncio.create_task(b.start_plan(9, demand=demand))
                async with asyncio.timeout(3):
                    while j.active() is None:
                        await asyncio.sleep(0.01)

        b.rig.bench.before_reply = incoming
        owner_request = uuid.uuid4().hex
        b.c.command(owner_request, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        b.rig.bench.before_reply = None
        daily = await daily_job if daily_job is not None else await b.start_plan(9, demand=demand)
        owner_tag = starts(b.rig)[0][3]["idTag"]
        assert (owner_tag != b.device.plans[9]["idTag"]) == distinct
        pending_after_daily = j.get("control")["start_pending"]
        await healthy(b.c)
        pause = uuid.uuid4().hex
        b.c.command(pause, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        await b.close_transaction(daily)
        for _ in range(3):
            await b.c.tick()
        assert b.c.requests.receipt(pause)["state"] != "completed"
        b.clock.now += 60
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, 987659)
        b.rig.peer.permission_enabled = demand
        b.device.requested_current = 32 if demand else 0
        body = START | {"idTag": owner_tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()}
        result = await b.rig.bench.call(b.sock, "actual-owner-report", "StartTransaction", body)
        assert result[0] == 3 and pending_after_daily
        local = result[2]["transactionId"]
        b.rig.peer.plan_session = (local, 987659)
        for _ in range(3):
            await b.c.tick()
        assert local in b.stops()
        await b.close_transaction(local)
        for _ in range(4):
            await b.c.tick()
        assert (await healthy(b.c))["local_hold_verified"]
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        await b.c.tests.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        b.session.gateway.policy = g.policy
        b.session.gateway.controls.policy = g.policy
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "source-recovery-boot")
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            daily2 = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
            assert daily2 != daily and (await healthy(b.c))["current_a"] == 6
            assert daily2 not in b.stops()
            await b.advance_running_plan(9, daily2, int(datetime(2026, 11, 12, 6, tzinfo=ZONE).timestamp()))
            assert (await healthy(b.c))["power_w"] == 0


# Intent: retain an older accepted owner command when a distinct native timer reports on the same tag.
# Why: the candidate explicitly supports older acknowledgements while the one-off copies the admitted native tag.
@pytest.mark.parametrize("same_tag", [False, True])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_pending_cloud_same_tag(tmp_path, tls, unused_tcp_port, monkeypatch, same_tag, restart, demand, first):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        g, j = b.session.gateway, b.session.journal
        tag = b.device.plans[b.timer]["idTag"] if same_tag else "CLOUD_OWNER"
        prior_guard = g.remote_start_guard
        g.remote_start_guard = None  # Same pre-upgrade-ACK setup as supplied TimerBench helper.
        try:
            accepted = await b.rig.bench.cloud_call(
                "older-cloud", "RemoteStartTransaction", {"connectorId": 1, "idTag": tag}
            )
            assert accepted[0] == 3 and accepted[2]["status"] == "Accepted"
        finally:
            g.remote_start_guard = prior_guard
        grant = dict(j.get("vendor_start_grant"))
        b.clock.now = b.start
        temporary = await b.fire(b.timer, demand=demand)
        await healthy(b.c)
        assert j.get("vendor_start_grant") == grant
        end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        b.clock.now = end
        await b.c.tick()
        assert b.stops() == [temporary] and b.device.effective_limit() == 0
        pause = uuid.uuid4().hex
        b.c.command(pause, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        if first == "native":
            b.device.active = None
            b.device.requested_current = 0
            b.rig.peer.permission_enabled = False
            b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        else:
            await b.close_transaction(temporary)
            b.device.active = temporary
        for _ in range(2):
            await b.c.tick()
        assert b.c.requests.receipt(pause)["state"] != "completed"
        assert b.device.effective_limit() == 0
        if first == "native":
            await b.close_transaction(temporary)
        else:
            b.device.active = None
        for _ in range(4):
            await b.c.tick()
        premature = b.c.requests.receipt(pause)["state"] == "completed"
        replacements = AsyncExitStack()
        if restart:
            await b.c.tests.close()
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
            b.sock = await replacements.enter_async_context(b.rig.charger(b.session))
            await introduced(b.rig, b.session, b.sock, "lost-debt-restart")
            for _ in range(4):
                await b.c.tick()
            j = b.session.journal
            assert (b.c.requests.receipt(pause)["state"] == "completed") is False
        b.clock.now += 60
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, 987659)
        b.rig.peer.permission_enabled = demand
        b.device.requested_current = 32 if demand else 0
        late = await b.rig.bench.call(
            b.sock,
            "actual-cloud-owner",
            "StartTransaction",
            START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        assert not premature and late[0] == 3
        assert b.device.effective_limit() == 0
        local = late[2]["transactionId"]
        b.rig.peer.plan_session = (local, 987659)
        for _ in range(3):
            await b.c.tick()
        assert local in b.stops()
        await b.close_transaction(local)
        for _ in range(4):
            await b.c.tick()
        assert (await healthy(b.c))["local_hold_verified"]
        # A healthy physical Pause can precede cloud Stop delivery; Return requires those actual receipts too.
        await until(
            lambda: not b.session.journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            and not b.session.gateway.vendor._pending
        )
        returning = uuid.uuid4().hex
        b.c.command(returning, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert b.c.requests.receipt(returning)["state"] == "completed"
        assert set(b.device.plans) == {9}
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in b.stops() and (await healthy(b.c))["power_w"] > 0
        await b.close_transaction(following)
        await replacements.aclose()


# Intent: never complete an owner Pause from two closed same-tag records unless the owner source was identified.
# Why: even both real protocol closures between polls do not record which physical occurrence spent the owner's command.
@pytest.mark.parametrize("source_observed", [False, True])
async def test_same_tag_lifecycles_close_between_polls(tmp_path, tls, unused_tcp_port, monkeypatch, source_observed):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.clock.now = int(datetime(2026, 11, 11, tzinfo=ZONE).timestamp())
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        tag = starts(b.rig)[0][3]["idTag"]
        assert tag == b.device.plans[9]["idTag"]
        daily = await b.start_plan(9, demand=False)
        b.clock.now += 10
        await b.close_transaction(daily)
        b.clock.now += 10
        body = START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()}
        reply = await b.rig.bench.call(b.sock, "owner-between-polls", "StartTransaction", body)
        assert reply[0] == 3
        owner = reply[2]["transactionId"]
        assert owner != daily
        b.rig.peer.plan_session = (owner, 987655)
        if source_observed:
            await b.c.native.read()
        b.clock.now += 10
        await b.close_transaction(owner)
        pause = uuid.uuid4().hex
        b.c.command(pause, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        for _ in range(4):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        assert b.session.journal.get("control")["start_pending"] is not source_observed
        assert (b.c.requests.receipt(pause)["state"] == "completed") is source_observed
