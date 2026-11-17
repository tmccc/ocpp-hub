"""Eligible fault repair removes only receipted timers and carries independent cutoff duties under Pause."""

import asyncio
import copy
import json
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_cutoffs import idle_cutoff_pending
from ocpp_hub_core.prototype.managed_release_recovery import ACK, reconcile_owner_release
from ocpp_hub_core.prototype.managed_schedule import KEY
from ocpp_hub_core.prototype.recovery_contracts import require_allowed
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, ReleaseDeferredError

from tests import test_managed_schedule_cutoff as cutoff_bench
from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_release_recovery import explicit_release_pause, fail_one_return
from tests.test_managed_schedule import SchedulePeer
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_idle_cutoff import early_cleanup
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Establish an actual measured fault beside the owner timer, then deliver genuine Pause and session closure.
async def faulted_timer(b, monkeypatch):
    await b.arm()
    local = await b.start_plan(9)
    await healthy(b.c)
    actual = b.rig.peer.native

    # Keep genuine device identity, history and resources; only independently measured output exceeds selection.
    def overdraw(key):
        value = actual(key)
        if key == "METER" and b.device.active == local:
            value[0]["meterData"].update(cur=[80, 0, 0], power=[1920, 0, 0, 1920])
        return value

    monkeypatch.setattr(b.rig.peer, "native", overdraw)
    for _ in range(6):
        await b.c.tick()
        if b.session.journal.get("managed_control_fault"):
            break
    assert b.session.journal.get("managed_control_fault")["classification"] == "measured_output_limit_exceeded"
    monkeypatch.setattr(b.rig.peer, "native", actual)
    await explicit_release_pause(b)
    await b.close_transaction(local)
    await until(lambda: b.session.journal.next_delivery() is None)


# Intent: an explicit Pause repairs a genuine measured fault after the real daily session closes beside a timer.
# Why: restrictive removal must work across the fault's invalidated revision without enabling positive authority.
@pytest.mark.parametrize("clear_failure", ["none", "rejected", "lost_ack"])
async def test_fault_pause_removes_only_owned_timer(tmp_path, tls, unused_tcp_port, monkeypatch, clear_failure):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await faulted_timer(b, monkeypatch)
        original = copy.deepcopy(b.device.plans[9])
        cleared = []
        answer, send = b.device.answer, b.sock.send

        # Refuse one selective clear before application; all other independent firmware behavior stays unchanged.
        def clear_answer(action, payload):
            if (
                clear_failure == "rejected"
                and not cleared
                and action == "DataTransfer"
                and payload.get("messageId") == "clearPlans"
            ):
                cleared.append("rejected")
                return {"status": "Rejected", "data": ""}
            return answer(action, payload)

        # Lose one clear reply after real firmware application, without dropping any subsequent response.
        async def clear_reply(raw):
            response = json.loads(raw)
            call = next((f for f in b.rig.bench.device_calls if f[1] == response[1]), None)
            if (
                clear_failure == "lost_ack"
                and not cleared
                and response[0] == 3
                and call is not None
                and call[2] == "DataTransfer"
                and call[3].get("messageId") == "clearPlans"
            ):
                cleared.append("lost_ack")
                return
            await send(raw)

        monkeypatch.setattr(b.device, "answer", clear_answer)
        monkeypatch.setattr(b.sock, "send", clear_reply)
        b.session.gateway.charger.timeout = 0.1
        mark = len(b.rig.bench.received_limits)
        for _ in range(12):
            await b.c.tick()
            assert b.device.effective_limit() == 0
            assert all(limit == 0 for limit in b.rig.bench.received_limits[mark:])
            if b.session.journal.get("managed_control_fault") is None:
                break
            b.clock.now += 31
        assert b.session.journal.get("managed_control_fault") is None, b.session.journal.get(KEY)
        assert set(b.device.plans) == {9} and b.device.plans[9] == original
        assert b.session.journal.get(KEY)["state"] == "complete"
        assert b.c.requests.intent().mode == "paused" and not starts(b.rig)
        assert len(cleared) == int(clear_failure != "none")
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == 6


# Intent: a timer already absent retains its future idle cutoff through fault repair until successful owner Return.
# Why: inventory absence and zero are not evidence that the separate cutoff obligation has been discharged.
async def test_fault_repair_carries_completed_idle_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await early_cleanup(b)
        b.charger = b.sock
        assert idle_cutoff_pending(b.session.journal.get(KEY))
        await fail_one_return(b, monkeypatch, "lost")
        await explicit_release_pause(b)
        for _ in range(12):
            await b.c.tick()
            assert b.device.effective_limit() == 0
            if b.session.journal.get("managed_control_fault") is None:
                break
        assert b.session.journal.get("managed_control_fault") is None
        assert idle_cutoff_pending(b.session.journal.get(KEY))
        assert b.c.requests.intent().mode == "paused" and set(b.device.plans) == {9}
        assert (await healthy(b.c))["local_hold_verified"]
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert not idle_cutoff_pending(b.session.journal.get(KEY))
        assert b.session.journal.get(KEY)["idle_cutoff_released"] == "owner_return"
        assert not starts(b.rig) and not b.stops()


class SelectiveConfigPeer(SchedulePeer):
    """The independent HTTP station can fail one chosen configuration request before sending its body."""

    # Count real received configuration requests without asking the engine to select its own failure boundary.
    def __init__(self, bench):
        super().__init__(bench)
        self.config_requests = 0
        self.fail_at = None
        self.failed_at = None
        self.gate_at = None
        self.entered, self.release = asyncio.Event(), asyncio.Event()

    # Replay ordinary complete HTTP requests after deciding this independent peer's one-shot availability.
    async def handle(self, reader, writer):
        header = await reader.readuntil(b"\r\n\r\n")
        fields = {
            line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
            for line in header.split(b"\r\n")[1:]
            if b":" in line
        }
        body = await reader.readexactly(int(fields[b"content-length"]))
        request = json.loads(body)
        if request["action"] == "getConfig":
            self.config_requests += 1
            if self.config_requests == self.gate_at:
                self.entered.set()
                await self.release.wait()
            if self.config_requests == self.fail_at:
                self.failed_at = self.config_requests
                writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return
        replay = asyncio.StreamReader()
        replay.feed_data(header + body)
        replay.feed_eof()
        await super().handle(replay, writer)


# Intent: a real HTTP failure at automatic restoration's final native check defers before positive dispatch.
# Why: temporary unavailability must retain verified zero without stranding uncertainty or cancelling a timer.
async def test_automatic_timer_restoration_retries_predispatch_http_failure(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    monkeypatch.setattr(cutoff_bench, "SchedulePeer", SelectiveConfigPeer)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "automatic-timer-restoration")
            if b.session.journal.get("trial_phase") == "recovering":
                await b.c.native.resume()
            assert b.device.effective_limit() == 0 and b.c.requests.intent().mode == "app"
            b.rig.peer.fail_at = b.rig.peer.config_requests + 7
            mark = len(b.rig.bench.received_limits)
            with pytest.raises(ReleaseDeferredError):
                progress = await b.c.recovery.restore()
                require_allowed(progress.pending)
            assert b.rig.peer.failed_at == b.rig.peer.fail_at
            assert not b.session.journal.get("managed_control_fault")
            assert b.session.journal.get("control")["uncertain"] is False
            assert b.device.effective_limit() == 0
            assert all(limit == 0 for limit in b.rig.bench.received_limits[mark:])
            assert b.session.journal.get(KEY)["state"] == "armed" and "hold_since" not in b.session.journal.get(KEY)
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            assert not starts(b.rig) and not b.stops()


# Intent: a newer real Pause revokes a timer clear while its own native validation RPC is still pending.
# Why: scoped repair must not borrow old fault/ACK/ticket authority across an awaited response.
async def test_fault_timer_clear_is_revoked_during_native_validation(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(cutoff_bench, "SchedulePeer", SelectiveConfigPeer)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await faulted_timer(b, monkeypatch)
        j, g, peer = b.session.journal, b.session.gateway, b.rig.peer
        fault = copy.deepcopy(j.get("managed_control_fault"))
        mark = len(b.rig.bench.device_calls)
        peer.gate_at = peer.config_requests + 4
        repair = asyncio.create_task(reconcile_owner_release(b.c))
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            assert len(g.charger._pending) == 1 and next(iter(g.charger._pending)).startswith("ft-")
            b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
            newer_ack = copy.deepcopy(j.get(ACK))
            peer.release.set()
            with pytest.raises((RecoveryRequiredError, UnavailableError)):
                await repair
            assert j.get("managed_control_fault") == fault and j.get(ACK) == newer_ack
            assert b.timer in b.device.plans and b.device.effective_limit() == 0
            assert not any(
                f[2] == "DataTransfer" and f[3].get("messageId") == "clearPlans"
                for f in b.rig.bench.device_calls[mark:]
            )
            if b.c.pause_task is not None:
                await b.c.pause_task
            b.clock.now += 31  # The revoked unsent attempt retains the ordinary bounded cleanup retry delay.
            for _ in range(5):
                await b.c.tick()
            assert j.get("managed_control_fault") is None and set(b.device.plans) == {9}
            assert b.c.requests.intent().mode == "paused" and b.device.effective_limit() == 0
        finally:
            peer.release.set()
            await asyncio.gather(repair, return_exceptions=True)


# Intent: unrelated real RPC work and retained unnumbered Start debt cannot use the clear call's pending exemption.
# Why: the exact own-token exception must preserve every independent operation and source obligation.
@pytest.mark.parametrize("blocker", ["pending_rpc", "temporary_attempt"])
async def test_fault_timer_clear_retains_unrelated_debt(tmp_path, tls, unused_tcp_port, monkeypatch, blocker):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await faulted_timer(b, monkeypatch)
        j, g = b.session.journal, b.session.gateway
        fault, ack = copy.deepcopy(j.get("managed_control_fault")), copy.deepcopy(j.get(ACK))
        entered, release = asyncio.Event(), asyncio.Event()
        pending = None

        # The independent charger holds only this real read reply; native HTTP remains independently available.
        async def hold_read(action, payload):
            if action == "GetConfiguration":
                entered.set()
                await release.wait()

        if blocker == "pending_rpc":
            b.rig.bench.before_reply = hold_read
            pending = asyncio.create_task(g.charger.call("GetConfiguration", {"key": ["HeartbeatInterval"]}))
            await asyncio.wait_for(entered.wait(), 2)
            assert g.charger._pending and not any(key.startswith("ft-") for key in g.charger._pending)
        else:
            # Durable refusal input represents a prior unnumbered report awaiting independent reconciliation.
            j.put(KEY, j.get(KEY) | {"temporary_attempts": {"retained-attempt": [b.start * 1000, 1000, None]}})
        mark = len(b.rig.bench.device_calls)
        try:
            with pytest.raises((RecoveryRequiredError, UnavailableError)):
                await reconcile_owner_release(b.c)
            assert j.get("managed_control_fault") == fault and j.get(ACK) == ack
            assert b.timer in b.device.plans and b.device.effective_limit() == 0
            assert not any(
                f[2] == "DataTransfer" and f[3].get("messageId") == "clearPlans"
                for f in b.rig.bench.device_calls[mark:]
            )
        finally:
            release.set()
            if pending is not None:
                await pending
            b.rig.bench.before_reply = None
        if blocker == "temporary_attempt":
            assert j.get(KEY)["temporary_attempts"]
