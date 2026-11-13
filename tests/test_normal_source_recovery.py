"""A verified normal local command remains supervised during real native loss and ordinary closure disagreement."""

import copy
import uuid
from contextlib import AsyncExitStack

import pytest

from tests import test_native_bounded_session as native_bench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, starts
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig, owner_start, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_start_recovery_sequences import close_daily, observe


# Intent: a fully verified normal local session retains zero protection, fresh source recovery and the next daily.
# Why: completed Start receipt with running metadata must not exclude that ordinary session from outage supervision.
@pytest.mark.parametrize("current", [6, 16, 32])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("closure", ["native", "ledger"])
@pytest.mark.parametrize("reconnect", [False, True])
async def test_verified_normal_source_outage_and_closure(
    tmp_path, model_tls, monkeypatch, current, demand, closure, reconnect
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, current, owner_tag="APP_OWNER"
    ) as b:
        c, s, rig, sock = b.controller, b.session, b.rig, b.charger
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        await c.tests.settle()
        local = await positive_flow(b)
        if not demand:
            rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await healthy(c)
        assert c.requests.receipt(identifier)["state"] == "completed" and c.tests.lease is None
        assert c.tests.state()["state"] == "running"
        rig.peer.unavailable = True
        await observe(c, 5)
        assert rig.bench.firmware.effective_limit() == 0 and s.journal.active() == local
        rig.peer.unavailable = False
        await healthy(c)
        assert rig.bench.firmware.effective_limit() == current
        async with AsyncExitStack() as stack:
            if reconnect:
                await sock.close()
                await until(lambda: not s.runtime.status.charger_connected)
                sock = await stack.enter_async_context(rig.charger(s))
                await introduced(rig, s, sock, "normal-local-reconnect")
                assert rig.bench.firmware.effective_limit() == 0
                await healthy(c)
                assert rig.bench.firmware.effective_limit() == current
            fresh = ChargeRig(rig, s, c, sock)
            await close_daily(fresh, local, closure)
            assert not s.journal.get("managed_control_fault") and c.tests.state()["state"] == "completed"
            next_local = await positive_flow(
                fresh, "daily", call_id=uuid.uuid4().hex, meter_start=1100, timestamp="2026-01-01T00:02:00Z"
            )
            assert next_local != local
            await healthy(c)
            rig.peer.unavailable = True
            await observe(c, 5)
            assert rig.bench.firmware.effective_limit() == 0
            rig.peer.unavailable = False
            await healthy(c)
            await close_daily(fresh, next_local, "native", timestamp="2026-01-01T00:03:00Z", meter_stop=1200)
            assert len(starts(rig)) == 1
            assert not any(row[2] == "RemoteStopTransaction" for row in rig.bench.device_calls)


# Intent: lost exact command/source proof or unrelated fault cannot release a held verified normal session.
# Why: allowing normal sessions into outage supervision must not broaden source attribution or waive uncertainty.
@pytest.mark.parametrize("changed", ["native-marker", "receipt-request", "request-state", "unrelated-fault"])
async def test_normal_recovery_requires_unchanged_exact_completed_source(tmp_path, model_tls, monkeypatch, changed):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, 6, owner_tag="APP_OWNER"
    ) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        await c.tests.settle()
        local = await positive_flow(b)
        await healthy(c)
        rig.peer.unavailable = True
        await observe(c, 5)
        assert rig.bench.firmware.effective_limit() == 0
        receipt, request = copy.deepcopy(j.get("local_start_receipt")), c.requests.receipt(identifier)
        if changed == "native-marker":
            rig.peer.plan_session = local, 987655
        elif changed == "receipt-request":
            j.put("local_start_receipt", {**receipt, "request_id": uuid.uuid4().hex})
        elif changed == "request-state":
            j.db.execute("UPDATE managed_requests SET state='uncertain' WHERE id=?", (identifier,))
        else:
            c.fault("unrelated_synthetic_fault")
        rig.peer.unavailable = False
        await observe(c, 4)
        assert rig.bench.firmware.effective_limit() == 0 and len(starts(rig)) == 1
        if changed == "unrelated-fault":
            assert j.get("managed_control_fault")["classification"] == "unrelated_synthetic_fault"
        else:
            rig.peer.plan_session = local, 987654
            j.put("local_start_receipt", receipt)
            j.db.execute("UPDATE managed_requests SET state=? WHERE id=?", (request["state"], identifier))
            assert (await healthy(c))["station_ceiling_a"] == 6
        rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await rig.bench.call(
            b.charger,
            "normal-negative-natural-stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )


# Intent: a closed old normal request cannot veto a later independently proven daily/cloud source between polls.
# Why: recovery must use the active immutable lifecycle, even before a poll retires old running metadata.
@pytest.mark.parametrize("source", ["daily", "cloud"])
async def test_old_normal_source_does_not_block_new_external_session(tmp_path, model_tls, monkeypatch, source):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, 6, owner_tag="APP_OWNER"
    ) as b:
        c, rig = b.controller, b.rig
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.tests.settle()
        old_local = await positive_flow(b)
        await healthy(c)
        rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await rig.bench.call(
            b.charger,
            "old-normal-natural-stop",
            "StopTransaction",
            {"transactionId": old_local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        await until(lambda: b.session.journal.next_delivery() is None)
        assert c.tests.state()["state"] == "running" and c.tests.lease is None
        if source == "cloud":
            await owner_start(b)
        local = await positive_flow(
            b, source, call_id=uuid.uuid4().hex, meter_start=1100, timestamp="2026-01-01T00:02:00Z"
        )
        assert local != old_local
        rig.peer.unavailable = True
        await observe(c, 5)
        assert rig.bench.firmware.effective_limit() == 0
        rig.peer.unavailable = False
        assert (await healthy(c))["station_ceiling_a"] == 6
        assert b.session.journal.active() == local and c.tests.state()["state"] == "completed"
        assert not any(row[2] == "RemoteStopTransaction" for row in rig.bench.device_calls)
        await close_daily(b, local, "native", timestamp="2026-01-01T00:03:00Z", meter_stop=1200)
