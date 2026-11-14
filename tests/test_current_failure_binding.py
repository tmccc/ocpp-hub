"""Recovery uses the particular current request's durable cause while optional diagnostics remain unavailable."""

import asyncio
import copy
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_release_recovery import ACK

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_current import adjustable
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.test_managed_proxy import introduced, managed_session
from tests.test_managed_release_recovery import explicit_release_pause, recover_then_return
from tests.test_prototype_tls import tls as tls


# Fail the real request during native HTTP and refuse only its optional legacy diagnostic write.
async def bound_collision(b):
    c, j, peer = b.controller, b.session.journal, b.rig.peer
    j.put("managed_current_failure", {"type": "SecurityError", "detail": "unrelated earlier request"})
    j.db.execute(
        "CREATE TEMP TRIGGER review_optional_diagnostic BEFORE INSERT ON meta "
        "WHEN NEW.key='managed_current_failure' BEGIN SELECT RAISE(ABORT,'optional diagnostic unavailable'); END"
    )
    peer.arm = True
    request = uuid.uuid4().hex
    c.command(request, "set_current", c.requests.intent().revision, current_a=16)
    try:
        await asyncio.wait_for(peer.entered.wait(), 2)
        c.fault("owner_control_or_delivery_uncertain")
        peer.release.set()
        await asyncio.wait_for(c.current.settle(), 3)
        if c.pause_task is not None:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        cause = j.get("managed_current_change")["failure"]
        assert cause["id"] == request and cause["current_a"] == 16
        assert cause["type"] == "AuditSupersededError"
        assert j.get("managed_current_failure") == {"type": "SecurityError", "detail": "unrelated earlier request"}
        assert b.rig.bench.firmware.effective_limit() == 0
        return request
    finally:
        peer.release.set()


# Intent: a diagnostic-only outage cannot erase the real cause, before or after an actual replacement Boot.
# Why: the old global error may belong to a different operation and supplies no authority for this recovery.
@pytest.mark.parametrize("restart", [False, True])
async def test_bound_failure_recovers_without_optional_diagnostic(
    tmp_path, tls, unused_tcp_port, monkeypatch, restart
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        request = await bound_collision(b)
        saved = copy.deepcopy(b.session.journal.get("managed_current_change")["failure"])
        if not restart:
            await recover_then_return(SimpleNamespace(c=b.controller, rig=b.rig))
            assert b.controller.current.selected() == b.rig.bench.firmware.effective_limit() == 6
            assert b.controller.requests.receipt(request)["state"] == "failed"
        else:
            await crash(b.session)
            replacement = managed_session(b.rig)
            await replacement.start()
            try:
                c = ManagedController(replacement)
                async with b.rig.charger(replacement) as charger:
                    await introduced(b.rig, replacement, charger, "bound-cause-replacement")
                    assert replacement.journal.get("managed_current_change")["failure"] == saved
                    assert (await c.tick())["state"] == "attention"
                    assert not replacement.journal.get(ACK)
                    await recover_then_return(SimpleNamespace(c=c, rig=b.rig))
                    assert c.current.selected() == b.rig.bench.firmware.effective_limit() == 6
                    assert c.requests.receipt(request)["state"] == "failed"
            finally:
                await replacement.close()


# Intent: an ineligible or changed cause never borrows a stale global collision diagnostic to clear the fault.
# Why: request identity, submitted revision and the Pause snapshot remain live authority through native HTTP.
@pytest.mark.parametrize("blocker", ["foreign_request", "submitted_revision", "unrelated_cause", "after_pause"])
async def test_bound_failure_refuses_unrelated_or_changed_evidence(
    tmp_path, tls, unused_tcp_port, monkeypatch, blocker
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await bound_collision(b)
        c, j = b.controller, b.session.journal
        j.db.execute("DROP TRIGGER review_optional_diagnostic")
        j.put("managed_current_failure", {
            "type": "AuditSupersededError", "detail": "native bounded audit was superseded by local authority"
        })
        fault = copy.deepcopy(j.get("managed_control_fault"))
        if blocker == "after_pause":
            await explicit_release_pause(SimpleNamespace(c=c))
            assert j.get(ACK)
        change = j.get("managed_current_change")
        cause = dict(change["failure"])
        if blocker == "foreign_request":
            cause["id"] = uuid.uuid4().hex
        elif blocker == "submitted_revision":
            cause["revision"] += 1
        else:
            cause.update(type="SecurityError", detail="independent conflicting evidence")
        j.put("managed_current_change", {**change, "failure": cause})
        if blocker != "after_pause":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                await c.pause_task
        for _ in range(3):
            assert (await c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault
        assert j.get("managed_current_change")["state"] == "uncertain"
        assert c.current.selected() == 6 and b.rig.bench.firmware.effective_limit() == 0
        assert j.get("control")["owner"] == "local" and j.get("control")["inhibit"] is True
