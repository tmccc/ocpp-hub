"""A new owner Pause can reconcile the exact historical current/tick collision through verified zero."""

import copy
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed_release_recovery import ACK

from tests.handover_helpers import crash
from tests.test_managed_current import adjustable
from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_managed_release_recovery import recover_then_return, release_bench
from tests.test_prototype_tls import tls as tls


# Preserve the old version's on-disk shape after a real current request is superseded and physically stopped.
async def old_current_collision(b, previous, failure):
    c, j = b.c, b.session.journal
    if c.current.selected() != previous:
        c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=previous)
        await c.current.settle()
    await healthy(c)
    request = uuid.uuid4().hex
    c.command(request, "set_current", c.requests.intent().revision, current_a=32)
    c.fault("owner_control_or_delivery_uncertain")
    await c.current.settle()
    if c.pause_task is not None:
        await c.pause_task
    await c.hold_fault()
    # The old failure branch lost the prior selection and retained the never-confirmed target.
    j.put("managed_current_change", {"id": request, "current_a": 32, "state": "uncertain"})
    j.put("control", {**j.get("control"), "resume_limit": 32})
    j.put("managed_current_failure", failure)
    assert b.rig.bench.firmware.effective_limit() == 0
    return copy.deepcopy(j.get("managed_control_fault"))


# Intent: a new Pause repairs either historical collision diagnostic while restoring only a prior completed setting.
# Why: polling cannot clear a fault, and the failed target cannot become a future Return selection.
@pytest.mark.parametrize("previous", [6, 16])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        {"type": "UnavailableError", "detail": "current update was superseded by another control operation"},
        {"type": "AuditSupersededError", "detail": "a newer native observation superseded this read"},
    ],
)
async def test_old_current_collision_requires_pause_and_previous_selection(
    tmp_path, tls, unused_tcp_port, monkeypatch, previous, failure, restart
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        fault = await old_current_collision(b, previous, failure)
        assert (await b.c.tick())["state"] == "attention"
        assert b.session.journal.get("managed_control_fault") == fault
        if restart:
            await crash(b.session)
            replacement = managed_session(b.rig)
            await replacement.start()
            try:
                from ocpp_hub_core.prototype.managed import ManagedController

                c = ManagedController(replacement)
                async with b.rig.charger(replacement) as charger:
                    await introduced(b.rig, replacement, charger, "legacy-current-replacement")
                    fresh = SimpleNamespace(rig=b.rig, session=replacement, c=c, charger=charger)
                    assert (await c.tick())["state"] == "attention"
                    await recover_then_return(fresh)
                    assert c.current.selected() == b.rig.bench.firmware.effective_limit() == previous
                    assert replacement.journal.get("managed_current_change")["state"] == "cancelled"
            finally:
                await replacement.close()
        else:
            await recover_then_return(b)
            assert b.c.current.selected() == b.rig.bench.firmware.effective_limit() == previous
            assert b.session.journal.get("managed_current_change")["state"] == "cancelled"


# Intent: a legacy journal without any earlier completed current selection falls back to commissioned six.
# Why: the failed target supplies no evidence that an increase was ever successfully selected.
async def test_old_current_without_completed_selection_retains_commissioned_six(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        b = SimpleNamespace(rig=test.rig, session=test.session, c=test.controller)
        assert not test.session.journal.db.execute(
            "SELECT 1 FROM managed_requests WHERE action='set_current' AND state='completed'"
        ).fetchone()
        await old_current_collision(
            b, 6, {"type": "UnavailableError", "detail": "current update was superseded by another control operation"}
        )
        await recover_then_return(b)
        assert b.c.current.selected() == b.rig.bench.firmware.effective_limit() == 6


# Intent: changed diagnostics, a different failure, absent request correlation and unrelated mutation retain the latch.
# Why: recovery of one historical race must not acknowledge arbitrary owner or delivery uncertainty.
@pytest.mark.parametrize("blocker", ["security", "changed_failure", "missing_request", "unrelated_write"])
async def test_old_current_pause_preserves_unrelated_uncertainty(tmp_path, tls, unused_tcp_port, monkeypatch, blocker):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        j = b.session.journal
        fault = await old_current_collision(
            b, 6, {"type": "UnavailableError", "detail": "current update was superseded by another control operation"}
        )
        if blocker == "security":
            j.put("managed_current_failure", {"type": "SecurityError", "detail": "contradictory physical evidence"})
        if blocker == "missing_request":
            j.db.execute("DELETE FROM managed_current_requests WHERE id=?", (j.get("managed_current_change")["id"],))
        b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
        if b.c.pause_task is not None:
            await b.c.pause_task
        if blocker == "changed_failure":
            assert j.get(ACK)
            j.put("managed_current_failure", {"type": "UnavailableError", "detail": "different later failure"})
        if blocker == "unrelated_write":
            j.mutation_intent("vendor", "ChangeConfiguration", {})
        for _ in range(2):
            assert (await b.c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault
        assert j.get("managed_current_change")["state"] == "uncertain"
        assert b.rig.bench.firmware.effective_limit() == 0
