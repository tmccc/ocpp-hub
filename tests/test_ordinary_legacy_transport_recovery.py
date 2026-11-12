"""Only an explicitly acknowledged, classified legacy availability latch can reconcile through verified zero."""

import copy
import time
import uuid

import pytest
from ocpp_hub_core.prototype.managed_release_recovery import ACK

from tests.test_managed_release_recovery import recover_then_return, release_bench
from tests.test_prototype_tls import tls as tls


# Preserve an exact old-version durable diagnostic; current runtime no longer creates this transport-only latch.
async def legacy_fault(b, kind):
    b.session.journal.put(
        "managed_native_failure",
        {
            "type": kind,
            "detail": "synthetic legacy native observation unavailable",
            "at_epoch": time.time(),
        },
    )
    b.c.fault("native_authority_or_preservation_failed")
    await b.c.hold_fault()
    assert b.rig.bench.firmware.effective_limit() == 0
    return copy.deepcopy(b.session.journal.get("managed_control_fault"))


# Intent: both historical transport/staleness classifications require a new Pause and retain zero until Return.
# Why: fresh evidence repairs availability, while a durable explicit owner acknowledgement scopes fault clearing.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("kind", ["NativeTransportUnavailableError", "StaleEvidenceError"])
async def test_classified_legacy_availability_fault_requires_new_pause(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, kind
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        fault = await legacy_fault(b, kind)
        for _ in range(2):
            assert (await b.c.tick())["state"] == "attention"
        assert b.session.journal.get("managed_control_fault") == fault
        assert b.session.journal.get(ACK) is None
        await recover_then_return(b)


# Intent: generic drift, old diagnostics, changed diagnostics and outstanding writes retain their original latch.
# Why: the explicit transport repair is not a general native authority/history acknowledgement or debt eraser.
@pytest.mark.parametrize("blocker", ["security", "old_diagnostic", "changed_diagnostic", "unrelated_write"])
async def test_legacy_availability_pause_cannot_clear_unrelated_authority(
    tmp_path, tls, unused_tcp_port, monkeypatch, blocker
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        fault = await legacy_fault(b, "SecurityError" if blocker == "security" else "NativeTransportUnavailableError")
        if blocker == "old_diagnostic":
            failure = b.session.journal.get("managed_native_failure")
            b.session.journal.put("managed_native_failure", {**failure, "at_epoch": failure["at_epoch"] - 10})
        identifier = uuid.uuid4().hex
        b.c.command(identifier, "pause", b.c.requests.intent().revision)
        if b.c.pause_task is not None:
            await b.c.pause_task
        if blocker == "changed_diagnostic":
            failure = b.session.journal.get("managed_native_failure")
            b.session.journal.put("managed_native_failure", {**failure, "detail": "a different later failure"})
        elif blocker == "unrelated_write":
            b.session.journal.mutation_intent("vendor", "ChangeConfiguration", {})
        before = list(b.session.journal.db.execute("SELECT * FROM mutations"))
        for _ in range(3):
            assert (await b.c.tick())["state"] == "attention"
        assert b.session.journal.get("managed_control_fault") == fault
        assert list(b.session.journal.db.execute("SELECT * FROM mutations")) == before
        assert b.rig.bench.firmware.effective_limit() == 0
