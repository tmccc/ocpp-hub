"""An actual native audit's failed SQLite cleanup cannot become a successful unsent classification."""

import asyncio
import sqlite3

import pytest
from ocpp_hub_core.prototype.managed_native_repair import AUDITS
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.store import fingerprint

from tests.current_rig import release_bench
from tests.runtime_rig import until
from tests.test_managed_release_recovery import explicit_release_pause
from tests.test_native_repair_storage import refused_write
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_rpc_validation_deadlines import NativeAuditPeer
from tests.tls_support import tls as tls


# Intent: real SQLite publication errors survive preemption/timeout; external cancellation retains its cleanup cause.
# Why: cancelling a native read may itself fail to classify durable audit debt and must never permit release deferral.
@pytest.mark.parametrize("boundary", ["preemption", "timeout", "cancellation"])
async def test_native_validation_preserves_failed_cancel_publication(
    tmp_path, tls, unused_tcp_port, monkeypatch, boundary
):
    async with release_bench(tmp_path, tls, unused_tcp_port, 6, native_peer=NativeAuditPeer) as b:
        c, g, j, peer = b.c, b.session.gateway, b.session.journal, b.rig.peer
        # This exact older persisted input exercises the existing supported repair compatibility path.
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        await explicit_release_pause(b)
        await until(lambda: not g.controls._operations and not g.charger._pending)
        peer.arm = "hold"
        if boundary == "timeout":
            g.charger.validation_timeout = 1
        before = len(b.rig.bench.device_calls)
        positive = asyncio.create_task(g.charger.call("SetChargingProfile", c.native.choice.payload(6), priority=1))
        urgent = None
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            assert peer.selected_count == 1 and peer.selected_request["action"] == "getConfig"
            attempt = c.native.repair_audits.active
            assert attempt is not None and fingerprint(fault) in j.get(AUDITS, {})
            with refused_write(j, AUDITS, "statement"):
                if boundary == "preemption":
                    urgent = asyncio.create_task(
                        g.charger.call("SetChargingProfile", c.native.choice.payload(0), priority=0)
                    )
                    await asyncio.wait_for(g.charger.gate.restrictive_waiting.wait(), 1)
                elif boundary == "cancellation":
                    positive.cancel()
                observed = None
                try:
                    await positive
                except (asyncio.CancelledError, sqlite3.Error, UnavailableError) as error:
                    observed = error
                if urgent is not None:
                    assert (await asyncio.wait_for(urgent, 2))[2]["status"] == "Accepted"
                # Preserve the actual failed publication witness before checking which error the caller receives.
                assert c.native.repair_audits.pending is not None
                assert c.native.repair_audits.pending[0] is attempt
                assert c.native.repair_audits.pending[1] == "availability"
                assert fingerprint(fault) in j.get(AUDITS, {}) and not j.db.in_transaction
                if boundary == "cancellation":
                    assert isinstance(observed, asyncio.CancelledError)
                    assert isinstance(observed.__cause__, sqlite3.IntegrityError)
                    assert "independent repair storage unavailable" in str(observed.__cause__)
                else:
                    assert isinstance(observed, sqlite3.IntegrityError), observed
                    assert "independent repair storage unavailable" in str(observed)
            assert not j.db.in_transaction and not peer.release.is_set()
            await asyncio.wait_for(peer.client_closed.wait(), 2)
            assert c.native.repair_audits.pending is not None
            assert c.native.repair_audits.pending[0] is attempt
            assert fingerprint(fault) in j.get(AUDITS, {}) and j.get("managed_control_fault") == fault
            assert not any(
                frame[2] == "SetChargingProfile"
                and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
                for frame in b.rig.bench.device_calls[before:]
            )
            assert not g.charger._pending and not g.charger.gate.busy
            assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            peer.release.set()
            if not positive.done():
                positive.cancel()
            await asyncio.gather(positive, *([] if urgent is None else [urgent]), return_exceptions=True)
            await asyncio.wait_for(peer.finished.wait(), 2)
        c.native.repair_audits.flush()
        assert not j.get(AUDITS) and c.native.repair_audits.pending is None
