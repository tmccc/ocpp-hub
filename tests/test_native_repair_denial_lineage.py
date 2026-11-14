"""A real Stop operator bridge cannot discard native repair facts attached to its denied source fault."""

import copy
import json
import sqlite3
from contextlib import contextmanager

import pytest
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.managed_denial_faults import current_operator_proof
from ocpp_hub_core.prototype.managed_native_repair import AUDITS, REVOCATIONS, repair_pending, repair_revoked
from ocpp_hub_core.prototype.managed_release_recovery import release_evidence
from ocpp_hub_core.prototype.managed_start_delivery import DENIALS, denied_start_evidence
from ocpp_hub_core.prototype.managed_stop_delivery import WIRE_ACK
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.store import fingerprint

from tests import test_managed_proxy as mp
from tests.cloud_start_helpers import two_later_sessions
from tests.handover_helpers import crash
from tests.test_combined_recovery_proofs import pause_before_reconciliation, queue_closed_pairs
from tests.test_denial_operator_composition import selected
from tests.test_managed_stop_reply_recovery import pause, reconcile, running
from tests.test_managed_test_charge import ChargeRig
from tests.test_native_repair_storage import refused_write
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Permit durable begin, then refuse only actual settlement after a known availability outcome.
@contextmanager
def refuse_empty_audit_settlement(j):
    j.db.execute(
        "CREATE TEMP TRIGGER refuse_empty_audit BEFORE INSERT ON meta "
        "WHEN NEW.key='managed_native_repair_audits' AND NEW.value='{}' "
        "BEGIN SELECT RAISE(ABORT,'actual availability outcome settlement refusal'); END"
    )
    try:
        yield
    finally:
        j.db.execute("DROP TRIGGER refuse_empty_audit")


# Intent: actual operator proof replacement preserves strict and interrupted native repair facts of its exact prior.
# Why: a fresh Pause for the installed bridge may not consume a denied lifecycle whose source remains ineligible.
@pytest.mark.parametrize("marker", ["strict", "interrupted", "interrupted-availability", "none-control"])
async def test_real_stop_fault_bridge_retains_denial_native_repair_lineage(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, marker
):
    async with selected(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as original:
        rig, s, c = original.rig, original.session, original.controller
        ids = await queue_closed_pairs(original, ["Blocked"])
        await until(lambda: s.journal.next_delivery() is None)
        j = s.journal
        source = copy.deepcopy(j.get("managed_control_fault"))
        assert source["classification"] == "owner_control_or_delivery_uncertain"
        denial = copy.deepcopy(j.get(DENIALS)[str(ids[0])])
        wire = copy.deepcopy(j.get(WIRE_ACK))
        assert denial["fault"] == wire["fault"] == source
        assert denial["wire_ack"]["fault"] == source and denial["completed"] is None
        assert wire["local_transaction_id"] == ids[0]
        assert len(rig.bench.backend_starts) == 1 and rig.bench.backend_starts[0]["closed"]
        assert not rig.bench.backend_errors
        await c.hold_fault()
        await pause_before_reconciliation(c)
        assert rig.bench.firmware.effective_limit() == 0
        assert release_evidence(c, source) is not None
        diagnostic = copy.deepcopy(j.get("managed_native_failure"))
        configuration = copy.deepcopy(rig.peer.configuration)
        try:
            if marker == "interrupted-availability":
                rig.peer.unavailable = True
                with refuse_empty_audit_settlement(j), pytest.raises(sqlite3.Error):
                    await c.native.read(closed=True)
                assert not rig.peer.failed_reads.empty()
                assert repair_pending(j, source) and not repair_revoked(j, source)
                assert c.native.repair_audits.pending[1] == "availability"
            elif marker != "none-control":
                rig.peer.configuration["untouched"] = "actual strict contradiction before operator bridge"
                if marker == "strict":
                    with pytest.raises(NativeContentError, match="configuration or saved schedule changed"):
                        await c.native.read(closed=True)
                    assert repair_revoked(j, source) and not repair_pending(j, source)
                else:
                    with refused_write(j, REVOCATIONS, "statement"), pytest.raises(sqlite3.Error):
                        await c.native.read(closed=True)
                    assert repair_pending(j, source) and not repair_revoked(j, source)
                    assert c.native.repair_audits.pending[1] == "strict"
            else:
                native = await c.native.read(closed=True)
                assert not native.physical.transaction_open
                assert not repair_revoked(j, source) and not repair_pending(j, source)
        finally:
            rig.peer.configuration = configuration
            rig.peer.unavailable = False
        assert j.get("managed_control_fault") == source and j.get("managed_native_failure") == diagnostic
        assert j.get(DENIALS)[str(ids[0])]["completed"] is None
        original_markers = copy.deepcopy(j.get(REVOCATIONS)), copy.deepcopy(j.get(AUDITS))
        await c.current.close()
        await c.tests.close()
        await crash(s)

        # The real exclusive tool verifies the existing genuine Stop receipt and builds every proof header itself.
        await reconcile(rig, tmp_path, capsys, "stop-fault")
        async with running(rig, "native-denial-lineage-bridge") as (fresh, controller, sock):
            j = fresh.journal
            installed = copy.deepcopy(j.get("managed_control_fault"))
            proof = current_operator_proof(j, installed)
            assert installed != source and installed["classification"] == "operator_debt_reconciled_at_zero"
            assert proof is not None and proof["kind"] == "stop-fault" and proof["prior"]["fault"] == source
            assert proof["prior"]["stop_wire_ack"] == wire
            assert denied_start_evidence(controller, installed)["fault"] == source
            assert (j.get(REVOCATIONS), j.get(AUDITS)) == original_markers
            assert j.next_delivery() is None and j.get(DENIALS)[str(ids[0])]["completed"] is None
            positive_limits = sum(limit > 0 for limit in rig.bench.received_limits)
            identifier, _ = await pause(controller)
            status = await controller.tick()
            witness = {
                "marker": marker,
                "source_fault": source,
                "source_fingerprint": fingerprint(source),
                "installed_fault": installed,
                "actual_operator_prior_fault": proof["prior"]["fault"],
                "actual_stop_wire_ack": wire,
                "source_markers_retained": (j.get(REVOCATIONS), j.get(AUDITS)) == original_markers,
                "after_new_pause": {
                    "fault": j.get("managed_control_fault"),
                    "denial": j.get(DENIALS)[str(ids[0])],
                    "operator_entry": j.get(KEY)[installed["proof"]],
                    "pause_receipt": controller.requests.receipt(identifier),
                    "status_state": status["state"],
                    "physical_limit": rig.bench.firmware.effective_limit(),
                    "physical_current": rig.bench.firmware.current,
                },
            }
            (tmp_path / "denial-native-lineage-witness.json").write_text(json.dumps(witness, indent=2) + "\n")
            assert rig.bench.firmware.effective_limit() == rig.bench.firmware.current == 0
            assert sum(limit > 0 for limit in rig.bench.received_limits) == positive_limits
            if marker != "none-control":
                assert j.get("managed_control_fault") == installed, "operator bridge erased ineligible source fault"
                assert j.get(DENIALS)[str(ids[0])]["completed"] is None
                assert j.get(KEY)[installed["proof"]]["completed"] is None
                assert controller.requests.receipt(identifier)["state"] != "completed"
                assert status["state"] == "attention" and not status["owner_control_enabled"]
                assert (j.get(REVOCATIONS), j.get(AUDITS)) == original_markers
                for _ in range(3):
                    assert (await controller.tick())["state"] == "attention"
                    assert j.get("managed_control_fault") == installed
                return
            assert (await mp.healthy(controller))["local_hold_verified"]
            assert j.get("managed_control_fault") is None
            assert j.get(DENIALS)[str(ids[0])]["completed"]["pause"] == identifier
            assert j.get(KEY)[installed["proof"]]["completed"]["pause"] == identifier
            assert controller.requests.receipt(identifier)["state"] == "completed"
            terminal = copy.deepcopy(j.get(DENIALS)), copy.deepcopy(j.get(KEY))
            await two_later_sessions(ChargeRig(rig, fresh, controller, sock), selected=6, original_starts=0)
            assert (j.get(DENIALS), j.get(KEY)) == terminal
            assert not rig.bench.backend_errors and all(entry["closed"] for entry in rig.bench.backend_starts)
