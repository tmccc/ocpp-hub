"""A strict native refusal during an acknowledged legacy repair must not disappear with restored data."""

import copy
import json
import sys
import uuid

import pytest
from ocpp_hub_core.control import HUB_PROFILE_ID
from ocpp_hub_core.prototype.managed_release_recovery import ACK, PROOF, recoverable_fault
from ocpp_hub_core.prototype.security import SecurityError

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_release_recovery import explicit_release_pause, release_bench
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_prototype_tls import tls as tls


# Intent: actual strict HTTP content during supported legacy repair cannot later be acknowledged as availability alone.
# Why: restoring the original content must not revive a Pause permission after three real authority refusals.
@pytest.mark.parametrize("kind", ["NativeTransportUnavailableError", "StaleEvidenceError"])
@pytest.mark.parametrize("conflict", ["configuration", "history", "none-control"])
async def test_legacy_native_repair_retains_strict_refusal_after_original_content_returns(
    tmp_path, tls, unused_tcp_port, monkeypatch, kind, conflict
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, g = b.c, b.session.journal, b.session.gateway
        original_configuration = copy.deepcopy(b.rig.peer.configuration)
        original_history = copy.deepcopy(b.rig.bench.history)
        original_plans = copy.deepcopy(b.rig.bench.firmware.plans)
        epoch, boot = g._charger_epoch, g.boot_observation

        # This existing helper explicitly constructs an older persisted input; current code no longer creates it.
        fault = await legacy_fault(b, kind)
        original_diagnostic = copy.deepcopy(j.get("managed_native_failure"))
        assert recoverable_fault(c, fault)
        assert (await c.tick())["state"] == "attention" and j.get(ACK) is None
        pause_id = await explicit_release_pause(b)
        acknowledgement = copy.deepcopy(j.get(ACK))
        assert acknowledgement["pause_request"] == pause_id and acknowledgement["fault"] == fault
        assert recoverable_fault(c, fault) and c.requests.intent().mode == "paused"
        assert c.pause_task is None or c.pause_task.done()
        native = await c.native.read(closed=True)
        assert not native.physical.transaction_open and native.output_limit_a == 0
        assert all(value == 0 for value in native.currents_a + native.powers_w)
        station = [row for row in b.session.book.owned_present(native.physical) if row.identifier == HUB_PROFILE_ID]
        assert len(station) == 1 and station[0].canonical.get("limit") == 0
        row = j.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        assert row["state"] == "confirmed" and json.loads(row["confirmed"]) == station[0].canonical
        assert g.controls.ready and g.controls.desired() == b.rig.bench.firmware.effective_limit() == 0
        assert not j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL").fetchone()
        assert not j.db.execute("SELECT 1 FROM outbox").fetchone()
        assert not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed'").fetchone()

        if conflict == "none-control":
            assert (await healthy(c))["local_hold_verified"] and j.get("managed_control_fault") is None
            assert c.requests.receipt(pause_id)["state"] == "completed"
            request = uuid.uuid4().hex
            c.command(request, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["owner_control_enabled"]
            assert c.requests.receipt(request)["state"] == "completed" and not starts(b.rig)
            return

        if conflict == "configuration":
            b.rig.peer.configuration["untouched"] = "valid-independent-external-change"
            expected = "native bounded configuration or saved schedule changed"
        else:
            b.rig.bench.history[0]["original_record"]["preserve"].append(4)
            expected = "native bounded handover must preserve every original history record"
        observed_errors = []
        previous_trace = sys.gettrace()

        # Observe actual propagated exceptions at the repair caller without replacing any engine decision or result.
        def trace_refusal(frame, event, argument):
            if (
                event == "exception"
                and frame.f_code.co_name == "reconcile_owner_release"
                and frame.f_code.co_filename.endswith("/managed_release_recovery.py")
            ):
                _, error, _ = argument
                observed_errors.append(
                    {
                        "type": type(error).__name__,
                        "detail": str(error),
                        "security_error": isinstance(error, SecurityError),
                    }
                )
            return trace_refusal

        sequence, requests = c.native.read_sequence, len(b.rig.peer.requests)
        try:
            sys.settrace(trace_refusal)
            for _ in range(3):
                assert (await c.tick())["state"] == "attention"
                if not recoverable_fault(c, j.get("managed_control_fault", {})):
                    break
        finally:
            sys.settrace(previous_trace)
            b.rig.peer.configuration = original_configuration
            b.rig.bench.history = original_history
        strict_errors = [row for row in observed_errors if row["security_error"] and row["detail"] == expected]
        assert strict_errors, observed_errors
        assert c.native.read_sequence >= sequence + len(strict_errors) and len(b.rig.peer.requests) > requests
        after_refusal = {
            "fault_unchanged": j.get("managed_control_fault") == fault,
            "ack_unchanged": j.get(ACK) == acknowledgement,
            "diagnostic_unchanged": j.get("managed_native_failure") == original_diagnostic,
            "legacy_permission_still_eligible": recoverable_fault(c, j.get("managed_control_fault", {})),
        }
        assert g._charger_epoch == epoch and g.boot_observation == boot
        assert b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
        for _ in range(5):
            status = await c.tick()
        (tmp_path / "strict-repair-witness.json").write_text(
            json.dumps(
                {
                    "legacy_input": kind,
                    "peer_conflict": conflict,
                    "observed_reconciliation_exceptions": strict_errors,
                    "after_refusal_before_restore": after_refusal,
                    "after_restore": {
                        "fault": j.get("managed_control_fault"),
                        "ack": j.get(ACK),
                        "native_failure": j.get("managed_native_failure"),
                        "reconciliation_proof": j.get(PROOF),
                        "pause_receipt": c.requests.receipt(pause_id),
                        "status_state": status["state"],
                        "phase": j.get("trial_phase"),
                        "physical_ceiling": b.rig.bench.firmware.effective_limit(),
                        "physical_current": b.rig.bench.firmware.current,
                        "start_count": len(starts(b.rig)),
                        "preservation_fault": j.get("trial_preservation_fault"),
                        "inventory_fault": j.get("trial_inventory_fault"),
                    },
                },
                indent=2,
            )
            + "\n"
        )
        print(json.dumps({"kind": kind, "conflict": conflict, "strict_reads": len(strict_errors), **after_refusal}))
        assert j.get("managed_control_fault") is not None, (
            "restored peer data erased an actually observed strict refusal through the old availability Pause proof",
            after_refusal,
            c.requests.receipt(pause_id),
            j.get(PROOF),
        )
        assert j.get("managed_control_fault") != fault or j.get(ACK) != acknowledgement
        assert not recoverable_fault(c, j.get("managed_control_fault"))
        assert c.requests.receipt(pause_id)["state"] != "completed"
        assert not status["owner_control_enabled"] and b.rig.bench.firmware.effective_limit() == 0
        assert b.rig.peer.configuration == original_configuration and b.rig.bench.firmware.plans == original_plans
