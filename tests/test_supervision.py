"""Health decisions and public status retain separate evidence, authority and pending-work contracts."""

from itertools import product

import pytest
from ocpp_hub_core.prototype.control_state import ControlState
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_view import poll_base, poll_status
from ocpp_hub_core.prototype.supervision import ControlProof, Observation, PollStatus, assess

from tests.managed_rig import healthy, introduced, managed_bench, managed_session
from tests.tls_support import tls as tls


# Intent: live claims can only retain the matching proof while the published scope still matches.
# Why: a remaining local inhibit or a shared grant cannot manufacture a newer native observation.
def test_current_scope_can_only_retain_observed_proof():
    for local, published, pause, scope, released, uncertain in product((False, True), repeat=6):
        control = ControlState({"owner": "local" if local else "shared", "inhibit": local, "uncertain": uncertain})
        proof = ControlProof(shared=published, hold_verified=pause).retain(
            control, scope_current=scope, released=released
        )
        assert proof.shared is (published and scope and released and not local and not uncertain)
        assert proof.hold_verified is (pause and scope and local)


# Intent: all combinations of health prerequisites retain fault, retirement, freshness and cloud constraints.
# Why: the pure decision must stay conservative for simultaneous conditions rather than relying on branch order.
def test_health_product_preserves_independent_constraints():
    for shared, hold, stopping, active, uncertain, blocked, pending, cloud in product((False, True), repeat=8):
        proof = ControlProof(shared=shared, hold_verified=hold) if not stopping else ControlProof()
        result = assess(
            proof,
            stopping=stopping,
            locally_ready=proof.locally_ready(active=active, control_uncertain=uncertain, delivery_blocked=blocked),
            cloud_pending=pending,
            cloud_ready=cloud,
        )
        assert result.proof == proof
        assert result.local_healthy is (
            (shared or hold) and not stopping and active and not uncertain and not blocked and not pending
        )
        assert result.healthy is (result.local_healthy and cloud)
        assert result.cloud_ready is cloud
        if stopping:
            assert result.state == "stopping"
        elif result.healthy:
            assert result.state == "healthy"
        elif result.local_healthy:
            assert result.state == "local_only"
        else:
            assert result.state == "degraded"


# Intent: a real admitted controller preserves both public status shapes and lazy post-fault cloud reads.
# Why: pending work cannot look freshly observed, and a malformed unrelated grant must not replace the real fault.
@pytest.mark.parametrize("corrupt", ["vendor_start_grant", "uncertain_vendor_command"])
async def test_real_poll_shapes_and_post_fault_read_order(tmp_path, tls, unused_tcp_port, monkeypatch, corrupt):
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "supervision-contract")
            live = await healthy(c)
            assert live["owner_control_enabled"] is True and live["cloud_connected"] is True
            common = {
                "checked_at",
                "mode",
                "desired_control",
                "control_revision",
                "local_start_disabled",
                "charger_connected",
                "cloud_connected",
                "boot_verified",
                "cloud_start_pending",
                "owner_control_enabled",
                "local_hold_verified",
            }
            unknown = {
                "native_observed_at",
                "native_limit_a",
                "current_a",
                "power_w",
                "station_ceiling_a",
                "charging_session_open",
                "vehicle_drawing_power",
            }
            pending = poll_status(c, PollStatus("reconciling", "current_update_pending"), pending=True)
            assert set(pending) == common | unknown | {"state", "fault"}
            assert all(pending[key] is None for key in unknown)
            assert pending["owner_control_enabled"] is False and pending["local_hold_verified"] is False
            assert set(poll_base(c)) == common | {
                "phase",
                "pending_lifecycle_events",
                "pending_observations",
                "automatic_charge_restart",
                "vendor_write_uncertain",
                "vendor_reply_storage_pending",
                "operator_reconciliation_required",
            }
            native = await c.settled_native()
            session.journal.control.hold(session.journal.control.read().values)
            session.journal.db.execute("INSERT INTO meta VALUES(?,'{')", (corrupt,))
            try:
                result = c._assess_observation(Observation(native, False))
                assert not isinstance(result, PollStatus)
                assert result.state == "degraded" and not result.proof.available and not result.local_healthy
                assert (
                    session.journal.get("managed_control_fault")["classification"]
                    == "owner_control_or_delivery_uncertain"
                )
            finally:
                session.journal.db.execute("DELETE FROM meta WHERE key=?", (corrupt,))
