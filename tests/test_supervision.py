"""Health decisions and public status retain separate evidence, authority and pending-work contracts."""

from itertools import product

import pytest
from ocpp_hub_core.prototype.control_state import ControlState
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_view import poll_base, poll_status
from ocpp_hub_core.prototype.supervision import ControlFacts, Observation, PollStatus, assess

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_prototype_tls import tls as tls


# Intent: a complete observation has one explicit verdict for app control, intentional holds and lost authority.
# Why: health labels must never be used to infer a Start, manufacture closure or waive uncertain cloud delivery.
@pytest.mark.parametrize(
    ("owner", "pause", "uncertain", "delivery_uncertain", "vendor_uncertain", "latch"),
    [
        ("shared", False, False, False, False, False),
        ("local", True, False, False, False, False),
        ("local", False, False, False, False, True),
        ("shared", False, True, False, False, True),
        ("shared", False, False, True, False, True),
        ("shared", False, False, False, True, True),
        ("local", True, False, True, False, True),
    ],
)
def test_observation_authority_and_delivery_are_independent(
    owner, pause, uncertain, delivery_uncertain, vendor_uncertain, latch
):
    state = ControlState({"owner": owner, "inhibit": owner == "local", "uncertain": uncertain})
    facts = ControlFacts(state, pause, delivery_uncertain, vendor_uncertain)
    assert facts.requires_fault is latch
    result = assess(
        facts, faulted=latch, stopping=False, active=True, delivery_blocked=False, cloud_pending=False, cloud_ready=True
    )
    assert result.healthy is (not latch)
    assert result.shared is (owner == "shared" and not latch)
    assert result.hold_verified is (pause and not latch)
    assert result.state == ("degraded" if latch else "healthy")


# Intent: all combinations of health prerequisites retain fault, retirement, freshness and cloud constraints.
# Why: the pure decision must stay conservative for simultaneous conditions rather than relying on branch order.
def test_health_product_preserves_independent_constraints():
    for shared, hold, faulted, stopping, active, uncertain, blocked, pending, cloud in product((False, True), repeat=9):
        facts = ControlFacts(
            ControlState({"owner": "shared" if shared else "local", "inhibit": not shared, "uncertain": uncertain}),
            hold,
            False,
            False,
        )
        result = assess(
            facts,
            faulted=faulted,
            stopping=stopping,
            active=active,
            delivery_blocked=blocked,
            cloud_pending=pending,
            cloud_ready=cloud,
        )
        if faulted or stopping:
            assert not result.shared and not result.hold_verified and not result.local_healthy
        if not active or uncertain or blocked or pending:
            assert not result.local_healthy
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
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
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
                "operator_reconciliation_required",
            }
            native = await c.settled_native()
            session.journal.control.hold(session.journal.control.read().values)
            session.journal.db.execute("INSERT INTO meta VALUES(?,'{')", (corrupt,))
            try:
                result = c._assess_observation(Observation(native, False))
                assert not isinstance(result, PollStatus)
                assert result.state == "degraded" and not result.shared and not result.local_healthy
                assert (
                    session.journal.get("managed_control_fault")["classification"]
                    == "owner_control_or_delivery_uncertain"
                )
            finally:
                session.journal.db.execute("DELETE FROM meta WHERE key=?", (corrupt,))
