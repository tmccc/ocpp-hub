"""Actual producer vocabularies and pending status remain truthful at the HA projection boundary."""

import ast
import importlib.util
import uuid
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.diagnostics import STAGES, FaultDiagnostics
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_diagnostics import FAULTS, snapshot
from ocpp_hub_core.prototype.managed_view import health_snapshot

from tests.test_managed_current import adjustable
from tests.test_managed_proxy import healthy
from tests.test_managed_service_cli import healthy_status
from tests.test_prototype_tls import tls as tls
from tools.managed_reporting import Reporting

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "custom_components/ocpp_hub/core/prototype"


# Parse actual producer files independently of either diagnostic consumer's accepted vocabulary.
def producer_tree(path):
    return ast.parse(path.read_text())


# Every literal branch must be represented; a new dynamic producer needs an explicitly reviewed contract.
def literal_branches(value):
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return {value.value}
    if isinstance(value, ast.IfExp):
        return literal_branches(value.body) | literal_branches(value.orelse)
    raise AssertionError("review dynamic diagnostic producer: " + ast.unparse(value))


# Select only controller status methods and the actual service's top-level status publications.
def emitted_states():
    methods = [
        node
        for node in ast.walk(producer_tree(CORE / "managed.py"))
        if isinstance(node, ast.AsyncFunctionDef) and node.name in ("tick", "_tick")
    ]
    methods.append(producer_tree(ROOT / "tools/managed_proxy.py"))
    values = set()
    for method in methods:
        for node in ast.walk(method):
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values, strict=True):
                    if isinstance(key, ast.Constant) and key.value == "state":
                        values |= literal_branches(value)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "update":
                for keyword in node.keywords:
                    if keyword.arg == "state":
                        values |= literal_branches(keyword.value)
    assert {"healthy", "stopped", "waiting_for_cloud"} <= values
    return sorted(values)


# Trial-phase writes across the bounded engine are the public phase producer, including cleanup transitions.
def emitted_phases():
    values = set()
    for path in CORE.glob("*.py"):
        for node in ast.walk(producer_tree(path)):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "put"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "trial_phase"
            ):
                values |= literal_branches(node.args[1])
    assert {"rollback", "backend_restore_pending", "active"} <= values
    return sorted(values)


# Unpack parallel phase assignments so sending() cannot evade the producer-derived contract.
def assignment_pairs(target, value):
    if isinstance(target, ast.Tuple):
        assert isinstance(value, ast.Tuple)
        for child, assigned in zip(target.elts, value.elts, strict=True):
            yield from assignment_pairs(child, assigned)
    else:
        yield target, value


# Defaults and actual phase writes define this set; the HA allowlist never supplies expected values.
def emitted_rpc_phases():
    values = set()
    for name in ("diagnostics.py", "rpc.py"):
        for node in ast.walk(producer_tree(CORE / name)):
            pairs = []
            if isinstance(node, ast.AnnAssign) and node.value is not None:
                pairs = [(node.target, node.value)]
            elif isinstance(node, ast.Assign):
                pairs = [pair for target in node.targets for pair in assignment_pairs(target, node.value)]
            for target, value in pairs:
                if (
                    isinstance(target, ast.Attribute)
                    and target.attr == "phase"
                    or (isinstance(target, ast.Name) and target.id == "phase")
                ):
                    values |= literal_branches(value)
    assert {"idle", "complete", "sending"} <= values
    return sorted(values)


# Load only the HA-free projection, without initializing Home Assistant or the legacy integration.
def client_projection():
    path = Path(__file__).resolve().parents[1] / "custom_components/ics_proxy/diagnostic_view.py"
    spec = importlib.util.spec_from_file_location("diagnostic_contract_view", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Intent: an actual cloud-wait transition survives the reporter and HA reconstruction.
# Why: outage history must not disappear at the producer/consumer vocabulary boundary.
@pytest.mark.parametrize("state", emitted_states())
def test_cloud_wait_timeline_survives_both_projections(tmp_path, state):
    value = Reporting(tmp_path).update({"state": state, "checked_at": 10000, "diagnostics": {"available": True}})
    row = value["diagnostics"]["timeline"][0]
    assert row["state"] == state
    assert client_projection().transition(row)["state"] == state


# Intent: every actual phase write survives the process reporter and client, including both cleanup phases.
# Why: checking a handwritten allowlist cannot notice a new producer value dropped by both consumers.
@pytest.mark.parametrize("phase", emitted_phases())
def test_emitted_phase_survives_both_projections(tmp_path, phase):
    value = Reporting(tmp_path).update(
        {
            "state": "stopping",
            "phase": phase,
            "checked_at": 10000,
            "diagnostics": {"available": True},
        }
    )
    row = value["diagnostics"]["timeline"][0]
    assert row["phase"] == phase and client_projection().transition(row)["phase"] == phase


# Intent: every genuine protocol failure stage reaches the client unchanged, including Boot probes.
# Why: a new producer stage cannot silently become a generic session failure or null display.
@pytest.mark.parametrize("stage", sorted(STAGES | {"boot_probe"}))
def test_fault_stage_round_trips_through_client(stage):
    fault = FaultDiagnostics()
    fault.record(TimeoutError(), stage, "TriggerMessage")
    assert fault.stage == stage
    result = client_projection().protocol({"charger": {"fault": fault.snapshot()}})
    assert result["charger"]["fault"]["stage"] == stage


# Intent: successful and interrupted RPC phases remain distinct in the HA diagnostic projection.
# Why: normal successful traffic must not look like absent protocol evidence.
@pytest.mark.parametrize("phase", emitted_rpc_phases())
def test_rpc_phase_round_trips_through_client(phase):
    result = client_projection().protocol(
        {
            "cloud": {
                "rpc": {
                    "action": "Heartbeat",
                    "phase": phase,
                    "failed_phase": phase,
                }
            }
        }
    )
    assert result["cloud"]["rpc"]["phase"] == phase
    assert result["cloud"]["rpc"]["failed_phase"] == phase


# Intent: actual runtime stage emitters must be declared before FaultDiagnostics can collapse them to session.
# Why: a registry-only round trip misses newly emitted labels which never reached that registry.
def test_every_actual_fault_stage_emitter_is_declared():
    tree, stages = producer_tree(CORE / "runtime.py"), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_vendor_stage":
            for returned in ast.walk(node):
                if isinstance(returned, ast.Return):
                    stages |= literal_branches(returned.value)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if any(isinstance(target, ast.Name) and target.id == "stage" for target in node.targets):
                stages |= literal_branches(node.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method = ast.unparse(node.func)
            if method.endswith("fault.record") or method.endswith("fault.record_close"):
                value = node.args[1 if node.func.attr == "record" else 0]
                if isinstance(value, ast.Constant):
                    stages |= literal_branches(value)
                else:
                    assert ast.unparse(value) in ("stage", "self._vendor_stage()")
    assert "boot_probe" in stages and stages <= STAGES


# Intent: every emitted known fault has a bounded human explanation in the client.
# Why: producer additions must not silently become unrecognized diagnostic data.
def test_fault_vocabulary_is_accepted_by_client():
    assert FAULTS <= client_projection().FAULTS.keys()


# Intent: each actual temporary-schedule alarm remains a current diagnostic fault.
# Why: a useful base alarm must not become an empty support-download cause.
@pytest.mark.parametrize(
    "alarm",
    [
        "temporary_schedule_cleanup_unconfirmed",
        "temporary_schedule_awaiting_start_report",
        "temporary_schedule_stop_unconfirmed",
    ],
)
async def test_temporary_schedule_current_fault_is_preserved(tmp_path, tls, unused_tcp_port, monkeypatch, alarm):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        value = snapshot(b.controller, (await healthy(b.controller)) | {"fault": alarm})
        assert value["available"] and value["current_fault"] == alarm
        assert client_projection().diagnostics(value)["current_fault"] == alarm


# Intent: absent pending-operation connection facts remain unknown without creating positive health.
# Why: unknown does not mean disconnected, and correcting that label cannot grant control authority.
def test_unknown_connections_do_not_become_disconnected():
    value = healthy_status(10000)
    for key in ("charger_connected", "cloud_connected", "boot_verified"):
        value.pop(key)
    result = health_snapshot(value, 10000)
    assert not result["healthy"] and not result["local_healthy"]
    assert not {"charger_disconnected", "cloud_disconnected", "boot_unverified"} & set(result["health_blockers"])
    assert "local_health_unverified" in result["health_blockers"]


# Intent: a pending Start/current snapshot cannot become a fictitious new Pause at a nonzero revision.
# Why: the API may merge current intent without inventing physical progress or disconnected connections.
@pytest.mark.parametrize("mode", ["app", "paused"])
@pytest.mark.parametrize("fault", ["current_update_pending", "start_operation_pending"])
async def test_partial_pending_status_keeps_its_operation(tmp_path, tls, unused_tcp_port, monkeypatch, mode, fault):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c = b.controller
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        await healthy(c)
        if mode == "app":
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            await healthy(c)
        published = {"state": "reconciling", "fault": fault, "checked_at": 10000}
        api = ManagementServer(ManagementSettings(12000, tmp_path / "unused", "a" * 64), c, lambda: published)
        result = api.read_only(uuid.uuid4().hex, "status")["result"]
        assert result["state"] == "reconciling" and result["fault"] == fault
        assert result["desired_control"] == mode
        assert result["control_revision"] == c.requests.intent().revision
        assert not result["local_healthy"] and not result["can_start"]
