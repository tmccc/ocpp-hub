"""A real managed-service process dies at actual SQL boundaries while independent physical peers survive."""

import asyncio
import copy
import os
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.process_helpers import process_rig
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import starts
from tests.test_prototype import START
from tests_model.catalog import input_digest
from tests_model.deep_cases import process_current_candidates, process_start_candidates
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_schedule_sequences import tls_for_case


# Intent: real SIGKILL at every pre-dispatch atomic boundary preserves no-send proof and fresh owner recovery.
# Why: graceful runtime cancellation cannot establish what survives actual interpreter death and SQLite WAL recovery.
@pytest.mark.parametrize(
    "case",
    process_start_candidates()[::6] if os.environ.get("MODEL_SMOKE") else process_start_candidates(),
    ids=lambda case: case.identifier,
)
async def test_process_sigkill_before_start_dispatch(
    tmp_path, model_tls, monkeypatch, unused_tcp_port_factory, record_property, case
):
    phase, events, complete = case.phase, [], False
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    listener, management = unused_tcp_port_factory(), unused_tcp_port_factory()
    assert listener != management
    async with process_rig(
        tmp_path, tls_for_case(tmp_path, model_tls), listener, management, monkeypatch, case.selected
    ) as child:
        plans, config = copy.deepcopy(child.rig.bench.firmware.plans), copy.deepcopy(child.rig.peer.configuration)
        await child.start()
        ready = await child.status()
        events.append("healthy")
        identifier = uuid.uuid4().hex
        await child.arm(phase, identifier)
        events.append("armed")
        request = asyncio.create_task(child.request("start", id=identifier, revision=ready["control_revision"]))
        marker = await child.gate(phase, identifier)
        events.append("sql-boundary")
        durable = child.durable()["metadata"]
        if phase in ("after-prepared-commit", "before-issued-commit"):
            assert durable["managed_test_charge"]["start_attempted"] is True
            assert durable["local_start_receipt"]["stage"] == "prepared"
            assert marker["transaction_open"] is (phase == "before-issued-commit")
        else:
            assert durable["managed_test_charge"]["start_attempted"] is False
            assert "local_start_receipt" not in durable
        assert not starts(child.rig)
        events.append("durable-read")
        await child.kill()
        events.append("sigkill")
        await asyncio.gather(request, return_exceptions=True)
        await child.start()
        events.append("restart")
        await child.status(lambda value: value.get("charger_connected") is True)
        state = (await child.request("status"))["result"]
        assert (await child.request("pause", revision=state["control_revision"]))["ok"]
        paused = await child.status(lambda value: value.get("local_hold_verified") is True)
        assert child.rig.bench.firmware.effective_limit() == 0
        events.append("pause")
        assert (await child.request("receipt", request_id=identifier))["result"]["state"] == "failed"
        events.append("retired")
        assert (await child.request("app_control", revision=paused["control_revision"]))["ok"]
        await child.status(
            lambda value: value.get("owner_control_enabled") is True and value.get("station_ceiling_a") == case.selected
        )
        events.append("return")
        await child.ordinary_recovery(case.selected, case.demand)
        events.extend(case.events[-4:])
        assert len(child.kills) == 1 and not starts(child.rig)
        assert child.rig.bench.firmware.plans == plans and child.rig.peer.configuration == config
        assert events == list(case.events)
        complete = True
    record_property("model_case", case.identifier)
    record_property("model_family", "process_start")
    record_property("model_input_sha256", input_digest("process_start", case))
    record_property("completed_events", events)
    record_property("suffix_complete", complete)
    record_property("actual_sigkill", child.kills[0]["returncode"] == -9)
    record_property("independent_durable_read", True)
    record_property("peers_survived", True)


# Intent: process death around real current-selection COMMIT retains exactly the last committed choice.
# Why: an applied target ceiling is not proof of a completed owner selection, including while paused.
@pytest.mark.parametrize(
    "case",
    process_current_candidates()[::6] if os.environ.get("MODEL_SMOKE") else process_current_candidates(),
    ids=lambda case: case.identifier,
)
async def test_process_sigkill_current_selection(
    tmp_path, model_tls, monkeypatch, unused_tcp_port_factory, record_property, case
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events = []
    async with process_rig(
        tmp_path,
        tls_for_case(tmp_path, model_tls),
        unused_tcp_port_factory(),
        unused_tcp_port_factory(),
        monkeypatch,
        case.initial,
    ) as child:
        await child.start()
        state = await child.status()
        if case.intent == "paused":
            assert (await child.request("pause", revision=state["control_revision"]))["ok"]
            state = await child.status(lambda value: value.get("local_hold_verified") is True)
        identifier = uuid.uuid4().hex
        await child.arm(case.phase, identifier)
        request = asyncio.create_task(
            child.request("set_current", id=identifier, revision=state["control_revision"], current_a=case.target)
        )
        events.append("choose")
        await child.gate(case.phase, identifier)
        events.append("sql-boundary")
        snapshot = child.durable()
        change = snapshot["metadata"]["managed_current_change"]
        assert change["id"] == identifier
        committed = case.phase == "after-current-success-commit"
        assert change["state"] == ("completed" if committed else "pending")
        row = next(row for row in snapshot["requests"] if row["id"] == identifier)
        assert row["state"] == ("completed" if committed else "accepted")
        events.append("durable-read")
        await child.kill()
        events.append("sigkill")
        await asyncio.gather(request, return_exceptions=True)
        await child.start()
        events.append("restart")
        await child.ordinary_recovery(case.target if committed else case.initial, True)
        receipt = (await child.request("receipt", request_id=identifier))["result"]
        assert receipt["state"] == ("completed" if committed else "failed")
        events.extend(case.events[-4:])
        assert len(child.kills) == 1 and not starts(child.rig)
        assert events == list(case.events)
    record_property("model_case", case.identifier)
    record_property("model_family", "process_current")
    record_property("model_input_sha256", input_digest("process_current", case))
    record_property("completed_events", events)
    record_property("suffix_complete", True)
    record_property("actual_sigkill", child.kills[0]["returncode"] == -9)
    record_property("independent_durable_read", True)
    record_property("peers_survived", True)


# Intent: a real accepted one-shot command can report its actual lifecycle after a process dies before publication.
# Why: durable acceptance and an exact later closed lifecycle must settle the original receipt without replaying Start.
@pytest.mark.parametrize("phase", ["after-accepted-commit", "after-managed-success-commit"])
async def test_process_accepted_start_late_lifecycle(tmp_path, model_tls, monkeypatch, unused_tcp_port_factory, phase):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with process_rig(
        tmp_path, tls_for_case(tmp_path, model_tls), unused_tcp_port_factory(), unused_tcp_port_factory(), monkeypatch
    ) as child:
        await child.start()
        ready = await child.status()
        identifier = uuid.uuid4().hex
        await child.arm(phase, identifier)
        request = asyncio.create_task(child.request("start", id=identifier, revision=ready["control_revision"]))
        await child.gate(phase, identifier)
        before = child.durable()["metadata"]
        assert before["local_start_receipt"]["stage"] == "accepted"
        assert before["managed_test_charge"].get("local_transaction") is None
        assert len(starts(child.rig)) == 1
        tag = starts(child.rig)[0][3]["idTag"]
        await child.kill()
        await asyncio.gather(request, return_exceptions=True)
        await child.start()
        await child.status(lambda value: value.get("charger_connected") is True)
        result = await child.rig.bench.call(child.socket, uuid.uuid4().hex, "StartTransaction", START | {"idTag": tag})
        assert result[0] == 3 and result[2]["idTagInfo"]["status"] == "Accepted"
        local = result[2]["transactionId"]
        child.rig.peer.plan_session = (local, 987654)
        child.rig.peer.permission_enabled = False
        result = await child.rig.bench.call(
            child.socket,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"},
        )
        assert result[0] == 3
        state = (await child.request("status"))["result"]
        assert (await child.request("pause", revision=state["control_revision"]))["ok"]
        await child.status(lambda value: value.get("local_hold_verified") is True)
        receipt = (await child.request("receipt", request_id=identifier))["result"]
        assert receipt["state"] == "completed", {"receipt": receipt, "metadata": child.durable()["metadata"]}
        assert len(starts(child.rig)) == 1
        await child.ordinary_recovery(6, True)


# Intent: actual Return success and intent publish atomically around process death after real positive work.
# Why: acceptance, a physical ceiling and a durable terminal receipt are separate facts during replacement.
@pytest.mark.parametrize(
    "phase", ["after-return-request-commit", "before-return-success-commit", "after-return-success-commit"]
)
@pytest.mark.parametrize("selected", [6, 16, 32])
async def test_process_return_commit_preserves_honest_receipt(
    tmp_path, model_tls, monkeypatch, unused_tcp_port_factory, phase, selected
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with process_rig(
        tmp_path,
        tls_for_case(tmp_path, model_tls),
        unused_tcp_port_factory(),
        unused_tcp_port_factory(),
        monkeypatch,
        selected,
    ) as child:
        await child.start()
        state = await child.status()
        assert (await child.request("pause", revision=state["control_revision"]))["ok"]
        state = await child.status(lambda value: value.get("local_hold_verified") is True)
        identifier = uuid.uuid4().hex
        await child.arm(phase, identifier)
        request = asyncio.create_task(child.request("app_control", id=identifier, revision=state["control_revision"]))
        await child.gate(phase, identifier)
        snapshot = child.durable()
        row = next(row for row in snapshot["requests"] if row["id"] == identifier)
        committed = phase == "after-return-success-commit"
        assert snapshot["metadata"]["managed_control_intent"]["mode"] == ("app" if committed else "paused")
        assert row["state"] == (
            "completed" if committed else "accepted" if phase == "after-return-request-commit" else "running"
        )
        assert child.rig.bench.firmware.effective_limit() == (0 if phase == "after-return-request-commit" else selected)
        await child.kill()
        await asyncio.gather(request, return_exceptions=True)
        await child.start()
        await child.status(lambda value: value.get("charger_connected") is True)
        receipt = (await child.request("receipt", request_id=identifier))["result"]
        assert receipt["state"] == ("completed" if committed else "uncertain")
        await child.ordinary_recovery(selected, True)
        receipt = (await child.request("receipt", request_id=identifier))["result"]
        assert receipt["state"] == ("completed" if committed else "failed")
        assert not starts(child.rig) and len(child.kills) == 1


# Intent: an issued command stays quarantined when process death prevents durable evidence of its actual outcome.
# Why: physical zero or a lost successful reply cannot prove an issued Start was unsent or permit automatic replay.
@pytest.mark.parametrize("phase", ["after-issued-commit", "before-accepted-commit"])
async def test_process_unknown_issued_start_retains_debt(
    tmp_path, model_tls, monkeypatch, unused_tcp_port_factory, phase
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with process_rig(
        tmp_path, tls_for_case(tmp_path, model_tls), unused_tcp_port_factory(), unused_tcp_port_factory(), monkeypatch
    ) as child:
        await child.start()
        state = await child.status()
        identifier = uuid.uuid4().hex
        await child.arm(phase, identifier)
        request = asyncio.create_task(child.request("start", id=identifier, revision=state["control_revision"]))
        await child.gate(phase, identifier)
        assert child.durable()["metadata"]["local_start_receipt"]["stage"] == "issued"
        sent = len(starts(child.rig))
        assert sent == int(phase == "before-accepted-commit")
        await child.kill()
        await asyncio.gather(request, return_exceptions=True)
        await child.start()
        await child.status(lambda value: value.get("charger_connected") is True)
        state = (await child.request("status"))["result"]
        assert (await child.request("pause", revision=state["control_revision"]))["ok"]
        # Unknown issued debt intentionally prevents settled Pause; its restrictive physical command still runs.
        await child.status(
            lambda value: value.get("can_start") is False and child.rig.bench.firmware.effective_limit() == 0
        )
        assert (await child.request("receipt", request_id=identifier))["result"]["state"] == "uncertain"
        assert child.durable()["metadata"]["local_start_receipt"]["stage"] == "issued"
        assert child.rig.bench.firmware.effective_limit() == 0 and len(starts(child.rig)) == sent
