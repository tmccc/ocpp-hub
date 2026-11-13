"""Local consumption and cloud delivery recover separately after combined real process and reply failures."""

import asyncio
import uuid

import pytest

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench
from tests.process_helpers import process_rig
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import starts
from tests.test_managed_start_delivery_recovery import approve_retry, map_unknown_retry
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.catalog import input_digest
from tests_model.review_cases import bound_owner_delivery_candidates
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_schedule_sequences import tls_for_case


# Intent: an unpublished bound closed owner command cannot obstruct independent exact cloud delivery recovery.
# Why: each failure can recover alone while their combination otherwise leaves every supported operator tool blocked.
@pytest.mark.parametrize("case", bound_owner_delivery_candidates(), ids=lambda case: case.identifier)
async def test_bound_owner_delivery_sequences(
    tmp_path, model_tls, monkeypatch, unused_tcp_port_factory, capsys, record_property, case
):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, complete = [], False
    try:
        async with process_rig(
            tmp_path,
            tls_for_case(tmp_path, model_tls),
            unused_tcp_port_factory(),
            unused_tcp_port_factory(),
            monkeypatch,
            case.selected,
        ) as child:
            await child.start()
            state = await child.status()
            identifier = uuid.uuid4().hex
            await child.arm(case.phase, identifier)
            request = asyncio.create_task(child.request("start", id=identifier, revision=state["control_revision"]))
            events.append("request")
            await child.gate(case.phase, identifier)
            stage = child.durable()["metadata"]["local_start_receipt"]["stage"]
            assert stage == ("issued" if case.phase == "before-accepted-commit" else "accepted")
            tag = starts(child.rig)[-1][3]["idTag"]
            await child.kill()
            await asyncio.gather(request, return_exceptions=True)
            events.append("sigkill")
            await child.start()
            await child.status(lambda value: value.get("charger_connected") is True)
            restarted = child.durable()
            assert next(row for row in restarted["requests"] if row["id"] == identifier)["state"] == "uncertain"
            assert restarted["metadata"]["managed_test_charge"]["start_rpc_accepted"] is False
            child.rig.bench.start_replies = ["silent"] if case.cloud == "silent" else ["error"] * 3
            result = await child.rig.bench.call(
                child.socket, uuid.uuid4().hex, "StartTransaction", START | {"idTag": tag}
            )
            assert result[0] == 3
            local = result[2]["transactionId"]
            child.rig.peer.plan_session, child.rig.peer.permission_enabled = (local, 987654), False
            assert child.durable()["metadata"]["local_start_receipt"]["local"] == local
            events.append("late-start")
            expected = "ambiguous" if case.cloud == "silent" else "rejected"
            await until(
                lambda: bool(child.durable()["outbox"]) and child.durable()["outbox"][0]["state"] == expected,
                timeout=230,
            )
            head = child.durable()["outbox"][0]
            quarantined = child.durable()
            assert next(row for row in quarantined["requests"] if row["id"] == identifier)["state"] == "uncertain"
            assert quarantined["metadata"]["managed_test_charge"]["start_rpc_accepted"] is False
            fault = quarantined["metadata"]["managed_control_fault"]
            assert fault["classification"] == "owner_control_or_delivery_uncertain"
            events.append("cloud-quarantine")
            assert (
                await child.rig.bench.call(
                    child.socket,
                    uuid.uuid4().hex,
                    "StopTransaction",
                    {
                        "transactionId": local,
                        "meterStop": 1100,
                        "timestamp": "2026-01-01T00:01:00Z",
                    },
                )
            )[0] == 3
            events.append("closed")
            state = (await child.request("status"))["result"]
            assert (await child.request("pause", revision=state["control_revision"]))["ok"]
            await child.status(
                lambda value: any(
                    row["id"] == identifier and row["state"] == "completed" for row in child.durable()["requests"]
                )
            )
            settled = child.durable()
            assert settled["metadata"]["managed_test_charge"]["state"] == "completed"
            assert settled["metadata"]["managed_test_charge"]["start_rpc_accepted"] is False
            assert settled["metadata"]["local_start_receipt"]["stage"] == stage
            assert settled["outbox"][0] == head and settled["metadata"].get("managed_control_fault") == fault
            assert next(row for row in settled["transactions"] if row["local"] == local)["vendor"] is None
            assert child.rig.bench.firmware.effective_limit() == 0
            state = (await child.request("status"))["result"]
            assert state["desired_control"] == "paused"
            refused = await child.request("app_control", revision=state["control_revision"])
            assert refused == {"ok": False, "error": "not_ready"}
            events.append("local-settled")
            await child.close()
            if case.cloud == "silent":
                await map_unknown_retry(child.rig, tmp_path, capsys)
            else:
                await approve_retry(child.rig, tmp_path, capsys)
            events.append("attest")
            await child.start()
            await child.delivered(local, closed=True)
            events.append("drain")
            state = (await child.request("status"))["result"]
            assert (await child.request("pause", revision=state["control_revision"]))["ok"]
            await child.status(lambda value: value.get("local_hold_verified") is True)
            assert (await child.request("receipt", request_id=identifier))["result"]["state"] == "completed"
            events.append("recover")
            later = await child.ordinary_recovery(case.selected, (case.demand, not case.demand), start_minute=10)
            assert len(set(later)) == 2 and len(starts(child.rig)) == 1
            assert len(child.rig.bench.backend_starts) == 3
            assert (
                all(entry["closed"] for entry in child.rig.bench.backend_starts) and not child.rig.bench.backend_errors
            )
            assert len(child.kills) == 1 and child.kills[0]["returncode"] == -9
            events.append("two-lifecycles")
            assert events == list(case.events)
            complete = True
    finally:
        record_property("model_case", case.identifier)
        record_property("model_family", "bound_owner_delivery")
        record_property("model_input_sha256", input_digest("bound_owner_delivery", case))
        record_property("completed_events", events)
        record_property("suffix_complete", complete)
        record_property("actual_sigkill", complete)
        record_property("independent_mapping", complete)
