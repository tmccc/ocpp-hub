"""Unknown issued commands require exact independent proof before recovery, even after real process death."""

import asyncio
import json
import time
import uuid

import pytest
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as managed_bench_module
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench
from tests.peer_start_ledger import PeerStartLedger
from tests.process_helpers import process_rig
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import starts
from tests_model.catalog import input_digest
from tests_model.review_cases import issued_start_candidates
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_schedule_sequences import tls_for_case
from tools import reconcile_managed_debt as operator


# Intent: every issued/publication death boundary has a real exact-proof route back to independent later charging.
# Why: elapsed time and zero cannot consume a command, but an unsupported quarantine is not complete recovery.
@pytest.mark.parametrize("case", issued_start_candidates(), ids=lambda case: case.identifier)
async def test_issued_start_proof_sequences(
    tmp_path, model_tls, monkeypatch, capsys, record_property, unused_tcp_port_factory, case
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(managed_bench_module, "OwnerBench", StartReplyBench)
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
            ledger = PeerStartLedger(child.rig.bench)
            child.rig.bench.before_reply = ledger.observe
            await child.start()
            ready = await child.status()
            if case.paused:
                assert (await child.request("pause", revision=ready["control_revision"]))["ok"]
                ready = await child.status(lambda value: value.get("local_hold_verified") is True)
            identifier = uuid.uuid4().hex
            await child.arm(case.phase, identifier)
            pending = asyncio.create_task(child.request("start", id=identifier, revision=ready["control_revision"]))
            events.append("request")
            await child.gate(case.phase, identifier)
            snapshot = child.durable()
            state, receipt = snapshot["metadata"]["managed_test_charge"], snapshot["metadata"]["local_start_receipt"]
            assert receipt["stage"] == ("accepted" if case.phase == "after-accepted-commit" else "issued")
            assert state["start_rpc_accepted"] is False and receipt["request_id"] == identifier
            wire_count = int(case.phase != "after-issued-commit")
            assert len(starts(child.rig)) == wire_count
            events.append("durable-read")
            await child.kill()
            events.append("sigkill")
            await asyncio.gather(pending, return_exceptions=True)
            await child.start()
            events.append("restart")
            ready = await child.status(lambda value: value.get("charger_connected") is True)
            assert (await child.request("pause", revision=ready["control_revision"]))["ok"]
            await child.status(
                lambda value: (
                    value.get("desired_control") == "paused" and child.rig.bench.firmware.effective_limit() == 0
                )
            )
            events.append("pause")
            assert (await child.request("receipt", request_id=identifier))["result"]["state"] == "uncertain"
            assert child.durable()["metadata"]["control"]["start_pending"] is True
            # Keep the real hard deadline and the peer-owned command evidence separate from elapsed time.
            await asyncio.sleep(max(0, state["hard_deadline_epoch"] - time.time()) + 0.05)
            await child.close()
            before = list(child.rig.bench.device_calls)
            settlement = ledger.settle(identifier, starts(child.rig))
            args = (tmp_path / "trial.json", tmp_path / "process-strategy.json")
            port = child.rig.prepared.observer.port
            assert await operator.run("inspect", *args, None, port, kind="local-start") == 0
            candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
            assert candidate["request_id"] == identifier
            path = tmp_path / "issued-evidence.json"
            save_private(
                path,
                candidate
                | {
                    "command_no_longer_pending": True,
                    "evidence_sha256": fingerprint(settlement),
                    "verified_at_epoch": time.time(),
                },
            )
            events.append("attest")
            assert await operator.run("reconcile", *args, path, port, kind="local-start") == 0
            settled = child.durable()
            request = next(row for row in settled["requests"] if row["id"] == identifier)
            assert request["state"] == "failed"
            assert settled["metadata"]["local_start_receipt"]["stage"] == "reconciled"
            assert settled["metadata"]["control"]["start_pending"] is False
            assert child.rig.bench.device_calls == before and child.rig.bench.firmware.effective_limit() == 0
            events.append("retire")
            after_proof_calls = len(child.rig.bench.device_calls)
            await child.start()
            ready = await child.status(lambda value: value.get("state") == "attention")
            assert ready.get("owner_control_enabled") is not True and child.rig.bench.firmware.effective_limit() == 0
            assert child.durable()["metadata"]["control"]["owner"] == "local"
            assert child.durable()["metadata"]["control"]["inhibit"] is True
            assert (
                child.durable()["metadata"]["managed_control_fault"]["classification"]
                == "operator_debt_reconciled_at_zero"
            )
            assert all(
                frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                for frame in child.rig.bench.device_calls[after_proof_calls:]
                if frame[2] == "SetChargingProfile"
            )
            assert (await child.request("pause", revision=ready["control_revision"]))["ok"]
            await child.status(lambda value: value.get("local_hold_verified") is True)
            assert (await child.request("receipt", request_id=identifier))["result"]["state"] == "failed"
            events.append("recover")
            locals_seen = await child.ordinary_recovery(case.selected, (case.demand, not case.demand), start_minute=10)
            assert len(set(locals_seen)) == 2 and len(starts(child.rig)) == wire_count
            assert len(child.rig.bench.backend_starts) == 2
            assert all(entry["closed"] for entry in child.rig.bench.backend_starts)
            assert not child.rig.bench.backend_errors
            assert len(child.kills) == 1 and child.kills[0]["returncode"] == -9
            events.append("two-lifecycles")
            assert events == list(case.events)
            complete = True
    finally:
        record_property("model_case", case.identifier)
        record_property("model_family", "issued_start_proof")
        record_property("model_input_sha256", input_digest("issued_start_proof", case))
        record_property("completed_events", events)
        record_property("suffix_complete", complete)
        record_property("actual_sigkill", complete)
        record_property("independent_durable_read", complete)
        record_property("peers_survived", complete)
        record_property("independent_mapping", complete)
