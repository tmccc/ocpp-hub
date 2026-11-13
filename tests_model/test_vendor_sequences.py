"""Retained accepted/uncertain cloud Start debt needs exact independent settlement and fresh restrictive intent."""

import json
import time
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig
from tests_model.catalog import input_digest
from tests_model.control_cases import vendor_candidates
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_start_recovery_sequences import observe
from tests_model.test_stop_sequences import stop_suffix
from tools import reconcile_managed_debt as operator


# Intent: every retained-cloud-Start Pause/replacement/observation order requires exact proof before ordinary recovery.
# Why: zero, elapsed time and an accepted cloud reply do not prove an unbound or uncertain command has gone away.
@pytest.mark.parametrize("case", vendor_candidates(), ids=lambda case: case.identifier)
async def test_vendor_debt_sequences(tmp_path, model_tls, monkeypatch, capsys, record_property, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, suffix_complete = [], False
    async with mp.managed_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as rig:
        stack = AsyncExitStack()
        try:
            s, c, sock = await stack.enter_async_context(running(rig, "vendor-debt-prefix"))
            await mp.healthy(c)
            send = sock.send

            # Change only the independently applied RemoteStart response into an actual correlated CALLERROR.
            async def reply(raw):
                frame = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
                if (
                    case.outcome == "uncertain"
                    and frame[0] == 3
                    and call is not None
                    and call[2] == "RemoteStartTransaction"
                ):
                    raw = build_call_error(frame[1], "InternalError", "synthetic cloud Start outcome unknown")
                await send(raw)

            monkeypatch.setattr(sock, "send", reply)
            response = await rig.bench.cloud_call(
                uuid.uuid4().hex, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
            )
            assert response[0] == (3 if case.outcome == "accepted" else 4)
            kind = "vendor-start" if case.outcome == "accepted" else "vendor-write"
            marker = "vendor_start_grant" if case.outcome == "accepted" else "uncertain_vendor_command"
            saved = s.journal.get(marker)
            assert saved and len(mp.starts(rig)) == 1
            for event in case.events:
                if event == "pause":
                    await pause(c)
                elif event == "observe":
                    await observe(c)
                else:
                    await stack.aclose()
                    stack = AsyncExitStack()
                    s, c, sock = await stack.enter_async_context(running(rig, "vendor-debt-replacement"))
                assert s.journal.get(marker) == saved and len(mp.starts(rig)) == 1
                assert not c.tests.available(c.latest_native, normal=True)
                events.append(event)
            await pause(c)
            assert rig.bench.firmware.effective_limit() == 0 and s.journal.active() is None
            await stack.aclose()
            before = list(rig.bench.device_calls)
            strategy, port, config = strategy_file(rig, tmp_path), rig.prepared.observer.port, tmp_path / "trial.json"
            assert await operator.run("inspect", config, strategy, None, port, kind=kind) == 0
            candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
            evidence = candidate | {
                "original_state_independently_verified": True,
                "command_no_longer_pending" if kind == "vendor-start" else "command_quiescent": True,
                "evidence_sha256": fingerprint({"independent_peer_command_settlement": candidate}),
                "verified_at_epoch": time.time(),
            }
            path = tmp_path / "vendor-evidence.json"
            save_private(path, evidence)
            assert await operator.run("reconcile", config, strategy, path, port, kind=kind) == 0
            assert rig.bench.device_calls == before
            stack = AsyncExitStack()
            s, c, sock = await stack.enter_async_context(running(rig, "vendor-after-independent-proof"))
            assert (await c.tick())["state"] == "attention"
            await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            await stop_suffix(ChargeRig(rig, s, c, sock), case, 1)
            assert events == list(case.events)
            suffix_complete = True
        finally:
            await stack.aclose()
            record_property("model_case", case.identifier)
            record_property("model_family", "vendor_debt")
            record_property("model_input_sha256", input_digest("vendor_debt", case))
            record_property("completed_events", events)
            record_property("suffix_complete", suffix_complete)
