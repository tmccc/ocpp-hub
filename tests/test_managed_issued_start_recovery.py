"""Ordinary lost or erroneous RemoteStart replies recover only after exact independently attested settlement."""

import asyncio
import json
import time
import uuid

import pytest
from ocpp_hub_core.prototype.managed_debt_recovery import local_start_candidate
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as managed_bench_module
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.handover_helpers import crash
from tests.peer_start_ledger import PeerStartLedger
from tests.test_managed_debt_reconciliation import ErrorReplyPeer, strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig, ready
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tools import reconcile_managed_debt as operator


# Intent: reply timeout and CALLERROR both have a supported exact-proof exit, without pretending the Start was unsent.
# Why: process-death-only coverage misses ordinary transport failures that strand the same issued command receipt.
@pytest.mark.parametrize("fault", ["late-reply", "call-error"])
@pytest.mark.parametrize("paused", [False, True])
async def test_ordinary_issued_start_exact_recovery(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, fault, paused):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(managed_bench_module, "OwnerBench", StartReplyBench)
    trace = []
    if fault == "call-error":
        actual = Bench.read_charger

        # Change only the independent device's actual correlated reply after the real command arrives.
        async def erroneous_peer(bench, connection):
            await actual(bench, ErrorReplyPeer(connection, trace))

        monkeypatch.setattr(Bench, "read_charger", erroneous_peer)
    async with ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, startup=3, duration=0.02, owner_tag="APP_OWNER", cli_route=True
    ) as b:
        c, s, rig = b.controller, b.session, b.rig
        ledger = PeerStartLedger(rig.bench)
        rig.bench.before_reply = ledger.observe
        if paused:
            await pause(c)
            assert (await healthy(c))["local_hold_verified"]
        if fault == "late-reply":
            # A real response becomes too late for its genuine RPC future, while no charger lifecycle follows.
            async def delay_reply(action, payload):
                await ledger.observe(action, payload)
                if action == "RemoteStartTransaction":
                    await asyncio.sleep(6)

            rig.bench.before_reply = delay_reply
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        await c.tests.settle()
        rig.bench.before_reply = None
        assert len(starts(rig)) == 1
        receipt = s.journal.get("local_start_receipt")
        state = c.tests.state()
        assert receipt["stage"] == "issued" and state["start_rpc_accepted"] is False
        assert s.journal.get("control")["start_pending"] is True
        await asyncio.sleep(max(0, state["hard_deadline_epoch"] - time.time()) + 0.05)
        await pause(c)
        status = await c.tick()
        assert not status["local_hold_verified"] and not status["owner_control_enabled"]
        returned = uuid.uuid4().hex
        c.command(returned, "app_control", c.requests.intent().revision)
        await c.tick()
        assert c.requests.receipt(returned)["state"] in ("accepted", "running")
        assert c.requests.intent().mode == "paused" and rig.bench.firmware.effective_limit() == 0
        await pause(c)
        assert c.requests.receipt(returned)["state"] == "failed"
        strategy = strategy_file(rig, tmp_path)
        config, port = tmp_path / "trial.json", rig.prepared.observer.port
        await c.tests.close()
        await crash(s)
        assert await operator.run("inspect", config, strategy, None, port, kind="local-start") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["request_id"] == identifier
        evidence = candidate | {
            "command_no_longer_pending": True,
            "evidence_sha256": fingerprint(
                {
                    "independent_command_settlement": starts(rig),
                    "request": identifier,
                }
            ),
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "local-start-evidence.json"
        for changed in (
            {"command_no_longer_pending": False},
            {"receipt_digest": "0" * 64},
            {"state_digest": "0" * 64},
            {"request_id": "0" * 32},
        ):
            save_private(path, evidence | changed)
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, port, kind="local-start")
        save_private(path, evidence)
        before = list(rig.bench.device_calls)
        settlement = ledger.settle(identifier, starts(rig))
        evidence["evidence_sha256"] = fingerprint(settlement)
        save_private(path, evidence)
        assert await operator.run("reconcile", config, strategy, path, port, kind="local-start") == 0
        assert rig.bench.device_calls == before and rig.bench.firmware.effective_limit() == 0
        async with running(rig, "issued-command-reconciled") as (replacement, owner, sock):
            status = await owner.tick()
            assert status["state"] == "attention"
            assert replacement.journal.get("control")["owner"] == "local"
            assert replacement.journal.get("control")["inhibit"] is True
            assert rig.bench.firmware.effective_limit() == 0
            assert owner.requests.receipt(identifier)["state"] == "failed"
            assert replacement.journal.get("local_start_receipt")["stage"] == "reconciled"
            assert replacement.journal.get("control")["start_pending"] is False
            await pause(owner)
            assert (await healthy(owner))["local_hold_verified"]
            await two_later_sessions(ChargeRig(rig, replacement, owner, sock), selected=6, original_starts=1)


# Intent: each exact correlation field independently excludes a different issued or unpublished command.
# Why: permitting issued receipts must not infer modern request identity, accepted publication or revised authority.
@pytest.mark.parametrize(
    "change",
    [
        "missing-id",
        "wrong-id",
        "wrong-revision",
        "not-attempted",
        "issued-published",
        "wrong-tag",
        "bound",
        "not-pending",
    ],
)
def test_issued_candidate_requires_complete_provenance(tmp_path, change):
    from ocpp_hub_core.prototype.store import Journal

    j = Journal(tmp_path / "guard.db", "ISSUED_GUARD")
    request = {"id": "a" * 32, "action": "start", "revision": 1, "state": "uncertain", "result": "{}"}
    state = {
        "id": request["id"],
        "kind": "normal",
        "source": "local",
        "start_rpc_accepted": False,
        "start_attempted": True,
        "local_transaction": None,
        "state": "interrupted",
        "hard_deadline_epoch": time.time() - 1,
        "revision": 1,
        "tag_digest": "b" * 64,
    }
    receipt = {"request_id": request["id"], "stage": "issued", "local": None, "ticket": "c" * 32, "digest": "b" * 64}
    j.put("local_start_receipt", receipt)
    j.put("control", {"start_pending": True})
    snapshot = {"test": state, "requests": [request], "grant": None, "mutations": [], "deferred": None}
    try:
        assert local_start_candidate(j, snapshot)["request_id"] == request["id"]
        if change == "missing-id":
            receipt.pop("request_id")
        elif change == "wrong-id":
            receipt["request_id"] = "d" * 32
        elif change == "wrong-revision":
            state["revision"] += 1
        elif change == "not-attempted":
            state["start_attempted"] = False
        elif change == "issued-published":
            state["start_rpc_accepted"], state["revision"] = True, 2
        elif change == "wrong-tag":
            receipt["digest"] = "e" * 64
        elif change == "bound":
            receipt["local"] = 123
        else:
            j.put("control", {"start_pending": False})
        j.put("local_start_receipt", receipt)
        with pytest.raises(RecoveryRequiredError):
            local_start_candidate(j, snapshot)
    finally:
        j.close()
