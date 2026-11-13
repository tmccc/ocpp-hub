"""Repeated cloud refusal and failures during its approved recovery retain real lifecycle evidence."""

import json
import sqlite3
import time
import uuid

import pytest
from ocpp_hub_core.prototype import managed_start_delivery
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_stop_reply_recovery import statement as stop_statement
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_debt as operator
from tools import reconcile_managed_delivery as mapping


# Independent nonacceptance permits retry of this event only, and never supplies a fictional cloud number.
def retry_statement(candidate):
    return candidate | {
        "cloud_start_not_accepted": True,
        "fault_origin_independently_verified": True,
        "original_state_independently_verified": True,
        "evidence_sha256": fingerprint({"backend_refused": candidate}),
        "verified_at_epoch": time.time(),
    }


# Offline tooling owns the stopped journal and observes native state without sending any charger command.
async def approve_retry(rig, tmp_path, capsys, *, storage_failure=False):
    args = (tmp_path / "trial.json", strategy_file(rig, tmp_path))
    port = rig.prepared.observer.port
    assert await operator.run("inspect", *args, None, port, kind="start-delivery") == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    assert not any(entry["token"] == candidate["delivery_token"] for entry in rig.bench.backend_starts)
    path = tmp_path / "start-retry-evidence.json"
    evidence = retry_statement(candidate)
    for changed in ({"cloud_start_not_accepted": False}, {"payload_digest": "0" * 64}, {"fault_digest": "0" * 64}):
        save_private(path, evidence | changed)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            await operator.run("reconcile", *args, path, port, kind="start-delivery")
    save_private(path, evidence)
    before = list(rig.bench.device_calls)
    if storage_failure:
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        original = [dict(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")]
        j.db.execute(
            "CREATE TRIGGER reject_start_proof BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_operator_debt_reconciliations' "
            "BEGIN SELECT RAISE(ABORT,'synthetic proof failure'); END"
        )
        j.close()
        with pytest.raises(sqlite3.IntegrityError, match="synthetic proof failure"):
            await operator.run("reconcile", *args, path, port, kind="start-delivery")
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        assert [dict(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")] == original
        assert j.get(KEY) is None
        j.db.execute("DROP TRIGGER reject_start_proof")
        j.close()
    assert await operator.run("reconcile", *args, path, port, kind="start-delivery") == 0
    assert await operator.run("reconcile", *args, path, port, kind="start-delivery") == 0
    assert rig.bench.device_calls == before
    return candidate


# A genuinely allocated cloud number is supplied only from the independent backend's original response record.
async def map_unknown_retry(rig, tmp_path, capsys):
    args = (tmp_path / "trial.json", strategy_file(rig, tmp_path))
    port = rig.prepared.observer.port
    assert await mapping.run("inspect", *args, None, port) == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    response = rig.bench.underlying_start_replies[-1]
    path = tmp_path / "mapping-evidence.json"
    save_private(
        path,
        candidate
        | {
            "vendor": response[2]["transactionId"],
            "evidence_sha256": fingerprint(response),
            "verified_at_epoch": time.time(),
        },
    )
    assert await mapping.run("reconcile-start", *args, path, port) == 0


# Approve the Stop separately using the independently observed backend lifecycle as evidence provenance.
async def approve_stop_after_start(rig, tmp_path, capsys):
    args = (tmp_path / "trial.json", strategy_file(rig, tmp_path))
    port = rig.prepared.observer.port
    assert await operator.run("inspect", *args, None, port, kind="stop-delivery") == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    path = tmp_path / "stop-after-start-evidence.json"
    save_private(path, stop_statement(candidate) | {"evidence_sha256": fingerprint(rig.bench.backend_starts)})
    assert await operator.run("reconcile", *args, path, port, kind="stop-delivery") == 0


# Intent: an exhausted retry has exact recovery, including another error, lost reply, malformed reply or restart.
# Why: a repair that succeeds only on its first attempt can create a second unsupported permanent zero hold.
@pytest.mark.parametrize(
    "recovery_fault",
    [
        "none",
        "error-once",
        "silent",
        "missing-id",
        "bad-info",
        "exhaust-again",
        "restart-inflight",
        "stop-error",
        "stop-bad-info",
    ],
)
async def test_start_retry_can_itself_fail_and_recover(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, recovery_fault
):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(managed_start_delivery, "RETRY_INTERVAL", 0.01)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "start-exhausted") as (s, c, sock):
            await mp.healthy(c)
            rig.bench.start_replies = ["error"] * 3
            result = await rig.bench.call(sock, "start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
            local = result[2]["transactionId"]
            await until(lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "rejected")
            await c.tick()
            retained = s.journal.next_delivery()
            assert len(rig.bench.lifecycle_wires) == 3
            assert (
                await rig.bench.call(
                    sock,
                    "stop",
                    "StopTransaction",
                    {
                        "transactionId": local,
                        "meterStop": 1100,
                        "timestamp": "2026-01-01T00:01:00Z",
                    },
                )
            )[0] == 3
            await pause(c)
        candidate = await approve_retry(rig, tmp_path, capsys, storage_failure=recovery_fault == "none")
        assert candidate["delivery_token"] == retained.token
        if recovery_fault == "restart-inflight":
            j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            j.mark_sent(j.next_delivery())
            j.close()
            j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            assert j.next_delivery().state == "ambiguous"
            assert any(entry.get("superseded") for entry in j.get(KEY).values())
            j.close()
            await approve_retry(rig, tmp_path, capsys)
        plan = (
            ["error"]
            if recovery_fault == "error-once"
            else ["error"] * 3
            if recovery_fault == "exhaust-again"
            else [recovery_fault]
            if recovery_fault in ("silent", "missing-id", "bad-info")
            else []
        )
        rig.bench.start_replies = plan
        if recovery_fault in ("silent", "missing-id", "bad-info", "exhaust-again"):
            async with running(rig, "retry-fails-again") as (s, c, sock):
                await until(
                    lambda: (
                        s.journal.next_delivery() is not None
                        and s.journal.next_delivery().state in ("ambiguous", "rejected")
                    ),
                    timeout=10,
                )
                assert any(entry.get("superseded") for entry in s.journal.get(KEY).values())
                await pause(c)
            if recovery_fault == "exhaust-again":
                await approve_retry(rig, tmp_path, capsys)
            else:
                await map_unknown_retry(rig, tmp_path, capsys)
            rig.bench.start_replies = []
        if recovery_fault in ("stop-error", "stop-bad-info"):
            rig.bench.stop_replies = ["error" if recovery_fault == "stop-error" else "bad-info"]
            async with running(rig, "start-recovered-stop-refused") as (s, c, sock):
                await until(
                    lambda: (
                        s.journal.next_delivery() is not None
                        and s.journal.next_delivery().action == "StopTransaction"
                        and s.journal.next_delivery().state == "rejected"
                    )
                )
                assert s.journal.vendor_id(local) is not None
                assert any(entry.get("superseded") for entry in s.journal.get(KEY).values())
                await pause(c)
            await approve_stop_after_start(rig, tmp_path, capsys)
        async with running(rig, "recovered-start-delivery") as (s, c, sock):
            await until(lambda: s.journal.next_delivery() is None)
            assert s.journal.vendor_id(local) is not None
            stop = [f for f in rig.bench.lifecycle_wires if f[2] == "StopTransaction"]
            assert len(stop) == (2 if recovery_fault.startswith("stop-") else 1)
            assert all(
                frame[3]["transactionId"] == rig.bench.backend_starts[0]["answer"]["transactionId"] for frame in stop
            )
            assert len({frame[1] for frame in stop}) == 1
            await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert s.journal.get("managed_control_fault") is None
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=6, original_starts=0)


# Intent: repeated denied ID zero can close, recover and repeat even when Stop drains before a supervisor poll.
# Why: denial mapping and recovery evidence cannot depend on fault-poll scheduling or global historical uniqueness.
@pytest.mark.parametrize("poll_before_stop", [False, True])
async def test_denied_zero_id_recovery_is_repeatable(tmp_path, tls, unused_tcp_port, monkeypatch, poll_before_stop):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "repeated-zero-denial") as (s, c, sock):
            await mp.healthy(c)
            rig.bench.fixed_vendor = 0
            locals_seen = []
            for number in range(2):
                approved = await rig.bench.call(sock, uuid.uuid4().hex, "Authorize", {"idTag": "NATIVE_TIMER"})
                assert approved[2]["idTagInfo"]["status"] == "Accepted"
                rig.bench.start_replies = ["Blocked"]
                response = await rig.bench.call(
                    sock,
                    uuid.uuid4().hex,
                    "StartTransaction",
                    START
                    | {
                        "idTag": "NATIVE_TIMER",
                        "timestamp": f"2026-01-0{number + 1}T00:00:00Z",
                    },
                )
                local = response[2]["transactionId"]
                locals_seen.append(local)
                await until(lambda: s.journal.vendor_id(local) == 0 and s.journal.next_delivery() is None)
                assert s.journal.get("managed_control_fault") is not None
                if poll_before_stop:
                    await c.tick()
                assert (
                    await rig.bench.call(
                        sock,
                        uuid.uuid4().hex,
                        "StopTransaction",
                        {
                            "transactionId": local,
                            "meterStop": 1100,
                            "timestamp": f"2026-01-0{number + 1}T00:01:00Z",
                        },
                    )
                )[0] == 3
                await until(lambda: s.journal.next_delivery() is None)
                await pause(c)
                assert (await mp.healthy(c))["local_hold_verified"]
                assert s.journal.get("managed_control_fault") is None
                assert s.gateway.auth_info.get("NATIVE_TIMER")["status"] == "Blocked"
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                assert (await mp.healthy(c))["station_ceiling_a"] == 6
            assert locals_seen[0] != locals_seen[1] and s.journal.historical_local_id(0) is None
            rig.bench.fixed_vendor = None
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=6, original_starts=0)
