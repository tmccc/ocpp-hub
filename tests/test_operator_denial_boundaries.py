"""Actual recorded operator proofs retain denial identity and share one atomic Pause completion."""

import copy
import sqlite3
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed_debt_recovery import KEY, operator_recovery_evidence
from ocpp_hub_core.prototype.managed_denial_faults import denial_source_fault, retained_denial_fault
from ocpp_hub_core.prototype.managed_release_recovery import release_evidence
from ocpp_hub_core.prototype.managed_start_delivery import DENIALS, denied_start_evidence

from tests import test_managed_proxy as mp
from tests.handover_helpers import crash
from tests.test_combined_recovery_proofs import queue_closed_pairs
from tests.test_denial_operator_composition import selected
from tests.test_managed_start_delivery_recovery import approve_retry
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Generate every lifecycle, receipt and proof through real TLS and the real offline tool before testing boundaries.
@asynccontextmanager
async def recorded_operator_denial(tmp_path, tls, port, monkeypatch, capsys):
    async with selected(tmp_path, tls, port, monkeypatch, 6) as original:
        rig, s, c = original.rig, original.session, original.controller
        rig.bench.drop_start_receipts = {2}
        ids = await queue_closed_pairs(original, ["Blocked", "Accepted"])
        await until(
            lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "ambiguous",
            timeout=12,
        )
        source = copy.deepcopy(s.journal.get("managed_control_fault"))
        await pause(c)
        await c.current.close()
        await c.tests.close()
        await crash(s)
        await approve_retry(rig, tmp_path, capsys)
        rig.bench.start_replies = []
        async with running(rig, "operator-denial-boundary") as (s, c, sock):
            await until(lambda: s.journal.next_delivery() is None)
            yield SimpleNamespace(rig=rig, session=s, controller=c, charger=sock, ids=ids, source=source)


# Intent: changed proof headers, postimages and own Stop identities cannot borrow denial recovery authority.
# Why: the bridge may follow only this exact supported active proof, while final release still needs its whole outcome.
async def test_actual_operator_denial_proof_boundaries(tmp_path, tls, unused_tcp_port, monkeypatch, capsys):
    async with recorded_operator_denial(tmp_path, tls, unused_tcp_port, monkeypatch, capsys) as b:
        j, c = b.session.journal, b.controller
        fault, records, denials = (copy.deepcopy(j.get(key)) for key in ("managed_control_fault", KEY, DENIALS))
        identifier, entry = next(iter(records.items()))
        proof = entry["proof"]
        assert operator_recovery_evidence(c, fault) == proof
        assert denied_start_evidence(c, fault) is not None
        assert retained_denial_fault(j, proof) == b.source
        for change in (
            "completed",
            "superseded",
            "installed-fault",
            "attestation",
            "foreign-prior",
            "unknown-kind",
            "wrong-kind-shape",
            "changed-generic-prior",
            "changed-prior-outbox",
            "changed-prior-denials",
            "anchor",
            "postimage",
            "start-wire",
            "stop-wire",
        ):
            altered = copy.deepcopy(records)
            value = altered[identifier]
            if change in ("completed", "superseded"):
                value[change] = {"reason": "persisted negative control"}
            elif change == "installed-fault":
                value["proof"]["installed_fault"] = {"classification": "foreign"}
            elif change == "attestation":
                value["proof"]["attestation"]["candidate_digest"] = "0" * 64
            elif change == "foreign-prior":
                value["proof"]["prior"]["fault"] = {"classification": "native_authority_or_preservation_failed"}
            elif change in ("unknown-kind", "wrong-kind-shape"):
                value["proof"]["kind"] = "unknown-kind" if change == "unknown-kind" else "observation"
            elif change == "changed-generic-prior":
                value["proof"]["prior"]["fault"]["at_epoch"] += 1
            elif change == "changed-prior-outbox":
                value["proof"]["prior"]["outbox"][0]["token"] = "e-" + "f" * 32
            elif change == "changed-prior-denials":
                value["proof"]["prior"]["denials"][str(b.ids[0])]["reply_digest"] = "0" * 64
            elif change == "anchor":
                value["proof"]["anchor"]["selected_a"] = 32
            elif change == "postimage":
                value["proof"]["resolved"]["retained"] = [{"token": "foreign"}]
            else:
                value["proof"].pop("start_wire_ack" if change == "start-wire" else "stop_wire_ack")
            j.put(KEY, altered)
            assert release_evidence(c, fault) is None, change
            assert j.get(DENIALS) == denials
            j.put(KEY, records)
        for changed_fault in (None, {"classification": "foreign-historical-fault"}):
            altered = copy.deepcopy(denials)
            altered[str(b.ids[0])]["fault"] = changed_fault
            j.put(DENIALS, altered)
            assert denied_start_evidence(c, fault) is None
            assert retained_denial_fault(j, proof) is None
            assert denial_source_fault(j, {"classification": "foreign-historical-fault"}) is None
            j.put(DENIALS, denials)
        for change in ("reply_digest", "denied_at", "start_token"):
            altered = copy.deepcopy(denials)
            altered[str(b.ids[0])][change] = altered[str(b.ids[0])][change] + 1 if change == "denied_at" else "0" * 64
            j.put(DENIALS, altered)
            assert release_evidence(c, fault) is None, change
            assert retained_denial_fault(j, proof) is None
            j.put(DENIALS, denials)
        for change in ("fault", "local_transaction_id", "vendor_transaction_id", "start_digest", "stop_digest"):
            altered = copy.deepcopy(denials)
            altered[str(b.ids[0])]["wire_ack"][change] = (
                {"classification": "foreign"} if change == "fault" else 999999 if change.endswith("_id") else "0" * 64
            )
            j.put(DENIALS, altered)
            assert release_evidence(c, fault) is None, change
            j.put(DENIALS, denials)
        identifier, _ = await pause(c)
        assert (await mp.healthy(c))["local_hold_verified"]
        assert j.get(DENIALS)[str(b.ids[0])]["completed"]["pause"] == identifier
        assert j.get(KEY)[next(iter(records))]["completed"]["pause"] == identifier


# Intent: failed operator/member writes and COMMIT roll back both consumptions, then the same Pause can repair.
# Why: consuming the operator first cannot disable the captured denial or leave partial completion on storage error.
@pytest.mark.parametrize("boundary", ["operator", "denial", "commit"])
async def test_operator_and_denial_completion_are_one_real_transaction(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, boundary
):
    async with recorded_operator_denial(tmp_path, tls, unused_tcp_port, monkeypatch, capsys) as b:
        j, c = b.session.journal, b.controller
        original = (
            copy.deepcopy(j.get(KEY)),
            copy.deepcopy(j.get(DENIALS)),
            copy.deepcopy(j.get("managed_control_fault")),
        )
        target = KEY if boundary == "operator" else DENIALS
        if boundary == "commit":
            j.db.execute("CREATE TABLE denial_atomic_parent(id INTEGER PRIMARY KEY)")
            j.db.execute(
                "CREATE TABLE denial_atomic_child(id REFERENCES denial_atomic_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
            body = "INSERT INTO denial_atomic_child VALUES(1);"
        else:
            body = "SELECT RAISE(ABORT,'actual joint completion refusal');"
        j.db.execute(
            "CREATE TRIGGER deny_joint_completion BEFORE INSERT ON meta WHEN NEW.key='" + target + "' "
            "AND instr(NEW.value,'\"completed\":{')>0 BEGIN " + body + " END"
        )
        identifier = uuid.uuid4().hex
        c.command(identifier, "pause", c.requests.intent().revision)
        if c.pause_task is not None:
            await c.pause_task
        with pytest.raises(sqlite3.IntegrityError):
            await c.tick()
        assert not j.db.in_transaction
        assert (j.get(KEY), j.get(DENIALS), j.get("managed_control_fault")) == original
        assert b.rig.bench.firmware.effective_limit() == 0
        j.db.execute("DROP TRIGGER deny_joint_completion")
        if boundary == "commit":
            assert j.db.execute("SELECT COUNT(*) FROM denial_atomic_child").fetchone()[0] == 0
        assert (await mp.healthy(c))["local_hold_verified"]
        assert j.get("managed_control_fault") is None
        assert all(entry["completed"]["pause"] == identifier for entry in j.get(DENIALS).values())
        assert all(entry["completed"]["pause"] == identifier for entry in j.get(KEY).values())


# Intent: an actual uncertain vendor command reconciled before any generic fault poll still completes its own proof.
# Why: valid operator headers may have prior.fault=None; only denied-session association requires a generic prior fault.
async def test_operator_proof_without_prior_fault_completes(tmp_path, tls, unused_tcp_port, monkeypatch, capsys):
    import json
    import time

    from ocpp_hub_core.prototype.managed_denial_faults import current_operator_proof
    from ocpp_hub_core.prototype.store import fingerprint
    from ocpp_hub_core.prototype.trial_session import save_private

    from tests.test_managed_debt_reconciliation import ErrorReplyPeer, strategy_file
    from tests.test_managed_test_charge import ChargeRig
    from tests.test_trial_session import Bench
    from tools import reconcile_managed_debt as operator

    trace, actual = [], Bench.read_charger

    # The actual fake firmware consumes the command; its real TLS response is changed to an ambiguous CALLERROR.
    async def charger_peer(bench, connection):
        await actual(bench, ErrorReplyPeer(connection, trace))

    monkeypatch.setattr(Bench, "read_charger", charger_peer)
    async with selected(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as original:
        rig, s, c = original.rig, original.session, original.controller
        strategy = strategy_file(rig, tmp_path)
        response = await rig.bench.cloud_call(
            uuid.uuid4().hex, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
        )
        assert response[0] == 4 and response[2] == "InternalError" and len(trace) == 1
        identifier = uuid.uuid4().hex
        c.command(identifier, "pause", c.requests.intent().revision)
        await c.pause_task
        assert s.journal.get("managed_control_fault") is None
        assert rig.bench.firmware.effective_limit() == 0
        await c.tests.close()
        await crash(s)
        args = tmp_path / "trial.json", strategy
        native_port = rig.prepared.observer.port
        assert await operator.run("inspect", *args, None, native_port, kind="vendor-write") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        path = tmp_path / "no-prior-fault-statement.json"
        save_private(
            path,
            candidate
            | {
                "command_quiescent": True,
                "original_state_independently_verified": True,
                "evidence_sha256": fingerprint(trace),
                "verified_at_epoch": time.time(),
            },
        )
        before = copy.deepcopy(rig.bench.device_calls)
        assert await operator.run("reconcile", *args, path, native_port, kind="vendor-write") == 0
        assert rig.bench.device_calls == before
        async with running(rig, "no-prior-fault-completion") as (s, c, sock):
            fault = s.journal.get("managed_control_fault")
            proof = current_operator_proof(s.journal, fault)
            assert proof is not None and proof["prior"]["fault"] is None
            assert denial_source_fault(s.journal, fault) is None and retained_denial_fault(s.journal, proof) is None
            identifier, _ = await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert s.journal.get("managed_control_fault") is None
            assert all(entry["completed"]["pause"] == identifier for entry in s.journal.get(KEY).values())
            b = ChargeRig(rig, s, c, sock)
            from tests.cloud_start_helpers import two_later_sessions

            await two_later_sessions(b, selected=6, original_starts=1)
