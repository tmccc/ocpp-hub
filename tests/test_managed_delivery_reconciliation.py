"""One cloud-accepted Start is reconciled without replay, release or a guessed transaction identity."""

import asyncio
import json
import sqlite3
import sys
import time
import uuid
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.managed_delivery_recovery import KEY
from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tools import reconcile_managed_delivery as operator


class CloudReplyFilter:
    """The independent cloud allocates an ID normally and loses one real Start result on its transport."""

    # Keep independent backend observations separate from the gateway's durable state.
    def __init__(self, connection, trace):
        self.connection, self.trace = connection, trace

    # Retain real request bytes while observing their opaque upstream correlation IDs.
    def __aiter__(self):
        # This iterator adds no engine wait or altered peer request.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2:
                    self.trace["calls"].append(frame)
                yield raw

        return frames()

    # Lose only the first cloud Start acknowledgement, after its actual independent allocation.
    async def send(self, raw):
        frame = json.loads(raw)
        start_ids = [f[1] for f in self.trace["calls"] if f[2] == "StartTransaction"]
        if frame[0] == 3 and frame[1] in start_ids:
            self.trace["vendor"] = frame[2]["transactionId"]
            if not self.trace["dropped"]:
                self.trace["dropped"] = True
                return
        stop_ids = [f[1] for f in self.trace["calls"] if f[2] == "StopTransaction"]
        if frame[0] == 3 and frame[1] in stop_ids and self.trace.get("hold_stop"):
            self.trace["stop_seen"].set()
            await self.trace["release_stop"].wait()
        await self.connection.send(raw)


# Intent: native6 and commissioned native32 recover only after exact operator evidence, genuine Stop ACK and new Pause.
# Why: zero/closure alone cannot prove cloud identity, and neither mapping nor restart may silently resume charging.
@pytest.mark.parametrize("ceiling", [6, 32])
@pytest.mark.parametrize("local_owner", [False, True])
async def test_operator_delivery_recovery_retains_zero_until_separate_return(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, ceiling, local_owner
):
    trace = {
        "calls": [],
        "dropped": False,
        "hold_stop": ceiling == 6,
        "stop_seen": asyncio.Event(),
        "release_stop": asyncio.Event(),
    }
    actual_cloud = Bench.vendor

    # The fake cloud's normal transaction implementation remains responsible for allocation and other replies.
    async def cloud_peer(bench, connection):
        await actual_cloud(bench, CloudReplyFilter(connection, trace))

    monkeypatch.setattr(Bench, "vendor", cloud_peer)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        value = rig.spec.value()
        value["trial_route"].update(sDomain=rig.prepared.config.listener_name, sURL="/charger/")
        rig.spec = NativeBoundedSpec.parse(value)
        strategy = tmp_path / "native-spec.json"
        save_private(strategy, value)
        config = tmp_path / "trial.json"
        native_port = rig.prepared.observer.port
        requests_before_check = list(rig.peer.requests)
        assert await operator.run("check", config, strategy, None, native_port) == 0
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            operator.__file__,
            "check",
            "--config",
            str(config),
            "--native-bounded-spec",
            str(strategy),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await child.communicate()
        assert child.returncode == 0 and not stderr and json.loads(stdout)["inputs_valid"]
        assert not rig.prepared.config.journal.exists() and rig.peer.requests == requests_before_check
        if local_owner:
            rig.prepared.config = replace(rig.prepared.config, local_tags=frozenset({"APP_OWNER", "NATIVE_TIMER"}))
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "delivery-owner-boot")
            await healthy(c)
            selected = 6 if ceiling == 6 else 16
            if ceiling == 32:
                j = session.journal
                j.put("managed_current_capability", capability_value(j, rig.spec))
                session.gateway.policy = replace(session.gateway.policy, max_current_a=32)
                session.gateway.controls.policy = session.gateway.policy
                rig.peer.configuration["AmpLmt"], rig.peer.runtime_user = "32", 320
                await healthy(c)
                c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=selected)
                await c.current.settle()
                await healthy(c)
            if local_owner:
                c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
                await c.tests.settle()
                tag = starts(rig)[-1][3]["idTag"]
            else:
                assert (
                    await rig.bench.cloud_call(
                        "owner-app-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
                    )
                )[2]["status"] == "Accepted"
                tag = "CLOUD_OWNER"
            opened = await rig.bench.call(charger, "owner-report", "StartTransaction", START | {"idTag": tag})
            local = opened[2]["transactionId"]
            if local_owner:
                rig.peer.plan_session = (local, 987654)
            rig.peer.paused, rig.peer.permission_enabled = False, True
            rig.bench.cp, rig.bench.connection = "C", "Charging"
            rig.bench.firmware.fire_plan(9)
            assert rig.bench.firmware.current == selected
            if local_owner:
                await healthy(c)
                assert c.tests.state()["state"] == "running"
            await until(lambda: session.runtime.status.delivery_blocked, timeout=12)
            for _ in range(3):
                await c.tick()
            assert (
                session.journal.get("managed_control_fault")["classification"] == "owner_control_or_delivery_uncertain"
            )
            assert rig.bench.firmware.effective_limit() == 0
            old_pause, old_revision = uuid.uuid4().hex, c.requests.intent().revision
            c.command(old_pause, "pause", old_revision)
            await asyncio.wait_for(rig.bench.remote_stop.wait(), 5)
            await c.pause_task
            assert (
                await rig.bench.call(
                    charger,
                    "real-stop",
                    "StopTransaction",
                    {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
                )
            )[0] == 3
            rig.peer.permission_enabled = False
            for _ in range(2):
                await c.tick()
            with pytest.raises(RecoveryRequiredError):
                await operator.run("inspect", config, strategy, None, native_port)
            counter, binding = session.journal.get("next_transaction"), session.journal.get("trial_binding")
            fault = session.journal.get("managed_control_fault")
            await session.close()
        if local_owner and ceiling == 6:
            journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            original = journal.get("managed_test_charge")
            journal.close()
            for invalid in ({"kind": "test"}, {"state": "starting"}, {"id": "wrong"}, {"local_transaction": local + 1}):
                journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
                journal.put("managed_test_charge", original | invalid)
                journal.close()
                with pytest.raises(RecoveryRequiredError):
                    await operator.run("inspect", config, strategy, None, native_port)
            journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            journal.put("managed_test_charge", original)
            journal.close()
        assert await operator.run("inspect", config, strategy, None, native_port) == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["local"] == local and candidate["start_token"] == next(
            f[1] for f in trace["calls"] if f[2] == "StartTransaction"
        )
        evidence = candidate | {
            "vendor": trace["vendor"],
            "evidence_sha256": fingerprint(trace["calls"]),
            "verified_at_epoch": time.time(),
        }
        evidence_path = tmp_path / "cloud-attestation.json"
        save_private(evidence_path, evidence)
        if ceiling == 6:
            for invalid in (
                {"vendor": True},
                {"vendor": -1},
                {"local": local + 1},
                {"identity_digest": "0" * 64},
                {"spec_digest": "0" * 64},
                {"start_digest": "0" * 64},
                {"stop_digest": "0" * 64},
                {"start_token": "e-" + "0" * 32},
                {"evidence_sha256": "invalid"},
                {"verified_at_epoch": time.time() + 60},
                {"verified_at_epoch": time.time() - 7200},
            ):
                save_private(evidence_path, evidence | invalid)
                with pytest.raises((SecurityError, RecoveryRequiredError)):
                    await operator.run("reconcile-start", config, strategy, evidence_path, native_port)
                journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
                try:
                    assert journal.vendor_id(local) is None and journal.next_delivery().state == "ambiguous"
                    assert journal.get(KEY) is None and journal.get("next_transaction") == counter
                finally:
                    journal.close()
            save_private(evidence_path, evidence)
            original_user = rig.peer.runtime_user
            rig.peer.runtime_user = 320
            try:
                with pytest.raises(SecurityError):
                    await operator.run("reconcile-start", config, strategy, evidence_path, native_port)
            finally:
                rig.peer.runtime_user = original_user
            journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            try:
                journal.db.execute(
                    "CREATE TRIGGER refuse_delivery_audit BEFORE INSERT ON meta "
                    "WHEN NEW.key='managed_delivery_reconciliations' "
                    "BEGIN SELECT RAISE(ABORT,'audit storage fault'); END"
                )
            finally:
                journal.close()
            with pytest.raises(sqlite3.IntegrityError, match="audit storage fault"):
                await operator.run("reconcile-start", config, strategy, evidence_path, native_port)
            journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            try:
                assert journal.vendor_id(local) is None and journal.next_delivery().state == "ambiguous"
                assert journal.get(KEY) is None
                journal.db.execute("DROP TRIGGER refuse_delivery_audit")
            finally:
                journal.close()
        assert await operator.run("reconcile-start", config, strategy, evidence_path, native_port) == 0
        if local_owner:
            journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            assert journal.get("managed_test_charge")["state"] == "completed"
            assert journal.get(KEY)[str(local)]["proof"]["prior_closed_normal_start"]["state"] == "running"
            journal.close()
        save_private(evidence_path, evidence | {"evidence_sha256": "0" * 64})
        with pytest.raises(RecoveryRequiredError, match="cannot be replaced"):
            await operator.run("reconcile-start", config, strategy, evidence_path, native_port)
        save_private(evidence_path, evidence)
        assert await operator.run("reconcile-start", config, strategy, evidence_path, native_port) == 0
        replacement = managed_session(rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "delivery-replacement-boot")
            j = replacement.journal
            if ceiling == 6:
                await asyncio.wait_for(trace["stop_seen"].wait(), 3)
                before_ack = uuid.uuid4().hex
                before_revision = recovered.requests.intent().revision
                recovered.command(before_ack, "pause", before_revision)
                await recovered.pause_task
                await recovered.tick()
                assert j.get("managed_control_fault") == fault and j.get(KEY)[str(local)].get("completed") is None
                assert j.next_delivery().action == "StopTransaction" and rig.bench.firmware.effective_limit() == 0
                trace["release_stop"].set()
            await until(lambda: j.next_delivery() is None)
            if ceiling == 6:
                recovered.command(before_ack, "pause", before_revision)
                await recovered.tick()
                assert j.get("managed_control_fault") == fault and j.get(KEY)[str(local)].get("completed") is None
            assert j.get("managed_control_fault") == fault
            assert (await recovered.tick())["state"] == "attention"
            assert recovered.command(old_pause, "pause", old_revision)["state"] == (
                "failed" if ceiling == 6 else "accepted"
            )
            assert j.get("managed_control_fault") == fault and rig.bench.firmware.effective_limit() == 0
            new_pause = uuid.uuid4().hex
            recovered.command(new_pause, "pause", recovered.requests.intent().revision)
            await recovered.pause_task
            status = await healthy(recovered)
            assert status["local_hold_verified"] and rig.bench.firmware.effective_limit() == 0
            assert not j.get("managed_control_fault") and j.get(KEY)[str(local)]["completed"]["pause"] == new_pause
            assert j.get("next_transaction") == counter and j.get("trial_binding") == binding
            assert recovered.current.selected() == selected
            returning = uuid.uuid4().hex
            recovered.command(returning, "app_control", recovered.requests.intent().revision)
            assert (await healthy(recovered))["owner_control_enabled"]
            assert recovered.requests.receipt(returning)["state"] == "completed"
            assert rig.bench.firmware.effective_limit() == selected
            starts_seen = [f for f in trace["calls"] if f[2] == "StartTransaction"]
            stops_seen = [f for f in trace["calls"] if f[2] == "StopTransaction"]
            assert len(starts_seen) == len(stops_seen) == 1
            assert stops_seen[0][3]["transactionId"] == trace["vendor"] and len(starts(rig)) == 1
            await replacement.close()
