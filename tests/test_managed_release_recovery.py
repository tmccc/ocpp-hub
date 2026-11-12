"""Explicit owner reconciliation after a real positive release ACK is lost or late."""

import asyncio
import copy
import sqlite3
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.managed_release_recovery import (
    ACK,
    reconcile_owner_release,
)
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_prototype import BOOT
from tests.test_prototype_tls import tls as tls


# Exercise the already established synthetic native32 commissioning fixture without changing engine decisions.
@asynccontextmanager
async def release_bench(tmp_path, tls, port, monkeypatch, native_limit):
    async with managed_bench(tmp_path, tls, port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "release-recovery-boot")
            await healthy(c)
            if native_limit == 32:
                session.journal.put("managed_current_capability", capability_value(session.journal, rig.spec))
                session.gateway.policy = replace(session.gateway.policy, max_current_a=32)
                session.gateway.controls.policy = session.gateway.policy
                rig.peer.configuration["AmpLmt"] = "32"
                rig.peer.runtime_user = 320
                await healthy(c)
                c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=32)
                await c.current.settle()
                await healthy(c)
            yield SimpleNamespace(rig=rig, session=session, c=c, charger=charger)


# Lose only one actual fake-device TLS reply, keeping firmware application and later rollback handling independent.
async def fail_one_return(b, monkeypatch, mode):
    c, rig = b.c, b.rig
    c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
    await healthy(c)
    ordinary_send = b.charger.send
    seen, delayed = [], []

    # A six-second late ACK must not block the fake receiver from applying the subsequent zero write.
    async def late_reply(raw):
        await asyncio.sleep(6)
        await ordinary_send(raw)

    # Intercept only the response to the first physically applied positive station-profile request.
    async def send(raw):
        response = decode(raw)
        call = next((f for f in rig.bench.device_calls if f[1] == response[1]), None)
        if not seen and response[0] == 3 and call is not None and call[2] == "SetChargingProfile":
            limit = call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
            if limit > 0:
                seen.append(rig.bench.firmware.effective_limit())
                if mode == "late":
                    delayed.append(asyncio.create_task(late_reply(raw)))
                return
        await ordinary_send(raw)

    monkeypatch.setattr(b.charger, "send", send)
    c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
    for _ in range(3):
        await c.tick()
        if c.session.journal.get("managed_control_fault"):
            break
    if delayed:
        await asyncio.gather(*delayed)
    assert seen == [c.current.selected()]
    assert c.session.journal.get("managed_control_fault")["classification"] == "owner_release_unconfirmed"
    for _ in range(3):
        assert (await c.tick())["state"] == "attention"
    assert rig.bench.firmware.effective_limit() == 0
    return copy.deepcopy(c.session.journal.get("managed_control_fault"))


# A real new Pause must record its acknowledgement through the production command transaction.
async def explicit_release_pause(b):
    c = b.c
    identifier = uuid.uuid4().hex
    c.command(identifier, "pause", c.requests.intent().revision)
    assert c.session.journal.get(ACK)["pause_request"] == identifier
    if c.pause_task is not None:
        await c.pause_task
    await c.hold_fault()
    return identifier


# Drive the production fault supervisor after a new Pause; Return remains a separate positive owner action.
async def recover_then_return(b):
    c, rig, j = b.c, b.rig, b.c.session.journal
    originals = copy.deepcopy(rig.peer.configuration), copy.deepcopy(rig.bench.firmware.plans)
    await explicit_release_pause(b)
    positives = sum(limit > 0 for limit in rig.bench.received_limits)
    for _ in range(10):
        await c.tick()
        assert rig.bench.firmware.effective_limit() == 0
        assert sum(limit > 0 for limit in rig.bench.received_limits) == positives
        if j.get("managed_control_fault") is None:
            break
    assert j.get("managed_control_fault") is None
    assert c.requests.intent().mode == "paused" and rig.bench.firmware.effective_limit() == 0
    assert c.native.introduced_revision == c.session.gateway.controls._revision
    status = await healthy(c)
    assert status["station_ceiling_a"] == 0 and not status["owner_control_enabled"]
    c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
    await healthy(c)
    assert rig.bench.firmware.effective_limit() == c.current.selected()
    assert (rig.peer.configuration, rig.bench.firmware.plans) == originals


# Intent: one explicit new Pause reconciles lost/late release ACKs at both native ceilings, including replacement.
# Why: retrying polls, a late ACK or fresh Boot alone must not become positive authority or clear an old fault.
@pytest.mark.parametrize("native_limit", [6, 32])
@pytest.mark.parametrize("mode", ["lost", "late"])
@pytest.mark.parametrize("restart", [False, True], ids=["same-socket", "fresh-boot"])
async def test_release_reconciliation_requires_pause_then_return(
    tmp_path, tls, unused_tcp_port, monkeypatch, native_limit, mode, restart
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, native_limit) as b:
        fault = await fail_one_return(b, monkeypatch, mode)
        assert not await reconcile_owner_release(b.c)
        assert b.session.journal.get("managed_control_fault") == fault
        if not restart:
            # This assertion specifically covers fault().invalidate() leaving the old introduction revision behind.
            assert b.c.native.introduced_revision != b.session.gateway.controls._revision
            await recover_then_return(b)
        else:
            await crash(b.session)
            replacement = managed_session(b.rig)
            await replacement.start()
            c = ManagedController(replacement)
            async with b.rig.charger(replacement) as charger:
                await introduced(b.rig, replacement, charger, "release-recovery-replacement")
                replacement_bench = SimpleNamespace(rig=b.rig, session=replacement, c=c, charger=charger)
                assert (await c.tick())["state"] == "attention"
                assert replacement.journal.get("managed_control_fault") == fault
                assert not await reconcile_owner_release(c)
                await recover_then_return(replacement_bench)


# Intent: recovering one release fault cannot acknowledge a different fault, vendor write, Start, queue or timer duty.
# Why: independent zero alone cannot erase unrelated command/lifecycle uncertainty or an archived cutoff obligation.
@pytest.mark.parametrize(
    "blocker",
    [
        "different-fault",
        "vendor-mutation",
        "other-mutation",
        "uncertain-vendor",
        "pending-start",
        "changed-config",
        "archived-cutoff",
        "positive-native-profile",
    ],
)
async def test_release_reconciliation_preserves_unrelated_uncertainty(
    tmp_path, tls, unused_tcp_port, monkeypatch, blocker
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        await fail_one_return(b, monkeypatch, "lost")
        await explicit_release_pause(b)
        j = b.session.journal
        if blocker == "different-fault":
            j.put("managed_control_fault", {"classification": "native_authority_or_preservation_failed"})
        elif blocker in ("vendor-mutation", "other-mutation"):
            j.mutation_intent("vendor" if blocker == "vendor-mutation" else "local", "ChangeConfiguration", {})
        elif blocker == "uncertain-vendor":
            j.put("uncertain_vendor_command", {"action": "SetChargingProfile", "digest": "f" * 64})
        elif blocker == "pending-start":
            j.put("vendor_start_grant", {"digest": "a" * 64, "token": "b" * 32})
        elif blocker == "changed-config":
            b.rig.peer.configuration["untouched"] = "external-change"
        elif blocker == "archived-cutoff":
            j.put(
                "managed_temporary_occurrences",
                {
                    "old": {
                        "id": "old",
                        "state": "complete",
                        "result": "no_scheduled_session",
                        "idle_cutoff_hold": True,
                        "start_epoch": 1,
                    }
                },
            )
        else:
            b.rig.bench.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 6
        before = copy.deepcopy(j.get("managed_control_fault"))
        mutations = list(j.db.execute("SELECT * FROM mutations"))
        try:
            await reconcile_owner_release(b.c)
        except RecoveryRequiredError, SecurityError, UnavailableError:
            pass
        assert j.get("managed_control_fault") == before
        assert list(j.db.execute("SELECT * FROM mutations")) == mutations


# Intent: accepting a reconciliation Pause is atomic, and retrying its request ID cannot create new authority.
# Why: failed receipt insertion cannot leave acknowledgement or a new control ticket behind to clear the fault.
async def test_release_pause_acknowledgement_is_atomic_and_idempotent(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        await fail_one_return(b, monkeypatch, "lost")
        j, c = b.session.journal, b.c
        before = c.requests.intent(), copy.deepcopy(j.get("control")), copy.deepcopy(j.get(ACK))
        calls = len(b.rig.bench.device_calls)
        failed = uuid.uuid4().hex
        j.db.execute(
            "CREATE TRIGGER refuse_pause BEFORE INSERT ON managed_requests "
            "BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END"
        )
        try:
            with pytest.raises(sqlite3.Error):
                c.command(failed, "pause", c.requests.intent().revision)
        finally:
            j.db.execute("DROP TRIGGER refuse_pause")
        assert (c.requests.intent(), j.get("control"), j.get(ACK)) == before
        assert c.requests.receipt(failed)["state"] == "not_found"
        assert len(b.rig.bench.device_calls) == calls
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        first = c.command(identifier, "pause", revision)
        recorded = copy.deepcopy(j.get(ACK))
        new_revision = c.requests.intent().revision
        duplicate = c.command(identifier, "pause", revision)
        assert first == duplicate and j.get(ACK) == recorded
        assert c.requests.intent().revision == new_revision
        if c.pause_task is not None:
            await c.pause_task


# Intent: the acknowledgement helper itself performs only native reads before recording successful recovery.
# Why: proving zero must never be a hidden charger write, Stop or positive release.
async def test_release_reconciliation_helper_has_no_charger_rpc(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        await fail_one_return(b, monkeypatch, "lost")
        await explicit_release_pause(b)
        calls = len(b.rig.bench.device_calls)
        assert await reconcile_owner_release(b.c)
        assert b.rig.bench.device_calls[calls:] == []
        assert b.rig.bench.firmware.effective_limit() == 0


# Intent: new Pause, replacement fault or an actual fresh Boot during native HTTP proof supersedes the old attempt.
# Why: no pre-read fault/intent/Boot snapshot can authorize a post-await deletion of newer durable authority.
@pytest.mark.parametrize("crossed", ["new-pause", "new-fault", "fresh-boot"])
async def test_release_reconciliation_rechecks_authority_after_http(
    tmp_path, tls, unused_tcp_port, monkeypatch, crossed
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        await fail_one_return(b, monkeypatch, "lost")
        await explicit_release_pause(b)
        j = b.session.journal
        original_fault = copy.deepcopy(j.get("managed_control_fault"))
        original_revision = b.c.requests.intent().revision
        ordinary_native = b.rig.peer.native
        changed = []
        tasks = []

        # Mutate through a public owner command or genuine charger Boot while the native socket read is in flight.
        async def cross_authority():
            if crossed == "new-pause":
                b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
            elif crossed == "new-fault":
                j.put("managed_control_fault", {"classification": "other_fault", "at_epoch": 123})
            else:
                await b.rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT)
            changed.append(True)

        # The independent HTTP peer observes the request and schedules the competing authority on the real loop.
        def native(key):
            value = ordinary_native(key)
            if not tasks:
                tasks.append(asyncio.create_task(cross_authority()))
            return value

        monkeypatch.setattr(b.rig.peer, "native", native)
        try:
            await reconcile_owner_release(b.c)
        except RecoveryRequiredError, SecurityError, UnavailableError:
            pass
        await asyncio.gather(*tasks)
        if b.c.pause_task is not None:
            await asyncio.gather(b.c.pause_task, return_exceptions=True)
        assert changed
        if crossed == "new-fault":
            assert j.get("managed_control_fault")["classification"] == "other_fault"
        else:
            assert j.get("managed_control_fault") == original_fault
        if crossed == "new-pause":
            assert b.c.requests.intent().revision > original_revision
        assert b.rig.bench.firmware.effective_limit() == 0
