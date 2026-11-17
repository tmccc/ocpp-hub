"""Initial history and failed strict-diagnostic storage cannot be mistaken for readiness recovery."""

import asyncio
import copy
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.test_introduction_readiness_retry import no_synthetic_lifecycle
from tests.test_managed_introduction_retry import selected_ready
from tests.test_managed_proxy import managed_bench, managed_session
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import failing_cleanup

pytestmark = pytest.mark.asyncio


# Intent: first-Boot history beyond the reserved allocation is refused even when the output offer is not yet zero.
# Why: a readiness wait cannot hide an observed strict history contradiction that may later roll off the charger.
async def test_initial_history_refusal_precedes_closed_zero_readiness(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c, g, j = ManagedController(session), session.gateway, session.journal
        rig.route_to_trial()
        history = copy.deepcopy(rig.bench.history)
        rig.bench.history.append({"transactionId": 9000001, "origin": "independent-history-before-first-boot"})
        rig.peer.permission_enabled = True
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch, boot = g._charger_epoch, g.boot_observation
                await until(lambda: j.get("trial_phase") == "blocked", timeout=3)
                status = await c.tick()
                assert status["state"] == "attention" and not status["owner_control_enabled"]
                diagnostic = copy.deepcopy(j.get("managed_native_failure"))
                assert diagnostic["type"] == "SecurityError"
                assert diagnostic["detail"] == (
                    "native history changed beyond the reserved range before first introduction"
                )
                assert j.get("trial_binding") is None and j.get("trial_starting") is True
                assert c.native.introduced_epoch != epoch and not g.controls.ready
                assert rig.bench.device_calls == []
                rig.bench.history = history
                rig.peer.permission_enabled = False
                for _ in range(3):
                    assert (await c.tick())["state"] == "attention"
                assert j.get("managed_native_failure") == diagnostic
                assert g._charger_epoch == epoch and g.boot_observation == boot
                no_synthetic_lifecycle(rig, j)
        finally:
            rig.bench.history = history
            rig.peer.permission_enabled = False
            await c.charging.close()
            await session.close()


# Intent: actual strict-diagnostic SQL refusal retries only its retained latch after the original peer data returns.
# Why: storage recovery must not strand the Boot probe or erase a proved contradiction through a replacement read.
async def test_introduction_strict_latch_retries_after_real_sql_refusal(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        rig, c, g, j = b.rig, b.controller, b.session.gateway, b.session.journal
        await b.charger.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        original = rig.peer.configuration["untouched"]
        rig.peer.configuration["untouched"] = "independent-strict-conflict-before-storage-refusal"
        reached, attempts = failing_cleanup(j, "managed_native_failure", minimum_attempts=1)
        first_profile = len(rig.bench.received_limits)
        try:
            async with rig.charger(b.session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch, boot = g._charger_epoch, g.boot_observation
                await asyncio.wait_for(reached.wait(), 3)
                assert attempts and j.get("managed_control_fault") is None
                assert j.get("control")["inhibit"] is True and not g.controls.ready
                assert c.native.introduced_epoch != epoch
                requests = len(rig.peer.requests)
                rig.peer.configuration["untouched"] = original
                # Foreground supervision must honor the retained strict refusal while its durable latch is unwritable.
                for _ in range(3):
                    status = await c.tick()
                    assert not status["owner_control_enabled"] and c.native.introduced_epoch != epoch
                    assert all(limit == 0 for limit in rig.bench.received_limits[first_profile:])
                    no_synthetic_lifecycle(rig, j)
                # The first native refusal remains the cause while a real second SQLite statement is attempted.
                await until(lambda: len(attempts) >= 2, timeout=3)
                assert len(rig.peer.requests) == requests
                j.db.execute("DROP TRIGGER review_cleanup_unavailable")
                # A foreground poll publishes before the sleeping Boot probe resumes; that probe must stay terminal.
                status = await c.tick()
                assert status["state"] == "attention" and not status["owner_control_enabled"]
                await asyncio.sleep(b.session.runtime.settings.retry_max + 0.1)
                assert j.get("managed_control_fault")["classification"] == "native_authority_or_preservation_failed"
                diagnostic = j.get("managed_native_failure")
                assert diagnostic["type"] == "SecurityError"
                assert diagnostic["detail"] == "native bounded configuration or saved schedule changed"
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert c.native.introduced_epoch != epoch and len(rig.peer.requests) == requests
                assert all(limit == 0 for limit in rig.bench.received_limits[first_profile:])
                no_synthetic_lifecycle(rig, j)
        finally:
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
            rig.peer.configuration["untouched"] = original
