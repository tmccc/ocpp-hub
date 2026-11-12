"""History acknowledgement under the capability commissioned by the production preparation path."""

import copy
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current_prepare import prepare_current_capability
from ocpp_hub_core.prototype.native_history_recovery import acknowledge_history_policy
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_resources import ResourceBook

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_native_history_recovery import closed_records
from tests.test_prototype_tls import tls as tls


# Intent: a normally commissioned USER32 installation can reconcile later native history rollover.
# Why: native current commissioning does not require a prior loss of history or archive acknowledgement.
@pytest.mark.parametrize("native_limit", [6, 32])
@pytest.mark.parametrize("fault", ["none", "missing-capability", "mutation-debt", "changed-config"])
async def test_native32_history_acknowledgement(tmp_path, tls, unused_tcp_port, monkeypatch, native_limit, fault):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        rig.bench.history = closed_records()
        first = managed_session(rig)
        await first.start()
        controller = ManagedController(first)
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await introduced(rig, first, charger, "first-boot")
            await healthy(controller)
            controller.command(uuid.uuid4().hex, "pause", 0)
            await healthy(controller)
            original = copy.deepcopy(first.journal.get("native_bounded_handover")["original_history"])
            path, identity = first.journal.path, first.journal.identity
            await first.close()

        journal = Journal(path, identity)
        try:
            book = ResourceBook(journal, rig.prepared.baseline)

            # The simulated native device receives only the supported USER32 write.
            async def patch_user32():
                rig.peer.configuration["AmpLmt"] = "32"
                rig.peer.runtime_user = 320

            if native_limit == 32:
                result = await prepare_current_capability(
                    journal, book, rig.spec, rig.prepared.observer.observe_handover, patch_user32
                )
                assert result["prepared"]
            assert journal.get("native_history_policy") is None
        finally:
            journal.close()

        replacement = managed_session(rig)
        await replacement.start()
        controller = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "second-boot")
            await healthy(controller)
            rig.bench.history = copy.deepcopy(original[2:])
            for _ in range(4):
                status = await controller.tick()
            assert status["state"] == "attention"
            assert replacement.journal.get("managed_native_failure")["detail"] == (
                "native bounded handover must preserve every original history record"
            )
            assert rig.bench.firmware.effective_limit() == 0
            await replacement.close()

        journal = Journal(path, identity)
        try:
            if fault == "missing-capability":
                journal.db.execute("DELETE FROM meta WHERE key='managed_current_capability'")
            elif fault == "mutation-debt":
                journal.mutation_intent("vendor", "ChangeConfiguration", {"key": "Synthetic", "value": "pending"})
            elif fault == "changed-config":
                rig.peer.configuration["timeZone"] = "UTC"
            native = await rig.prepared.observer.observe_handover()
            assert native.saved_user_a == native_limit
            book = ResourceBook(journal, rig.prepared.baseline)
            if fault in ("mutation-debt", "changed-config") or fault == "missing-capability" and native_limit == 32:
                before = "\n".join(journal.db.iterdump())
                with pytest.raises((SecurityError, RecoveryRequiredError)):
                    acknowledge_history_policy(journal, book, rig.spec, native)
                assert "\n".join(journal.db.iterdump()) == before
            else:
                result = acknowledge_history_policy(journal, book, rig.spec, native)
                assert result["fault_cleared"] and not result["charging_started"]
                assert journal.get("control")["inhibit"]
        finally:
            journal.close()
