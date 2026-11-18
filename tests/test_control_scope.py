"""Exact control identity is rebuilt from the real connection, without turning absence into permission."""

from dataclasses import replace

from ocpp_hub_core.prototype.connection_scope import control_scope_matches, current_control_scope
from ocpp_hub_core.prototype.managed import ManagedController

from tests.managed_rig import healthy, introduced, managed_bench, managed_session
from tests.prototype_rig import BOOT
from tests.tls_support import tls as tls


# Intent: all identity components must agree, and a new real Boot must invalidate an earlier captured identity.
# Why: shared checks must neither accept two missing observations nor reuse pre-await permission after replacement.
async def test_exact_control_scope_requires_current_complete_observation(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = managed_session(rig)
        await session.start()
        c, g = ManagedController(session), session.gateway
        assert current_control_scope(g) is None and not control_scope_matches(g, None)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            assert current_control_scope(g) is None and not control_scope_matches(g, None)
            await introduced(rig, session, charger, "identity-first-boot")
            await healthy(c)
            held = current_control_scope(g)
            assert held is not None and control_scope_matches(g, held)
            assert not control_scope_matches(g, None)
            scope, revision = held
            for changed in (
                (replace(scope, epoch=scope.epoch + 1), revision),
                (replace(scope, boot="another-boot"), revision),
                (replace(scope, metadata_revision=scope.metadata_revision + 1), revision),
                (scope, revision + 1),
            ):
                assert not control_scope_matches(g, changed), changed
            assert (await rig.bench.call(charger, "identity-new-boot", "BootNotification", BOOT))[0] == 3
            assert not control_scope_matches(g, held)
            current = current_control_scope(g)
            assert current is not None and control_scope_matches(g, current)
