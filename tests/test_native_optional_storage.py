"""Optional early observation notes and required latched fault proof have distinct storage obligations."""

import sqlite3

import pytest

from tests.current_rig import selected_ready
from tests.managed_rig import healthy
from tests.native_peer_failures import InterruptedNativePeer
from tests.test_worker_storage_recovery import failing_cleanup
from tests.tls_support import tls as tls


# Intent: refusing the first two optional strict-measurement notes does not stop polling or discard failure counts.
# Why: optional notes must not escape tick, while the third fault's atomic diagnostic remains required storage.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("third_failure", [False, True])
async def test_optional_strict_diagnostic_does_not_replace_required_fault_storage(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, third_failure
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=InterruptedNativePeer
    ) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        original = peer.native

        # Only the actual independent meter changes; all native identity, resource and protocol checks still run.
        def excess_native(key):
            value = original(key)
            if key == "METER":
                measured = int(round((current + 0.6) * 10))
                value[0]["meterData"].update(cur=[measured, 0, 0], power=[0, 0, 0, 0])
            return value

        monkeypatch.setattr(peer, "native", excess_native)
        _, attempts = failing_cleanup(j, "managed_native_failure", minimum_attempts=1)
        try:
            for expected in (1, 2):
                assert (await c.tick())["state"] == "degraded"
                assert c.failures == expected and len(attempts) == expected
                assert not j.get("managed_control_fault")
            if third_failure:
                with pytest.raises(sqlite3.IntegrityError, match="independent cleanup storage unavailable"):
                    await c.tick()
                assert c.failures == 3 and not j.get("managed_control_fault")
                assert not j.db.in_transaction
            monkeypatch.setattr(peer, "native", original)
            assert (await healthy(c))["station_ceiling_a"] == current
            assert c.failures == 0 and not j.get("managed_control_fault")
        finally:
            monkeypatch.setattr(peer, "native", original)
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
