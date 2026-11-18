"""Connected native6 admission uses independent HTTP evidence and both actual TLS protocol legs."""

from __future__ import annotations

import copy
import ssl
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.security import SecurityError

from tests.connected_rig import connected_bench
from tests.prototype_rig import BOOT
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: a lead connected to a full or idle car cannot opt into native6 admission through ordinary startup.
# Why: the explicit contract must not weaken the established unplugged commissioning path.
async def test_connected_admission_requires_explicit_native6_spec(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        with pytest.raises(SecurityError, match="native bounded specification"):
            rig.session(bounded=False)
        assert not rig.sessions
        assert rig.bench.device_calls == [] and rig.bench.calls == []
        assert not rig.prepared.config.journal.exists()


# Intent: independent USER6 evidence permits closed connected admission, but only genuine Boot establishes readiness.
# Why: old cloud history reserves numbers without being adopted as proxy lifecycle facts or synthetic delivery ACKs.
async def test_connected_native6_boot_binds_without_adopting_old_history(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        old_history = copy.deepcopy(rig.bench.history)
        session = rig.session()
        await session.start()
        assert session.journal.get("trial_starting")
        assert session.journal.get("trial_binding") is None
        assert not session.gateway.controls.ready and rig.bench.device_calls == []
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            reply = await rig.bench.call(charger, "connected-boot", "BootNotification", BOOT)
            assert reply[0] == 3
            await until(lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted")
            assert session.gateway.current_boot_seen(session.gateway._charger_epoch)
            assert session.journal.get("trial_binding") is not None
            assert not session.journal.get("trial_starting")
            assert rig.bench.firmware.effective_limit() == 0
            await session.command({"command": "release"})
            assert rig.bench.firmware.effective_limit() == 6
            assert rig.peer.configuration["AmpLmt"] == "6" and rig.peer.runtime_user == 60
            assert rig.bench.history == old_history
            assert session.journal.active() is None
            assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
            assert session.journal.get("next_transaction") == 9000000
            assert not any(f[0] == 2 and f[2] in ("StartTransaction", "StopTransaction") for f in rig.bench.calls)
            await session.close()
            assert rig.bench.firmware.effective_limit() == 0
            assert rig.peer.runtime_user == 60 and rig.peer.configuration["AmpLmt"] == "6"
            assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)


# Intent: a replacement owner before the first Boot reuses the durable allocation and introduces at zero.
# Why: an admitted but unbound journal has no owned profile to recover and must not require an invented binding.
async def test_replacement_before_first_boot_preserves_allocation_and_zero_hold(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        first = rig.session()
        await first.start()
        allocation = copy.deepcopy(first.journal.get("trial_allocation"))
        old_history = copy.deepcopy(rig.bench.history)
        assert first.journal.get("trial_binding") is None
        assert first.journal.get("next_transaction") == 9000000
        rig.route_to_trial()
        await first.close()
        assert not rig.bench.firmware.profiles
        replacement = rig.session()
        await replacement.start()
        assert replacement.journal.get("trial_phase") == "active"
        assert replacement.journal.get("trial_starting")
        assert replacement.journal.get("trial_binding") is None
        assert replacement.journal.get("trial_allocation") == allocation
        assert replacement.journal.get("next_transaction") == 9000000
        async with rig.charger(replacement) as charger:
            await rig.bench.call(charger, "replacement-first-boot", "BootNotification", BOOT)
            await until(
                lambda: replacement.gateway.controls.ready and replacement.gateway.vendor_registration == "Accepted"
            )
            assert replacement.gateway.current_boot_seen(replacement.gateway._charger_epoch)
            assert replacement.journal.get("trial_binding") is not None
            assert not replacement.journal.get("trial_starting")
            assert replacement.journal.get("trial_allocation") == allocation
            assert replacement.journal.get("next_transaction") == 9000000
            assert replacement.journal.active() is None
            assert replacement.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
            assert rig.bench.received_limits and all(limit == 0 for limit in rig.bench.received_limits)
            assert rig.bench.firmware.effective_limit() == 0
            assert rig.bench.history == old_history
            await replacement.close()


# Intent: saved and effective USER, installer config, closed session and complete history are all admission inputs.
# Why: zero power or a saved6 setting alone cannot establish an independent current ceiling and clean lifecycle.
@pytest.mark.parametrize("fault", ["saved-user", "runtime-user", "installer", "open-session", "history", "schedule"])
async def test_bad_native6_admission_never_binds_or_creates_lifecycle(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, fault: str
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        if fault == "saved-user":
            rig.peer.configuration["AmpLmt"] = "32"
        elif fault == "runtime-user":
            rig.peer.runtime_user = 320
        elif fault == "installer":
            rig.peer.configuration["maxOutCurrent0"] = "31"
        elif fault == "open-session":
            rig.bench.firmware.active = 9000000
        elif fault == "history":
            rig.bench.history.append(copy.deepcopy(rig.bench.history[0]))
        else:
            rig.bench.firmware.plans[9]["limitCurrent"] = 6
        session = rig.session()
        with pytest.raises(SecurityError):
            await session.start()
        assert session.runtime._server is None and rig.bench.device_calls == []
        assert session.journal.active() is None
        assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
        assert session.journal.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 0


# Intent: a correct pre-bind reading cannot authorise the first profile after native USER or history changes.
# Why: the independent ceiling and fresh allocator boundary must survive the actual backend handoff and new Boot.
@pytest.mark.parametrize("fault", ["runtime-user", "new-history", "open-session", "configuration"])
async def test_connected_first_boot_rechecks_native_guard_and_history(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, fault: str
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = rig.session()
        await session.start()
        rig.route_to_trial()
        if fault == "runtime-user":
            rig.peer.runtime_user = 320
        elif fault == "new-history":
            rig.bench.history.append({"transactionId": 9000001, "origin": "original-backend"})
        elif fault == "open-session":
            rig.bench.firmware.active = 9000000
        else:
            rig.peer.configuration["untouched"] = "external-change"
        async with rig.charger(session) as charger:
            await rig.bench.call(charger, "raced-boot", "BootNotification", BOOT)
            await until(lambda: session.runtime.status.charger_fault.stage == "boot_or_ceiling")
            assert not session.gateway.controls.ready
            assert session.journal.get("trial_phase") == ("active" if fault == "open-session" else "blocked")
            assert rig.bench.received_limits == [] and not rig.bench.firmware.profiles
            assert session.journal.active() is None
            assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
            assert session.journal.get("next_transaction") == 9000000
            if fault == "open-session":
                epoch, boot = session.gateway._charger_epoch, session.gateway.boot_observation
                assert session.journal.get("trial_binding") is None and session.journal.get("trial_starting") is True
                rig.bench.firmware.active = None
                await until(lambda: session.gateway.controls.ready)
                assert session.gateway._charger_epoch == epoch and session.gateway.boot_observation == boot
                assert session.journal.active() is None
                assert set(rig.bench.received_limits) == {0}
