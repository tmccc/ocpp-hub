"""Connected native6 admission uses independent HTTP evidence and both actual TLS protocol legs."""

from __future__ import annotations

import asyncio
import copy
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec, capture_native_bounded_baseline
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_evidence import NativeObserver
from ocpp_hub_core.prototype.trial_session import PreparedTrial, TrialSession, preflight, save_private
from websockets.asyncio.server import serve

from tests.handover_helpers import ORIGINAL, NativeVehiclePeer
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tests.trial_helpers import config_files


@dataclass
class ConnectedBench:
    """The native peer owns physical state; the engine never supplies its observation outcomes."""

    bench: Bench
    peer: NativeVehiclePeer
    prepared: PreparedTrial
    spec: NativeBoundedSpec
    sessions: list[TrialSession] = field(default_factory=list)

    # Construct the ordinary session unless this one explicitly supplies the separate native6 contract.
    def session(self, bounded: bool = True) -> TrialSession:
        session = TrialSession(self.prepared, FAST, native_bounded=self.spec if bounded else None)
        self.sessions.append(session)
        return session

    # Model the explicit backend switch only after the listener has admitted the original route under USER6.
    def route_to_trial(self) -> None:
        self.peer.configuration.update(self.spec.trial_route)

    # Answer real proxy commands independently while device CALLs and their replies use the encrypted socket.
    @asynccontextmanager
    async def charger(self, session: TrialSession) -> AsyncIterator[Any]:
        async with self.bench.connection_to(session) as charger:
            reader = asyncio.create_task(self.bench.read_charger(charger))
            try:
                yield charger
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)


# Capture the original connected closed baseline over real HTTP before changing the fake device's USER and route.
@asynccontextmanager
async def connected_bench(
    directory: Path, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path], listener_port: int
) -> AsyncIterator[ConnectedBench]:
    bench = Bench()
    bench.cp, bench.connection = "B", "Plug"
    bench.history = [{"transactionId": 9000000, "original_record": {"preserve": [1, 2, 3]}}]
    bench.firmware.plans[9] = {"planId": 9, "limitCurrent": 32, "original": "preserve"}
    peer = NativeVehiclePeer(bench)
    original = copy.deepcopy(ORIGINAL) | {"connectorNum": "1"}
    peer.configuration = copy.deepcopy(original)
    native = await asyncio.start_server(peer.handle, "127.0.0.1", 0)
    rig: ConnectedBench | None = None
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=tls_contexts[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            config, path = config_files(directory, listener_port, cloud.sockets[0].getsockname()[1])
            observer = NativeObserver(config, native.sockets[0].getsockname()[1])
            snapshot = await observer.observe_handover()
            baseline = capture_native_bounded_baseline(snapshot, config.identity, config.hardware_identity)
            save_private(config.baseline, baseline.value())
            save_private(config.baseline.with_suffix(".original-config.json"), original)
            route = {
                "sDomain": "127.0.0.1",
                "sPort": str(config.listener_port),
                "sURL": config.charger_path,
                "allowSelfSign": "1",
            }
            spec = NativeBoundedSpec(
                config.hardware_identity,
                original,
                route,
                snapshot.saved_schedule_digest,
                fingerprint("synthetic-native6-reboot-proof"),
                fingerprint(BOOT),
            )
            peer.configuration = original | {"AmpLmt": "6"}
            peer.runtime_user = 60
            prepared = preflight(path)
            prepared.observer.port = native.sockets[0].getsockname()[1]
            rig = ConnectedBench(bench, peer, prepared, spec)
            yield rig
    finally:
        if rig is not None:
            for session in rig.sessions:
                await session.close()
        native.close()
        await native.wait_closed()


# Intent: a lead connected to a full or idle car cannot opt into native6 admission through ordinary startup.
# Why: the explicit contract must not weaken the established unplugged commissioning path.
async def test_connected_admission_requires_explicit_native6_spec(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = rig.session(bounded=False)
        with pytest.raises(SecurityError):
            await session.start()
        assert session.runtime._server is None
        assert rig.bench.device_calls == [] and rig.bench.calls == []
        assert session.journal.get("trial_phase") == "prepared"


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
            await until(lambda: session.runtime.status.last_failure == "BootOrCeilingUnavailable")
            assert not session.gateway.controls.ready
            assert session.journal.get("trial_phase") == "blocked"
            assert rig.bench.received_limits == [] and not rig.bench.firmware.profiles
            assert session.journal.active() is None
            assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
            assert session.journal.get("next_transaction") == 9000000
