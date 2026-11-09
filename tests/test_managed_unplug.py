"""Normal lead removal stays healthy without weakening initial admission or closed-zero native authority."""

from __future__ import annotations

import copy
import ssl
from dataclasses import replace
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.trial_handover_evidence import HandoverEvidence
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_native_bounded import IDENTITY, native, spec
from tests.test_prototype_tls import tls as tls


# Model the actual idle native pilot and connection pair while retaining independent measurements and history.
def unplugged(route: str = "trial", user: int = 6) -> HandoverEvidence:
    evidence = native(route, user)
    return replace(evidence, physical=replace(evidence.physical, cp="A", connection="Unplug"))


# Intent: only the explicit managed trial option accepts a closed, fully zero unplugged station under native6.
# Why: normal cable removal must not weaken connected initial admission or original32 restoration.
def test_unplugged_option_is_narrow_and_defaults_to_strict_connected_contract() -> None:
    policy, evidence = spec(), unplugged()
    with pytest.raises(SecurityError):
        policy.require(evidence, IDENTITY, "trial")
    policy.require(evidence, IDENTITY, "trial", allow_unplugged=True)
    policy.require(evidence, IDENTITY, "trial", closed=True, allow_unplugged=True)
    with pytest.raises(SecurityError):
        policy.require_admission(evidence, IDENTITY)
    with pytest.raises(SecurityError):
        policy.require(unplugged("original"), IDENTITY, "original", allow_unplugged=True)
    with pytest.raises(SecurityError):
        policy.require(unplugged("original", 32), IDENTITY, "original", user_a=32, allow_unplugged=True)


# Intent: contradictory connection, open session, offered output or any measured phase power still fails closed.
# Why: an unplugged label alone cannot authorize release if actual physical evidence disagrees.
@pytest.mark.parametrize("fault", ["cp", "connection", "open", "current", "phase2", "power", "output"])
def test_unplugged_option_rejects_unsafe_or_inconsistent_evidence(fault: str) -> None:
    evidence = unplugged()
    if fault == "cp":
        evidence = replace(evidence, physical=replace(evidence.physical, cp="B"))
    elif fault == "connection":
        evidence = replace(evidence, physical=replace(evidence.physical, connection="Plug"))
    elif fault == "open":
        evidence = replace(
            evidence, physical=replace(evidence.physical, transaction_open=True, native_transaction_id=1777)
        )
    elif fault == "current":
        evidence = replace(evidence, currents_a=(0.1, 0, 0), physical=replace(evidence.physical, current_a=0.1))
    elif fault == "phase2":
        evidence = replace(evidence, currents_a=(0, 0.1, 0), physical=replace(evidence.physical, current_a=0.1))
    elif fault == "power":
        evidence = replace(evidence, powers_w=(1, 0, 0, 1), physical=replace(evidence.physical, power_w=1))
    else:
        evidence = replace(evidence, output_limit_a=6)
    with pytest.raises(SecurityError):
        spec().require(evidence, IDENTITY, "trial", allow_unplugged=True)


# Intent: several unplugged samples and reconnection remain healthy without any extra profile write or Start.
# Why: routine lead removal previously latched a fault after three otherwise safe native observations.
async def test_managed_unplug_and_replug_remain_healthy_without_control_commands(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "unplug-boot")
            assert (await healthy(controller))["vehicle_connected"]
            old_history = copy.deepcopy(rig.bench.history)
            old_limits = list(rig.bench.received_limits)
            rig.bench.cp, rig.bench.connection = "A", "Unplug"
            await rig.bench.call(
                charger,
                "unplug-status",
                "StatusNotification",
                {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
            )
            for _ in range(5):
                status = await healthy(controller)
                assert not status["vehicle_connected"] and status["power_w"] == 0
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == 6
            rig.bench.cp, rig.bench.connection = "B", "Plug"
            await rig.bench.call(
                charger,
                "replug-status",
                "StatusNotification",
                {"connectorId": 1, "status": "Preparing", "errorCode": "NoError"},
            )
            assert (await healthy(controller))["vehicle_connected"]
            assert session.journal.get("managed_control_fault") is None
            assert rig.bench.received_limits == old_limits and not starts(rig)
            assert rig.bench.history == old_history and session.journal.active() is None


# Intent: an already admitted managed service can restart, prove owned zero and release6 while the car is absent.
# Why: unattended process recovery must not depend on somebody physically reconnecting a charging lead.
async def test_managed_restart_while_unplugged_preserves_zero_proofs_and_never_starts(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        first = managed_session(rig)
        await first.start()
        controller = ManagedController(first)
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await introduced(rig, first, charger, "before-unplug-boot")
            await healthy(controller)
            allocation = copy.deepcopy(first.journal.get("trial_allocation"))
            history = copy.deepcopy(rig.bench.history)
            rig.bench.cp, rig.bench.connection = "A", "Unplug"
            assert not (await healthy(controller))["vehicle_connected"]
            await first.close()
        replacement = managed_session(rig)
        await replacement.start()
        recovered = ManagedController(replacement)
        before = len(rig.bench.received_limits)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "unplugged-replacement-boot")
            assert rig.bench.firmware.effective_limit() == 0
            assert rig.bench.received_limits[before:] and set(rig.bench.received_limits[before:]) == {0}
            status = await healthy(recovered)
            assert not status["vehicle_connected"] and status["owner_control_enabled"]
            assert status["phase"] == "active" and status["station_ceiling_a"] == 6
            assert replacement.journal.get("trial_allocation") == allocation
            assert replacement.journal.active() is None and rig.bench.history == history and not starts(rig)


# Intent: enabling ongoing unplugged monitoring never admits a fresh trial without its original connected proof.
# Why: this operational option must not bypass the independently tested commissioning transition.
async def test_managed_unplugged_fresh_admission_remains_blocked(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        rig.bench.cp, rig.bench.connection = "A", "Unplug"
        session = managed_session(rig)
        with pytest.raises(SecurityError):
            await session.start()
        assert session.runtime._server is None and rig.bench.device_calls == []


# Intent: the extra monitoring capability cannot be combined with assistant Start or an unrelated native mode.
# Why: every operational relaxation must remain explicit at construction rather than acquired by accident.
@pytest.mark.parametrize("bounded,local_start", [(True, True), (False, False)])
async def test_unplugged_option_requires_native_bounded_mode_and_disabled_local_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    bounded: bool,
    local_start: bool,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        with pytest.raises(SecurityError, match="disabled local Start"):
            TrialSession(
                rig.prepared,
                native_bounded=rig.spec if bounded else None,
                allow_local_start=local_start,
                allow_unplugged_native=True,
            )
        assert not rig.prepared.config.journal.exists()
