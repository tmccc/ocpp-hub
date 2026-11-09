"""Independent native limits bound current without creating car permission or relaxing unplugged admission."""

from __future__ import annotations

import copy
import time
from dataclasses import FrozenInstanceError, replace

import pytest
from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec, capture_native_bounded_baseline
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_evidence import Resource
from ocpp_hub_core.prototype.trial_handover_evidence import HandoverEvidence, handover_evidence
from ocpp_hub_core.prototype.trial_resources import Baseline

IDENTITY = "BOUNDED001"
HARDWARE = "BOUNDEDHW001"
ORIGINAL = {
    "Id": IDENTITY,
    "bPSN": HARDWARE,
    "AmpLmt": "32",
    "maxOutCurrent0": "32",
    "minOutCurrent0": "6",
    "connectorNum": "1",
    "sDomain": "original.invalid",
    "sPort": "443",
    "sURL": "/ocpp/",
    "allowSelfSign": "0",
    "installer_untouched": {"nested": ["preserved"]},
}
ROUTE = {"sDomain": "127.0.0.1", "sPort": "19001", "sURL": "/trial/", "allowSelfSign": "1"}
SCHEDULE = {"planList": [{"planId": 11, "startTime": 0, "limitCurrent": 320}], "defaultEco": {"ecoMode": "Fast"}}
BOOT = {"chargePointVendor": "Synthetic", "firmwareVersion": "verified-1"}


# Build a separate native firmware snapshot rather than deriving physical values from the policy under test.
def native(route: str = "trial", user: int = 6, opened: bool = False, online: bool = True) -> HandoverEvidence:
    configuration = copy.deepcopy(ORIGINAL) | (ROUTE if route == "trial" else {}) | {"AmpLmt": str(user)}
    values = {
        "SMART": {
            "conn0": {"ChargePointMaxProfile": [], "TxDefaultProfile": [], "TxProfile": []},
            "conn1": {"ChargePointMaxProfile": [], "TxDefaultProfile": [], "TxProfile": []},
        },
        "SETPLAN": copy.deepcopy(SCHEDULE),
        "SESSION": {"saveData": {"sessionList": [{"_transactionId": 1777 if opened else -1}]}},
        "METER": [{"meterData": {"cur": [61, 0, 0], "power": [1518, 0, 0, 1517]}}]
        if opened
        else [{"meterData": {"cur": [0, 0, 0], "power": [0, 0, 0, 0]}}],
        "CONNECTOR": [
            {
                "info": {"CP": "C" if opened else "B"},
                "connStatus": "Charging" if opened else "Plug",
                "outLimit": 60 if opened else 0,
                "offerCur": 60 if opened else 320,
            }
        ],
        "OCPP": {"online": online},
        "STATE": [{"currLimit": {"USER": user * 10, "HWCFG": 320}}],
        "TRANSACTIONS": [{"transactionId": 1001, "startTime": 100, "stopTime": 200, "MeterValues": ["old"]}],
    }
    return handover_evidence(configuration, values, time.time())


# Supply exact independently saved configuration, connection delta and historical firmware/persistence bindings.
def spec() -> NativeBoundedSpec:
    return NativeBoundedSpec(
        HARDWARE,
        copy.deepcopy(ORIGINAL),
        dict(ROUTE),
        fingerprint(SCHEDULE),
        fingerprint("persistence"),
        fingerprint(BOOT),
    )


# Intent: a connected closed original32 baseline can be captured without changing the legacy unplugged path.
# Why: the lead remains attached, but a fabricated CP A observation would weaken unrelated admission contracts.
def test_capture_is_separate_from_unchanged_unplugged_capture() -> None:
    evidence = native("original", 32)
    baseline = capture_native_bounded_baseline(evidence, IDENTITY, HARDWARE)
    assert baseline.configuration_digest == fingerprint(ORIGINAL)
    assert baseline.resources == evidence.physical.resources
    with pytest.raises(SecurityError, match="unplugged"):
        Baseline.capture(IDENTITY, evidence.physical)


# Intent: current-limit monitoring supports genuine resumed charging and car-side suspension without adopting it.
# Why: native USER6 is a maximum current guard, not a claim that the car is paused or no transaction exists.
@pytest.mark.parametrize("online", [True, False])
def test_guard_survives_cloud_outage_and_actual_resumption(online: bool) -> None:
    policy = spec()
    charging = native(opened=True, online=online)
    policy.require(charging, IDENTITY, "trial")
    suspended = replace(
        charging,
        physical=replace(charging.physical, cp="B", current_a=0, power_w=0),
        currents_a=(0, 0, 0),
        powers_w=(0, 0, 0, 0),
    )
    policy.require(suspended, IDENTITY, "trial")
    assert suspended.physical.native_transaction_id == 1777
    assert not {"paused", "release_only", "pause_valid_until", "permission_digest"}.intersection(policy.value())


# Intent: a bounded open native transaction cannot authorize new listener admission or original current release.
# Why: real lifecycle closure remains a separate necessary condition even when the power ceiling is safe.
def test_open_session_blocks_admission_and_original32() -> None:
    policy = spec()
    with pytest.raises(SecurityError):
        policy.require_admission(native(opened=True), IDENTITY)
    with pytest.raises(SecurityError):
        policy.require(native("original", 32, opened=True), IDENTITY, "original", user_a=32)
    policy.require_admission(native(), IDENTITY)
    policy.require(native("original", 32), IDENTITY, "original", user_a=32)


# Intent: after an authorized release, actual ordinary32A charging is valid evidence without becoming proxy ownership.
# Why: final settings verification must distinguish resumption after release from releasing a currently active6A guard.
def test_normal_charging_after_authorized_release_is_independently_validated() -> None:
    policy = spec()
    evidence = native("original", 32, opened=True)
    evidence = replace(
        evidence,
        currents_a=(32.3, 0, 0),
        powers_w=(8040, 0, 0, 8039),
        output_limit_a=32,
        offered_current_a=32,
        physical=replace(evidence.physical, current_a=32.3, power_w=8040),
    )
    policy.require_normal(evidence, IDENTITY)
    with pytest.raises(SecurityError):
        policy.require(evidence, IDENTITY, "original", user_a=32)


# Intent: no invalid ordinary manufacturer state can be used to mark the independent-limit handover complete.
# Why: malformed phases, contradictory closure, backend loss and source/config drift remain material after release.
@pytest.mark.parametrize(
    "fault", ["power", "current", "nan", "aggregate", "closed_flow", "cp", "history", "schedule", "offline"]
)
def test_normal_restoration_rejects_incomplete_or_excessive_physical_facts(fault: str) -> None:
    evidence = native("original", 32, opened=True)
    if fault == "power":
        evidence = replace(evidence, powers_w=(9000, 0, 0, 9000))
    elif fault == "current":
        evidence = replace(evidence, currents_a=(32.6, 0, 0))
    elif fault == "nan":
        evidence = replace(evidence, powers_w=(float("nan"), 0, 0, 0))
    elif fault == "aggregate":
        evidence = replace(evidence, physical=replace(evidence.physical, current_a=0))
    elif fault == "closed_flow":
        evidence = replace(
            evidence,
            physical=replace(evidence.physical, transaction_open=False, native_transaction_id=None, cp="B"),
        )
    elif fault == "cp":
        evidence = replace(evidence, physical=replace(evidence.physical, cp="A"))
    elif fault == "history":
        evidence = replace(evidence, physical=replace(evidence.physical, history=None))
    elif fault == "schedule":
        evidence = replace(evidence, saved_schedule_digest=fingerprint("changed"))
    else:
        evidence = replace(evidence, physical=replace(evidence.physical, upstream_connected=False))
    with pytest.raises(SecurityError):
        spec().require_normal(evidence, IDENTITY)


# Intent: residual current and retained effective output block closed admission even at a bounded six amperes.
# Why: CP B and an old offerCur value alone do not prove that a previous session has settled.
@pytest.mark.parametrize("fault", ["residual", "output", "open_zero"])
def test_closed_admission_requires_actual_settled_zero(fault: str) -> None:
    evidence = native()
    if fault == "residual":
        evidence = replace(evidence, currents_a=(0.5, 0, 0), physical=replace(evidence.physical, current_a=0.5))
    elif fault == "output":
        evidence = replace(evidence, output_limit_a=6)
    else:
        evidence = replace(
            evidence, physical=replace(evidence.physical, transaction_open=True, native_transaction_id=1777)
        )
    with pytest.raises(SecurityError):
        spec().require_admission(evidence, IDENTITY)


# Intent: complete every-phase readings, true identities and the independent saved/runtime hardware guard are required.
# Why: a missing, contradictory or excessive field must not be replaced by the desired OCPP profile.
@pytest.mark.parametrize(
    "fault",
    ["saved", "runtime", "hardware", "identity", "config_digest", "schedule", "aggregate", "phase", "bool", "nan"],
)
def test_bad_independent_native_guard_rejects(fault: str) -> None:
    evidence = native(opened=True)
    if fault == "saved":
        evidence = replace(evidence, saved_user_a=32)
    elif fault == "runtime":
        evidence = replace(evidence, runtime_user_a=32)
    elif fault == "hardware":
        evidence = replace(evidence, runtime_hardware_a=16)
    elif fault == "identity":
        evidence = replace(evidence, hardware_digest=fingerprint("other"))
    elif fault == "config_digest":
        evidence = replace(evidence, configuration=evidence.configuration | {"untouched": "changed"})
    elif fault == "schedule":
        evidence = replace(evidence, saved_schedule_digest=fingerprint("new midnight schedule"))
    elif fault == "aggregate":
        evidence = replace(evidence, physical=replace(evidence.physical, power_w=0))
    elif fault == "phase":
        evidence = replace(evidence, currents_a=(6.1, 0.1, 0))
    elif fault == "bool":
        evidence = replace(evidence, output_limit_a=True)
    else:
        evidence = replace(evidence, currents_a=(float("nan"), 0, 0))
    with pytest.raises(SecurityError):
        spec().require(evidence, IDENTITY, "trial")


# Intent: native history must belong to this actual complete read rather than a caller's altered or stale tuple.
# Why: later allocation and resource cleanup must not inherit fabricated or differently dated lifecycle evidence.
@pytest.mark.parametrize("fault", ["missing", "changed", "duplicate", "stale"])
def test_history_evidence_is_independently_bound(fault: str) -> None:
    evidence = native()
    if fault == "missing":
        evidence = replace(evidence, physical=replace(evidence.physical, history=None))
    elif fault == "changed":
        evidence = replace(evidence, history=({"transactionId": 1778},))
    elif fault == "duplicate":
        evidence = replace(evidence, history=evidence.history + evidence.history)
    else:
        evidence = replace(evidence, physical=replace(evidence.physical, observed_at=time.time() - 60))
    with pytest.raises(SecurityError):
        spec().require(evidence, IDENTITY, "trial")


# Intent: a native plan remains original while any pre-existing profile blocks new connected trial admission.
# Why: selective Clear has not been proved safe alongside even inactive unrelated profiles on this firmware.
def test_any_profile_blocks_admission_and_baseline_capture() -> None:
    profile = Resource("profile", 99, {"inactive": True}, {"unsupported": True})
    for user in (6, 32):
        evidence = native("original", user)
        evidence = replace(
            evidence, physical=replace(evidence.physical, resources=evidence.physical.resources + (profile,))
        )
        with pytest.raises(SecurityError, match="profiles"):
            if user == 6:
                spec().require_admission(evidence, IDENTITY, "original")
            else:
                capture_native_bounded_baseline(evidence, IDENTITY, HARDWARE)


# Intent: firmware, route and installer drift never become another accepted configuration through normalization.
# Why: the only permissible native deltas are the explicit four connection fields and the USER scalar.
def test_exact_route_installer_and_boot_bindings() -> None:
    policy = spec()
    policy.require(native("original"), IDENTITY, "original")
    with pytest.raises(SecurityError):
        policy.require(native(), IDENTITY, "original")
    with pytest.raises(SecurityError):
        policy.require(native(), IDENTITY, "trial", user_a=32)
    policy.require_boot(BOOT)
    with pytest.raises(SecurityError):
        policy.require_boot(BOOT | {"firmwareVersion": "unproved-2"})
    with pytest.raises(SecurityError):
        NativeBoundedSpec(
            HARDWARE, ORIGINAL | {"maxOutCurrent0": "16"}, ROUTE, fingerprint(SCHEDULE), "0" * 64, "1" * 64
        )


# Intent: the immutable saved strategy survives restart and caller mutation without exposing its private contents.
# Why: changing a shared dictionary must not silently alter the handover baseline or credential-bearing snapshots.
def test_spec_is_immutable_private_and_roundtrips() -> None:
    original, route = copy.deepcopy(ORIGINAL), dict(ROUTE)
    policy = NativeBoundedSpec(HARDWARE, original, route, fingerprint(SCHEDULE), "0" * 64, fingerprint(BOOT))
    saved = policy.value()
    original["installer_untouched"]["nested"].append("changed")
    route["sPort"] = "9"
    policy.original_configuration["installer_untouched"]["nested"].append("also changed")
    policy.trial_route["sDomain"] = "changed.invalid"
    assert policy.value() == saved and NativeBoundedSpec.parse(saved).digest == policy.digest
    assert IDENTITY not in repr(policy) and "original.invalid" not in repr(policy)
    with pytest.raises(FrozenInstanceError):
        policy.hardware_identity = "other"  # type: ignore[misc]


# Intent: restart proofs and the exact connection-field allowlist cannot be extended or omitted silently.
# Why: credentials, default capability claims and unknown native setting names do not belong to this strategy.
@pytest.mark.parametrize("fault", ["extra_route", "missing_route", "port", "capability", "boot", "partial"])
def test_invalid_spec_never_defaults_authority(fault: str) -> None:
    saved = spec().value()
    if fault == "extra_route":
        saved["trial_route"]["AuthorizationKey"] = "never overwrite"
    elif fault == "missing_route":
        del saved["trial_route"]["allowSelfSign"]
    elif fault == "port":
        saved["trial_route"]["sPort"] = "0"
    elif fault == "partial":
        del saved["boot_digest"]
    else:
        saved[fault + "_digest"] = None
    with pytest.raises(SecurityError):
        NativeBoundedSpec.parse(saved)
