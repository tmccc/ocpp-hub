"""Present physical refusal needs real reversible charging and fresh complete native observations."""

from __future__ import annotations

import time
from dataclasses import replace
from typing import Any

import pytest
from ocpp_hub_core.prototype.physical_pause import PhysicalPauseSample, PhysicalPauseVerifier
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_handover_evidence import handover_evidence

from tests.handover_helpers import ORIGINAL


class Clock:
    # A controllable monotonic clock makes the required real-duration policy testable without weakening production.
    def __init__(self) -> None:
        self.value = 1000.0

    # Report the current synthetic monotonic time.
    def __call__(self) -> float:
        return self.value

    # Advance only the test source; production always uses time.monotonic directly.
    def advance(self, seconds: float) -> None:
        self.value += seconds


# Build complete independent native values with positive offer, separate USER/HW limits and all phase channels.
def sample(clock: Clock, charging: bool, delta: dict[str, Any] | None = None) -> PhysicalPauseSample:
    clock.advance(0.01)
    started = clock()
    values = {
        "SMART": {
            key: {p: [] for p in ("ChargePointMaxProfile", "TxDefaultProfile", "TxProfile")}
            for key in ("conn0", "conn1")
        },
        "SETPLAN": {"planList": [{"planId": 9, "unchanged": True}]},
        "SESSION": {"saveData": {"sessionList": [{"_transactionId": 700}]}},
        "METER": [
            {
                "meterData": {
                    "cur": [61 if charging else 0, 0, 0],
                    "power": [1464 if charging else 0, 0, 0, 1464 if charging else 0],
                }
            }
        ],
        "CONNECTOR": [
            {
                "info": {"CP": "C" if charging else "B"},
                "connStatus": "Charging" if charging else "Plug",
                "outLimit": 60,
                "offerCur": 60,
            }
        ],
        "OCPP": {"online": True},
        "STATE": [{"currLimit": {"USER": 60, "HWCFG": 320}}],
        "TRANSACTIONS": [],
    }
    values.update(delta or {})
    native = handover_evidence(ORIGINAL | {"AmpLmt": "6"}, values, time.time())
    clock.advance(0.01)
    return PhysicalPauseSample(native, started, clock())


# Start a fresh process-local verifier with real charging, before issuing any car control intent.
def begun() -> tuple[Clock, PhysicalPauseVerifier]:
    clock = Clock()
    verifier = PhysicalPauseVerifier("OFFLINE001", "HARDWARE001", fingerprint("vehicle"), "a" * 32, clock)
    verifier.observe(sample(clock, True))
    return clock, verifier


# Supply distinct observations at intervals below the maximum permitted gap.
def zero_window(clock: Clock, verifier: PhysicalPauseVerifier) -> None:
    for _ in range(14):
        clock.advance(5)
        verifier.observe(sample(clock, False))


# Complete both owned Stop windows and the mandatory return to genuine charging.
def complete(clock: Clock, verifier: PhysicalPauseVerifier) -> None:
    verifier.control_intent("stop", "b" * 32)
    zero_window(clock, verifier)
    assert verifier.phase == "paused_first"
    verifier.control_intent("start", "c" * 32)
    verifier.observe(sample(clock, True))
    verifier.control_intent("stop", "d" * 32)
    zero_window(clock, verifier)


# Intent: distinguish a freshly observed reversible car refusal from an indefinitely safe permission lease.
# Why: cloud fetch age, full-charge status and a historical pilot cannot provide this current behavioural proof.
def test_reversible_physical_challenge_and_expiry() -> None:
    clock, verifier = begun()
    complete(clock, verifier)
    result = verifier.summary()
    assert result["current_vehicle_refusal_verified"] and result["reversible_challenge_passed"]
    assert result["first_zero_samples"] >= 13 and result["last_zero_samples"] >= 13
    assert result["sampled_zero_seconds"] >= 60
    assert result["current_evidence_expires_at"] == result["native_observed_at"] + 5
    assert (
        result["current_pause_permission_verified"]
        is result["durable_pause_verified"]
        is result["handover_authority"]
        is False
    )
    clock.advance(6)
    assert not verifier.summary()["current_vehicle_refusal_verified"]
    assert verifier.summary()["current_evidence_expires_at"] is None


# Intent: a fresh source read after a historical success still validates actual current behaviour.
# Why: another native Start, a changed offer or a car resume can revoke a previously successful result.
@pytest.mark.parametrize("fault", ["resume", "session", "offer", "output", "schedule", "configuration", "phase"])
def test_later_changes_invalidate_current_pause(fault: str) -> None:
    clock, verifier = begun()
    complete(clock, verifier)
    current = sample(clock, fault == "resume")
    native = current.native
    if fault == "session":
        native = replace(native, physical=replace(native.physical, native_transaction_id=701))
    elif fault == "offer":
        native = replace(native, offered_current_a=0)
    elif fault == "output":
        native = replace(native, output_limit_a=0)
    elif fault == "schedule":
        native = replace(native, saved_schedule_digest=fingerprint("changed"))
    elif fault == "configuration":
        native = replace(native, configuration=native.configuration | {"unexpected": "change"})
    elif fault == "phase":
        native = replace(native, currents_a=(0, 0.1, 0))
    with pytest.raises(SecurityError):
        verifier.observe(replace(current, native=native))
    assert not verifier.summary()["current_vehicle_refusal_verified"]
    assert verifier.summary()["fresh_native_read_required"]


# Intent: an auto-full car or unavailable demand cannot pass the required return to real charging.
# Why: one charging-to-zero transition alone does not establish reversible owner pause behaviour.
def test_full_car_cannot_replace_resumption() -> None:
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    zero_window(clock, verifier)
    verifier.control_intent("start", "c" * 32)
    with pytest.raises(SecurityError, match="did not return"):
        for _ in range(20):
            clock.advance(5)
            verifier.observe(sample(clock, False))
    assert not verifier.summary()["reversible_challenge_passed"]


# Intent: exact USER6, offered6, effective6 and all complete current/power channels are mandatory.
# Why: a charger-side hold or a retained offerCur60 after charger Stop cannot look like a paused car.
@pytest.mark.parametrize(
    "field,value",
    [
        ("runtime_user_a", 32),
        ("runtime_hardware_a", 16),
        ("offered_current_a", 0),
        ("output_limit_a", 0),
        ("currents_a", (0, 0)),
        ("currents_a", (False, 0, 0)),
        ("currents_a", (float("nan"), 0, 0)),
        ("currents_a", (7, 0, 0)),
        ("powers_w", (0, 0, 0)),
        ("powers_w", (0, 0, float("inf"), 0)),
    ],
)
def test_incomplete_or_charger_limited_evidence_fails(field: str, value: Any) -> None:
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    current = sample(clock, False)
    with pytest.raises(SecurityError):
        verifier.observe(replace(current, native=replace(current.native, **{field: value})))


# Intent: stale, reused, future and unobserved gaps cannot be filled by assigning a new read completion time.
# Why: current proof requires a complete succession of actual distinct native observations.
@pytest.mark.parametrize("fault", ["replay", "gap", "future", "pre_intent", "slow", "stale"])
def test_freshness_is_bound_to_actual_reads(fault: str) -> None:
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    current = sample(clock, False)
    verifier.observe(current)
    if fault == "gap":
        clock.advance(8)
        current = sample(clock, False)
    elif fault == "future":
        current = replace(sample(clock, False), completed=clock() + 1)
    elif fault == "pre_intent":
        current = replace(sample(clock, False), started=999)
    elif fault == "slow":
        current = replace(sample(clock, False), started=clock() - 3)
    elif fault == "stale":
        clock.advance(3)
    with pytest.raises(SecurityError):
        verifier.observe(current)


# Intent: a later owner/original-permission Start or process replacement cannot inherit current pause authority.
# Why: saved historical success must not keep the car marked paused after restoration or restart.
def test_superseding_control_and_process_loss_revoke_current() -> None:
    clock, verifier = begun()
    complete(clock, verifier)
    verifier.supersede()
    assert verifier.summary()["reversible_challenge_passed"]
    assert not verifier.summary()["current_vehicle_refusal_verified"]
    fresh = PhysicalPauseVerifier("OFFLINE001", "HARDWARE001", fingerprint("vehicle"), "e" * 32, clock)
    assert not fresh.summary()["current_vehicle_refusal_verified"]


# Intent: restoring Start before the first Stop and replayed command nonces cannot advance the behavioural proof.
# Why: an accepted-looking command receipt cannot substitute for the measured A→B→A→B sequence.
def test_control_order_and_nonce_are_owned() -> None:
    clock, verifier = begun()
    with pytest.raises(SecurityError):
        verifier.control_intent("start", "b" * 32)
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    zero_window(clock, verifier)
    with pytest.raises(SecurityError):
        verifier.control_intent("start", "b" * 32)


# Intent: a genuine successful transition first arriving beyond90s is rejected despite fresh intermediate samples.
# Why: checking success before the deadline would silently lengthen the bounded car-control settling window.
@pytest.mark.parametrize("action", ["stop", "start"])
def test_late_success_does_not_override_control_deadline(action: str) -> None:
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    if action == "start":
        zero_window(clock, verifier)
        verifier.control_intent("start", "c" * 32)
    for _ in range(17):
        clock.advance(5)
        verifier.observe(sample(clock, action == "stop"))
    clock.advance(5.1)
    with pytest.raises(SecurityError, match="bounded window"):
        verifier.observe(sample(clock, action == "start"))


# Intent: five-second freshness starts when the native read starts, including its bounded acquisition duration.
# Why: a two-second read cannot extend a current result to seven seconds after its underlying observation began.
def test_read_duration_does_not_extend_current_evidence() -> None:
    clock, verifier = begun()
    complete(clock, verifier)
    clock.advance(1)
    current = sample(clock, False)
    clock.advance(1.9)
    verifier.observe(replace(current, completed=clock()))
    assert verifier.summary()["current_vehicle_refusal_verified"]
    clock.advance(3.2)
    result = verifier.summary()
    assert result["reversible_challenge_passed"] and not result["current_vehicle_refusal_verified"]


# Intent: observed CP/status/meter publication lag during owned Stop counts as settling, never a zero proof sample.
# Why: the charger publishes CP B before Charging becomes Plug and the residual0.5A reading reaches zero.
def test_publication_lag_does_not_create_pause_evidence() -> None:
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    for _ in range(6):
        clock.advance(5)
        current = sample(clock, False)
        native = replace(
            current.native,
            currents_a=(0.5, 0, 0),
            physical=replace(current.native.physical, connection="Charging", current_a=0.5),
        )
        verifier.observe(replace(current, native=native))
        assert verifier.phase == "stopping_first" and verifier.window_count == 0
    zero_window(clock, verifier)
    assert verifier.phase == "paused_first" and not verifier.summary()["reversible_challenge_passed"]


# Intent: the actual active row's meter samples may grow, while old completed history remains immutable.
# Why: normal live MeterValues updates are observations rather than a new owner setting or another transaction.
@pytest.mark.parametrize("changed_id", [700, 600])
def test_active_meter_updates_do_not_relax_completed_history(changed_id: int) -> None:
    clock = Clock()
    verifier = PhysicalPauseVerifier("OFFLINE001", "HARDWARE001", fingerprint("vehicle"), "a" * 32, clock)
    history = [
        {"transactionId": 600, "MeterValues": "old", "meterStart": 10, "stopTime": 100},
        {"transactionId": 700, "MeterValues": "initial", "meterStart": 20, "stopTime": 0},
    ]
    verifier.observe(sample(clock, True, {"TRANSACTIONS": history}))
    verifier.control_intent("stop", "b" * 32)
    changed = [row | ({"MeterValues": "new"} if row["transactionId"] == changed_id else {}) for row in history]
    if changed_id == 600:
        with pytest.raises(SecurityError, match="inventory changed"):
            verifier.observe(sample(clock, False, {"TRANSACTIONS": changed}))
    else:
        verifier.observe(sample(clock, False, {"TRANSACTIONS": changed}))
        assert verifier.phase == "zero_first" and not verifier.summary()["current_vehicle_refusal_verified"]


# Intent: a completed or ambiguous history row is fully pinned even if its ID matches the reported native session.
# Why: the live MeterValues exception belongs only to an explicitly open row, not to every matching identifier.
@pytest.mark.parametrize("stop_time", [100, None, False])
def test_meter_exception_requires_explicit_open_history(stop_time: Any) -> None:
    clock = Clock()
    verifier = PhysicalPauseVerifier("OFFLINE001", "HARDWARE001", fingerprint("vehicle"), "a" * 32, clock)
    row = {"transactionId": 700, "MeterValues": "initial", "meterStart": 20, "stopTime": stop_time}
    verifier.observe(sample(clock, True, {"TRANSACTIONS": [row]}))
    verifier.control_intent("stop", "b" * 32)
    with pytest.raises(SecurityError, match="inventory changed"):
        verifier.observe(sample(clock, False, {"TRANSACTIONS": [row | {"MeterValues": "changed"}]}))


# Intent: the real iCS session-level Charging label persists while CP B and all phases show vehicle refusal.
# Why: connStatus describes the open session and must not override the actual CP, positive offer and phase readings.
def test_open_paused_session_can_retain_charging_label() -> None:
    clock, verifier = begun()
    verifier.control_intent("stop", "b" * 32)
    for _ in range(14):
        clock.advance(5)
        current = sample(clock, False)
        native = replace(current.native, physical=replace(current.native.physical, connection="Charging"))
        verifier.observe(replace(current, native=native))
    assert verifier.phase == "paused_first" and not verifier.summary()["reversible_challenge_passed"]
