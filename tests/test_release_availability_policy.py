"""Whole-release results distinguish confirmed zero waits from strict and possibly positive failures."""

import sqlite3
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.connection_scope import CloudAttemptScope
from ocpp_hub_core.prototype.managed_intent import ControlIntent
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.release_attempt import ReleaseAttempt, ReleaseOutcome, native_availability
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError, StaleEvidenceError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, ReleaseDeferredError


# Intent: genuine native availability accepts only the two existing exact retry wrappers, never stricter subtypes.
# Why: source supersession, storage errors and readiness refusals cannot inherit a transient exception's cause.
@pytest.mark.parametrize("native_type", [NativeTransportUnavailableError, StaleEvidenceError])
@pytest.mark.parametrize(
    "wrapper,expected",
    [
        (None, True),
        (UnavailableError, True),
        (ReleaseDeferredError, True),
        (AuditSupersededError, False),
        (RecoveryRequiredError, False),
        (sqlite3.IntegrityError, False),
    ],
)
def test_only_exact_native_availability_wrappers_are_retryable(native_type, wrapper, expected):
    error = native_type("typed native input")
    if wrapper is not None:
        outer = wrapper("wrapper input")
        outer.__cause__ = error
        error = outer
    assert native_availability(error) is expected


# Intent: the actual two-level wrapper is accepted, but absent, repeated or cyclic causes remain unknown.
# Why: arbitrary exception chains are not proof of the specific native audit that made this release retryable.
def test_native_availability_cause_depth_is_bounded_by_its_contract():
    native = NativeTransportUnavailableError("typed native input")
    unavailable = UnavailableError("ordinary hold wrapper")
    unavailable.__cause__ = native
    deferred = ReleaseDeferredError("automatic release wrapper")
    deferred.__cause__ = unavailable
    assert native_availability(deferred)
    repeated = ReleaseDeferredError("unrecognized repeated wrapper")
    repeated.__cause__ = deferred
    assert not native_availability(repeated)
    assert not native_availability(ReleaseDeferredError("no evidence"))
    unavailable.__cause__ = unavailable
    assert not native_availability(unavailable)


class UnrecognizedDeferredError(ReleaseDeferredError):
    """An unrelated subtype cannot grant the shared bare-prerequisite contract."""


# Construct pure completed rollback facts; real-socket tests independently prove how production records them.
def completed_zero_attempt(error):
    return ReleaseAttempt(
        "automatic",
        ControlIntent("app", 4),
        CloudAttemptScope(2, "b" * 32, 3),
        7,
        lambda: None,
        ticket="owned-release-ticket",
        zero_may_have_dispatched=True,
        zero_confirmed=True,
        finished=True,
        failure=error,
        positive_failure=error,
    )


# Intent: one result distinguishes typed availability and bare prerequisites from strict or unknown failures.
# Why: current, ordinary and timer callers must share policy without guessing from wording or arbitrary causes.
@pytest.mark.parametrize(
    "kind,expected",
    [
        ("source", ReleaseOutcome.PREREQUISITE_PENDING),
        ("native", ReleaseOutcome.NATIVE_UNAVAILABLE),
        ("stale", ReleaseOutcome.NATIVE_UNAVAILABLE),
        ("native-wrapper", ReleaseOutcome.NATIVE_UNAVAILABLE),
        ("strict", ReleaseOutcome.FAILED),
        ("storage", ReleaseOutcome.FAILED),
        ("readiness", ReleaseOutcome.FAILED),
        ("unavailable", ReleaseOutcome.FAILED),
        ("unknown", ReleaseOutcome.FAILED),
        ("subtype", ReleaseOutcome.FAILED),
        ("strict-cause", ReleaseOutcome.FAILED),
        ("unknown-cause", ReleaseOutcome.FAILED),
        ("superseded-native", ReleaseOutcome.FAILED),
    ],
)
def test_whole_release_outcome_uses_exact_verdict_contract(kind, expected):
    errors = {
        "source": ReleaseDeferredError("same message"),
        "native": NativeTransportUnavailableError("same message"),
        "stale": StaleEvidenceError("same message"),
        "native-wrapper": ReleaseDeferredError("same message"),
        "strict": SecurityError("same message"),
        "storage": sqlite3.IntegrityError("same message"),
        "readiness": RecoveryRequiredError("same message"),
        "unavailable": UnavailableError("same message"),
        "unknown": RuntimeError("same message"),
        "subtype": UnrecognizedDeferredError("same message"),
        "strict-cause": ReleaseDeferredError("same message"),
        "unknown-cause": ReleaseDeferredError("same message"),
        "superseded-native": AuditSupersededError("same message"),
    }
    errors["native-wrapper"].__cause__ = UnavailableError("same message")
    errors["native-wrapper"].__cause__.__cause__ = errors["native"]
    errors["strict-cause"].__cause__ = errors["strict"]
    errors["unknown-cause"].__cause__ = errors["unavailable"]
    errors["superseded-native"].__cause__ = errors["native"]
    assert completed_zero_attempt(errors[kind]).outcome() is expected


# Intent: unfinished work, cancellation, physical effects and failed cleanup dominate an otherwise retryable verdict.
# Why: a later successful zero or an identical error message cannot retire an earlier positive or unknown result.
@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"finished": False}, ReleaseOutcome.PENDING),
        ({"cancelled": True}, ReleaseOutcome.CANCELLED),
        ({"positive_may_have_dispatched": True}, ReleaseOutcome.POSSIBLE_POSITIVE),
        ({"positive_confirmed": True}, ReleaseOutcome.POSSIBLE_POSITIVE),
        ({"zero_confirmed": False}, ReleaseOutcome.ZERO_UNCONFIRMED),
        ({"positive_failure": ReleaseDeferredError("same message")}, ReleaseOutcome.FAILED),
        ({"failure": sqlite3.IntegrityError("cleanup publication failed")}, ReleaseOutcome.FAILED),
        ({"failure": None}, ReleaseOutcome.FAILED),
        (
            {
                "failure": None,
                "positive_failure": None,
                "positive_may_have_dispatched": True,
                "positive_confirmed": True,
                "zero_may_have_dispatched": False,
                "zero_confirmed": False,
            },
            ReleaseOutcome.SUCCEEDED,
        ),
    ],
)
@pytest.mark.parametrize("failure_type", [ReleaseDeferredError, NativeTransportUnavailableError])
def test_whole_release_outcome_retains_effect_and_cleanup_precedence(changes, expected, failure_type):
    attempt = completed_zero_attempt(failure_type("same message"))
    assert replace(attempt, **changes).outcome() is expected
