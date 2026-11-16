"""Only the evidenced native availability exception shapes permit automatic release retry."""

import sqlite3

import pytest
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.release_attempt import native_availability
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import StaleEvidenceError
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
