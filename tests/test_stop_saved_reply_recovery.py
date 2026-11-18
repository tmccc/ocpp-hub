"""Offline Stop retry must not erase a durable response awaiting ordinary settlement."""

import time
from dataclasses import replace

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.delivery_reply import DeliveryReply
from ocpp_hub_core.prototype.managed_debt_recovery import debt_candidate
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.managed_recovery_zero import require_original_zero
from ocpp_hub_core.prototype.native_bounded import capture_native_bounded_baseline
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_allocation import initialize_allocation, observe_history
from ocpp_hub_core.prototype.trial_evidence import Resource, profile_projection
from ocpp_hub_core.prototype.trial_resources import ResourceBook

from tests.prototype_rig import START
from tests.test_native_bounded import BOOT, HARDWARE, IDENTITY, native, spec


# Establish the complete original-zero proof and exact mapped closed lifecycle using production constructors.
def saved_stop(path, reply):
    policy = spec()
    original = native("original", 32)
    baseline = capture_native_bounded_baseline(original, IDENTITY, HARDWARE)
    j = Journal(path, IDENTITY)
    book = ResourceBook(j, baseline)
    initialize_allocation(
        j, observe_history(list(original.history), IDENTITY, HARDWARE, time.time()), IDENTITY, HARDWARE
    )
    ManagedRequests(j)
    j.put("trial_phase", "active")
    j.put("boot", BOOT)
    j.put("managed_mode", {"mode": "app_schedule", "spec_digest": policy.digest, "local_start": False})
    j.put(
        "native_bounded_handover",
        {"stage": "admitted", "spec": policy.value(), "original_history": list(original.history)},
    )
    j.put("managed_control_intent", {"mode": "paused", "revision": 1})
    j.put("control", {"owner": "local", "inhibit": True, "ticket": "synthetic-closed-zero", "resume_limit": 6})
    j.put(
        "trial_binding",
        {"configuration_digest": native().physical.configuration_digest, "boot_digest": policy.boot_digest},
    )
    profile = commands.set_charging_profile(0, "ChargePointMaxProfile", 0, 7001)
    book.before_send("SetChargingProfile", profile)
    book.after_result("SetChargingProfile", profile, [3, "synthetic-reply", {"status": "Accepted"}])
    canonical = profile_projection(profile)
    resource = Resource("profile", 7001, {"independent-synthetic-zero-resource": True}, canonical)
    observed = native()
    observed = replace(
        observed, physical=replace(observed.physical, resources=observed.physical.resources + (resource,))
    )
    local = j.start("synthetic-start", START)["transactionId"]
    j.stop("synthetic-stop", {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"})
    head = j.next_delivery()
    j.mark_sent(head)
    j.acknowledge(j.next_delivery(), {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}})
    j.put("managed_control_fault", {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()})
    stop = j.next_delivery()
    j.mark_sent(stop)
    stop = j.next_delivery()
    answer = {
        "ack": [3, stop.token, {}],
        "error": [4, stop.token, "InternalError", "synthetic cloud failure", {}],
        "invalid_authorization": [3, stop.token, {"idTagInfo": {"status": "Expired"}}],
    }[reply]
    DeliveryReply.received(stop, answer).save(j, stop)
    return j, baseline, policy, observed, stop.token


# Intent: every saved correlated Stop outcome blocks operator retry both before and after a journal replacement.
# Why: changing inflight to queued deletes the response, which ordinary settlement must consume first.
@pytest.mark.parametrize("reply", ["ack", "error", "invalid_authorization"])
@pytest.mark.parametrize("reopen", [False, True])
def test_saved_stop_reply_cannot_become_no_reply_candidate(tmp_path, reply, reopen):
    path = tmp_path / "journal.db"
    j, baseline, policy, observed, token = saved_stop(path, reply)
    if reopen:
        j.close()
        j = Journal(path, IDENTITY)
    try:
        book = ResourceBook(j, baseline)
        require_original_zero(j, book, policy, observed)
        before = list(j.db.iterdump())
        assert j.next_delivery().state == "inflight"
        with pytest.raises(RecoveryRequiredError, match="ordinary settlement of its saved reply"):
            debt_candidate(j, book, policy, observed, "stop-delivery")
        assert list(j.db.iterdump()) == before
        assert j.db.execute("SELECT token FROM delivery_replies").fetchone()[0] == token
    finally:
        j.close()
