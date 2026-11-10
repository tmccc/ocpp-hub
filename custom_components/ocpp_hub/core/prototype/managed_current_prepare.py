"""Exclusive, explicit native32 preparation while a verified owned zero profile protects the connected car."""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ..control import HUB_PROFILE_ID
from .managed_current import capability_value, managed_current_ceiling
from .native_bounded import NativeBoundedSpec
from .native_history import audit_history
from .security import SecurityError
from .station import StationChoice
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook


# Preparation has no listener or Start sender and requires the existing managed service to relinquish its journal.
async def prepare_current_capability(
    journal: Journal,
    book: ResourceBook,
    spec: NativeBoundedSpec,
    observe: Callable[[], Awaitable[HandoverEvidence]],
    patch_user32: Callable[[], Awaitable[None]],
) -> dict[str, Any]:
    expected = capability_value(journal, spec)

    # Recheck before and after the sole possible native write; configuration and original records remain exact.
    def check(native: HandoverEvidence, user32: bool) -> None:
        control = journal.get("control", {})
        selection = journal.get("station_selection", {})
        choice = selection.get("choice")
        if not isinstance(choice, dict) or set(choice) != {"kind", "stack", "start"}:
            raise SecurityError("current preparation requires the exact commissioned station selection")
        station_choice = StationChoice(**choice)
        spec.require_boot(journal.get("boot"))
        if selection.get("boot_digest") != spec.boot_digest or journal.get("trial_binding") != {
            "configuration_digest": fingerprint(spec.configuration("trial", 6)),
            "station": station_choice.value(),
            "boot_digest": spec.boot_digest,
        }:
            raise SecurityError("current preparation admission or actual Boot binding changed")
        if (
            journal.get("managed_mode") != {"mode": "app_schedule", "spec_digest": spec.digest, "local_start": False}
            or journal.get("native_bounded_handover", {}).get("spec") != spec.value()
            or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
            or journal.get("managed_control_intent", {}).get("mode") != "paused"
            or journal.get("trial_phase") not in ("active", "recovering")
            or journal.get("managed_control_fault")
            or not control.get("inhibit")
            or control.get("owner") != "local"
            or control.get("start_pending")
            or control.get("uncertain")
            or journal.get("managed_current_change", {}).get("state") in ("pending", "uncertain", "interrupted")
            or journal.get("managed_test_charge", {}).get("state")
            in ("armed", "starting", "awaiting_power", "charging", "running", "stopping")
            or journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE state IN ('accepted','running') LIMIT 1"
            ).fetchone()
            or journal.active() is not None
            or journal.get("uncertain_vendor_command")
            or journal.get("vendor_start_grant")
            or journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        ):
            raise RecoveryRequiredError("current preparation requires a quiescent paused managed journal")
        if user32:
            spec.require_managed(native, journal.identity, closed=True, allow_unplugged=True)
        else:
            spec.require(native, journal.identity, "trial", closed=True, allow_unplugged=True)
        journal.require_allocation()
        audit_history(journal, spec.digest, list(native.history), False)
        present = book.owned_present(native.physical)
        row = journal.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        station = [r for r in present if r.identifier == HUB_PROFILE_ID]
        if (
            len(station) != 1
            or station[0].canonical.get("limit") != 0
            or row is None
            or row["state"] != "confirmed"
            or row["confirmed"] is None
            or json.loads(row["confirmed"]) != station[0].canonical
        ):
            raise SecurityError("current preparation requires independently verified owned station zero")

    native = await observe()
    existing = journal.get("managed_current_capability")
    intent = journal.get("managed_current_prepare_intent", {})
    if existing is not None:
        managed_current_ceiling(journal, spec)
        check(native, True)
        return {"prepared": True, "native_ceiling_a": 32, "already_prepared": True, "charging_started": False}
    already32 = native.saved_user_a == 32
    if already32 and intent.get("capability") != expected:
        raise SecurityError("native32 requires this exact earlier preparation intent")
    check(native, already32)
    if not already32:
        journal.put(
            "managed_current_prepare_intent",
            {"capability": expected, "stage": "native_write_intent", "at_epoch": time.time()},
        )
        await patch_user32()
    verification = await observe()
    check(verification, True)
    with journal.atomic():
        journal.put("managed_current_capability", expected)
        journal.put(
            "managed_current_prepare_intent",
            {"capability": expected, "stage": "complete", "verified_at": verification.physical.observed_at},
        )
    return {"prepared": True, "native_ceiling_a": 32, "already_prepared": False, "charging_started": False}
