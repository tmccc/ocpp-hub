"""Shared original-schedule and owner-command prerequisites for daily charging."""

from __future__ import annotations

from ..control import HUB_PROFILE_ID
from .local_control import local_start_receipt
from .store import Journal, fingerprint


# Bind a restrictive write to the admitted installation and a station profile already owned by this journal.
def recorded_station_profile(j: Journal, spec_digest: str, boot_digest: str) -> bool:
    row = j.db.execute("SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)).fetchone()
    return (
        j.phase.read().operating
        and j.get("managed_mode", {}).get("spec_digest") == spec_digest
        and j.get("trial_binding", {}).get("boot_digest") == boot_digest
        and j.get("native_bounded_handover", {}).get("stage") == "admitted"
        and row is not None
        and row["state"] != "absent_verified"
        and not j.fault.latched()
        and not j.get("trial_inventory_fault")
        and not j.get("trial_preservation_fault")
    )


# A vendor grant identifies a separately accepted command only when its exact mutation and tag agree.
def confirmed_vendor_start(j: Journal, tag_digest: str, local: int | None, *, settled: bool = False) -> bool:
    grant = j.get("vendor_start_grant", {})
    command = j.outcomes.vendor(grant.get("mutation"))
    return bool(
        grant.get("local") == local
        and grant.get("digest") == tag_digest
        and command is not None
        and command.action == "RemoteStartTransaction"
        and command.status == "Accepted"
        and (command.session.local if command.session is not None else None) == local
        and (not settled or command.settled)
    )


# Only a positively receipted, distinct owner command can stay pending alongside a native lifecycle.
def awaiting_owner_start(j: Journal, tag: str | None = None) -> bool:
    digest = fingerprint(tag.casefold()) if tag is not None else None
    if j.get("control", {}).get("start_pending"):
        owner = local_start_receipt(j)
        owner_digest = owner.get("digest")
        if (
            digest is None
            or owner.get("stage") != "accepted"
            or not isinstance(owner_digest, str)
            or len(owner_digest) != 64
            or owner_digest == digest
        ):
            return True
    grant = j.get("vendor_start_grant", {})
    if not grant or grant.get("local") is not None or j.outcomes.status(grant.get("mutation")) == "Rejected":
        return False
    grant_digest = grant.get("digest")
    return (
        digest is None
        or not isinstance(grant_digest, str)
        or len(grant_digest) != 64
        or grant_digest == digest
        or not confirmed_vendor_start(j, grant_digest, None)
    )
