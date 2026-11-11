"""Match a command to its actual transaction when the native schedule uses the same tag."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .local_control import local_start_receipt
from .managed_occurrences import occurrences
from .store import fingerprint

if TYPE_CHECKING:
    from .gateway import Gateway
    from .trial_handover_evidence import HandoverEvidence


class ManagedStartBinding:
    """Keep the acknowledged command pending until the two independent session reports agree."""

    # Store only tag digests for the verified original timetable; its identity already binds the installation.
    def __init__(self, gateway: Gateway, original: str) -> None:
        self.gateway = gateway
        self.journal = gateway.journal
        self.original = original

    # Preserve the exact pending requests at allocation, before a later acknowledgement, Pause, or restart.
    def defer(self, local: int, tag: str) -> bool:
        j = self.journal
        digest = fingerprint(tag.casefold())
        owner, vendor = local_start_receipt(j), j.get("vendor_start_grant", {})
        local_match = bool(
            j.get("control", {}).get("start_pending")
            and owner.get("local") is None
            and owner.get("stage") in ("issued", "accepted")
            and owner.get("digest") == digest
        )
        cloud_match = bool(vendor.get("local") is None and vendor.get("digest") == digest and vendor.get("token"))
        if not (local_match or cloud_match):
            return False
        catalogue = j.get("managed_schedule_tags")
        if catalogue is not None and digest not in catalogue:
            return False
        j.put(
            "managed_deferred_start",
            {
                "local": local,
                "owner": local_match,
                "ticket": owner.get("ticket"),
                "vendor": vendor.get("token") if cloud_match else None,
            },
        )
        return True

    # Classify the same ledger ID using a verified native read. A scheduled transaction never spends a command.
    def observe(self, n: HandoverEvidence) -> None:
        j = self.journal
        if n.saved_schedule is None:
            return
        plans = n.saved_schedule["planList"]
        timers = occurrences(j)
        originals = plans
        digest = n.saved_schedule_digest
        for lease in timers.values():
            if digest != self.original and digest == lease.get("augmented_digest"):
                originals = [plan for plan in plans if plan != lease.get("body")]
                digest = fingerprint(n.saved_schedule | {"planList": originals})
        if digest == self.original:
            catalogue: dict[str, list[int]] = {}
            for plan in originals:
                tag, identifier = plan.get("idTag"), plan.get("planId")
                if isinstance(tag, str) and type(identifier) is int:
                    catalogue.setdefault(fingerprint(tag.casefold()), []).append(identifier)
            if catalogue != j.get("managed_schedule_tags"):
                j.put("managed_schedule_tags", catalogue)
        pending = j.get("managed_deferred_start", {})
        local = pending.get("local")
        if (
            local is None
            or local != j.active()
            or local != n.physical.native_transaction_id
            or not n.physical.transaction_open
        ):
            return
        start = json.loads(j.db.execute("SELECT start FROM transactions WHERE local=?", (local,)).fetchone()["start"])
        tag = start["idTag"]
        catalogue = j.get("managed_schedule_tags")
        if catalogue is None:
            return
        digest = fingerprint(tag.casefold())
        if digest in catalogue:
            plan_id = n.session_plan_id
            if (
                plan_id is None
                or plan_id in catalogue[digest]
                or any(plan_id == lease["body"]["planId"] for lease in timers.values())
                or any(plan.get("planId") == plan_id for plan in plans)
            ):
                return
        if pending["owner"] and pending["vendor"] is not None:
            return
        with j.atomic():
            if pending["owner"] and local_start_receipt(j).get("ticket") == pending["ticket"]:
                self.gateway.controls.bind_start(tag, local)
                if self.gateway.start_observer is not None:
                    self.gateway.start_observer(local, start)
            elif pending["vendor"] is not None and j.get("vendor_start_grant", {}).get("token") == pending["vendor"]:
                self.gateway.authorisation.consume(tag, local)
            j.put("managed_deferred_start", {})
