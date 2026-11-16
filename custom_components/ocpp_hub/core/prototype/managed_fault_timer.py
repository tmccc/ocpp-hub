"""A paused, acknowledged recovery proof can remove one exact owned timer while retaining zero."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .managed_cutoffs import OBSERVABLE
from .managed_occurrences import HEAD, IDENTITY, KEY, occurrences
from .store import Journal, RecoveryRequiredError, encoded

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence


# Native observations may refresh source facts, but never replace the receipted body, window or preimage.
def occurrence_identity(lease: dict[str, Any]) -> str:
    return encoded({key: lease[key] for key in IDENTITY if key in lease})


# These are real source obligations; neither inventory removal nor a zero meter settles them.
def timer_source_debt(lease: dict[str, Any]) -> bool:
    return any(
        lease.get(key)
        for key in (
            "recovery_session",
            "closure_gap",
            "unreported_native",
            "pending_native_reports",
            "temporary_attempts",
            "unidentified_local",
        )
    )


# Each inventory exception retains the exact durable timer argument and management request revision.
def receipted_timer_requests(j: Journal) -> set[tuple[str, str, int]]:
    result = set()
    for lease in occurrences(j).values():
        row = j.db.execute(
            "SELECT r.id,r.action,r.revision,s.start_epoch FROM managed_requests r "
            "JOIN managed_schedule_requests s ON s.id=r.id WHERE r.id=?",
            (lease.get("id"),),
        ).fetchone()
        if (
            row is None
            or row["action"] != "temporary_schedule"
            or row["revision"] != lease.get("revision")
            or row["start_epoch"] != lease.get("start_epoch")
        ):
            raise RecoveryRequiredError("fault timer inventory lost its exact request receipt")
        result.add((row["id"], row["action"], row["revision"]))
    return result


@dataclass
class PausedReleaseAuthority:
    """The existing release policy supplies the common exact fault/Pause/Boot/debt guard."""

    c: ManagedController
    common: Callable[[FaultTimerRemoval | None], None]
    inventory: str
    head: Any

    # Freeze only immutable receipt identities, leaving independently observed closure updates possible.
    @classmethod
    def capture(
        cls, c: ManagedController, common: Callable[[FaultTimerRemoval | None], None]
    ) -> PausedReleaseAuthority:
        j = c.session.journal
        return cls(c, common, encoded({key: occurrence_identity(v) for key, v in occurrences(j).items()}), j.get(HEAD))

    # A newer or replaced occurrence revokes the old proof even when its physical plan ID happens to match.
    def require_inventory(self) -> None:
        j = self.c.session.journal
        receipted_timer_requests(j)
        if (
            self.inventory != encoded({key: occurrence_identity(v) for key, v in occurrences(j).items()})
            or j.get(HEAD) != self.head
        ):
            raise RecoveryRequiredError("fault recovery timer receipts changed")

    # Reading under the exact acknowledged Pause grants no timer write or positive authority.
    def guard_timer_observation(self) -> None:
        self.common(None)
        self.require_inventory()

    # Completed inventory may retain cutoff duties, while every genuine source/report debt remains blocking.
    def guard_settled(self) -> None:
        self.guard_timer_observation()
        j = self.c.session.journal
        if (
            any(v.get("state") != "complete" or timer_source_debt(v) for v in occurrences(j).values())
            or j.db.execute("SELECT 1 FROM managed_pending_native_reports LIMIT 1").fetchone()
        ):
            raise RecoveryRequiredError("fault recovery awaits timer inventory and genuine source reconciliation")
        if any(
            j.db.execute(
                "SELECT 1 FROM managed_requests WHERE id=? AND state IN ('accepted','running')", (key,)
            ).fetchone()
            for key in occurrences(j)
        ):
            raise RecoveryRequiredError("fault recovery awaits terminal timer request receipts")

    # Only this capability may exempt its own current clear RPC from the otherwise unchanged pending-call fence.
    def guard_timer_removal(self, removal: FaultTimerRemoval) -> None:
        if removal.authority is not self or self.c.session.book.fault_timer_removal is not removal:
            raise RecoveryRequiredError("fault timer removal scope changed")
        self.common(removal)
        removal.require_live_occurrence()
        if self.c.session.gateway.charger._pending:
            removal.require_validation_call()

    # The same closed known session, original configuration and independently receipted owned zero prove both stages.
    def _require_zero(self, native: HandoverEvidence, schedule_digest: str) -> None:
        c, j = self.c, self.c.session.journal
        c.native.require(native, closed=True)
        c.native.require_known_session(native)
        if (
            native.physical.transaction_open
            or any(v != 0 for v in native.currents_a + native.powers_w)
            or native.saved_schedule_digest != schedule_digest
            or j.get("trial_binding")
            != {
                "configuration_digest": c.native.binding_digest(native),
                "station": c.native.choice.value(),
                "boot_digest": c.native.spec.boot_digest,
            }
        ):
            raise RecoveryRequiredError("release reconciliation awaits closed native zero and exact configuration")
        station = [
            r
            for r in c.session.book.owned_present(native.physical)
            if r.kind == "profile" and r.identifier == HUB_PROFILE_ID
        ]
        row = j.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        if (
            len(station) != 1
            or station[0].canonical.get("limit") != 0
            or row is None
            or row["state"] != "confirmed"
            or row["confirmed"] is None
            or json.loads(row["confirmed"]) != station[0].canonical
        ):
            raise RecoveryRequiredError("release reconciliation awaits independently confirmed owned station zero")

    # Only the exact receipted augmented digest is admissible before its restrictive removal.
    def require_timer_zero(self, native: HandoverEvidence, removal: FaultTimerRemoval) -> None:
        self.guard_timer_removal(removal)
        self._require_zero(native, removal.require_live_occurrence()["augmented_digest"])

    # Final fault retirement always requires restoration of the original saved timetable.
    def require_original_zero(self, native: HandoverEvidence) -> None:
        self.guard_settled()
        self._require_zero(native, self.c.native.spec.saved_schedule_digest)

    # Observation can see only the original timetable or one exact receipted installed variant.
    def require_observed_zero(self, native: HandoverEvidence) -> None:
        self.guard_timer_observation()
        allowed = {self.c.native.spec.saved_schedule_digest} | {
            v.get("augmented_digest")
            for v in occurrences(self.c.session.journal).values()
            if v.get("state") in OBSERVABLE
        }
        if native.saved_schedule_digest not in allowed:
            raise RecoveryRequiredError("fault recovery observed an unreceipted timetable")
        self._require_zero(native, native.saved_schedule_digest)

    # Capture a new one-call removal only after the preceding native read published its actual source observations.
    def removal(self) -> FaultTimerRemoval:
        return FaultTimerRemoval.capture(self)


@dataclass
class FaultTimerRemoval:
    """One exact restrictive clear, bound to live native validation and never usable for another command."""

    authority: PausedReleaseAuthority
    identifier: str
    identity: str
    payload: dict[str, Any]
    payload_json: str
    token: str = field(default_factory=lambda: "ft-" + uuid.uuid4().hex)
    verified_native_digest: str | None = None
    future: asyncio.Future[Frame] | None = None

    # Bind the clear payload to the one current observable owned occurrence, independently of mutable observations.
    @classmethod
    def capture(cls, authority: PausedReleaseAuthority) -> FaultTimerRemoval:
        j = authority.c.session.journal
        lease = j.get(KEY, {})
        if lease.get("state") not in OBSERVABLE:
            raise RecoveryRequiredError("fault cleanup requires an observable owned timer")
        payload = {"vendorId": "IoCharger", "messageId": "clearPlans", "data": encoded([lease["body"]["planId"]])}
        return cls(authority, lease["id"], occurrence_identity(lease), payload, encoded(payload))

    # A refreshed observation is allowed; a replaced working occurrence or new genuine source obligation is not.
    def require_live_occurrence(self) -> dict[str, Any]:
        c, j = self.authority.c, self.authority.c.session.journal
        self.authority.require_inventory()
        lease: dict[str, Any] = j.get(KEY, {})
        saved = occurrences(j)
        if (
            lease.get("id") != self.identifier
            or lease.get("state") not in OBSERVABLE
            or occurrence_identity(lease) != self.identity
            or any(timer_source_debt(v) for v in saved.values())
            or any(v.get("state") != "complete" for key, v in saved.items() if key != self.identifier)
            or c.schedule.awaiting_start()
            or j.db.execute("SELECT 1 FROM managed_pending_native_reports LIMIT 1").fetchone()
        ):
            raise RecoveryRequiredError("fault timer removal awaits its exact settled occurrence")
        return lease

    # This lightweight mid-audit check must not reject the native read's own durable repair-audit fence.
    def require_validation_call(self) -> None:
        c = self.authority.c
        pending = c.session.gateway.charger._pending
        future = pending.get(self.token)
        if (
            c.session.book.fault_timer_removal is not self
            or set(pending) != {self.token}
            or future is None
            or future.done()
            or c.session.gateway.controls._operations
            or self.future is not None
            and self.future is not future
        ):
            raise RecoveryRequiredError("fault timer validation requires its exact pending call")
        self.future = future
        self.require_live_occurrence()

    # Object identity, immutable content and exact plan ID bind every write barrier to this single call.
    def require_payload(self, action: str, payload: dict[str, Any]) -> None:
        if (
            self.authority.c.session.book.fault_timer_removal is not self
            or action != "DataTransfer"
            or payload is not self.payload
            or encoded(payload) != self.payload_json
            or payload["data"] != encoded([self.require_live_occurrence()["body"]["planId"]])
        ):
            raise RecoveryRequiredError("fault timer removal does not name its exact owned payload")
