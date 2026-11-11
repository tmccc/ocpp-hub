"""One owner-authorized, date-bound native timer alongside the immutable daily schedule."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .ics_writes import validate_write
from .policy import DeniedError, integer
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, encoded, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .managed import ManagedController

KEY = "managed_temporary_schedule"
OBSERVABLE = {"installing", "armed", "clearing"}


# A temporary plan is a separate, explicitly receipted native resource; the original schedule stays pinned.
def installed_variant(journal: Journal, original: str, actual: str) -> str:
    lease = journal.get(KEY, {})
    if (
        lease.get("state") in OBSERVABLE
        and lease.get("original_digest") == original
        and actual == lease.get("augmented_digest")
    ):
        return actual
    return original


class ManagedSchedule:
    """The supervised tick owns both timer installation and exact removal, including after replacement."""

    # Receipt arguments outlive the active timer so old command IDs cannot arm a new time.
    def __init__(self, controller: ManagedController) -> None:
        self.c = controller
        self.j = controller.session.journal
        self.j.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_schedule_requests(id TEXT PRIMARY KEY, start_epoch INTEGER NOT NULL)"
        )
        lease = self.j.get(KEY, {})
        if lease.get("state") == "requested":
            self.finish(lease, "cancelled_before_install")

    # Expose only timing and progress, without private native resources or tag data.
    def summary(self) -> dict[str, Any]:
        lease = self.j.get(KEY, {})
        return {
            k: lease[k]
            for k in (
                "state",
                "start_epoch",
                "result",
                "verified_at",
                "session_seen",
                "timer_present",
                "native_plan_confirmed",
                "last_response",
            )
            if k in lease
        }

    # Commit the immutable request, independent prior ledger boundary, expected native bodies and restoration intent.
    def accept(self, identifier: str, revision: int, start_epoch: int) -> dict[str, Any]:
        c, j = self.c, self.j
        start_epoch = integer(start_epoch, 1)
        previous = c.requests.existing(identifier, "temporary_schedule", revision)
        if previous is not None:
            arg = j.db.execute("SELECT start_epoch FROM managed_schedule_requests WHERE id=?", (identifier,)).fetchone()
            if arg is None or arg[0] != start_epoch:
                raise SecurityError("schedule request ID already binds another time")
            return previous
        n = c.latest_native
        now = time.time()
        if (
            j.get(KEY, {}).get("state") not in (None, "complete")
            or c.requests.intent().revision != revision
            or not self.can_install(n)
            or n is None
            or n.saved_schedule is None
        ):
            raise RecoveryRequiredError("temporary schedule needs fresh idle app control")
        try:
            zone = ZoneInfo(n.configuration["timeZone"])
        except KeyError, TypeError, ValueError, ZoneInfoNotFoundError:
            raise SecurityError("a configured charger time zone is required") from None
        start = datetime.fromtimestamp(start_epoch, zone)
        if (
            start_epoch % 60
            or not 30 <= start_epoch - now <= 24 * 3600
            or start.date() != datetime.fromtimestamp(now, zone).date()
            or start.replace(fold=0).utcoffset() != start.replace(fold=1).utcoffset()
        ):
            raise SecurityError("temporary timer requires a future whole minute on the current local date")
        plans = n.saved_schedule["planList"]
        if len(plans) != 1 or n.saved_schedule_digest != c.native.spec.saved_schedule_digest:
            raise SecurityError("temporary schedule requires the single admitted daily plan")
        original = plans[0]
        if (
            original.get("kindType") != "Recurring"
            or original.get("recurring") != 127
            or original.get("startTime") != 0
            or original.get("stopModel") != "Duration"
            or original.get("ConnectorId") != 1
            or original.get("maxEnergy") != 0
            or original.get("type") != "Plan"
            or original.get("eco") != "Fast"
        ):
            raise SecurityError("unsupported native daily timer")
        duration = integer(original["stopValue"], 1, 1440)
        next_midnight = (start + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        if start_epoch + duration * 60 >= next_midnight.timestamp():
            raise SecurityError("temporary timer would overlap the next daily start")
        amp = integer(original["limitCurrent"], 60, 320)
        amp = min(amp, c.current.selected() * 10)
        # Use a higher free 16-bit ID, as in the captured two-plan native ordering.
        lower = max(10000, integer(original["planId"], 1, 65534) + 1)
        plan_id = lower + int(identifier[:8], 16) % (65536 - lower)
        if (
            any(r.kind == "plan" and r.identifier == plan_id for r in n.physical.resources)
            or j.db.execute("SELECT 1 FROM plans WHERE local=? OR vendor=?", (plan_id, plan_id)).fetchone()
        ):
            raise RecoveryRequiredError("temporary plan identifier already exists")
        body = {
            **original,
            "kindType": "Relative",
            "recurring": 0,
            "planId": plan_id,
            "startTime": start.hour * 60 + start.minute,
            "validTime": datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%dT%H:%M:00Z"),
            "limitCurrent": amp,
            "stopValue": duration,
        }
        wire = {k: v for k, v in body.items() if k not in ("ConnectorId", "maxEnergy")}
        wire.update(connectorId=1, limitCurrent=amp / 10, stopValue=float(duration))
        payload = {"vendorId": "IoCharger", "messageId": "setPlan", "data": encoded(wire)}
        validate_write(payload, c.session.gateway.policy)
        lease = {
            "id": identifier,
            "revision": revision,
            "state": "requested",
            "start_epoch": start_epoch,
            "floor": j.get("next_transaction"),
            "body": body,
            "install": payload,
            "original_digest": n.saved_schedule_digest,
            "augmented_digest": fingerprint(n.saved_schedule | {"planList": [*plans, body]}),
            "epoch": c.session.gateway._charger_epoch,
            "ticket": j.get("control", {}).get("ticket"),
        }
        with j.atomic():
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?, 'temporary_schedule', ?, 'accepted', ?)",
                (identifier, revision, encoded({"accepted_at": now})),
            )
            j.db.execute("INSERT INTO managed_schedule_requests VALUES(?,?)", (identifier, start_epoch))
            j.put(KEY, lease)
        return c.requests.receipt(identifier)

    # Reuse current-selection readiness, including idle native evidence, connected peers, debt, Boot and ownership.
    def can_install(self, n: HandoverEvidence | None) -> bool:
        c = self.c
        return (
            c.requests.intent().mode == "app"
            and c.current.available(n)
            and n is not None
            and n.physical.connection == "Plug"
            and c.session.runtime.status.vendor_connected
            and c.session.gateway.vendor_registration == "Accepted"
        )

    # Finishing a timer never changes control intent, selected amps, the original plan or a transaction.
    def finish(self, lease: dict[str, Any], reason: str) -> None:
        with self.j.atomic():
            self.j.put(KEY, {**lease, "state": "complete", "result": reason, "verified_at": time.time()})
            self.c.requests.finish(
                lease["id"], "completed" if reason == "session_closed" else "failed", {"classification": reason}
            )

    # Mark sent intent at the final transport boundary, and reconcile acknowledgements or lost replies by readback.
    async def send(self, lease: dict[str, Any], adding: bool) -> None:
        c, j = self.c, self.j
        g = c.session.gateway
        epoch = g._charger_epoch
        payload = (
            lease["install"]
            if adding
            else {"vendorId": "IoCharger", "messageId": "clearPlans", "data": encoded([lease["body"]["planId"]])}
        )

        # Check again inside the write gate after waiting for the independent native reads.
        def check() -> None:
            current = j.get(KEY, {})
            if (
                current != lease
                or c.stopping
                or j.get("trial_phase") != "active"
                or j.get("managed_control_fault")
                or not g.current_boot_seen(epoch)
                or epoch != g._charger_epoch
                or j.active() is not None
            ):
                raise UnavailableError("timer write superseded")
            if adding and (
                lease["epoch"] != epoch
                or lease["ticket"] != j.get("control", {}).get("ticket")
                or c.requests.intent().revision != lease["revision"]
                or c.requests.intent().mode != "app"
                or j.get("control", {}).get("inhibit")
                or j.get("control", {}).get("uncertain")
                or j.get("control", {}).get("owner") != "shared"
                or j.get("control", {}).get("start_pending")
                or j.get("uncertain_vendor_command")
                or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
                or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
                or c.requests.pending_return() is not None
                or g.controls._operations
                or c.current.pending()
                or c.tests.lease is not None
                or j.get("vendor_start_grant")
                or not c.session.runtime.status.vendor_connected
                or g.vendor_registration != "Accepted"
                or not g.controls.ready
                or g.controls.desired() != c.current.selected()
                or lease["start_epoch"] - time.time() < 10
            ):
                raise UnavailableError("scheduled start authority changed")

        # Persist this exact send before the protocol can deliver it to the charger.
        def mark() -> None:
            j.put(KEY, {**lease, "state": "installing" if adding else "clearing", "sent_at": time.time()})
            c.session.book.temporary_plan_write = payload

        try:
            reply = await g.charger.call("DataTransfer", payload, check=check, before_send=mark)
            result = "accepted" if reply[0] == 3 and reply[2] == {"status": "Accepted", "data": ""} else "unconfirmed"
        except UnavailableError, DeniedError, RecoveryRequiredError:
            result = "unconfirmed"
        finally:
            c.session.book.temporary_plan_write = None
        current = j.get(KEY)
        if (
            current["id"] == lease["id"]
            and current["state"] in ("installing", "clearing")
            and g.current_boot_seen(epoch)
        ):
            j.put(KEY, {**current, "last_response": result})

    # Filter new ledger facts by timestamp and tag; only an independent active native plan ID can prove provenance.
    def sessions(self, lease: dict[str, Any]) -> dict[int, bool]:
        candidates = {}
        for row in self.j.db.execute(
            "SELECT local,start,stop FROM transactions WHERE local>? ORDER BY local", (lease["floor"],)
        ):
            start = json.loads(row["start"])
            when = datetime.fromisoformat(start["timestamp"]).timestamp()
            if (
                start["idTag"].casefold() == lease["body"]["idTag"].casefold()
                and lease["start_epoch"] - 5 <= when <= lease["start_epoch"] + 600
            ):
                candidates[int(row["local"])] = row["stop"] is not None
        return candidates

    # Retire a missed or superseded timer while still under verified zero, before Return or restart raises the ceiling.
    async def before_release(self, n: HandoverEvidence) -> None:
        lease = self.j.get(KEY, {})
        if lease.get("state") not in OBSERVABLE:
            return
        intent = self.c.requests.intent()
        if (
            lease.get("retire_reason")
            or lease["state"] == "clearing"
            or time.time() >= lease["start_epoch"]
            or intent.revision != lease["revision"]
            or intent.mode == "paused"
        ):
            if not lease.get("retire_reason"):
                self.j.put(
                    KEY,
                    {
                        **lease,
                        "retire_reason": "missed_while_held"
                        if time.time() >= lease["start_epoch"]
                        else "owner_superseded",
                    },
                )
            await self.observe(n)
            if self.j.get(KEY)["state"] != "complete":
                await self.observe(await self.c.native.read(closed=True))
            if self.j.get(KEY)["state"] != "complete":
                raise RecoveryRequiredError("positive release awaits verified native timer removal")

    # The daily plan stays enabled. Only a fresh verified closed session or missed/cancelled timer permits cleanup.
    async def observe(self, n: HandoverEvidence) -> None:
        c, j = self.c, self.j
        lease = j.get(KEY, {})
        state = lease.get("state")
        if state not in {"requested", *OBSERVABLE}:
            return
        now = time.time()
        present = n.saved_schedule_digest == lease["augmented_digest"]
        if state == "requested":
            if (
                not self.can_install(n)
                or c.requests.intent().revision != lease["revision"]
                or lease["epoch"] != c.session.gateway._charger_epoch
                or now >= lease["start_epoch"] - 10
            ):
                self.finish(lease, "cancelled_before_install")
            else:
                await self.send(lease, True)
            return
        candidates = self.sessions(lease)
        actual_id = n.physical.native_transaction_id
        changes: dict[str, Any] = {}
        if present and state == "installing":
            changes.update(state="armed", verified_at=now)
        if (
            lease.get("bound_local") is None
            and actual_id in candidates
            and n.physical.transaction_open
            and n.session_plan_id == lease["body"]["planId"]
        ):
            changes.update(native_plan_confirmed=True, bound_local=actual_id)
        if candidates and not lease.get("session_seen"):
            changes["session_seen"] = True
        if lease.get("timer_present") is not present:
            changes["timer_present"] = present
        if changes:
            lease = {**lease, **changes}
            j.put(KEY, lease)
        closed = candidates.get(lease.get("bound_local")) is True
        idle = not n.physical.transaction_open and j.active() is None and all(v == 0 for v in n.currents_a + n.powers_w)
        cancelled = c.requests.intent().mode == "paused" or c.requests.intent().revision != lease["revision"]
        reason = lease.get("retire_reason")
        if closed:
            reason = "session_closed"
        elif not reason and cancelled:
            reason = "owner_superseded"
        elif not reason and state == "armed" and not present and now < lease["start_epoch"] - 5:
            reason = "disappeared_before_start"
        elif not reason and now >= lease["start_epoch"] + 600:
            reason = "session_source_unverified" if any(candidates.values()) else "no_scheduled_session"
        should_remove = bool(reason)
        if not idle or not should_remove:
            return
        if not present:
            # A timed-out install can still arrive late. Retain its ownership through the start grace period.
            if state == "installing" and lease.get("last_response") != "accepted":
                if now < lease["start_epoch"] + 600:
                    return
                if not lease.get("absence_verified_at"):
                    j.put(KEY, {**lease, "absence_verified_at": now})
                    return
                if now - lease["absence_verified_at"] < 2:
                    return
            if n.saved_schedule_digest != lease["original_digest"]:
                raise SecurityError("original native timer is not confirmed")
            self.finish(lease, str(reason))
        elif state != "clearing" or now - lease["sent_at"] >= 30:
            lease = {**lease, "retire_reason": reason}
            j.put(KEY, lease)
            await self.send(lease, False)
