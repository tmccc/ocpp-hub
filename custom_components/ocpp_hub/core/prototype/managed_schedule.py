"""One owner-authorized, date-bound native timer alongside the immutable daily schedule."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..control import HUB_PROFILE_ID
from .ics_writes import validate_write
from .policy import DeniedError, integer
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, encoded, fingerprint
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


# Remember a verified cable removal even if the next supervisor tick sees a newly connected vehicle.
def remember_disconnect(journal: Journal, observed_at: float | None = None) -> None:
    lease = journal.get(KEY, {})
    if (
        lease.get("state") in {"requested", *OBSERVABLE}
        and not lease.get("retire_reason")
        # The station reports whole seconds. A fresh or future-dated departure cannot establish cable continuity.
        and (observed_at is None or observed_at >= int(lease.get("accepted_epoch", 0)) - 5)
    ):
        journal.put(KEY, {**lease, "retire_reason": "vehicle_disconnected"})


# An existing commissioned timer and an owned station receipt allow a zero-only restart during a telemetry outage.
def recorded_timer_hold(j: Journal, spec_digest: str, boot_digest: str) -> bool:
    row = j.db.execute("SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)).fetchone()
    return (
        j.get(KEY, {}).get("state") in OBSERVABLE
        and j.get("trial_phase") in ("active", "recovering")
        and j.get("managed_mode", {}).get("spec_digest") == spec_digest
        and j.get("trial_binding", {}).get("boot_digest") == boot_digest
        and j.get("native_bounded_handover", {}).get("stage") == "admitted"
        and row is not None
        and row["state"] != "absent_verified"
        and not j.get("managed_control_fault")
        and not j.get("trial_inventory_fault")
        and not j.get("trial_preservation_fault")
    )


# A matching transaction with a different original plan or accepted tag identifies independent work.
def independent_session(j: Journal, lease: dict[str, Any], n: HandoverEvidence, local: int) -> bool:
    if (
        not n.physical.transaction_open
        or n.physical.native_transaction_id != local
        or n.session_plan_id == lease["body"]["planId"]
    ):
        return False
    if n.saved_schedule is not None and any(
        plan["planId"] == n.session_plan_id for plan in n.saved_schedule["planList"]
    ):
        return True
    row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
    return row is not None and json.loads(row["start"])["idTag"].casefold() != lease["body"]["idTag"].casefold()


# A provisional hold may be lifted only for this same open transaction on a positively identified original plan.
def require_continuing_session(j: Journal, n: HandoverEvidence, local: int) -> None:
    lease = j.get(KEY, {})
    if (
        lease.get("state") not in OBSERVABLE
        or lease.get("unidentified_local") != local
        or j.active() != local
        or not independent_session(j, lease, n, local)
        or j.get("managed_control_intent", {"mode": "app"})["mode"] != "app"
    ):
        raise ReleaseDeferredError("the held transaction awaits positive identification of its original plan")


# The two independent closure indicators may arrive in either order for a previously identified one-off.
def known_timer_closure_gap(journal: Journal, native: HandoverEvidence) -> bool:
    lease = journal.get(KEY, {})
    local, physical = journal.active(), native.physical.native_transaction_id
    bound = lease.get("bound_local")
    return (
        lease.get("state") in OBSERVABLE
        and lease.get("native_plan_confirmed") is True
        and bound is not None
        and local != physical
        and {local, physical} == {None, bound}
    )


# Resolve the owner's wall-clock end on the date of the actual occurrence, including offset changes before 06:00.
def cutoff_epoch(start_epoch: int, zone: ZoneInfo) -> int:
    start = datetime.fromtimestamp(start_epoch, zone)
    return int(start.replace(hour=6, minute=0, second=0, microsecond=0, fold=0).timestamp())


class TimerVehicleDisconnectedError(UnavailableError):
    """The final native read saw the cable removed before the timer could be sent."""


class ManagedSchedule:
    """The supervised tick owns both timer installation and exact removal, including after replacement."""

    # Receipt arguments outlive the active timer so old command IDs cannot arm a new time.
    def __init__(self, controller: ManagedController) -> None:
        self.c = controller
        self.changed = asyncio.Event()
        self.j = controller.session.journal
        self.j.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_schedule_requests(id TEXT PRIMARY KEY, start_epoch INTEGER NOT NULL)"
        )
        controller.session.gateway.status_observer = self.observe_status
        controller.session.gateway.tag_guard = self.permits_tag
        controller.session.gateway.held_start_report = self.can_record_held_start
        controller.session.gateway.remote_start_guard = self.permits_remote_start
        lease = self.j.get(KEY, {})
        if lease.get("state") == "requested":
            self.finish(lease, "cancelled_before_install")

    # Keep a connector-1 Available transition even when the next status or native poll sees its replacement.
    def observe_status(self, payload: dict[str, Any]) -> None:
        if payload["connectorId"] == 1 and payload["status"] == "Available":
            when = datetime.fromisoformat(payload["timestamp"]).timestamp() if "timestamp" in payload else None
            remember_disconnect(self.j, when)

    # Revocation takes effect before asynchronous physical cleanup, including across a clock correction or restart.
    def revoked(self, lease: dict[str, Any]) -> bool:
        return lease.get("state") in OBSERVABLE and (
            bool(lease.get("retire_reason"))
            or "hold_since" in lease
            or time.time() >= lease.get("end_epoch", float("inf"))
        )

    # The one-off owns its start window until retirement. A second RemoteStart cannot share its lifecycle token.
    def permits_remote_start(self) -> bool:
        return self.j.get(KEY, {}).get("state") not in {"requested", *OBSERVABLE}

    # An older version may already have acknowledged a separate owner Start before the charger reports it.
    def awaiting_start(self) -> bool:
        grant = self.j.get("vendor_start_grant", {})
        return bool(self.j.get("control", {}).get("start_pending") or grant and grant.get("local") is None)

    # Record the genuine delayed lifecycle of an already accepted owner command under verified station zero.
    def can_record_held_start(self, tag: str) -> bool:
        c, j = self.c, self.j
        g = c.session.gateway
        lease = j.get(KEY, {})
        if (
            lease.get("state") not in OBSERVABLE
            or "hold_since" not in lease
            or j.active() is not None
            or c.requests.intent().mode != "app"
            or c.stopping
            or j.get("managed_control_fault")
            or not g.controls.ready
            or g.controls.desired() != 0
            or not self.hold_authorized()
        ):
            return False
        prior = c.tests.state()
        if (
            j.get("control", {}).get("start_pending")
            and prior.get("start_rpc_accepted")
            and prior.get("tag_digest") == fingerprint(tag.casefold())
        ):
            return True
        grant = j.get("vendor_start_grant", {})
        return bool(
            grant.get("local") is None
            and g.authorisation.allows(tag)
            and j.db.execute(
                "SELECT 1 FROM mutations WHERE token=? AND origin='vendor' "
                "AND action='RemoteStartTransaction' AND state='confirmed'",
                (grant.get("mutation"),),
            ).fetchone()
        )

    # Never grant a new Start to the retired timer's tag while its physical removal remains unverified.
    def permits_tag(self, tag: str) -> bool:
        lease = self.j.get(KEY, {})
        return not (self.revoked(lease) and tag.casefold() == lease["body"]["idTag"].casefold())

    # Expose only timing and progress, without private native resources or tag data.
    def summary(self) -> dict[str, Any]:
        lease = self.j.get(KEY, {})
        return {
            k: lease[k]
            for k in (
                "state",
                "start_epoch",
                "end_epoch",
                "cleanup_attempts",
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
        if not 30 <= start_epoch - now <= 24 * 3600:
            raise SecurityError("a future start within the next day is required")
        start = datetime.fromtimestamp(start_epoch, zone)
        end_epoch = cutoff_epoch(start_epoch, zone)
        if (
            start_epoch % 60
            or start_epoch >= end_epoch
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
            or original.get("stopValue") != 360
            or original.get("ConnectorId") != 1
            or original.get("maxEnergy") != 0
            or original.get("type") != "Plan"
            or original.get("eco") != "Fast"
        ):
            raise SecurityError("unsupported native daily timer")
        duration = (end_epoch - start_epoch) // 60
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
            "accepted_epoch": now,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch,
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
        self.changed.set()
        return c.requests.receipt(identifier)

    # Reuse current-selection readiness, including idle native evidence, connected peers, debt, Boot and ownership.
    def can_install(self, n: HandoverEvidence | None) -> bool:
        c = self.c
        return (
            c.requests.intent().mode == "app"
            and c.current.available(n, for_schedule=True)
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
            result = "accepted" if reply[0] == 3 and reply[2] == {"status": "Accepted", "data": ""} else "rejected"
        except TimerVehicleDisconnectedError:
            current = j.get(KEY, {})
            if adding and current.get("id") == lease["id"] and current.get("state") == "requested":
                self.finish(current, "cancelled_before_install")
            return
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

    # A matching live native plan and a new ledger transaction can identify a delayed Start outside the grace window.
    def sessions(self, lease: dict[str, Any], native_local: int | None) -> dict[int, bool]:
        candidates = {}
        for row in self.j.db.execute(
            "SELECT local,start,stop FROM transactions WHERE local>? ORDER BY local", (lease["floor"],)
        ):
            local = int(row["local"])
            start = json.loads(row["start"])
            when = datetime.fromisoformat(start["timestamp"]).timestamp()
            if start["idTag"].casefold() == lease["body"]["idTag"].casefold() and (
                local in (native_local, lease.get("bound_local"))
                or lease["start_epoch"] - 5 <= when <= lease["start_epoch"] + 600
            ):
                candidates[local] = row["stop"] is not None
        return candidates

    # Re-evaluate the persisted timer and owner intent at the last positive station write, after all awaited reads.
    def release_needs_cleanup(self, lease: dict[str, Any]) -> bool:
        intent = self.c.requests.intent()
        return lease.get("state") in OBSERVABLE and (
            self.revoked(lease)
            or lease["state"] == "clearing"
            or time.time() >= lease["start_epoch"]
            or intent.revision != lease.get("control_revision", lease["revision"])
            or intent.mode == "paused"
        )

    # A withheld positive write can be retried after a verified zero and independent native removal.
    def check_release(self) -> None:
        if self.release_needs_cleanup(self.j.get(KEY, {})):
            raise ReleaseDeferredError("positive release awaits verified native timer removal")

    # Retire a missed or superseded timer while still under verified zero, before Return or restart raises the ceiling.
    async def before_release(self, n: HandoverEvidence) -> None:
        lease = self.j.get(KEY, {})
        if self.release_needs_cleanup(lease):
            if not lease.get("retire_reason") and "hold_since" not in lease:
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
                raise ReleaseDeferredError("positive release awaits verified native timer removal")

    # Report unresolved physical work without freezing the supervisor that can still verify its recovery.
    def problem(self) -> str | None:
        lease = self.j.get(KEY, {})
        if lease.get("state") not in OBSERVABLE:
            return None
        now = time.time()
        if "cleanup_since" in lease and (
            lease.get("last_response") == "rejected" or now - lease["cleanup_since"] >= 60
        ):
            return "temporary_schedule_cleanup_unconfirmed"
        if (
            "hold_since" in lease
            and now - lease["hold_since"] >= 60
            and (
                lease.get("closure_gap")
                or self.j.active() is not None
                and self.j.active() in (lease.get("bound_local"), lease.get("unidentified_local"))
            )
        ):
            return "temporary_schedule_stop_unconfirmed"
        return None

    # Journal the hold before any I/O. Recovery owns the next positive write until original-plan verification.
    def begin_hold(self, lease: dict[str, Any]) -> None:
        self.c.released_epoch = None
        if "hold_since" not in lease:
            self.j.put(KEY, {**lease, "hold_since": time.time()})

    # A current matching Boot and the recorded station profile permit only a restrictive action during reintroduction.
    def hold_authorized(self) -> bool:
        c, j = self.c, self.j
        g = c.session.gateway
        if (
            c.stopping
            or not g.current_boot_seen(g._charger_epoch)
            or fingerprint(j.get("boot")) != c.native.spec.boot_digest
        ):
            return False
        if c.native.introduced_epoch == g._charger_epoch and c.native.introduced_revision == g.controls._revision:
            return True
        return recorded_timer_hold(j, c.native.spec.digest, c.native.spec.boot_digest)

    # Lower only the station ceiling, even if native HTTP is unavailable. A foreign ledger transaction has priority.
    async def protect_station(self, lease: dict[str, Any]) -> None:
        c, j = self.c, self.j
        g = c.session.gateway
        if not self.hold_authorized():
            return
        if j.active() not in (None, lease.get("bound_local"), lease.get("unidentified_local")):
            return
        self.begin_hold(lease)
        if g.controls.ready and g.controls.desired() == 0:
            return
        lease = j.get(KEY)
        now = time.time()
        if now < lease.get("next_cap", 0):
            return
        j.put(KEY, {**lease, "next_cap": now + 15})

        # Recheck the actual receipt and generation before every outgoing write; this path never requests a Stop.
        def guard() -> None:
            if (
                not self.hold_authorized()
                or j.get(KEY, {}).get("id") != lease["id"]
                or j.active() not in (None, lease.get("bound_local"), lease.get("unidentified_local"))
            ):
                raise RecoveryRequiredError("timer station hold was superseded")

        try:
            if j.get("trial_phase") == "recovering" and g.controls.desired() == 0:
                await g.controls.ensure_ceiling(guard=guard)
            else:
                await g.controls.set_limit(0, guard=guard)
        except DeniedError, UnavailableError, RecoveryRequiredError:
            # A separate scoped Stop can still succeed when a station write fails. Never release on this evidence.
            return

    # The service owns this independent deadline wait so a slow health poll or a concurrent Pause cannot defer zero.
    async def watch_deadline(self) -> None:
        while not self.c.stopping:
            self.changed.clear()
            lease = self.j.get(KEY, {})
            delay = None
            if lease.get("state") in {"requested", *OBSERVABLE}:
                end = lease.get("end_epoch")
                if end is None:
                    # A pre-upgrade lease keeps its physical payload while gaining the admitted local deadline.
                    zone = ZoneInfo(self.c.native.spec.original_configuration["timeZone"])
                    end = cutoff_epoch(lease["start_epoch"], zone)
                    lease = {**lease, "end_epoch": end}
                    self.j.put(KEY, lease)
                remaining = end - time.time()
                if remaining <= 0 and not self.j.get("managed_control_fault"):
                    try:
                        await self.protect_deadline(lease)
                    except DeniedError, UnavailableError, RecoveryRequiredError:
                        pass  # Durable attempt times bound retries. Only actual closure can release the hold.
                delay = min(remaining, 30) if remaining > 0 else 1
            try:
                await asyncio.wait_for(self.changed.wait(), delay)
            except TimeoutError:
                pass

    # Stop a previously identified, still-open OCPP transaction by its own ID, even while HTTP reads are unavailable.
    async def protect_deadline(self, lease: dict[str, Any]) -> None:
        j = self.j
        if (
            lease.get("state") not in OBSERVABLE
            or time.time() < lease.get("end_epoch", float("inf"))
            and lease.get("retire_reason") != "vehicle_disconnected"
            or not self.hold_authorized()
        ):
            return
        bound, active = lease.get("bound_local"), j.active()
        if active is not None and active != bound:
            if active == lease.get("other_local"):
                return
            if lease.get("unidentified_local") != active:
                lease = {**lease, "unidentified_local": active}
                j.put(KEY, lease)
        if bound is None or not lease.get("native_plan_confirmed") or j.active() != bound or lease.get("closure_gap"):
            await self.protect_station(lease)
            return
        self.begin_hold(lease)
        await self.request_stop_if_due(j.get(KEY))

    # Re-check ownership inside LocalControl after the zero ceiling has been attempted, before sending any Stop.
    async def request_stop_if_due(self, lease: dict[str, Any]) -> None:
        now = time.time()
        if now >= lease.get("next_stop", 0):
            self.j.put(KEY, {**lease, "next_stop": now + 15})

            # The charger can deliver a new Boot or transaction while the physical zero is being verified.
            def guard() -> None:
                current = self.j.get(KEY, {})
                if (
                    not self.hold_authorized()
                    or current.get("id") != lease["id"]
                    or current.get("bound_local") != lease["bound_local"]
                    or self.j.active() not in (None, lease["bound_local"])
                ):
                    raise RecoveryRequiredError("the identified cutoff Stop was superseded")

            await self.c.session.gateway.controls.stop(expected_local=lease["bound_local"], guard=guard)

    # A known closing transaction needs zero, with no guessed Stop, until native and ledger closure agree.
    async def await_closure_agreement(self, native: HandoverEvidence) -> None:
        lease = self.j.get(KEY, {})
        if known_timer_closure_gap(self.j, native):
            if not lease.get("closure_gap"):
                lease = {**lease, "closure_gap": True}
                self.j.put(KEY, lease)
            await self.protect_station(lease)
            raise RecoveryRequiredError("identified timer awaits both closure indicators")
        if lease.get("closure_gap"):
            self.j.put(KEY, {**lease, "closure_gap": False})

    # While a cutoff hold is active, allow only the known scheduled session to receive a Stop.
    async def hold_until_closed(self, n: HandoverEvidence | None = None) -> None:
        c, j = self.c, self.j
        g = c.session.gateway
        if c.native.introduced_epoch != g._charger_epoch or not g.current_boot_seen(g._charger_epoch):
            raise RecoveryRequiredError("deadline hold awaits the current charger introduction")
        n = n or await c.native.read()
        await self.await_closure_agreement(n)
        lease = j.get(KEY)
        c.native.require_known_session(n)
        active = j.active()
        lease, _ = self.identify(lease, n)
        bound = lease.get("bound_local")
        if active is not None and active != bound:
            if lease.get("unidentified_local") == active and independent_session(j, lease, n, active):
                await c.restore_owner_control(continuing_local=active)
                current = j.get(KEY)
                if current["id"] == lease["id"] and current.get("unidentified_local") == active:
                    j.put(
                        KEY,
                        {
                            k: v
                            for k, v in current.items()
                            if k not in ("unidentified_local", "hold_since", "next_cap", "next_stop")
                        },
                    )
                return
            raise RecoveryRequiredError("deadline hold awaits reconciliation of the scheduled session")
        if (
            active is None
            and g.controls.ready
            and g.controls.desired() == 0
            and (j.get("trial_phase") == "recovering" or not j.get("control", {}).get("uncertain"))
        ):
            c.require_paused(n)
            return
        if bound is not None:
            await self.request_stop_if_due(lease)
        else:
            await self.protect_station(lease)
        if active is None and j.active() is None and g.controls.ready and g.controls.desired() == 0:
            c.require_paused(await c.native.read(closed=True))
            return
        raise RecoveryRequiredError("deadline hold awaits verified closure")

    # Bind the independently read source on ordinary polls and while an unidentified session is held at zero.
    def identify(self, lease: dict[str, Any], n: HandoverEvidence) -> tuple[dict[str, Any], dict[int, bool]]:
        j = self.j
        present = n.saved_schedule_digest == lease["augmented_digest"]
        state, now = lease["state"], time.time()
        actual_id = n.physical.native_transaction_id
        native_local = (
            actual_id if n.session_plan_id == lease["body"]["planId"] and n.physical.transaction_open else None
        )
        candidates = self.sessions(lease, native_local)
        changes: dict[str, Any] = {}
        if present and state == "installing":
            changes.update(state="armed", verified_at=now)
        previous = lease.get("bound_local")
        if native_local is not None and native_local in candidates and native_local != previous:
            if previous is None or candidates.get(previous) is True:
                changes.update(native_plan_confirmed=True, bound_local=native_local)
                if previous is not None:
                    changes["retire_reason"] = "session_closed"
        if candidates and not lease.get("session_seen"):
            changes["session_seen"] = True
        if lease.get("timer_present") is not present:
            changes["timer_present"] = present
        if actual_id == j.active() and actual_id is not None and independent_session(j, lease, n, actual_id):
            if lease.get("other_local") != actual_id:
                changes["other_local"] = actual_id
        if changes:
            lease = {**lease, **changes}
            j.put(KEY, lease)
        return lease, candidates

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
                lease.get("retire_reason")
                or not self.can_install(n)
                or c.requests.intent().revision != lease["revision"]
                or lease["epoch"] != c.session.gateway._charger_epoch
                or now >= lease["start_epoch"] - 10
            ):
                self.finish(lease, "cancelled_before_install")
            else:
                await self.send(lease, True)
            return
        if "end_epoch" not in lease:
            # An older armed lease has the original six-hour payload. Preserve its digest and enforce its local cutoff.
            lease = {**lease, "end_epoch": cutoff_epoch(lease["start_epoch"], ZoneInfo(n.configuration["timeZone"]))}
            j.put(KEY, lease)
        actual_id = n.physical.native_transaction_id
        lease, candidates = self.identify(lease, n)
        if self.awaiting_start() and j.active() is None and not n.physical.transaction_open:
            # Preserve a previously accepted lifecycle while the station remains inside the authorized window.
            if now < lease["end_epoch"] and not lease.get("retire_reason") and c.requests.intent().mode == "app":
                return
        bound = lease.get("bound_local")
        closed = bound is not None and candidates.get(bound) is True
        idle = not n.physical.transaction_open and j.active() is None and all(v == 0 for v in n.currents_a + n.powers_w)
        intent = c.requests.intent()
        cancelled = intent.mode == "paused" or intent.revision != lease.get("control_revision", lease["revision"])
        if (
            not cancelled
            and self.revoked(lease)
            and actual_id is not None
            and actual_id == lease.get("bound_local") == j.active()
            and n.physical.transaction_open
        ):
            if "hold_since" not in lease:
                j.put(KEY, {**lease, "hold_since": now})
            c.released_epoch = None
            await self.hold_until_closed(n)
        grace_end = min(lease["end_epoch"], lease["start_epoch"] + 600)
        reason = lease.get("retire_reason")
        if closed:
            reason = "session_closed"
        elif not reason and n.physical.connection == "Unplug":
            reason = "vehicle_disconnected"
        elif not reason and cancelled:
            reason = "owner_superseded"
        elif not reason and state == "armed" and not present and now < lease["start_epoch"] - 5:
            reason = "disappeared_before_start"
        elif not reason and idle and now >= grace_end:
            reason = "session_source_unverified" if any(candidates.values()) else "no_scheduled_session"
        if reason and reason != lease.get("retire_reason"):
            lease = {**lease, "retire_reason": reason}
            j.put(KEY, lease)
        if (
            idle
            and reason
            and intent.mode == "app"
            and "hold_since" not in lease
            and (present or state == "installing")
        ):
            j.put(KEY, {**lease, "hold_since": now})
            c.released_epoch = None
            await self.hold_until_closed(n)
            lease = j.get(KEY)
        if not idle or not reason:
            return
        if not present:
            # A timed-out install can still arrive late. Retain its ownership through the start grace period.
            if state == "installing" and lease.get("last_response") != "accepted":
                if now < grace_end:
                    return
                if not lease.get("absence_verified_at"):
                    j.put(KEY, {**lease, "absence_verified_at": now})
                    return
                if now - lease["absence_verified_at"] < 2:
                    return
            if n.saved_schedule_digest != lease["original_digest"]:
                raise SecurityError("original native timer is not confirmed")
            self.finish(lease, str(reason))
        elif now >= lease.get("next_clear", 0):
            attempts = lease.get("cleanup_attempts", 0) + 1
            lease = {
                **lease,
                "cleanup_since": lease.get("cleanup_since", now),
                "cleanup_attempts": attempts,
                "next_clear": now + min(300, 30 * 2 ** min(attempts - 1, 4)),
            }
            j.put(KEY, lease)
            await self.send(lease, False)
