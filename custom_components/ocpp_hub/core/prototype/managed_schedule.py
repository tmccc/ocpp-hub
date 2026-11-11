"""One owner-authorized, date-bound native timer alongside the immutable daily schedule."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..control import HUB_PROFILE_ID
from .gateway import NativeStartReport, timestamp
from .ics_writes import validate_write
from .local_control import local_start_receipt
from .native_start_attempts import matching_unassigned_start, start_millisecond_range
from .policy import DeniedError, integer, number
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, encoded, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .gateway import Gateway
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
def remember_disconnect(journal: Journal) -> None:
    lease = journal.get(KEY, {})
    if lease.get("state") in {"requested", *OBSERVABLE} and not lease.get("vehicle_disconnected"):
        journal.put(
            KEY,
            {
                **lease,
                "vehicle_disconnected": True,
                "departed_local": journal.active(),
                "retire_reason": lease.get("retire_reason") or "vehicle_disconnected",
            },
        )


# An existing commissioned timer and an owned station receipt allow a zero-only restart during a telemetry outage.
def recorded_timer_hold(j: Journal, spec_digest: str, boot_digest: str) -> bool:
    return j.get(KEY, {}).get("state") in OBSERVABLE and recorded_station_profile(j, spec_digest, boot_digest)


# Bind a restrictive write to the admitted installation and a station profile already owned by this journal.
def recorded_station_profile(j: Journal, spec_digest: str, boot_digest: str) -> bool:
    row = j.db.execute("SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)).fetchone()
    return (
        j.get("trial_phase") in ("active", "recovering")
        and j.get("managed_mode", {}).get("spec_digest") == spec_digest
        and j.get("trial_binding", {}).get("boot_digest") == boot_digest
        and j.get("native_bounded_handover", {}).get("stage") == "admitted"
        and row is not None
        and row["state"] != "absent_verified"
        and not j.get("managed_control_fault")
        and not j.get("trial_inventory_fault")
        and not j.get("trial_preservation_fault")
    )


# A vendor grant identifies a separately accepted command only when its exact mutation and tag agree.
def confirmed_vendor_start(j: Journal, tag_digest: str, local: int | None) -> bool:
    grant = j.get("vendor_start_grant", {})
    return bool(
        grant.get("local") == local
        and grant.get("digest") == tag_digest
        and j.db.execute(
            "SELECT 1 FROM mutations WHERE token=? AND origin='vendor' "
            "AND action='RemoteStartTransaction' AND state='confirmed'",
            (grant.get("mutation"),),
        ).fetchone()
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
    if not grant or grant.get("local") is not None:
        return False
    grant_digest = grant.get("digest")
    return (
        digest is None
        or not isinstance(grant_digest, str)
        or len(grant_digest) != 64
        or grant_digest == digest
        or not confirmed_vendor_start(j, grant_digest, None)
    )


# A conflicting temporary plan field needs independent proof of the exact owner command and lifecycle.
def independent_session(j: Journal, lease: dict[str, Any], n: HandoverEvidence, local: int) -> bool:
    if (
        not n.physical.transaction_open
        or n.physical.native_transaction_id != local
        or n.physical.connection == "Unplug"
    ):
        return False
    temporary = n.session_plan_id == lease["body"]["planId"]
    if (
        not temporary
        and n.saved_schedule is not None
        and any(plan["planId"] == n.session_plan_id for plan in n.saved_schedule["planList"])
    ):
        return True
    row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
    if row is None:
        return False
    tag = json.loads(row["start"])["idTag"].casefold()
    if tag == lease["body"]["idTag"].casefold():
        return False
    if not temporary:
        return True
    digest = fingerprint(tag)
    owner = local_start_receipt(j)
    return confirmed_vendor_start(j, digest, local) or (
        owner.get("stage") == "accepted" and owner.get("local") == local and owner.get("digest") == digest
    )


# Publish the source of each fully verified, open native observation before any cutoff or recovery task can act.
def record_session_source(j: Journal, n: HandoverEvidence, boot_observation: str | None) -> None:
    lease = j.get(KEY, {})
    local = n.physical.native_transaction_id
    if lease.get("state") not in OBSERVABLE:
        return
    active = j.active()
    if (
        local is None
        and active is None
        and lease.get("pending_native_reports")
        and not n.physical.transaction_open
        and all(value == 0 for value in n.currents_a + n.powers_w)
    ):
        # A station may reuse a provisional ID after both native and ledger closure have been witnessed.
        j.db.execute(
            "DELETE FROM managed_pending_native_reports WHERE local IN "
            "(SELECT local FROM transactions WHERE stop IS NOT NULL)"
        )
        lease = {**lease, "pending_native_reports": False}
        j.put(KEY, lease)
    gap = known_timer_closure_gap(j, n)
    unreported = local is not None and active != local and not gap
    evidence = lease.get("unreported_source", {})
    if unreported and (evidence.get("native_id"), evidence.get("plan_id")) != (local, n.session_plan_id):
        evidence = {"native_id": local, "plan_id": n.session_plan_id, "observed": time.time()}
    elif not unreported:
        evidence = {}
    if (
        gap != lease.get("closure_gap", False)
        or unreported != lease.get("unreported_native", False)
        or evidence != lease.get("unreported_source", {})
    ):
        lease = {**lease, "unreported_native": unreported, "unreported_source": evidence, "closure_gap": gap}
        if active is not None and (unreported or gap and active == lease.get("other_local")):
            lease = {
                **lease,
                "unidentified_local": active,
                "bound_local": None,
                "native_plan_confirmed": False,
                "other_local": None,
            }
        j.put(KEY, lease)
    if local is None or local != active or local <= lease["floor"]:
        return
    previous = lease.get("bound_local")
    other = lease.get("other_local")
    changes: dict[str, Any] = {}
    row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
    if row is None:
        return
    if lease.get("source_boot") != boot_observation:
        changes["source_boot"] = boot_observation
    timer_source = (
        n.session_plan_id == lease["body"]["planId"]
        and json.loads(row["start"])["idTag"].casefold() == lease["body"]["idTag"].casefold()
    )
    if timer_source:
        if previous != local:
            changes.update(bound_local=local, native_plan_confirmed=True)
            if previous is not None:
                changes["retire_reason"] = "session_closed"
        if other == local:
            changes["other_local"] = None
    else:
        independent = independent_session(j, lease, n, local)
        if previous == local:
            changes.update(bound_local=None, native_plan_confirmed=False)
        if independent and "hold_since" in lease and lease.get("unidentified_local") != local:
            # This open session inherited a physical zero established for a different or as-yet unidentified ID.
            changes["unidentified_local"] = local
        if independent and lease.get("departed_local") == local:
            changes["departed_local"] = None
        if independent and other != local:
            changes["other_local"] = local
        elif not independent and other == local:
            changes["other_local"] = None
        if "hold_since" in lease and (previous == local or other == local and not independent):
            changes["unidentified_local"] = local
    if changes:
        j.put(KEY, {**lease, **changes})


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


# Only evidence from this actual Boot may exempt the current transaction from zero or authorize a timer Stop.
def current_source(gateway: Gateway, lease: dict[str, Any]) -> bool:
    return gateway.boot_observation is not None and lease.get("source_boot") == gateway.boot_observation


# Share a durable retry interval and the final source check across Boot recovery and ordinary deadline supervision.
async def request_timer_stop(g: Gateway, lease: dict[str, Any], authorized: Callable[[], bool]) -> None:
    j = g.journal
    now = time.time()
    if not current_source(g, lease) or now < lease.get("next_stop", 0):
        return
    j.put(KEY, {**lease, "hold_since": lease.get("hold_since", now), "next_stop": now + 15})

    # A replacement Boot or newer independent source can supersede the Stop during physical zero verification.
    def guard() -> None:
        current = j.get(KEY, {})
        if (
            not authorized()
            or not current_source(g, lease)
            or not current_source(g, current)
            or current.get("id") != lease["id"]
            or not current.get("native_plan_confirmed")
            or current.get("closure_gap")
            or current.get("unreported_native")
            or current.get("bound_local") != lease["bound_local"]
            or current.get("other_local") == lease["bound_local"]
            or j.active() not in (None, lease["bound_local"])
        ):
            raise RecoveryRequiredError("the identified cutoff Stop was superseded")

    await g.controls.stop(expected_local=lease["bound_local"], guard=guard)


# The independent closure reports may arrive in either order for any exact transaction tracked by this timer.
def known_timer_closure_gap(journal: Journal, native: HandoverEvidence) -> bool:
    lease = journal.get(KEY, {})
    local, physical = journal.active(), native.physical.native_transaction_id
    if lease.get("state") not in OBSERVABLE or (local is None) == (physical is None):
        return False
    closing = local if local is not None else physical
    return closing in (lease.get("unidentified_local"), lease.get("other_local")) or (
        lease.get("native_plan_confirmed") is True and closing == lease.get("bound_local")
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
        self.cutoff_checked: str | None = None
        self.prior_departure: tuple[int, str, str] | None = None
        self.j = controller.session.journal
        self.j.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_schedule_requests(id TEXT PRIMARY KEY, start_epoch INTEGER NOT NULL)"
        )
        self.j.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_pending_native_reports("
            "native_id INTEGER PRIMARY KEY, local INTEGER NOT NULL UNIQUE REFERENCES transactions(local))"
        )
        self.j.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_unnumbered_reports("
            "attempt TEXT NOT NULL, started_ms INTEGER NOT NULL,"
            "local INTEGER NOT NULL UNIQUE REFERENCES transactions(local),"
            "PRIMARY KEY(attempt,started_ms))"
        )
        controller.session.gateway.boot_observer = self.changed.set
        controller.session.gateway.status_observer = self.observe_status
        controller.session.gateway.tag_guard = self.permits_tag
        controller.session.gateway.prepare_report_hold = self.prepare_report_hold
        controller.session.gateway.held_start_report = self.can_record_held_start
        controller.session.gateway.native_start_report = self.prepare_native_report
        controller.session.gateway.remote_start_guard = self.permits_remote_start
        lease = self.j.get(KEY, {})
        if lease.get("state") == "requested":
            self.finish(lease, "cancelled_before_install")

    # Keep a connector-1 Available transition even when the next status or native poll sees its replacement.
    def observe_status(self, token: str, payload: dict[str, Any]) -> None:
        if payload["connectorId"] != 1 or payload["status"] != "Available":
            return
        # Only an exact, timed CALL witnessed on this connection before admission is a known replay.
        # A clock behind ours can put a genuine new cable departure before the request on paper.
        evidence = (
            (self.c.session.gateway._charger_epoch, token, fingerprint(payload)) if "timestamp" in payload else None
        )
        if self.j.get(KEY, {}).get("state") not in {"requested", *OBSERVABLE}:
            self.prior_departure = evidence
            return
        if evidence is not None and evidence == self.prior_departure:
            return
        remember_disconnect(self.j)
        self.changed.set()

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
    def awaiting_start(self, tag: str | None = None) -> bool:
        return awaiting_owner_start(self.j, tag)

    # Only an authenticated charger under an owned, verified zero profile can submit a delayed lifecycle report.
    def held_report_ready(self, *, owner_command: bool = False) -> bool:
        c, j = self.c, self.j
        g = c.session.gateway
        lease = j.get(KEY, {})
        return (
            (
                lease.get("state") in OBSERVABLE
                and "hold_since" in lease
                or owner_command
                and c.requests.intent().mode == "paused"
            )
            and j.active() is None
            and not c.stopping
            and not j.get("managed_control_fault")
            and g.controls.ready
            and g.controls.desired() == 0
            and self.hold_authorized()
        )

    # Match a previously accepted command; repeating its network request is never part of recovery.
    def matches_pending_start(self, tag: str) -> bool:
        j = self.j
        digest = fingerprint(tag.casefold())
        prior = local_start_receipt(j)
        return bool(
            j.get("control", {}).get("start_pending")
            and prior.get("local") is None
            and prior.get("stage") in ("issued", "accepted")
            and prior.get("digest") == digest
            or confirmed_vendor_start(j, digest, None)
        )

    # Close the gap between an independent session's actual Stop and a following Start before the next tick.
    async def prepare_report_hold(self, payload: dict[str, Any]) -> None:
        j, g = self.j, self.c.session.gateway
        lease, tag = j.get(KEY, {}), payload.get("idTag")
        if j.active() is not None or not isinstance(tag, str):
            return
        if self.c.requests.intent().mode == "paused" and self.matches_pending_start(tag):
            if not g.controls.ready or g.controls.desired() != 0:
                operations = set(g.controls._operations)
                if self.c.pause_task is not None and not self.c.pause_task.done():
                    operations.add(self.c.pause_task)
                if operations:
                    await asyncio.wait(operations, timeout=min(1.0, g.charger.timeout))
                if not self.held_report_ready(owner_command=True) or not self.matches_pending_start(tag):
                    raise UnavailableError("the acknowledged owner report awaits verified Pause zero")
            return
        if (
            not self.revoked(lease)
            or not lease.get("other_local")
            or not (tag.casefold() == lease["body"]["idTag"].casefold() or self.matches_pending_start(tag))
        ):
            return
        integer(payload.get("connectorId"), 1, 1)
        number(payload.get("meterStart"), 1e12)
        timestamp(payload.get("timestamp"))
        await self.protect_station(lease)
        if not g.controls.ready or g.controls.desired() != 0:
            raise UnavailableError("the following Start awaits the station's confirmed zero")

    # Record the genuine delayed lifecycle of an already accepted owner command under verified station zero.
    def can_record_held_start(self, tag: str) -> bool:
        return self.held_report_ready(owner_command=True) and self.matches_pending_start(tag)

    # A changed OCPP spelling of an already recorded occurrence cannot create a second local transaction.
    def recorded_start(self, floor: int, tag: str, millis: range, meter: float) -> bool:
        rows = self.j.db.execute(
            "SELECT t.start,r.started_ms FROM transactions t LEFT JOIN managed_unnumbered_reports r ON r.local=t.local "
            "WHERE t.local>? AND t.connector=1",
            (floor,),
        )
        for row in rows:
            start = json.loads(row["start"])
            if start["idTag"].casefold() != tag.casefold() or start["meterStart"] != meter:
                continue
            native_ms = row["started_ms"]
            if native_ms is None:
                previous = start_millisecond_range(start["timestamp"], timestamp(start["timestamp"]))
                if previous.start < millis.stop and millis.start < previous.stop:
                    return True
            elif native_ms in millis:
                return True
        return False

    # Record a witnessed physical occurrence under zero while the current hold still covers its authorized window.
    async def prepare_native_report(self, payload: dict[str, Any]) -> NativeStartReport | None:
        c, j = self.c, self.j
        lease = j.get(KEY, {})
        if not self.held_report_ready():
            return None
        tag = payload.get("idTag")
        if (
            payload.get("connectorId") != 1
            or not isinstance(tag, str)
            or tag.casefold() != lease["body"]["idTag"].casefold()
            or self.awaiting_start(tag)
        ):
            return None
        meter = number(payload.get("meterStart"), 1e12)
        reported = timestamp(payload.get("timestamp"))
        occurred = reported.timestamp()
        zone = ZoneInfo(c.native.spec.original_configuration["timeZone"])
        accepted_at = lease.get("accepted_epoch")
        if accepted_at is None:
            accepted_at = c.requests.receipt(lease["id"]).get("result", {}).get("accepted_at")
        if accepted_at is None or occurred < accepted_at or occurred > time.time():
            return None
        try:
            n = await c.native.read()
            present = c.session.book.owned_present(n.physical)
        except (SecurityError, TimeoutError) as error:
            raise UnavailableError("delayed timer report awaits verified native evidence") from error
        current = j.get(KEY, {})
        native_id = n.physical.native_transaction_id
        if (
            not self.held_report_ready()
            or self.awaiting_start(tag)
            or current.get("id") != lease["id"]
            or n.physical.connection == "Unplug"
            or n.saved_schedule is None
            or any(v != 0 for v in n.currents_a + n.powers_w)
            or not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present)
        ):
            return None
        plan = next((plan for plan in n.saved_schedule["planList"] if plan["planId"] == n.session_plan_id), None)
        if plan is None or plan["idTag"].casefold() != tag.casefold():
            return None
        temporary = plan["planId"] == lease["body"]["planId"]
        reported_day = datetime.fromtimestamp(occurred, zone)
        occurrence = int(reported_day.replace(hour=0, minute=0, second=0, microsecond=0, fold=0).timestamp())
        if temporary:
            if (
                current.get("vehicle_disconnected")
                or current.get("retire_reason") == "vehicle_disconnected"
                or occurred < lease["start_epoch"]
                or occurred >= cutoff_epoch(lease["start_epoch"], zone)
                or occurred > current["hold_since"]
            ):
                return None
        elif (
            reported_day.date()
            not in (
                datetime.fromtimestamp(time.time(), zone).date(),
                datetime.fromtimestamp(current.get("unreported_source", {}).get("observed", time.time()), zone).date(),
            )
            or occurred >= cutoff_epoch(occurrence, zone)
            or occurred > current["hold_since"]
            and c.requests.intent().mode != "app"
        ):
            return None
        attempt = None
        millis = start_millisecond_range(payload["timestamp"], reported)
        if native_id is None:
            if (
                temporary
                or n.physical.transaction_open
                or reported_day.date() != datetime.fromtimestamp(time.time(), zone).date()
            ):
                return None
            attempt = matching_unassigned_start(n.history, payload, reported)
            if (
                attempt is None
                or j.db.execute(
                    "SELECT 1 FROM managed_unnumbered_reports WHERE attempt=? AND started_ms=?", attempt
                ).fetchone()
            ):
                return None
            millis = range(attempt[1], attempt[1] + 1)
        elif (
            not n.physical.transaction_open
            or not current.get("unreported_native")
            or j.db.execute("SELECT 1 FROM transactions WHERE local=?", (native_id,)).fetchone() is not None
            or j.db.execute("SELECT 1 FROM managed_pending_native_reports WHERE native_id=?", (native_id,)).fetchone()
        ):
            return None
        if self.recorded_start(lease["floor"], tag, millis, meter):
            return None
        # The one-off can report once. Prior releases recorded its occurrence in the per-plan date map.
        if temporary and (
            current.get("temporary_start_reported") or str(plan["planId"]) in current.get("reported_occurrences", {})
        ):
            return None

        # Claim the occurrence together with its ledger ID. Only fresh matching native evidence permits release.
        def accepted(local: int) -> None:
            changes = {"unidentified_local": local}
            if attempt is not None:
                j.db.execute("INSERT INTO managed_unnumbered_reports VALUES(?,?,?)", (*attempt, local))
            else:
                j.db.execute("INSERT INTO managed_pending_native_reports VALUES(?,?)", (native_id, local))
                changes["pending_native_reports"] = True
            if temporary:
                changes["temporary_start_reported"] = True
            j.put(KEY, {**current, **changes})

        return NativeStartReport(native_id, plan["planId"], attempt, accepted)

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
                and not (
                    not adding
                    and j.get("trial_phase") == "recovering"
                    and "hold_since" in current
                    and c.native.introduced_epoch == epoch
                    and c.native.introduced_revision == g.controls._revision
                    and g.controls.ready
                    and g.controls.desired() == 0
                )
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
        if (
            lease.get("timer_present") is not False
            and "cleanup_since" in lease
            and (lease.get("last_response") == "rejected" or now - lease["cleanup_since"] >= 60)
        ):
            return "temporary_schedule_cleanup_unconfirmed"
        if (
            "hold_since" in lease
            and now - lease["hold_since"] >= 60
            and self.j.active() is None
            and self.awaiting_start()
        ):
            return "temporary_schedule_awaiting_start_report"
        if (
            "hold_since" in lease
            and now - lease["hold_since"] >= 60
            and (
                lease.get("closure_gap")
                or lease.get("unreported_native")
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

    # A verified introduction may retry only the existing zero profile if its physical acknowledgement was interrupted.
    async def retry_recovery_zero(self) -> None:
        c, j = self.c, self.j
        g = c.session.gateway
        epoch = g._charger_epoch

        # Remain on the same authenticated, restricted Boot and never turn a readback retry into positive permission.
        def guard() -> None:
            if (
                c.stopping
                or j.get(KEY, {}).get("state") not in OBSERVABLE
                or j.get("trial_phase") != "recovering"
                or c.native.introduced_epoch != epoch
                or c.native.introduced_revision != g.controls._revision
                or not g.current_boot_seen(epoch)
                or g.controls.desired() != 0
                or not recorded_timer_hold(j, c.native.spec.digest, c.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("timer zero readback awaits the current restricted introduction")

        if g.charger._pending or g.controls._operations:
            return
        guard()
        await g.controls.ensure_ceiling(guard=guard)

    # Lower only the station ceiling, even if native HTTP is unavailable. A foreign ledger transaction has priority.
    async def protect_station(self, lease: dict[str, Any]) -> None:
        c, j = self.c, self.j
        g = c.session.gateway
        if not self.hold_authorized():
            return
        if j.active() not in (None, lease.get("bound_local"), lease.get("unidentified_local")):
            return
        self.begin_hold(lease)
        if (
            g.controls.ready
            and g.controls.desired() == 0
            and (j.get("trial_phase") == "recovering" or not j.get("control", {}).get("uncertain"))
        ):
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

    # After a new Boot, a previously observed or recovering transaction must be held until its origin is re-established.
    def needs_source(self, lease: dict[str, Any]) -> bool:
        g, j = self.c.session.gateway, self.j
        active = j.active()
        if active is None:
            return False
        if not current_source(g, lease):
            return j.get("trial_phase") == "recovering" or active in (
                lease.get("other_local"),
                lease.get("bound_local"),
                lease.get("unidentified_local"),
            )
        if active != lease.get("other_local"):
            return False
        native = self.c.native
        cutoff = time.time() >= lease.get("end_epoch", float("inf"))
        return (
            native.last_verified_at == 0
            or cutoff
            and (
                not 0 <= native.observation_age() <= 5
                or self.cutoff_checked != lease["id"]
                and native.read_sequence > native.last_resolved_read
            )
        )

    # Renew a continuing independent source without delaying the watchdog's ability to hold the station at zero.
    async def refresh_independent_source(self) -> None:
        try:
            async with asyncio.timeout(5):
                await self.c.native.read()
        except SecurityError, UnavailableError, TimeoutError:
            pass  # An expired or failed observation leaves the next watchdog pass responsible for physical zero.
        finally:
            self.changed.set()

    # The service owns this independent deadline wait so a slow health poll or a concurrent Pause cannot defer zero.
    async def watch_deadline(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            await self.supervise_deadline(tasks)

    # At most one bounded source refresh supplements the five-second foreground poll near and after cutoff.
    async def supervise_deadline(self, tasks: asyncio.TaskGroup) -> None:
        refresh: asyncio.Task[None] | None = None
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
                due = (
                    remaining <= 0
                    or self.needs_source(lease)
                    or lease.get("retire_reason") == "vehicle_disconnected"
                    or lease.get("vehicle_disconnected")
                    or lease.get("unreported_native")
                    or lease.get("closure_gap")
                )
                if due and not self.j.get("managed_control_fault"):
                    try:
                        await self.protect_deadline(lease)
                    except DeniedError, UnavailableError, RecoveryRequiredError:
                        pass  # Durable attempt times bound retries. Only actual closure can release the hold.
                daily = self.j.active() is not None and self.j.active() == lease.get("other_local")
                native = self.c.native
                if (
                    remaining <= 5
                    and daily
                    and self.c.session.gateway.controls.desired() > 0
                    and native.observation_age() >= 2
                    and native.read_sequence == native.last_resolved_read
                    and (refresh is None or refresh.done())
                ):
                    refresh = tasks.create_task(self.refresh_independent_source())
                if due:
                    delay = 1
                elif daily:
                    delay = min(remaining, 1) if remaining <= 5 else min(remaining - 5, 30)
                else:
                    delay = min(remaining, 30)
            try:
                await asyncio.wait_for(self.changed.wait(), delay)
            except TimeoutError:
                pass

    # Stop a previously identified, still-open OCPP transaction by its own ID, even while HTTP reads are unavailable.
    async def protect_deadline(self, lease: dict[str, Any]) -> None:
        j = self.j
        needs_source = self.needs_source(lease)
        before_end = time.time() < lease.get("end_epoch", float("inf"))
        if (
            lease.get("state") not in OBSERVABLE
            or before_end
            and lease.get("retire_reason") != "vehicle_disconnected"
            and not lease.get("vehicle_disconnected")
            and not lease.get("unreported_native")
            and not lease.get("closure_gap")
            and not needs_source
            or not self.hold_authorized()
        ):
            return
        if not before_end:
            self.cutoff_checked = lease["id"]
        bound, active = lease.get("bound_local"), j.active()
        if active is not None and (needs_source or not current_source(self.c.session.gateway, lease)):
            # Historical IDs remain available for closure. They cannot exempt an unverified physical session.
            if lease.get("unidentified_local") != active:
                lease = {**lease, "unidentified_local": active}
                j.put(KEY, lease)
            await self.protect_station(lease)
            return
        if active is not None and active != bound:
            if (
                active == lease.get("other_local")
                and not lease.get("closure_gap")
                and active != lease.get("departed_local")
            ):
                return
            if lease.get("unidentified_local") != active:
                lease = {**lease, "unidentified_local": active}
                j.put(KEY, lease)
        if bound is None or not lease.get("native_plan_confirmed") or j.active() != bound or lease.get("closure_gap"):
            await self.protect_station(lease)
            return
        self.begin_hold(lease)
        await request_timer_stop(self.c.session.gateway, j.get(KEY), self.hold_authorized)

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
        if not g.current_boot_seen(g._charger_epoch):
            raise RecoveryRequiredError("deadline hold awaits the current charger Boot")
        if c.native.introduced_epoch != g._charger_epoch:
            if not current_source(g, j.get(KEY, {})):
                # A blocked introduction read must not prevent newer source evidence reaching the deadline task.
                await c.native.read()
            raise RecoveryRequiredError("deadline hold awaits the current charger introduction")
        n = n or await c.native.read()
        await self.await_closure_agreement(n)
        lease = j.get(KEY)
        c.native.require_known_session(n)
        active = j.active()
        bound = lease.get("bound_local")
        if active is not None and active != bound:
            if lease.get("unidentified_local") == active and independent_session(j, lease, n, active):
                if g.controls._operations or g.charger._pending:
                    raise RecoveryRequiredError("independent session awaits the in-flight station verification")
                await self.protect_station(lease)
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
            c.require_paused(n, timer_cleanup=True)
            return
        if bound is not None:
            await request_timer_stop(g, lease, self.hold_authorized)
        else:
            await self.protect_station(lease)
        if active is None and j.active() is None and g.controls.ready and g.controls.desired() == 0:
            c.require_paused(await c.native.read(closed=True), timer_cleanup=True)
            return
        raise RecoveryRequiredError("deadline hold awaits verified closure")

    # Reconcile native timer presence and transaction history after the verified read has published its source.
    def observe_timer(self, lease: dict[str, Any], n: HandoverEvidence) -> tuple[dict[str, Any], dict[int, bool]]:
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
        if candidates and not lease.get("session_seen"):
            changes["session_seen"] = True
        if lease.get("timer_present") is not present:
            changes["timer_present"] = present
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
        lease, candidates = self.observe_timer(lease, n)
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
            and actual_id == j.active()
            and actual_id != lease.get("other_local")
            and n.physical.transaction_open
        ):
            await self.protect_deadline(lease)
            self.begin_hold(j.get(KEY))
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
            if self.awaiting_start():
                # Physical removal is verified. Retain the restricted listener until the accepted lifecycle reports.
                self.begin_hold(lease)
                return
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
