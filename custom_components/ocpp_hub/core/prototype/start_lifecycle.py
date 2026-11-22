"""One journal-level owner for Start evidence and command outcomes."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..frames import Frame
from .managed_intent import ControlIntent, insert_request
from .policy import DeniedError
from .security import SecurityError
from .start_handoff import StartHandoffContext, StartPublication
from .start_settlement import (
    ChargeStart,
    LeaseIdentity,
    RetirementCandidate,
    RetirementRule,
    StartReceipt,
    StartRequest,
    StartSession,
    idle_receipt_ended,
    retire_start,
)
from .store import RecoveryRequiredError, encoded, fingerprint

if TYPE_CHECKING:
    from .managed_charging import ChargeLease
    from .store import Journal
    from .trial_handover_evidence import HandoverEvidence


@dataclass(frozen=True)
class TimedStartSource:
    """The exact session observed for a timed Start, with cloud provenance when available."""

    local: int
    cloud_start: tuple[str, str] | None = None


class StartLifecycle:
    """Own Start projections and local outcomes; generic request startup still records process replacement."""

    # One object belongs to one locked journal; no connection or process-local lease is restored from disk.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        self.lease: ChargeLease | None = None
        self.pending_reply: tuple[str, str] | None = None

    # Preserve the established fallback for journals written before a separate wire receipt existed.
    def receipt(self) -> dict[str, Any]:
        value = self.journal.get("local_start_receipt", {})
        if value:
            return dict(value)
        old = self.journal.get("managed_test_charge", {})
        if old.get("tag_digest") and old.get("source") == "local":
            return {
                "digest": old["tag_digest"],
                "stage": "accepted" if old.get("start_rpc_accepted") else "prepared",
                "local": old.get("local_transaction"),
            }
        return {}

    # Decode shared charge bookkeeping without interpreting opaque request results or granting any permission.
    def load(self, *, wire: bool = True, ledger: bool = True, request_id: str | None = None) -> ChargeStart | None:
        j, state = self.journal, self.journal.get("managed_test_charge", {})
        if not isinstance(state, dict) or not state:
            return None
        value = self.receipt() if wire else {}
        receipt = StartReceipt(
            encoded(value),
            value.get("request_id"),
            value.get("ticket"),
            value.get("digest"),
            value.get("stage"),
            value.get("local"),
        )
        row = j.db.execute("SELECT * FROM managed_requests WHERE id=?", (request_id or state.get("id"),)).fetchone()
        request = StartRequest(*row) if row is not None else None
        rows = (
            j.db.execute(
                "SELECT local,start,stop,vendor FROM transactions WHERE local IN (?,?) ORDER BY local",
                tuple(
                    value if isinstance(value, (int, float, str)) else None
                    for value in (value.get("local"), state.get("local_transaction"))
                ),
            ).fetchall()
            if wire and ledger
            else []
        )
        return ChargeStart(
            encoded(state),
            state.get("id"),
            state.get("revision"),
            state.get("state"),
            state.get("start_attempted"),
            state.get("start_rpc_accepted"),
            state.get("tag_digest"),
            state.get("local_transaction"),
            receipt,
            request,
            bool(j.get("control", {}).get("start_pending")),
            state.get("kind"),
            state.get("source"),
            state.get("start_source_verified"),
            encoded(j.get("local_start_receipt")) if wire else "null",
            tuple(StartSession(row["local"], row["start"], row["stop"], row["vendor"]) for row in rows),
        )

    # Normal identity continues to reject other charge kinds and sources before using the canonical effect.
    def normal(self) -> ChargeStart | None:
        state = self.journal.get("managed_test_charge", {})
        if (
            not isinstance(state, dict)
            or state.get("kind") != "normal"
            or state.get("source") != "local"
            or not isinstance(state.get("id"), str)
        ):
            return None
        return self.load()

    # One pure rule selects current evidence; each rule reads only the independent runtime fences it needs.
    def retirement(
        self, rule: RetirementRule, *, worker_pending: bool = False, lease: ChargeLease | None = None
    ) -> RetirementCandidate | None:
        j = self.journal
        if rule is RetirementRule.REFUSED and (
            self.lease is not None or j.get("managed_test_charge", {}).get("state") != "failed"
        ):
            return None
        raw = j.get("local_start_receipt") if rule in (RetirementRule.CANCELLED, RetirementRule.GENERIC) else None
        if raw is not None and not isinstance(raw, dict):
            return None
        if rule is RetirementRule.CANCELLED:
            state = j.get("managed_test_charge")
            if lease is None or not isinstance(state, dict) or state.get("id") != lease.identifier:
                return None
        record = (
            self.normal()
            if rule is RetirementRule.CLOSED
            else self.load(ledger=rule not in (RetirementRule.REFUSED, RetirementRule.UNISSUED))
        )
        if record is None:
            return None
        current = lease if rule is RetirementRule.CANCELLED else self.lease
        identity = LeaseIdentity(current.identifier, current.source, current.normal) if current is not None else None
        generic = rule is RetirementRule.GENERIC
        decision = retire_start(
            record,
            rule,
            identity,
            worker_pending=worker_pending,
            active=j.active() if generic else None,
            vendor_pending=bool(j.get("vendor_start_grant")) if generic else False,
            uncertain_vendor=j.outcomes.vendor_evidence().unresolved,
            mutation_pending=bool(j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone())
            if generic
            else False,
        )
        return RetirementCandidate.capture(record, decision, self.lease) if decision is not None else None

    # Idle/expiry scope is checked by the caller; this is only the old historical receipt-ended predicate.
    def receipt_ended(self) -> bool:
        record = self.load()
        return record is not None and idle_receipt_ended(record)

    # An already failed request can retire before native observation if its exact Start was never sent.
    def retire_refused(self) -> None:
        candidate = self.retirement(RetirementRule.REFUSED)
        if candidate is not None:
            self.retire(candidate)

    # Live polling and expiry share the local-zero transitions, separately from cloud delivery.
    def retire_local_zero(self, require_zero: Callable[[], None], *, closed_only: bool = False) -> tuple[bool, bool]:
        unissued = None if closed_only else self.retirement(RetirementRule.UNISSUED)
        closed = self.retirement(RetirementRule.CLOSED)
        if unissued is None and closed is None:
            return False, False
        try:
            require_zero()
        except SecurityError, DeniedError, RecoveryRequiredError:
            return False, False
        if unissued is not None:
            self.retire(unissued)
            closed = self.retirement(RetirementRule.CLOSED)
        if closed is not None:
            try:
                with self.journal.atomic():
                    self.retire(closed)
            except sqlite3.Error as error:
                if self.journal.db.in_transaction:
                    raise
                raise RecoveryRequiredError("verified normal Start closure awaits retirement") from error
            self.lease = None
        return unissued is not None, closed is not None

    # Fault supervision needs a native read only when a closed Start still has an unsettled local outcome.
    def fault_closure_pending(self) -> bool:
        candidate = self.retirement(RetirementRule.CLOSED)
        if candidate is None:
            return False
        record = candidate.record
        return record.phase != "completed" or (
            record.request is not None and record.request.state in candidate.decision.states
        )

    # Only this Start owner selects retirement once the caller can establish settled delivery at zero.
    def delivered_at_zero(self, *, worker_pending: bool, timed_only: bool) -> RetirementCandidate | None:
        candidate = self.retirement(RetirementRule.GENERIC, worker_pending=worker_pending)
        if timed_only and candidate is not None and not candidate.record.pending_local_test:
            return None
        return candidate

    # Historical ended evidence can retire an expired projection, independently of the original receipt outcome.
    def ended_at_idle(self, native: HandoverEvidence, *, unissued: bool = False) -> bool:
        j = self.journal
        return (
            not j.get("control", {}).get("start_pending")
            and j.active() is None
            and not native.physical.transaction_open
            and (unissued or self.receipt_ended())
        )

    # Recovery and polling can only complete the same expired projection that still has idle proof.
    def finish_expired(self, identifier: object, native: HandoverEvidence, *, unissued: bool = False) -> None:
        state = self.journal.get("managed_test_charge", {})
        if (
            state.get("id") == identifier
            and state.get("state") != "completed"
            and state.get("expired_normal")
            and self.ended_at_idle(native, unissued=unissued)
        ):
            self.complete_metadata(state)

    # Finish a timed charge's activity without abandoning any outstanding receipt that still needs delivery proof.
    def finish_timed_hold(self, lease: ChargeLease, observed_at: float) -> bool:
        record = self.load(wire=False)
        if (
            self.lease is not lease
            or record is None
            or record.identifier != lease.identifier
            or record.phase != "stopping"
            or record.pending_local_test
        ):
            return False
        self.complete_metadata(json.loads(record.body), stopped_at=observed_at)
        self.lease = None
        return True

    # A cloud test can act only on the original confirmed grant issued after its admission.
    def _timed_cloud_source(self, floor: int) -> TimedStartSource | None:
        j = self.journal
        grant = j.get("vendor_start_grant", {})
        local, token, mutation = grant.get("local"), grant.get("token"), grant.get("mutation")
        if (
            type(local) is not int
            or not isinstance(token, str)
            or not isinstance(mutation, str)
            or j.db.execute(
                "SELECT 1 FROM mutations WHERE rowid>? AND token=? AND origin='vendor' "
                "AND action='RemoteStartTransaction' AND state='confirmed' LIMIT 1",
                (floor, mutation),
            ).fetchone()
            is None
        ):
            return None
        return TimedStartSource(local, (token, mutation))

    # Decide whether an independent positive reading belongs to the timed test that requested it.
    def timed_flow_source(self, lease: ChargeLease, local: int) -> TimedStartSource | None:
        state = self.journal.get("managed_test_charge", {})
        if self.lease is not lease or state.get("id") != lease.identifier:
            return None
        cloud = self._timed_cloud_source(lease.first_mutation)
        if cloud is not None and cloud.local == local:
            return cloud if lease.source == "cloud" else None
        if lease.source == "local" and state.get("start_rpc_accepted") and self.receipt().get("local") == local:
            return TimedStartSource(local)
        return None

    # The same grant or local receipt can identify a Stop target before first power has been recorded.
    def timed_stop_target(self, state: dict[str, Any]) -> int | None:
        for key in ("stop_local", "local_transaction"):
            recorded = state.get(key)
            if type(recorded) is int:
                return recorded
        if state.get("source") == "local":
            receipt = self.receipt()
            local = receipt.get("local")
            if receipt.get("request_id") == state.get("id") and type(local) is int:
                return local
        elif type(state.get("mutation_floor")) is int:
            source = self._timed_cloud_source(state["mutation_floor"])
            if source is not None:
                return source.local
        return None

    # Compatibility callers can release or publish the sole lease slot without maintaining another copy.
    def set_lease(self, lease: ChargeLease | None) -> None:
        self.lease = lease

    # Only this effect's current control claim may arm the pending bit before dispatch.
    def armed(self, ticket: str) -> None:
        j, control = self.journal, self.journal.get("control", {})
        if control.get("ticket") == ticket:
            j.control.complete(ticket, base=control, inhibit=False, start_pending=True)

    # A proven operator outcome clears only the Start projection while retaining the supplied restrictive claim.
    def operator_control(self, control: dict[str, Any]) -> None:
        self.journal.control.hold(control, start_pending=False)

    # Admission owns the new metadata and receipt row, while retaining an older wire receipt until preparation.
    def admit(self, lease: ChargeLease, action: str, startup: float, duration: float) -> None:
        j = self.journal
        accepted_at = time.time()
        local = lease.source == "local"
        state = "accepted" if local else "completed"
        result = {"accepted_at": accepted_at} if local else {"armed": True, "charging_started": False}
        with j.atomic():
            j.put(
                "managed_test_charge",
                {
                    "id": lease.identifier,
                    "source": lease.source,
                    "kind": "normal" if lease.normal else "test",
                    "revision": lease.revision,
                    "state": "starting" if local else "armed",
                    "accepted_at": accepted_at,
                    "hard_deadline_epoch": accepted_at + startup + (0 if lease.normal else duration),
                    "duration_seconds": None if lease.normal else duration,
                    "start_rpc_accepted": False,
                    "start_attempted": False,
                    "current_a": lease.current_a,
                    **({"accepted_boot": lease.accepted_boot} if lease.accepted_boot is not None else {}),
                    **({"mutation_floor": lease.first_mutation} if not local else {}),
                },
            )
            insert_request(j, lease.identifier, action, lease.revision, result, state=state)
        self.lease = lease

    # Initial preparation joins the wire child's transaction after the caller's live permission check.
    def prepared_metadata(self, lease: ChargeLease, digest: str) -> None:
        j = self.journal
        state = j.get("managed_test_charge", {})
        if state.get("id") != lease.identifier:
            raise RecoveryRequiredError("Start preparation belongs to another request")
        j.put("managed_test_charge", {**state, "tag_digest": digest, "start_attempted": True})

    # The caller owns scope/intent publication; this owner joins its exact accepted metadata to the same COMMIT.
    def published(self, lease: ChargeLease, context: StartHandoffContext, intent: ControlIntent) -> StartPublication:
        j, receipt = self.journal, self.receipt()
        if (
            not j.db.in_transaction
            or context.request_id != lease.identifier
            or j.get("control", {}).get("ticket") != context.ticket
            or receipt.get("ticket") != context.ticket
            or receipt.get("digest") != context.tag_digest
            or receipt.get("request_id") != lease.identifier
            or receipt.get("stage") != "accepted"
        ):
            raise RecoveryRequiredError("owned Start publication was superseded")
        j.put(
            "managed_test_charge",
            {
                **j.get("managed_test_charge", {}),
                "state": "awaiting_power",
                "start_rpc_accepted": True,
                "revision": intent.revision,
            },
        )
        return StartPublication(lease.identifier, intent, lease.epoch)

    # Permission memory follows successful publication rather than receipt of an Accepted wire reply.
    def committed(self, lease: ChargeLease, publication: StartPublication) -> None:
        if lease.normal:
            lease.revision, lease.mode = publication.intent.revision, publication.intent.mode

    # One exact failed request's metadata remains alongside its critical management outcome.
    def fail_preflight(self, identifier: str, cause: dict[str, Any]) -> None:
        j = self.journal
        with j.atomic():
            self._finish(identifier, "failed", {"classification": "preflight_refused_unsent"})
            j.put("managed_test_charge", {**j.get("managed_test_charge", {}), "state": "failed", "failure": cause})
        self.lease = None

    # Expiry bookkeeping retains accepted/unknown evidence and cannot grant any physical Stop permission.
    def expire(
        self,
        lease: ChargeLease,
        reason: str,
        preserve: bool,
        failure: dict[str, Any] | None,
        *,
        target: int | None = None,
    ) -> None:
        j = self.journal
        j.put(
            "managed_test_charge",
            {
                **j.get("managed_test_charge", {}),
                "state": "expired" if preserve else "stopping",
                "stop_reason": reason,
                "expired_normal": lease.normal,
                **({"stop_local": target} if not lease.normal else {}),
                **({"failure": failure} if failure is not None else {}),
            },
        )
        self._finish(lease.identifier, "uncertain", {"classification": reason})

    # Publish verified timed flow and its receipt together inside the caller's transaction.
    def confirm_test_flow(
        self,
        lease: ChargeLease,
        state: dict[str, Any],
        local: int,
        observed_at: float,
        duration: float,
        *,
        cloud_start: tuple[str, str] | None = None,
    ) -> None:
        if lease.normal or state.get("kind") == "normal":
            raise RecoveryRequiredError("normal Start metadata requires its lifecycle transition")
        if state.get("id") != lease.identifier:
            raise RecoveryRequiredError("verified test flow belongs to another request")
        self.journal.put(
            "managed_test_charge",
            {
                **state,
                "state": "charging",
                "flow_started_at": observed_at,
                "stop_deadline_epoch": observed_at + duration,
                "local_transaction": local,
                "start_source_verified": True,
                **({"cloud_start": {"grant": cloud_start[0], "mutation": cloud_start[1]}} if cloud_start else {}),
            },
        )
        # The pending-state filter preserves the cloud arm receipt completed at admission.
        self._finish(lease.identifier, "completed", {"positive_power_verified": True, "duration_seconds": duration})

    # Startup normalization keeps the caller's independently committed Pause/intent claim in the same transaction.
    def interrupted(self, state: dict[str, Any], preserve: bool) -> None:
        self.journal.put(
            "managed_test_charge",
            {
                **state,
                "state": "expired" if preserve else "interrupted",
                "expired_normal": bool(preserve),
                "charging": False,
            },
        )

    # Native attribution records only the independently verified plan source for this current normal command.
    def independent_seen(self, state: dict[str, Any]) -> None:
        self.journal.put("managed_test_charge", {**state, "independent_seen": True})

    # Lifecycle binding remains physical evidence; current permission separately controls its managed projection.
    def managed_bound(self, lease: ChargeLease | None, local: int, tag: Any, guard: Callable[[], None]) -> None:
        j, state = self.journal, self.journal.get("managed_test_charge", {})
        if (
            lease is None
            or not lease.normal
            or state.get("kind") != "normal"
            or state.get("local_transaction") is not None
            or self.receipt().get("local") != local
            or not isinstance(tag, str)
            or state.get("tag_digest") != fingerprint(tag.casefold())
        ):
            return
        guard()
        j.put("managed_test_charge", {**state, "local_transaction": local, "actual_start_at": time.time()})

    # A closed record is identified once for Pause, denial and operator consumers, independently of native proof.
    def closed(self, *, strict_request: bool = False, strict_unpublished: bool = False) -> ChargeStart | None:
        record = self.normal()
        return (
            record
            if record is not None
            and record.correlated(closed=True, strict_request=strict_request, strict_unpublished=strict_unpublished)
            else None
        )

    # Consumers add their own native, denial or operator proof to this single exact lifecycle identity.
    def identity(
        self, local: int, *, strict_request: bool = False, strict_unpublished: bool = False
    ) -> dict[str, Any] | None:
        record = self.normal()
        if (
            record is None
            or record.local != local
            or record.request is None
            or not record.correlated(strict_request=strict_request, strict_unpublished=strict_unpublished)
        ):
            return None
        return {
            "id": record.identifier,
            "revision": record.revision,
            "tag_digest": record.digest,
            "local_transaction": local,
            "request_revision": record.request.revision,
        }

    # The lease's known real closure is sufficient even when native polling did not observe the open session.
    def finish_closed(self, lease: ChargeLease) -> bool:
        if self.journal.db.in_transaction:
            raise RecoveryRequiredError("lease completion must own its COMMIT")
        record = self.closed()
        if (
            self.lease is not lease
            or not lease.normal
            or record is None
            or record.identifier != lease.identifier
            or record.published is not True
        ):
            return False
        self.complete_normal(json.loads(record.body), record.local, stopped_at=time.time())
        self.lease = None
        return True

    # Compare exactly the rule's original persisted scope; generic candidates also pin the actual lease object.
    def require(self, candidate: RetirementCandidate) -> None:
        current = self.load()
        if current is None or RetirementCandidate.capture(current, candidate.decision, self.lease) != candidate:
            raise RecoveryRequiredError("Start retirement changed before settlement")

    # All locally confirmed normal completion paths share one writer and keep actual cloud debt independent.
    def observe_normal(self, native: HandoverEvidence, *, allow_open: bool = False, known_source: bool = True) -> bool:
        j, lease = self.journal, self.lease
        if j.db.in_transaction:
            raise RecoveryRequiredError("lease observation must own its COMMIT")
        record = self.normal()
        if record is None:
            return False
        state = json.loads(record.body)
        local, session = record.local, record.session
        if local is not None and not record.correlated():
            return False
        try:
            if lease is not None and self.finish_closed(lease):
                return True
            if record.phase == "running":
                if session is not None and session.stop is not None:
                    self.complete_normal(state, local, stopped_at=time.time(), finish_request=False)
                return True
        except sqlite3.Error as error:
            if j.db.in_transaction:
                raise
            raise RecoveryRequiredError("verified normal Start closure awaits publication") from error
        if (
            not allow_open
            or lease is None
            or not lease.normal
            or record.identifier != lease.identifier
            or record.published is not True
        ):
            return False
        opened = (
            local is not None and native.physical.transaction_open and native.physical.native_transaction_id == local
        )
        if opened and session is not None:
            charging = native.physical.power_w > 10
            try:
                with j.atomic():
                    j.put(
                        "managed_test_charge",
                        {
                            **state,
                            "state": "running",
                            "local_transaction": local,
                            "session_started_at": time.time(),
                            "charging": charging,
                            **(
                                {
                                    "native_source": {
                                        "marker": native.session_plan_id,
                                        "start": fingerprint(json.loads(session.start)),
                                        "schedule": native.saved_schedule_digest,
                                    }
                                }
                                if not known_source
                                else {}
                            ),
                        },
                    )
                    self._finish(
                        lease.identifier,
                        "completed",
                        {"local_session_verified": True, "charging": charging, "timed": False},
                    )
            except sqlite3.Error as error:
                if j.db.in_transaction:
                    raise
                # Keep this lease for publication and require the next poll to refresh the evidence.
                lease.deadline = max(lease.deadline, time.monotonic() + 15)
                raise RecoveryRequiredError("verified normal Start awaits receipt publication") from error
            self.lease = None
        return True

    # The surrounding Start claim transaction supplies atomicity and keeps preparation distinct from admission.
    def prepare(self, ticket: str, digest: str, request_id: str | None) -> None:
        self.journal.put(
            "local_start_receipt",
            {"ticket": ticket, "digest": digest, "stage": "prepared", "local": None, "request_id": request_id},
        )

    # A recorded wire stage is keyed to the original effect, so a newer Pause cannot erase its real reply.
    def record(self, ticket: str, stage: str) -> None:
        j = self.journal
        receipt = j.get("local_start_receipt", {})
        if receipt.get("ticket") != ticket:
            return
        with j.atomic():
            control = j.get("control", {})
            settled = stage == "rejected" and receipt.get("local") is None and control.get("ticket") == ticket
            j.put("local_start_receipt", {**receipt, "stage": stage, **({"settled_ticket": ticket} if settled else {})})
            if settled:
                j.control.project(control, start_pending=False)

    # Store only a definite reply, independently of permission to publish an accepted Start.
    def record_reply(self, ticket: str, answer: Frame) -> bool:
        if answer[0] != 3 or answer[2] not in ({"status": "Accepted"}, {"status": "Rejected"}):
            return False
        return self.record_reply_status(ticket, answer[2]["status"])

    # Live and saved wire replies settle through the same Start receipt transition.
    def record_reply_status(self, ticket: str, status: str) -> bool:
        if status not in ("Accepted", "Rejected"):
            return False
        stage = "accepted" if status == "Accepted" else "rejected"
        try:
            self.record(ticket, stage)
        except sqlite3.Error:
            self.pending_reply = ticket, stage
            raise
        return stage == "accepted"

    # Retry the original write without holding up Pause or reopening the Start's authority.
    def publish_pending_reply(self) -> None:
        if self.pending_reply is None:
            return
        ticket, stage = self.pending_reply
        try:
            wire = self.receipt()
            if wire.get("ticket") == ticket and wire.get("stage") == "issued":
                self.record(ticket, stage)
        except sqlite3.Error:
            if self.journal.db.in_transaction:
                raise
            return
        self.pending_reply = None

    # A newer Pause can settle an exact, unbound device rejection after verifying its own closed zero.
    def settle_rejected_pause(self, revision: int, require_hold: Callable[[dict[str, Any]], None]) -> None:
        j = self.journal
        control = j.get("control", {})
        if control.get("start_pending") is not True:
            return
        record = self.load(ledger=False)
        if record is None or record.request is None:
            return
        receipt, request = record.receipt, record.request
        ticket = control.get("ticket")
        if (
            record.source != "local"
            or record.kind not in ("normal", "test")
            or not record.request_matches()
            or record.attempted is not True
            or record.published is not False
            or record.reported_local is not None
            or request.state not in ("accepted", "running", "uncertain")
            or request.revision >= revision
            or receipt.request_id != request.identifier
            or receipt.stage != "rejected"
            or receipt.local is not None
            or not receipt.digest
            or receipt.digest != record.projection_digest
            or not receipt.ticket
            or not ticket
            or receipt.ticket == ticket
        ):
            return
        with j.atomic():
            require_hold(control)
            j.put(
                "local_start_receipt",
                {**json.loads(receipt.body), "settled_ticket": ticket, "pause_revision": revision},
            )
            j.control.project(control, start_pending=False)

    # Proven unissued preparation retires independently of whichever later owner currently holds zero.
    def retire_prepared(self, ticket: str | None) -> None:
        j = self.journal
        receipt = j.get("local_start_receipt", {})
        if not ticket or receipt.get("ticket") != ticket or receipt.get("stage") != "prepared":
            return
        control = j.get("control", {})
        with j.atomic():
            intent = j.get("managed_control_intent", {})
            j.put(
                "local_start_receipt",
                {
                    **receipt,
                    "stage": "unsent",
                    "settled_ticket": control.get("ticket"),
                    "pause_revision": intent.get("revision") if intent.get("mode") == "paused" else None,
                },
            )
            j.control.project(control, start_pending=False)

    # Old journals may have managed acceptance without a separate receipt; normalize only exact unbound evidence.
    def recover_legacy(self) -> None:
        j = self.journal
        existing = j.get("local_start_receipt", {})
        if existing:
            self.retire_prepared(existing.get("ticket"))
            return
        owner = j.get("managed_test_charge", {})
        if (
            owner.get("source") != "local"
            or owner.get("kind") != "normal"
            or owner.get("start_rpc_accepted") is not True
            or owner.get("local_transaction") is not None
            or owner.get("state") not in ("awaiting_power", "stopping", "interrupted")
            or not isinstance(owner.get("tag_digest"), str)
            or len(owner["tag_digest"]) != 64
        ):
            return
        control = j.get("control", {})
        if not control.get("start_pending") and any(
            fingerprint(json.loads(row["start"])["idTag"].casefold()) == owner["tag_digest"]
            for row in j.db.execute("SELECT start FROM transactions")
        ):
            return
        with j.atomic():
            j.put("local_start_receipt", {"digest": owner["tag_digest"], "stage": "accepted", "local": None})
            j.control.project(control, start_pending=True)

    # Binding joins the genuine StartTransaction commit and never grants positive permission to its old caller.
    def bind(self, tag: str, local: int) -> None:
        j = self.journal
        control, receipt = j.get("control", {}), self.receipt()
        if (
            control.get("start_pending")
            and receipt.get("local") is None
            and receipt.get("digest") == fingerprint(tag.casefold())
            and receipt.get("stage") in ("issued", "accepted")
        ):
            intent = j.get("managed_control_intent", {})
            j.put(
                "local_start_receipt",
                {
                    **receipt,
                    "local": local,
                    "bound_ticket": control.get("ticket"),
                    "pause_revision": intent.get("revision") if intent.get("mode") == "paused" else None,
                },
            )
            j.control.project(control, start_pending=False)

    # A current physical Pause proof may annotate the exact existing wire evidence without rewriting its outcome.
    def pause_verified(self, receipt: dict[str, Any], revision: int) -> None:
        self.journal.put("local_start_receipt", {**receipt, "verified_pause_revision": revision})

    # Complete one local request and its metadata together; outbox and cloud acknowledgement duties are untouched.
    def complete_normal(
        self,
        state: dict[str, Any],
        local: int | None,
        *,
        reason: str | None = None,
        stopped_at: float | None = None,
        finish_request: bool = True,
        allow_uncertain: bool = False,
        request_revision: int | None = None,
    ) -> None:
        j = self.journal
        if state.get("kind") != "normal" or state.get("source") != "local":
            raise RecoveryRequiredError("normal Start completion requires its exact local record")
        with nullcontext() if j.db.in_transaction else j.atomic():
            if finish_request:
                self._finish(
                    state["id"],
                    "completed",
                    {"local_session_verified": True, "charging": False, "timed": False},
                    action="start",
                    revision=request_revision,
                    states=("accepted", "running", "uncertain") if allow_uncertain else ("accepted", "running"),
                )
            self.complete_metadata(
                state if local is None else {**state, "local_transaction": local}, reason=reason, stopped_at=stopped_at
            )

    # Proof-specific callers settle exact terminal metadata without inventing a wire or management outcome.
    def complete_metadata(
        self, state: dict[str, Any], *, reason: str | None = None, stopped_at: float | None = None
    ) -> None:
        changes: dict[str, Any] = {"state": "completed", "charging": False}
        if reason is not None:
            changes["recovery_reason"] = reason
        if stopped_at is not None:
            changes["stopped_at"] = stopped_at
        self.journal.put("managed_test_charge", {**state, **changes})

    # An unrelated daily can retire expired test work without completing its pending Start successfully.
    def retire_independent_test(self, lease: ChargeLease, state: dict[str, Any]) -> None:
        with self.journal.atomic():
            self.complete_metadata(state, reason="independent_daily")
            self._finish(lease.identifier, "failed", {"classification": "independent_daily"})

    # All local management outcomes share encoding and exact optional filters, inside their existing transaction.
    def _finish(
        self,
        identifier: str,
        state: str,
        result: dict[str, Any],
        *,
        action: str | None = None,
        revision: int | None = None,
        states: tuple[str, ...] | None = ("accepted", "running"),
        compact: bool = False,
    ) -> int:
        query = "UPDATE managed_requests SET state=?,result=? WHERE id=?"
        parameters: list[Any] = [state, encoded(result) if compact else json.dumps(result, sort_keys=True), identifier]
        for key, value in (("action", action), ("revision", revision)):
            if value is not None:
                query += f" AND {key}=?"
                parameters.append(value)
        if states is not None:
            query += " AND state IN (" + ",".join("?" for _ in states) + ")"
            parameters.extend(states)
        return self.journal.db.execute(query, parameters).rowcount

    # Execute one selected retirement; original native/cancellation proofs and caller transactions stay external.
    def retire(self, candidate: RetirementCandidate) -> None:
        record, decision = candidate.record, candidate.decision
        j, rule, local = self.journal, decision.rule, decision.local
        if rule in (RetirementRule.CLOSED, RetirementRule.GENERIC):
            self.require(candidate)
        # Closed/generic/cancelled callers retain their exact preimage and transaction checks. Unissued owns its COMMIT.
        transaction = j.atomic() if rule is RetirementRule.UNISSUED else nullcontext()
        with transaction:
            state = json.loads(record.body)
            update = decision.update(record)
            if update.state is not None:
                request = record.request
                if request is None:
                    raise RecoveryRequiredError("Start retirement requires its exact request")
                changed = self._finish(
                    request.identifier,
                    update.state,
                    update.result,
                    action=request.action,
                    revision=request.revision,
                    states=decision.states,
                    compact=rule in (RetirementRule.CANCELLED, RetirementRule.GENERIC),
                )
            else:
                changed = 0
            if rule is RetirementRule.CANCELLED and changed != 1:
                raise RecoveryRequiredError("cancelled Start request changed before retirement")
            self.complete_metadata(
                {**state, **({"local_transaction": local} if decision.writes_local else {})}, reason=update.reason
            )

    # Operator proof clears a proven absent local command, while its other debt and outbox facts stay independent.
    def operator_no_pending(self, receipt: dict[str, Any], state: dict[str, Any], proof: str, identifier: str) -> None:
        j = self.journal
        j.put("local_start_receipt", {**receipt, "stage": "reconciled", "proof": proof})
        self.complete_metadata(state, reason="operator_verified_no_pending_start")
        self._finish(
            identifier,
            "failed",
            {"classification": "operator_reconciled_at_zero", "charging_started": False},
            states=None,
            compact=True,
        )
