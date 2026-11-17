"""One journal-level owner for Start evidence and the normal request's lifecycle projections."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .managed_intent import ControlIntent
from .start_handoff import StartHandoffContext, StartPublication
from .store import RecoveryRequiredError, encoded, fingerprint

if TYPE_CHECKING:
    from .managed_test_charge import TestLease
    from .store import Journal
    from .trial_handover_evidence import HandoverEvidence


@dataclass(frozen=True)
class StartRequest:
    identifier: str
    action: str
    revision: int
    state: str
    result: str


@dataclass(frozen=True)
class StartReceipt:
    """Receipt evidence can precede publication and may instead belong to an older settled request."""

    body: str
    request_id: str | None
    ticket: str | None
    digest: str | None
    stage: str | None
    local: int | None


@dataclass(frozen=True)
class StartSession:
    """Actual transaction evidence is read-only input; cloud delivery and source permission stay separate."""

    local: int
    start: str
    stop: str | None
    vendor: int | None
    digest: str


@dataclass(frozen=True)
class NormalStart:
    """The existing journal keys encode this aggregate; publication is distinct from a real wire Accepted."""

    body: str
    identifier: str
    revision: int
    phase: str
    attempted: bool | None
    published: bool | None
    projection_digest: str | None
    reported_local: int | None
    receipt: StartReceipt
    request: StartRequest | None
    pending: bool
    session: StartSession | None

    # Only this request's receipt, or its explicit pre-upgrade publication, identifies the canonical effect.
    @property
    def effect(self) -> StartReceipt | None:
        return (
            self.receipt
            if (
                self.receipt.request_id == self.identifier
                or self.receipt.request_id is None
                and self.published is True
                and self.receipt.digest == self.projection_digest
            )
            else None
        )

    # The wire receipt owns tag identity; the metadata copy is only a checked compatibility projection.
    @property
    def digest(self) -> str | None:
        return self.effect.digest if self.effect is not None else None

    # Before preparation an older receipt stays historical, while a bound current receipt supplies late identity.
    @property
    def local(self) -> int | None:
        return self.effect.local if self.effect is not None else None

    # One identity predicate covers current and legacy local completion without manufacturing publication.
    def owns(self, local: int, *, strict_unpublished: bool = False, strict_request: bool = False) -> bool:
        receipt, request = self.receipt, self.request
        return bool(
            type(local) is int
            and self.local == receipt.local == local
            and self.reported_local in (None, local)
            and (self.reported_local is not None or receipt.request_id == self.identifier)
            and receipt.request_id in (None, self.identifier)
            and receipt.digest == self.projection_digest
            and receipt.stage in ("issued", "accepted")
            and (self.published is True or receipt.request_id == self.identifier)
            and (
                receipt.stage != "issued"
                or self.published is False
                and self.attempted is True
                and receipt.request_id == self.identifier
            )
            and (not strict_unpublished or self.published is True or self.published is False and self.attempted is True)
            and request is not None
            and request.action == "start"
            and request.state in ("accepted", "running", "uncertain", "completed", "failed")
            and request.revision + int(self.published is True) == self.revision
            and (
                not strict_request or request.state in ("completed", "failed") or receipt.request_id == self.identifier
            )
        )


class StartLifecycle:
    """Own Start projections and local outcomes; generic request startup still records process replacement."""

    # One object belongs to one locked journal; no connection or process-local lease is restored from disk.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        self.lease: TestLease | None = None

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

    # Load the persisted aggregate once at a synchronous boundary, retaining legacy and pre-prepared identities.
    def normal(self) -> NormalStart | None:
        j = self.journal
        state = j.get("managed_test_charge", {})
        if state.get("kind") != "normal" or state.get("source") != "local" or not isinstance(state.get("id"), str):
            return None
        value = self.receipt()
        receipt = StartReceipt(
            encoded(value),
            value.get("request_id"),
            value.get("ticket"),
            value.get("digest"),
            value.get("stage"),
            value.get("local"),
        )
        row = j.db.execute(
            "SELECT id,action,revision,state,result FROM managed_requests WHERE id=?", (state["id"],)
        ).fetchone()
        request = StartRequest(*row) if row is not None else None
        belongs = value.get("request_id") == state["id"] or (
            value.get("request_id") is None
            and state.get("start_rpc_accepted") is True
            and value.get("digest") == state.get("tag_digest")
        )
        row = j.db.execute(
            "SELECT local,start,stop,vendor FROM transactions WHERE local=?", (value.get("local") if belongs else None,)
        ).fetchone()
        session = (
            StartSession(
                row["local"],
                row["start"],
                row["stop"],
                row["vendor"],
                fingerprint(json.loads(row["start"])["idTag"].casefold()),
            )
            if row is not None
            else None
        )
        return NormalStart(
            encoded(state),
            state["id"],
            state.get("revision"),
            state.get("state"),
            state.get("start_attempted"),
            state.get("start_rpc_accepted"),
            state.get("tag_digest"),
            state.get("local_transaction"),
            receipt,
            request,
            bool(j.get("control", {}).get("start_pending")),
            session,
        )

    # Compatibility callers can release or publish the sole lease slot without maintaining another copy.
    def set_lease(self, lease: TestLease | None) -> None:
        self.lease = lease

    # Only this effect's current control claim may arm the pending bit before dispatch.
    def armed(self, ticket: str) -> None:
        j, control = self.journal, self.journal.get("control", {})
        if control.get("ticket") == ticket:
            j.put("control", {**control, "inhibit": False, "start_pending": True})

    # A proven operator outcome clears only the Start projection while retaining the supplied restrictive claim.
    def operator_control(self, control: dict[str, Any]) -> None:
        self.journal.put(
            "control", {**control, "owner": "local", "inhibit": True, "uncertain": True, "start_pending": False}
        )

    # Admission owns the new metadata and receipt row, while retaining an older wire receipt until preparation.
    def admit(self, lease: TestLease, action: str, startup: float, duration: float) -> None:
        j = self.journal
        with j.atomic():
            j.put(
                "managed_test_charge",
                {
                    "id": lease.identifier,
                    "source": lease.source,
                    "kind": "normal" if lease.normal else "test",
                    "revision": lease.revision,
                    "state": "starting" if lease.source == "local" else "armed",
                    "accepted_at": time.time(),
                    "hard_deadline_epoch": time.time() + startup + (0 if lease.normal else duration),
                    "duration_seconds": None if lease.normal else duration,
                    "start_rpc_accepted": False,
                    "start_attempted": False,
                    "current_a": lease.current_a,
                },
            )
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'accepted',?)",
                (lease.identifier, action, lease.revision, json.dumps({"accepted_at": time.time()})),
            )
        self.lease = lease

    # Initial preparation joins the wire child's transaction after the caller's live permission check.
    def prepared_metadata(self, lease: TestLease, digest: str) -> None:
        j = self.journal
        state = j.get("managed_test_charge", {})
        if state.get("id") != lease.identifier:
            raise RecoveryRequiredError("Start preparation belongs to another request")
        j.put("managed_test_charge", {**state, "tag_digest": digest, "start_attempted": True})

    # The caller owns scope/intent publication; this owner joins its exact accepted metadata to the same COMMIT.
    def published(self, lease: TestLease, context: StartHandoffContext, intent: ControlIntent) -> StartPublication:
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
    def committed(self, lease: TestLease, publication: StartPublication) -> None:
        if lease.normal:
            lease.revision, lease.mode = publication.intent.revision, publication.intent.mode

    # One exact failed request's metadata remains alongside its critical management outcome.
    def fail_preflight(self, identifier: str, cause: dict[str, Any]) -> None:
        j = self.journal
        with j.atomic():
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? WHERE id=? AND state IN ('accepted','running')",
                (json.dumps({"classification": "preflight_refused_unsent"}, sort_keys=True), identifier),
            )
            j.put("managed_test_charge", {**j.get("managed_test_charge", {}), "state": "failed", "failure": cause})
        self.lease = None

    # Expiry bookkeeping retains accepted/unknown evidence and cannot grant any physical Stop permission.
    def expire(self, lease: TestLease, reason: str, preserve: bool, failure: dict[str, Any] | None) -> None:
        j = self.journal
        j.put(
            "managed_test_charge",
            {
                **j.get("managed_test_charge", {}),
                "state": "expired" if preserve else "stopping",
                "stop_reason": reason,
                "expired_normal": lease.normal,
                **({"failure": failure} if failure is not None else {}),
            },
        )
        j.db.execute(
            "UPDATE managed_requests SET state='uncertain',result=? WHERE id=? AND state IN ('accepted','running')",
            (json.dumps({"classification": reason}, sort_keys=True), lease.identifier),
        )

    # Timed/cloud metadata shares storage but never becomes an alternate writer of a normal Start record.
    def project_test(self, state: dict[str, Any]) -> None:
        if state.get("kind") == "normal":
            raise RecoveryRequiredError("normal Start metadata requires its lifecycle transition")
        self.journal.put("managed_test_charge", state)

    # A normal observation may update measurement without changing publication, intent or the request result.
    def measurement(self, state: dict[str, Any], charging: bool) -> None:
        self.journal.put("managed_test_charge", {**state, "charging": charging})

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
    def managed_bound(self, lease: TestLease | None, local: int, tag: Any, guard: Callable[[], None]) -> None:
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
    def closed(self, *, strict_request: bool = False, strict_unpublished: bool = False) -> NormalStart | None:
        record = self.normal()
        if (
            record is None
            or type(record.local) is not int
            or not record.owns(
                record.local,
                strict_request=strict_request,
                strict_unpublished=strict_unpublished,
            )
        ):
            return None
        if record.session is None or record.session.stop is None or record.session.digest != record.digest:
            return None
        return record

    # Consumers add their own native, denial or operator proof to this single exact lifecycle identity.
    def identity(
        self, local: int, *, strict_request: bool = False, strict_unpublished: bool = False
    ) -> dict[str, Any] | None:
        record = self.normal()
        if (
            record is None
            or not record.owns(local, strict_request=strict_request, strict_unpublished=strict_unpublished)
            or record.session is None
            or record.session.digest != record.digest
            or record.request is None
        ):
            return None
        return {
            "id": record.identifier,
            "revision": record.revision,
            "tag_digest": record.digest,
            "local_transaction": local,
            "request_revision": record.request.revision,
        }

    # Closed-zero retirement may settle a failed publication, but never adopts another live lease's permission.
    def close_candidate(self) -> NormalStart | None:
        record = self.closed()
        lease = self.lease
        if (
            record is None
            or record.pending
            or lease is not None
            and (not lease.normal or lease.source != "local" or lease.identifier != record.identifier)
        ):
            return None
        return record

    # A caller supplies fresh closed-zero proof and owns this transaction; local completion leaves cloud debt intact.
    def complete_closed_zero(self, record: NormalStart) -> None:
        self.require(record)
        self.complete_normal(
            json.loads(record.body),
            record.local,
            reason="owned_session_closed_at_verified_zero",
            allow_uncertain=True,
            request_revision=record.request.revision if record.request is not None else None,
        )

    # The lease's known real closure is sufficient even when native polling did not observe the open session.
    def finish_closed(self, lease: TestLease) -> bool:
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

    # The owner checks snapshots at publication; old frozen evidence cannot complete a replaced request.
    def require(self, record: NormalStart) -> None:
        if self.normal() != record:
            raise RecoveryRequiredError("normal Start changed before lifecycle settlement")

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
        if local is not None and (not record.owns(local) or session is None or session.digest != record.digest):
            return False
        if lease is not None and self.finish_closed(lease):
            return True
        if record.phase == "running":
            if session is not None and session.stop is not None:
                self.complete_normal(state, local, stopped_at=time.time(), finish_request=False)
            elif native is not None and native.physical.native_transaction_id == local and j.active() == local:
                self.measurement(state, native.physical.power_w > 10)
            return True
        if not allow_open or lease is None or not lease.normal or record.published is not True or native is None:
            return False
        closed = session is not None and session.stop is not None
        opened = (
            local is not None and native.physical.transaction_open and native.physical.native_transaction_id == local
        )
        if (opened or closed) and session is not None:
            charging = opened and native.physical.power_w > 10
            with nullcontext() if j.db.in_transaction else j.atomic():
                j.put(
                    "managed_test_charge",
                    {
                        **state,
                        "state": "completed" if closed else "running",
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
                            if opened and not known_source
                            else {}
                        ),
                    },
                )
                j.db.execute(
                    "UPDATE managed_requests SET state='completed',result=? WHERE id=? "
                    "AND state IN ('accepted','running')",
                    (
                        json.dumps(
                            {"local_session_verified": True, "charging": charging, "timed": False}, sort_keys=True
                        ),
                        lease.identifier,
                    ),
                )
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
                j.put("control", {**control, "start_pending": False})

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
            j.put("control", {**control, "start_pending": False})

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
            j.put("control", {**control, "start_pending": True})

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
            j.put("control", {**control, "start_pending": False})

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
                revision = " AND revision=?" if request_revision is not None else ""
                j.db.execute(
                    "UPDATE managed_requests SET state='completed',result=? WHERE id=? AND action='start' "
                    + (
                        "AND state IN ('accepted','running','uncertain')"
                        if allow_uncertain
                        else "AND state IN ('accepted','running')"
                    )
                    + revision,
                    (
                        json.dumps({"local_session_verified": True, "charging": False, "timed": False}, sort_keys=True),
                        state["id"],
                        *((request_revision,) if request_revision is not None else ()),
                    ),
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

    # A cancelled, explicitly unprepared request needs its critical outcome before the caller releases memory.
    def cancelled(self, identifier: str, action: str, revision: int, metadata: str) -> None:
        j = self.journal
        changed = j.db.execute(
            "UPDATE managed_requests SET state='failed',result=? WHERE id=? AND action=? AND revision=? "
            "AND state IN ('accepted','running')",
            (encoded({"classification": "cancelled_before_start_dispatch"}), identifier, action, revision),
        ).rowcount
        if changed != 1:
            raise RecoveryRequiredError("cancelled Start request changed before retirement")
        self.complete_metadata(json.loads(metadata), reason="cancelled_before_start_dispatch")

    # A fresh zero caller may fail only the exact uncertain unsent request, retaining original terminal bytes.
    def unissued_at_zero(self, state: dict[str, Any]) -> None:
        j = self.journal
        with j.atomic():
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? WHERE id=? AND revision=? "
                "AND action=? AND state='uncertain'",
                (
                    json.dumps({"classification": "start_not_accepted_at_verified_zero"}),
                    state["id"],
                    state["revision"],
                    "start" if state["kind"] == "normal" else "start_test",
                ),
            )
            self.complete_metadata(state, reason="start_not_accepted_at_verified_zero")

    # Generic Pause settlement supplies byte-exact preimages and cannot change an already terminal request.
    def settled_at_zero(self, state: dict[str, Any], request: tuple[Any, ...], local: int | None) -> None:
        unissued = local is None and state["source"] == "local"
        outcome = "completed"
        result: dict[str, Any]
        if unissued:
            outcome, result = "failed", {"classification": "start_not_accepted_at_verified_zero"}
        elif state["kind"] == "normal":
            result = {"local_session_verified": True, "charging": False, "timed": False}
        elif state["source"] == "cloud":
            result = {"armed": True, "charging_started": False}
        else:
            outcome, result = "failed", {"classification": "test_interrupted_at_verified_zero"}
        self.journal.db.execute(
            "UPDATE managed_requests SET state=?,result=? WHERE id=? AND action=? AND revision=? "
            "AND state IN ('accepted','running','uncertain')",
            (outcome, encoded(result), *request[:3]),
        )
        self.complete_metadata(
            {**state, "local_transaction": local},
            reason=("start_not_accepted_at_verified_zero" if unissued else "owned_operation_settled_at_verified_zero"),
        )

    # Operator proof clears a proven absent local command, while its other debt and outbox facts stay independent.
    def operator_no_pending(self, receipt: dict[str, Any], state: dict[str, Any], proof: str, identifier: str) -> None:
        j = self.journal
        j.put("local_start_receipt", {**receipt, "stage": "reconciled", "proof": proof})
        self.complete_metadata(state, reason="operator_verified_no_pending_start")
        j.db.execute(
            "UPDATE managed_requests SET state='failed',result=? WHERE id=?",
            (encoded({"classification": "operator_reconciled_at_zero", "charging_started": False}), identifier),
        )
