"""Pure selection for the Start lifecycle owner's failure settlement."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .store import fingerprint


class StartFailureAction(Enum):
    """Only the existing owner executes receipt writes or restrictive recovery."""

    UNRELATED = "unrelated"
    DIAGNOSTIC_ONLY = "diagnostic_only"
    FAIL_PREFLIGHT = "fail_preflight"
    STOP_REQUIRED = "stop_required"


@dataclass(frozen=True)
class StartFailureFacts:
    request_id: str
    expected_action: str
    lease_current: bool
    effects_started: bool


@dataclass(frozen=True)
class StartFailureDecision:
    action: StartFailureAction
    retire_prepared: bool = False


# Select settlement from exact current identities; initial zero work counts as effects even before RemoteStart.
def settle_start(record: ChargeStart | None, facts: StartFailureFacts) -> StartFailureDecision:
    if (
        record is None
        or record.identifier != facts.request_id
        or record.source != "local"
        or record.request is None
        or record.request.action != facts.expected_action
    ):
        return StartFailureDecision(StartFailureAction.UNRELATED)
    receipt = record.receipt
    retire = (
        receipt.request_id == facts.request_id
        and receipt.digest == record.projection_digest
        and receipt.stage == "prepared"
        and receipt.local is None
    )
    if (
        not facts.lease_current
        or record.phase not in ("armed", "starting", "awaiting_power", "charging", "running")
        or record.request.state in ("completed", "failed")
    ):
        return StartFailureDecision(StartFailureAction.DIAGNOSTIC_ONLY, retire)
    return StartFailureDecision(
        StartFailureAction.STOP_REQUIRED if facts.effects_started else StartFailureAction.FAIL_PREFLIGHT, retire
    )


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

    # Decode the tag only for rules that actually require correlation; cloud and cancellation keep opaque rows.
    @property
    def digest(self) -> str:
        return fingerprint(json.loads(self.start)["idTag"].casefold())


@dataclass(frozen=True)
class ChargeStart:
    """The existing journal keys encode this aggregate; publication is distinct from a real wire Accepted."""

    body: str
    identifier: str | None
    revision: int | None
    phase: str | None
    attempted: bool | None
    published: bool | None
    projection_digest: str | None
    reported_local: int | None
    receipt: StartReceipt
    request: StartRequest | None
    pending: bool
    kind: str | None
    source: str | None
    source_verified: bool | None
    raw_receipt: str
    sessions: tuple[StartSession, ...]

    # Local metadata wins for generic settlement; only the current receipt can fill its missing local projection.
    @property
    def resolved_local(self) -> int | None:
        return (
            self.reported_local
            if self.reported_local is not None
            else (self.receipt.local if self.receipt.request_id == self.identifier else None)
        )

    # All joins stay actual ledger evidence, without inferring ownership or physical permission.
    def session_for(self, local: int | None) -> StartSession | None:
        return next((row for row in self.sessions if row.local == local), None)

    # Normal callers use canonical effect identity, separately from generic/legacy settlement's local projection.
    @property
    def session(self) -> StartSession | None:
        return self.session_for(self.local)

    # An explicit never-attempted marker and an exact unsent receipt remain different accepted provenance.
    def unissued(self, *, unbound: bool) -> bool:
        return self.attempted is False or (
            self.receipt.request_id == self.identifier
            and self.receipt.digest == self.projection_digest
            and (not unbound or self.receipt.local is None)
            and self.receipt.stage in ("unsent", "rejected")
        )

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

    # Request matching shares the stored revision relation while callers retain their distinct state whitelists.
    def request_matches(self, *, publication: bool = False, action: str | None = None) -> bool:
        expected = action or (
            "arm_cloud_test" if self.source == "cloud" else "start" if self.kind == "normal" else "start_test"
        )
        return bool(
            self.request is not None
            and self.request.action == expected
            and self.request.revision + int(publication and self.kind == "normal" and self.published is True)
            == self.revision
        )

    # A local timed Start can still own a receipt after native power has reached zero.
    @property
    def pending_local_test(self) -> bool:
        return (
            self.kind == "test"
            and self.source == "local"
            and self.request is not None
            and self.request.state in ("accepted", "running", "uncertain")
            and self.request_matches()
        )

    # Generic settlement permits legacy unpublished binding; strict normal identity also needs a current issued request.
    def bound(self, local: int, *, strict_issued: bool = False) -> bool:
        receipt = self.receipt
        return bool(
            receipt.local == local
            and receipt.request_id in (None, self.identifier)
            and receipt.digest == self.projection_digest
            and receipt.stage in ("issued", "accepted")
            and (
                receipt.stage != "issued"
                or self.published is False
                and self.attempted is True
                and (not strict_issued or receipt.request_id == self.identifier)
            )
        )

    # One identity predicate covers current and legacy local completion without manufacturing publication.
    def owns(self, local: int, *, strict_unpublished: bool = False, strict_request: bool = False) -> bool:
        receipt, request = self.receipt, self.request
        return bool(
            type(local) is int
            and self.local == receipt.local == local
            and self.reported_local in (None, local)
            and (self.reported_local is not None or receipt.request_id == self.identifier)
            and self.bound(local, strict_issued=True)
            and (self.published is True or receipt.request_id == self.identifier)
            and (not strict_unpublished or self.published is True or self.published is False and self.attempted is True)
            and request is not None
            and self.request_matches(action="start", publication=True)
            and request.state in ("accepted", "running", "uncertain", "completed", "failed")
            and (
                not strict_request or request.state in ("completed", "failed") or receipt.request_id == self.identifier
            )
        )

    # Bound identity and real transaction tag are shared, while closure remains a caller-specific requirement.
    def correlated(
        self, *, closed: bool = False, strict_request: bool = False, strict_unpublished: bool = False
    ) -> bool:
        local, row = self.local, self.session
        return bool(
            type(local) is int
            and self.owns(local, strict_request=strict_request, strict_unpublished=strict_unpublished)
            and row is not None
            and row.digest == self.digest
            and (not closed or row.stop is not None)
        )


class RetirementRule(Enum):
    """Each caller retains its original evidence strength and request-update contract."""

    REFUSED = "refused"
    UNISSUED = "unissued_at_zero"
    CLOSED = "closed_at_zero"
    GENERIC = "generic_at_zero"
    CANCELLED = "cancelled_unprepared"


@dataclass(frozen=True)
class LeaseIdentity:
    identifier: str
    source: str
    normal: bool


@dataclass(frozen=True)
class RetirementUpdate:
    state: str | None
    reason: str
    result: dict[str, Any]


@dataclass(frozen=True)
class RetirementDecision:
    """A bookkeeping candidate grants no native, wire, restrictive or positive permission."""

    rule: RetirementRule
    local: int | None = None

    # Only generic settlement writes an explicit absent local; the other rules preserve that projection.
    @property
    def writes_local(self) -> bool:
        return self.rule in (RetirementRule.CLOSED, RetirementRule.GENERIC)

    # The pure decision owns outcome selection; execution only applies its original filter/encoding contract.
    def update(self, record: ChargeStart) -> RetirementUpdate:
        if self.rule is RetirementRule.REFUSED:
            return RetirementUpdate(None, "preflight_refused_unsent", {})
        if self.rule is RetirementRule.CANCELLED:
            reason = "cancelled_before_start_dispatch"
            return RetirementUpdate("failed", reason, {"classification": reason})
        if (
            self.rule is RetirementRule.UNISSUED
            or self.rule is RetirementRule.GENERIC
            and self.local is None
            and record.source == "local"
        ):
            reason = "start_not_accepted_at_verified_zero"
            return RetirementUpdate("failed", reason, {"classification": reason})
        closed_normal = self.rule is RetirementRule.CLOSED or (
            record.kind == "normal" and self.local == record.local and record.correlated(closed=True)
        )
        reason = (
            "owned_session_closed_at_verified_zero" if closed_normal else "owned_operation_settled_at_verified_zero"
        )
        if record.kind == "normal":
            return RetirementUpdate(
                "completed", reason, {"local_session_verified": True, "charging": False, "timed": False}
            )
        if record.source == "cloud":
            return RetirementUpdate("completed", reason, {"armed": True, "charging_started": False})
        return RetirementUpdate("failed", reason, {"classification": "test_interrupted_at_verified_zero"})

    # Native-zero unissued cleanup updates only uncertain rows; cancellation still requires one live request row.
    @property
    def states(self) -> tuple[str, ...]:
        return (
            ("uncertain",)
            if self.rule is RetirementRule.UNISSUED
            else (
                ("accepted", "running")
                if self.rule is RetirementRule.CANCELLED
                else ("accepted", "running", "uncertain")
            )
        )


@dataclass(frozen=True)
class RetirementCandidate:
    """Rule-specific equality preserves the original snapshot scope, including exact generic lease identity."""

    record: ChargeStart = field(compare=False)
    decision: RetirementDecision
    preimage: tuple[object, ...]
    lease: object | None = field(compare=False)

    # Capture only the records the old caller froze; unrelated historical ledger updates stay independent.
    @classmethod
    def capture(
        cls, record: ChargeStart, decision: RetirementDecision, lease: object | None = None
    ) -> RetirementCandidate:
        suffix = (
            (record.pending, record.session)
            if decision.rule is RetirementRule.CLOSED
            else (record.session_for(decision.local), id(lease))
            if decision.rule is RetirementRule.GENERIC
            else ()
        )
        return cls(
            record,
            decision,
            (
                record.body,
                record.request,
                record.raw_receipt if decision.rule is RetirementRule.CANCELLED else record.receipt,
                *suffix,
            ),
            lease,
        )

    # Managed request exclusion remains tied to the original full management row, not a derived local session.
    @property
    def identifier(self) -> str:
        assert self.record.request is not None
        return self.record.request.identifier

    # Existing current/charge settlement coordinators can distinguish the two record owners without copying Start data.
    @property
    def key(self) -> str:
        return "managed_test_charge"


# Cancellation accepts a missing raw receipt, or an exact distinct historical receipt whose local is already closed.
def previous_receipt(record: ChargeStart) -> bool:
    value = json.loads(record.raw_receipt)
    if value is None:
        return True
    if not isinstance(value, dict):
        return False
    prior, ticket, digest = (value.get(key) for key in ("request_id", "ticket", "digest"))
    if (
        not isinstance(prior, str)
        or re.fullmatch(r"[a-f0-9]{32}", prior) is None
        or prior == record.identifier
        or not isinstance(ticket, str)
        or re.fullmatch(r"[a-f0-9]{32}", ticket) is None
        or not isinstance(digest, str)
        or re.fullmatch(r"[a-f0-9]{64}", digest) is None
    ):
        return False
    local, stage = value.get("local"), value.get("stage")
    if local is None:
        return stage in ("unsent", "rejected", "reconciled")
    row = record.session_for(local)
    return (
        type(local) is int
        and local > 0
        and stage in ("issued", "accepted", "reconciled")
        and row is not None
        and row.stop is not None
    )


# Expiry may finish metadata from a historical ended receipt; it does not prove normal ownership or settle a request.
def idle_receipt_ended(record: ChargeStart) -> bool:
    row = record.session_for(record.receipt.local)
    return bool(record.receipt.stage in ("unsent", "rejected") or row is not None and row.stop is not None)


# Select the exact legacy-compatible bookkeeping rule; live proofs and preimage rechecks remain with the executor.
def retire_start(
    record: ChargeStart,
    rule: RetirementRule,
    lease: LeaseIdentity | None = None,
    *,
    worker_pending: bool = False,
    active: int | None = None,
    vendor_pending: bool = False,
    uncertain_vendor: bool = False,
    mutation_pending: bool = False,
) -> RetirementDecision | None:
    request, receipt = record.request, record.receipt
    if rule is RetirementRule.REFUSED:
        if record.phase != "failed" or lease is not None or request is None:
            return None
        result = json.loads(request.result)
        eligible = (
            request.action in ("start", "start_test")
            and request.state == "failed"
            and isinstance(result, dict)
            and result.get("classification") == "preflight_refused_unsent"
            and record.published is False
            and not record.pending
            and not uncertain_vendor
            and not (receipt.local is None and receipt.stage in ("prepared", "issued", "accepted"))
        )
        return RetirementDecision(rule) if eligible else None
    if request is None:
        return None
    if rule is RetirementRule.UNISSUED:
        eligible = (
            lease is None
            and record.kind in ("normal", "test")
            and record.source == "local"
            and record.published is False
            and record.reported_local is None
            and record.unissued(unbound=True)
            and record.request_matches()
            and request.state in ("failed", "uncertain")
        )
        return RetirementDecision(rule) if eligible else None
    if rule is RetirementRule.CANCELLED:
        eligible = (
            lease is not None
            and lease.source == record.source == "local"
            and lease.identifier == record.identifier
            and record.kind == ("normal" if lease.normal else "test")
            and record.phase == "starting"
            and record.attempted is False
            and record.published is False
            and record.reported_local is None
            and record.projection_digest is None
            and type(record.revision) is int
            and record.request_matches()
            and request.state in ("accepted", "running")
            and previous_receipt(record)
        )
        return RetirementDecision(rule) if eligible else None
    if rule is RetirementRule.CLOSED:
        local = record.local
        eligible = (
            record.kind == "normal"
            and record.source == "local"
            and isinstance(record.identifier, str)
            and record.correlated(closed=True)
            and not record.pending
            and (lease is None or (lease.normal and lease.source == "local" and lease.identifier == record.identifier))
        )
        if not eligible:
            return None
        decision = RetirementDecision(rule, local)
        if lease is None and request.state not in decision.states:
            state = json.loads(record.body)
            if (
                state.get("state") == "completed"
                and state.get("charging") is False
                and state.get("local_transaction") == local
                and state.get("recovery_reason") == decision.update(record).reason
            ):
                return None
        return decision
    if rule is not RetirementRule.GENERIC:
        return None
    normal, cloud = record.kind == "normal", record.source == "cloud"
    # Earlier versions could complete timed metadata before the bound Stop was delivered.
    legacy_timed = record.phase == "completed" and record.pending_local_test
    if (
        worker_pending
        or record.kind not in ("normal", "test")
        or record.source not in ("local", "cloud")
        or (
            not legacy_timed
            and record.phase not in ("interrupted", "expired", "stopping", "running", "charging", "awaiting_power")
        )
        or lease is not None
        and (lease.identifier != record.identifier or lease.source != record.source)
        or active is not None
        or record.pending
        or vendor_pending
        or uncertain_vendor
        or mutation_pending
        or not record.request_matches(publication=True)
    ):
        return None
    local = record.reported_local if cloud else record.resolved_local
    if cloud:
        if normal or (local is None and (lease is not None or record.phase != "interrupted")):
            return None
        if local is not None and record.source_verified is not True:
            return None
    elif local is None:
        if legacy_timed or record.published is not False or not record.unissued(unbound=False):
            return None
    elif not record.bound(local):
        return None
    if local is not None:
        row = record.session_for(local)
        if (
            type(local) is not int
            or row is None
            or row.stop is None
            or not cloud
            and row.digest != record.projection_digest
        ):
            return None
    return RetirementDecision(rule, local)
