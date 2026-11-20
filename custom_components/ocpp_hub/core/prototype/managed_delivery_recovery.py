"""Explicit operator mapping of one ambiguous cloud Start, retaining zero until a new verified Pause."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .control_state import ControlState
from .events import OBSERVATION_ACTIONS
from .managed_current import managed_current_ceiling
from .managed_denial_faults import DELIVERY_MAPPINGS as KEY
from .managed_denial_faults import operator_for_queued_lifecycle, operator_mapping_members_match
from .managed_intent import unfinished_owner_requests
from .managed_start_delivery import retire_start_retry
from .native_bounded import NativeBoundedSpec
from .native_history import audit_history
from .security import SecurityError
from .store import Delivery, Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook

if TYPE_CHECKING:
    from .managed import ManagedController

FAULT = "owner_control_or_delivery_uncertain"


# A completed local Start may retain running metadata when its delivery fault bypasses ordinary closure publication.
def closed_normal_delivery(j: Journal) -> bool:
    record, head = j.starts.closed(), j.next_delivery()
    return bool(
        record is not None
        and record.phase in ("running", "interrupted")
        and record.published is True
        and record.reported_local == record.local
        and record.receipt.stage == "accepted"
        and record.request is not None
        and record.request.state == "completed"
        and head is not None
        and head.action == "StartTransaction"
        and head.state in ("ambiguous", "rejected")
        and head.transaction == record.local
    )


@dataclass(frozen=True)
class DeliveryAttestation:
    """A private operator attestation to independently inspected cloud data, never a guessed transaction ID."""

    identity_digest: str
    spec_digest: str
    local: int
    vendor: int
    start_token: str
    start_digest: str
    stop_digest: str
    evidence_sha256: str
    verified_at_epoch: float

    # Exact shapes and recent operator verification prevent accidental reuse of another transaction's evidence.
    @classmethod
    def parse(cls, value: Any, *, recent: bool = True) -> DeliveryAttestation:
        fields = set(cls.__dataclass_fields__)
        if not isinstance(value, dict) or set(value) != fields:
            raise SecurityError("cloud transaction evidence has an invalid shape")
        for name in ("identity_digest", "spec_digest", "start_digest", "stop_digest", "evidence_sha256"):
            if not isinstance(value[name], str) or re.fullmatch(r"[a-f0-9]{64}", value[name]) is None:
                raise SecurityError("cloud transaction evidence has an invalid digest")
        if any(type(value[name]) is not int or not 0 <= value[name] < 2**31 for name in ("local", "vendor")):
            raise SecurityError("cloud transaction evidence has an invalid transaction ID")
        if not isinstance(value["start_token"], str) or re.fullmatch(r"e-[a-f0-9]{32}", value["start_token"]) is None:
            raise SecurityError("cloud transaction evidence has an invalid Start token")
        at = value["verified_at_epoch"]
        if (
            type(at) not in (int, float)
            or not math.isfinite(at)
            or at <= 0
            or time.time() - at < -2
            or recent
            and time.time() - at > 3600
        ):
            raise SecurityError("cloud transaction evidence needs recent independent verification")
        return cls(**value)


# These immutable admissions may not change between mapping, actual Stop acknowledgement and fault reconciliation.
def delivery_anchor(j: Journal, spec: NativeBoundedSpec) -> dict[str, Any]:
    return {
        "identity": fingerprint(j.identity),
        "spec": spec.digest,
        "allocation": fingerprint(j.get("trial_allocation")),
        "binding": fingerprint(j.get("trial_binding")),
        "capability": fingerprint(j.get("managed_current_capability")),
        "counter": j.get("next_transaction"),
        "mode": j.get("managed_mode"),
        "selected_a": j.get("control", {}).get("resume_limit"),
    }


# The offline tool has no charger writer; it verifies the already installed hold and all original resources.
def require_delivery_zero(j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence) -> None:
    ceiling = managed_current_ceiling(j, spec)
    if ceiling == 32:
        spec.require_managed(n, j.identity, closed=True, allow_unplugged=True)
        binding = fingerprint(n.configuration | {"AmpLmt": "6"})
    else:
        spec.require(n, j.identity, "trial", closed=True, allow_unplugged=True)
        binding = n.physical.configuration_digest
    spec.require_boot(j.get("boot"))
    j.require_allocation()
    intent, control = j.get("managed_control_intent", {}), j.get("control", {})
    if (
        j.get("managed_mode") != {"mode": "app_schedule", "spec_digest": spec.digest, "local_start": False}
        or intent.get("mode") != "paused"
        or type(intent.get("revision")) is not int
        or not 0 <= intent["revision"] < 2**53
        or not ControlState(control).explicit_local_hold
        or control.get("start_pending")
        or type(control.get("resume_limit")) not in (int, float)
        or not 6 <= control["resume_limit"] <= ceiling
        or int(control["resume_limit"]) != control["resume_limit"]
        or not j.phase.read().supervised
        or j.get("native_bounded_handover", {}).get("stage") != "admitted"
        or j.get("native_bounded_handover", {}).get("fault")
        or j.get("trial_preservation_fault")
        or j.get("trial_inventory_fault")
        or j.get("uncertain_vendor_command")
        or j.get("vendor_start_grant")
        or j.get("trial_binding", {}).get("configuration_digest") != binding
        or j.get("trial_binding", {}).get("boot_digest") != spec.boot_digest
        or n.saved_schedule_digest != spec.saved_schedule_digest
        or n.physical.transaction_open
        or any(v != 0 for v in n.currents_a + n.powers_w)
        or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL LIMIT 1").fetchone()
        or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or unfinished_owner_requests(j)
        or not j.current_change.read().settled_or_missing
        or j.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
        or j.get("managed_test_charge", {}).get("state") not in (None, "completed")
        and not closed_normal_delivery(j)
    ):
        raise RecoveryRequiredError("delivery reconciliation requires the existing paused, closed, original zero hold")
    audit_history(j, spec.digest, list(n.history), False)
    station = [r for r in book.owned_present(n.physical) if r.kind == "profile" and r.identifier == HUB_PROFILE_ID]
    row = j.db.execute(
        "SELECT state,confirmed FROM trial_resources WHERE key=?", (f"profile:{HUB_PROFILE_ID}",)
    ).fetchone()
    if not book.confirmed_station_zero(station, row):
        raise RecoveryRequiredError("delivery reconciliation requires independently confirmed owned station zero")


# Check that the ambiguous head owns an exact closed pair and later reports have no separate delivery uncertainty.
def require_queued_backlog(j: Journal, head: Delivery) -> None:
    unmapped = {row["local"] for row in j.db.execute("SELECT local FROM transactions WHERE vendor IS NULL")}
    started: set[int] = set()
    active: int | None = None
    for row in j.db.execute(
        "SELECT q.*, t.local AS known_local, t.start, t.stop, t.vendor, r.token AS reply "
        "FROM outbox q LEFT JOIN transactions t ON t.local=q.local "
        "LEFT JOIN delivery_replies r ON r.token=q.token ORDER BY q.seq"
    ):
        local, action = row["local"], row["action"]
        if row["reply"] is not None or (row["token"] != head.token and row["state"] != "queued"):
            raise RecoveryRequiredError("another report has an unresolved delivery outcome")
        if action == "StartTransaction":
            if active is not None or local not in unmapped or local in started or row["payload"] != row["start"]:
                raise RecoveryRequiredError("delivery reconciliation needs exact saved Start and Stop pairs in order")
            started.add(local)
            active = local
        elif action == "StopTransaction":
            if (
                active is None
                or local != active
                or local not in unmapped
                or row["payload"] != row["stop"]
                or json.loads(row["payload"]).get("transactionId") != local
            ):
                raise RecoveryRequiredError("delivery reconciliation needs exact saved Start and Stop pairs in order")
            unmapped.remove(local)
            active = None
        elif action == "MeterValues":
            report = json.loads(row["payload"])
            if (
                not isinstance(report, dict)
                or local is None
                and "transactionId" in report
                or local is not None
                and (
                    type(report.get("transactionId")) is not int
                    or report["transactionId"] != row["known_local"]
                    or row["vendor"] is None
                    and local not in started
                )
            ):
                raise RecoveryRequiredError("queued meter reading does not identify its saved session")
        elif action not in OBSERVATION_ACTIONS or (local is not None and row["known_local"] is None):
            raise RecoveryRequiredError("delivery reconciliation found an unsupported queued report")
    if unmapped or head.transaction not in started:
        raise RecoveryRequiredError("delivery reconciliation needs all genuine closed Start and Stop pairs")


# A later Start may be reconciled alongside an already wire-settled operator recovery for an earlier session.
def delivery_source(
    j: Journal, spec: NativeBoundedSpec, fault: dict[str, Any], head: Delivery
) -> tuple[dict[str, Any], str | None]:
    if fault.get("classification") == FAULT:
        return fault, None
    operator = operator_for_queued_lifecycle(j, fault, head)
    if operator is not None and operator["anchor"] == delivery_anchor(j, spec):
        return operator["prior"]["fault"], fault["proof"]
    raise RecoveryRequiredError("the Start belongs to a different or unfinished operator recovery")


# Inspection reveals opaque journal correlation/digests, never credentials, raw tags or private native bodies.
def delivery_candidate(j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence) -> dict[str, Any]:
    require_delivery_zero(j, book, spec, n)
    fault, head = j.fault.read({}), j.next_delivery()
    if head is None or head.action != "StartTransaction" or head.state not in ("ambiguous", "rejected"):
        raise RecoveryRequiredError("the delivery head is not one ambiguous managed Start")
    source, _ = delivery_source(j, spec, fault, head)
    if any(proof["prior_fault"] != source for proof in pending_delivery_proofs(j)):
        raise RecoveryRequiredError("earlier Start mappings belong to a different recovery")
    require_queued_backlog(j, head)
    row = j.db.execute("SELECT stop FROM transactions WHERE local=?", (head.transaction,)).fetchone()
    return {
        "identity_digest": fingerprint(j.identity),
        "spec_digest": spec.digest,
        "local": head.transaction,
        "start_token": head.token,
        "start_digest": fingerprint(head.payload),
        "stop_digest": fingerprint(json.loads(row["stop"])),
    }


# The existing exclusive Journal owner and one atomic acknowledgement commit bind mapping and immutable audit.
def reconcile_delivery_start(
    j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence, evidence: DeliveryAttestation
) -> dict[str, Any]:
    evidence = DeliveryAttestation.parse(asdict(evidence), recent=False)
    require_delivery_zero(j, book, spec, n)
    records = j.get(KEY, {})
    existing = records.get(str(evidence.local))
    if existing is not None:
        if existing["proof"]["input_digest"] != fingerprint(asdict(evidence)) or not matching_delivery_row(
            j, existing["proof"]
        ):
            raise RecoveryRequiredError("earlier delivery evidence cannot be replaced")
        if j.db.execute("SELECT 1 FROM outbox WHERE token=?", (evidence.start_token,)).fetchone() is None:
            retire_start_retry(j, evidence.start_token, evidence.local, evidence.start_digest)
        return {
            "mapping_recorded": True,
            "already_recorded": True,
            "zero_hold_retained": True,
            "charging_started": False,
        }
    DeliveryAttestation.parse(asdict(evidence))
    candidate = delivery_candidate(j, book, spec, n)
    if any(getattr(evidence, key) != value for key, value in candidate.items()):
        raise RecoveryRequiredError("operator evidence does not identify the exact ambiguous Start")
    stop = j.db.execute(
        "SELECT token FROM outbox WHERE local=? AND action='StopTransaction'", (evidence.local,)
    ).fetchone()
    anchor, fault = delivery_anchor(j, spec), j.fault.read({})
    head = j.next_delivery()
    assert head is not None
    source, parent = delivery_source(j, spec, fault, head)
    normal = j.get("managed_test_charge") if closed_normal_delivery(j) else None
    proof = {
        "version": 2,
        "input_digest": fingerprint(asdict(evidence)),
        "attestation": asdict(evidence),
        "anchor": anchor,
        "prior_fault": source,
        **({"operator_parent": parent} if parent is not None else {}),
        "prior_intent": j.get("managed_control_intent"),
        "stop_token": stop["token"],
        "stop_wire_ack": None,
        "native_digest": n.digest,
        "native_observed_at": n.physical.observed_at,
        "recorded_at": time.time(),
        "zero_hold_retained": True,
        "charging_started": False,
        "prior_closed_normal_start": normal,
    }

    # Check the updated mapping and write its proof in the transaction that removes the Start.
    def recorded() -> None:
        if delivery_anchor(j, spec) != anchor or j.fault.read() != fault or j.get(KEY, {}) != records:
            raise RecoveryRequiredError("delivery reconciliation authority changed")
        if not matching_delivery_row(j, proof):
            raise RecoveryRequiredError("delivery reconciliation lifecycle changed")
        if normal is not None:
            if j.get("managed_test_charge") != normal:
                raise RecoveryRequiredError("closed normal Start metadata changed")
            j.starts.complete_normal(
                normal,
                None,
                finish_request=False,
                stopped_at=time.time(),
                reason="operator_delivery_verified_closed_zero",
            )
        j.put(KEY, {**records, str(evidence.local): {"proof": proof, "completed": None}})

    j.reconcile_start(evidence.local, evidence.vendor, recorded)
    return {"mapping_recorded": True, "already_recorded": False, "zero_hold_retained": True, "charging_started": False}


# Reconciliation proves only the immutable accepted pair and independently attested mapping, never a new lifecycle.
def matching_delivery_row(j: Journal, proof: dict[str, Any]) -> bool:
    e = proof["attestation"]
    row = j.db.execute("SELECT start,stop,vendor FROM transactions WHERE local=?", (e["local"],)).fetchone()
    return bool(
        row is not None
        and row["stop"] is not None
        and row["vendor"] == e["vendor"]
        and fingerprint(json.loads(row["start"])) == e["start_digest"]
        and fingerprint(json.loads(row["stop"])) == e["stop_digest"]
    )


# Keep every outstanding mapping together so a new Pause cannot acknowledge a subset.
def pending_delivery_proofs(j: Journal) -> list[dict[str, Any]]:
    return [entry["proof"] for _, entry in sorted(j.get(KEY, {}).items()) if entry.get("completed") is None]


# A new Pause may acknowledge every mapping only after the real Stops and the rest of the queue have drained.
def delivery_recovery_evidence(
    c: ManagedController, fault: dict[str, Any], operator: dict[str, Any] | None = None
) -> list[dict[str, Any]] | None:
    j = c.session.journal
    proofs = pending_delivery_proofs(j)
    if not proofs or j.next_delivery() is not None or j.get("managed_control_intent", {}).get("mode") != "paused":
        return None
    source = fault
    if fault.get("classification") != FAULT:
        prior = operator["prior"] if operator is not None else {}
        if operator is None or not operator_mapping_members_match(j, operator):
            return None
        source = prior.get("fault") or {}
        if source.get("classification") != FAULT:
            return None
    anchor = delivery_anchor(j, c.native.spec)
    if any(
        proof.get("prior_fault") != source
        or proof.get("anchor") != anchor
        or not matching_delivery_row(j, proof)
        or proof.get("version") not in (1, 2)
        or proof["version"] == 2
        and (proof.get("stop_wire_ack") or {}).get("delivery_token") != proof["stop_token"]
        for proof in proofs
    ):
        return None
    return proofs


# Preserve the acknowledgement digest of older, single-mapping Pause receipts.
def delivery_recovery_digest(proofs: list[dict[str, Any]]) -> str:
    return fingerprint(proofs[0] if len(proofs) == 1 else proofs)


# Consume every exact audit in the same commit that clears the acknowledged managed fault.
def complete_delivery_recovery(j: Journal, proofs: list[dict[str, Any]], pause: str) -> None:
    records = j.get(KEY)
    completed = {"pause": pause, "at_epoch": time.time()}
    for proof in proofs:
        key = str(proof["attestation"]["local"])
        if records.get(key) != {"proof": proof, "completed": None}:
            raise RecoveryRequiredError("delivery acknowledgement was already consumed")
        records[key] = {"proof": proof, "completed": completed}
    j.put(KEY, records)
