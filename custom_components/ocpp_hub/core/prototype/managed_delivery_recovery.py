"""Explicit operator mapping of one ambiguous cloud Start, retaining zero until a new verified Pause."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .local_control import local_start_receipt
from .managed_current import managed_current_ceiling
from .managed_cutoffs import supervised_timer
from .managed_occurrences import occurrences
from .native_bounded import NativeBoundedSpec
from .native_history import audit_history
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook

if TYPE_CHECKING:
    from .managed import ManagedController

KEY = "managed_delivery_reconciliations"
FAULT = "owner_control_or_delivery_uncertain"


# A completed local Start may retain running metadata when its delivery fault bypasses ordinary closure publication.
def closed_normal_delivery(j: Journal) -> bool:
    state = j.get("managed_test_charge", {})
    local, owner, head = state.get("local_transaction"), local_start_receipt(j), j.next_delivery()
    row = j.db.execute("SELECT start,stop FROM transactions WHERE local=?", (local,)).fetchone()
    request = j.db.execute("SELECT action,state FROM managed_requests WHERE id=?", (state.get("id"),)).fetchone()
    return bool(
        state.get("kind") == "normal"
        and state.get("source") == "local"
        and state.get("state") in ("running", "interrupted")
        and state.get("start_rpc_accepted") is True
        and type(local) is int
        and owner.get("local") == local
        and owner.get("stage") == "accepted"
        and owner.get("digest") == state.get("tag_digest")
        and row is not None
        and row["stop"] is not None
        and fingerprint(json.loads(row["start"])["idTag"].casefold()) == state.get("tag_digest")
        and request is not None
        and tuple(request) == ("start", "completed")
        and head is not None
        and head.action == "StartTransaction"
        and head.state == "ambiguous"
        and head.transaction == local
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
        or control.get("owner") != "local"
        or control.get("inhibit") is not True
        or control.get("start_pending")
        or type(control.get("resume_limit")) not in (int, float)
        or not 6 <= control["resume_limit"] <= ceiling
        or int(control["resume_limit"]) != control["resume_limit"]
        or j.get("trial_phase") not in ("blocked", "recovering", "active")
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
        or j.db.execute(
            "SELECT 1 FROM managed_requests WHERE action!='pause' "
            "AND state IN ('accepted','running','uncertain') LIMIT 1"
        ).fetchone()
        or j.get("managed_current_change", {}).get("state") not in (None, "completed", "cancelled")
        or j.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
        or j.get("managed_test_charge", {}).get("state") not in (None, "completed")
        and not closed_normal_delivery(j)
        or any(supervised_timer(lease) or lease.get("state") == "requested" for lease in occurrences(j).values())
    ):
        raise RecoveryRequiredError("delivery reconciliation requires the existing paused, closed, original zero hold")
    audit_history(j, spec.digest, list(n.history), False)
    station = [r for r in book.owned_present(n.physical) if r.kind == "profile" and r.identifier == HUB_PROFILE_ID]
    row = j.db.execute(
        "SELECT state,confirmed FROM trial_resources WHERE key=?", (f"profile:{HUB_PROFILE_ID}",)
    ).fetchone()
    if (
        len(station) != 1
        or station[0].canonical.get("limit") != 0
        or row is None
        or row["state"] != "confirmed"
        or row["confirmed"] is None
        or json.loads(row["confirmed"]) != station[0].canonical
    ):
        raise RecoveryRequiredError("delivery reconciliation requires independently confirmed owned station zero")


# Inspection reveals opaque journal correlation/digests, never credentials, raw tags or private native bodies.
def delivery_candidate(j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence) -> dict[str, Any]:
    require_delivery_zero(j, book, spec, n)
    fault, head = j.get("managed_control_fault", {}), j.next_delivery()
    if (
        fault.get("classification") != FAULT
        or head is None
        or head.action != "StartTransaction"
        or head.state != "ambiguous"
    ):
        raise RecoveryRequiredError("the delivery head is not one ambiguous managed Start")
    row = j.db.execute("SELECT start,stop,vendor FROM transactions WHERE local=?", (head.transaction,)).fetchone()
    stop = j.db.execute(
        "SELECT token,payload,state FROM outbox WHERE local=? AND action='StopTransaction'", (head.transaction,)
    ).fetchone()
    if (
        row is None
        or row["vendor"] is not None
        or row["stop"] is None
        or row["start"] != json.dumps(head.payload, sort_keys=True, separators=(",", ":"))
        or stop is None
        or stop["state"] != "queued"
        or stop["payload"] != row["stop"]
        or j.db.execute(
            "SELECT 1 FROM transactions WHERE vendor IS NULL AND local!=? LIMIT 1", (head.transaction,)
        ).fetchone()
        or j.db.execute(
            "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') AND local!=? LIMIT 1",
            (head.transaction,),
        ).fetchone()
    ):
        raise RecoveryRequiredError("delivery reconciliation needs the exact genuine closed Start/Stop pair")
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
    anchor, fault = delivery_anchor(j, spec), j.get("managed_control_fault")
    normal = j.get("managed_test_charge") if closed_normal_delivery(j) else None
    proof = {
        "version": 1,
        "input_digest": fingerprint(asdict(evidence)),
        "attestation": asdict(evidence),
        "anchor": anchor,
        "prior_fault": fault,
        "prior_intent": j.get("managed_control_intent"),
        "stop_token": stop["token"],
        "native_digest": n.digest,
        "native_observed_at": n.physical.observed_at,
        "recorded_at": time.time(),
        "zero_hold_retained": True,
        "charging_started": False,
        "prior_closed_normal_start": normal,
    }

    # This callback runs inside acknowledge's transaction after mapping and Start removal, with no await.
    def recorded() -> None:
        if delivery_anchor(j, spec) != anchor or j.get("managed_control_fault") != fault or j.get(KEY, {}) != records:
            raise RecoveryRequiredError("delivery reconciliation authority changed")
        if not matching_delivery_row(j, proof):
            raise RecoveryRequiredError("delivery reconciliation lifecycle changed")
        if normal is not None:
            if j.get("managed_test_charge") != normal:
                raise RecoveryRequiredError("closed normal Start metadata changed")
            j.put(
                "managed_test_charge",
                {
                    **normal,
                    "state": "completed",
                    "charging": False,
                    "stopped_at": time.time(),
                    "recovery_reason": "operator_delivery_verified_closed_zero",
                },
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


# A new Pause may acknowledge this proof only after actual delivery has removed its retained genuine Stop.
def delivery_recovery_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    j = c.session.journal
    matches = [
        entry
        for entry in j.get(KEY, {}).values()
        if entry.get("completed") is None and entry["proof"].get("prior_fault") == fault
    ]
    if fault.get("classification") != FAULT or len(matches) != 1:
        return None
    proof: dict[str, Any] = matches[0]["proof"]
    if (
        proof.get("version") != 1
        or proof.get("anchor") != delivery_anchor(j, c.native.spec)
        or not matching_delivery_row(j, proof)
        or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
        or j.db.execute("SELECT 1 FROM outbox WHERE token=?", (proof["stop_token"],)).fetchone()
        or j.get("managed_control_intent", {}).get("mode") != "paused"
    ):
        return None
    return proof


# Consume only this exact audit in the same commit that clears its acknowledged managed fault.
def complete_delivery_recovery(j: Journal, proof: dict[str, Any], pause: str) -> None:
    records = j.get(KEY)
    key = str(proof["attestation"]["local"])
    if records[key] != {"proof": proof, "completed": None}:
        raise RecoveryRequiredError("delivery acknowledgement was already consumed")
    j.put(KEY, {**records, key: {"proof": proof, "completed": {"pause": pause, "at_epoch": time.time()}}})
