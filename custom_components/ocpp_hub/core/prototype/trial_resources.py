"""Write-ahead ownership survives transaction retirement and distinguishes original native resources."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .ics import is_safe_transaction_query
from .policy import DeniedError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, encoded, fingerprint
from .trial_evidence import Evidence, Resource, profile_projection
from .trial_handover_evidence import HandoverEvidence


@dataclass(frozen=True, repr=False)
class Baseline:
    """An immutable independently observed inventory, rather than an id-range ownership guess."""

    identity_digest: str
    observed_at: float
    configuration_digest: str
    resources: tuple[Resource, ...]

    # Capture an unplugged idle station, rejecting existing profiles which could defeat the reserved local stack.
    @classmethod
    def capture(cls, identity: str, evidence: Evidence, stack: int = 16) -> Baseline:
        evidence.require_unplugged(identity)
        for resource in evidence.resources:
            if resource.kind == "profile" and (
                resource.identifier == HUB_PROFILE_ID or resource.body.get("purpose") == "ChargePointMaxProfile"
            ):
                raise SecurityError("baseline conflicts with the reserved trial station profile")
        return cls(evidence.identity_digest, evidence.observed_at, evidence.configuration_digest, evidence.resources)

    # Only a private file or private journal stores native bodies; public status exposes counts instead.
    def value(self) -> dict[str, Any]:
        return {
            "identity_digest": self.identity_digest,
            "observed_at": self.observed_at,
            "configuration_digest": self.configuration_digest,
            "resources": [
                {"kind": r.kind, "id": r.identifier, "body": r.body, "canonical": r.canonical} for r in self.resources
            ],
        }

    # Revalidate a private baseline without contacting a device or inferring that its original resources are ours.
    @classmethod
    def parse(cls, value: dict[str, Any]) -> Baseline:
        try:
            if set(value) != {"identity_digest", "observed_at", "configuration_digest", "resources"}:
                raise ValueError
            if any(
                not isinstance(value[key], str) or len(value[key]) != 64
                for key in ("identity_digest", "configuration_digest")
            ):
                raise ValueError
            rows = value["resources"]
            if not isinstance(rows, list) or len(rows) > 128:
                raise ValueError
            resources = []
            for row in rows:
                if set(row) != {"kind", "id", "body", "canonical"} or row["kind"] not in ("profile", "plan"):
                    raise ValueError
                if (
                    type(row["id"]) is not int
                    or row["id"] < 1
                    or not isinstance(row["body"], dict)
                    or not isinstance(row["canonical"], dict)
                ):
                    raise ValueError
                resources.append(Resource(row["kind"], row["id"], row["body"], row["canonical"]))
            if len({r.key for r in resources}) != len(resources):
                raise ValueError
            when = value["observed_at"]
            if type(when) not in (float, int) or not 0 < when <= time.time() + 2:
                raise ValueError
            return cls(value["identity_digest"], float(when), value["configuration_digest"], tuple(resources))
        except ValueError, TypeError, KeyError:
            raise SecurityError("trial baseline is incomplete or malformed") from None


class ResourceBook:
    """Receipts persist after Stop retires vendor aliases and after a cleanup ACK is lost."""

    # A fresh trial cannot silently claim an older unowned journal or replace its original baseline.
    def __init__(self, journal: Journal, baseline: Baseline) -> None:
        self.journal, self.baseline = journal, baseline
        self.before_positive_release: Callable[[], None] | None = None
        self.temporary_plan_write: dict[str, Any] | None = None
        if baseline.identity_digest != fingerprint(journal.identity):
            raise SecurityError("baseline belongs to another charger")
        for r in baseline.resources:
            if r.kind == "profile" and (
                r.identifier == HUB_PROFILE_ID or r.body.get("purpose") == "ChargePointMaxProfile"
            ):
                raise SecurityError("baseline conflicts with the reserved station profile")
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS trial_resources("
            "key TEXT PRIMARY KEY,desired TEXT,confirmed TEXT,state TEXT NOT NULL,observed_digest TEXT)"
        )
        saved = journal.get("trial_baseline")
        if saved is not None and saved != baseline.value():
            raise SecurityError("journal belongs to another trial baseline")
        if saved is None:
            occupied = journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            occupied += journal.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0]
            if occupied:
                raise SecurityError("trial requires a new journal or its original verified baseline")
            journal.put("trial_baseline", baseline.value())
            journal.put("trial_phase", "prepared")

    # Public health information includes no endpoints, identifiers, body contents or credentials.
    def status(self) -> dict[str, Any]:
        fault = self.journal.get("trial_preservation_fault", {})
        inventory_fault = self.journal.get("trial_inventory_fault", {})
        return {
            "phase": self.journal.get("trial_phase"),
            "resources": self.journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0],
            "uncertain_resources": self.journal.db.execute(
                "SELECT COUNT(*) FROM trial_resources WHERE state='uncertain'"
            ).fetchone()[0],
            "transaction_open": self.journal.active() is not None,
            "original_profiles": self.original_profile_count,
            "new_profile_writes_blocked": bool(self.original_profile_count or fault or inventory_fault),
            "automatic_clear_blocked": bool(self.original_profile_count or fault or inventory_fault),
            "preservation_failed": bool(fault),
            "inventory_failed": bool(inventory_fault),
            "missing_original_resources": len(fault.get("missing", [])),
            "changed_original_resources": len(fault.get("changed", [])),
        }

    # Inactive transaction profiles are still original resources; selective deletion is not yet proven on this device.
    @property
    def original_profile_count(self) -> int:
        return sum(resource.kind == "profile" for resource in self.baseline.resources)

    # Block new trials and automatic clears without making older journals unreadable for diagnosis or safe recovery.
    def require_profile_free_baseline(self) -> None:
        if self.original_profile_count:
            raise SecurityError("existing original profiles block trial writes until selective cleanup is proven")

    # Freeze unexplained owned/unknown inventory changes without reclassifying them as original-resource loss.
    def _inventory_failure(self, reason: str, key: str | None = None) -> None:
        if not self.journal.get("trial_inventory_fault"):
            self.journal.put("trial_inventory_fault", {"reason": reason, "resource": key})
        self.journal.db.execute(
            "UPDATE trial_resources SET state='uncertain' WHERE key=? AND state!='absent_verified'",
            (key or "profile:" + str(HUB_PROFILE_ID),),
        )
        if self.journal.get("trial_phase") not in ("rollback", "backend_restore_pending", "complete"):
            self.journal.put("trial_phase", "blocked")
        raise SecurityError("native inventory fault: " + reason + "; independent reconciliation is required")

    # An existing hold may be lowered to zero, but verified absence must never authorize recreating it after cleanup.
    def _safety_write(self, action: str, payload: dict[str, Any]) -> bool:
        if action == "RemoteStopTransaction":
            active = self.journal.active()
            return active is not None and payload.get("transactionId") == active
        if action != "SetChargingProfile":
            return False
        profile = profile_projection(payload)
        receipt = self.journal.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        return (
            profile["id"] == HUB_PROFILE_ID
            and profile["connector"] == 0
            and profile["purpose"] == "ChargePointMaxProfile"
            and profile["limit"] == 0
            and receipt is not None
            and receipt["state"] != "absent_verified"
        )

    # Preserve every original resource, including adopted plans and inactive transaction-specific profiles.
    def check_originals(self, evidence: Evidence) -> None:
        if evidence.identity_digest != self.baseline.identity_digest:
            raise SecurityError("native evidence belongs to another charger")
        current = {r.key: r for r in evidence.resources}
        missing = [resource.key for resource in self.baseline.resources if resource.key not in current]
        changed = [
            resource.key
            for resource in self.baseline.resources
            if resource.key in current and current[resource.key].digest != resource.digest
        ]
        fault = self.journal.get("trial_preservation_fault")
        if not fault and (missing or changed):
            fault = {"missing": missing, "changed": changed, "observed_at": evidence.observed_at}
            self.journal.put("trial_preservation_fault", fault)
        if fault:
            if self.journal.get("trial_phase") not in ("rollback", "backend_restore_pending", "complete"):
                self.journal.put("trial_phase", "blocked")
            raise SecurityError("original resource preservation failed; independent reconciliation is required")

    # Arm only a fresh independently observed station with a genuinely empty lifecycle and outbox.
    def activate(self, evidence: Evidence) -> None:
        evidence.require_unplugged(self.journal.identity)
        self._activate(evidence)

    # Connected admission is explicitly separate and requires the independent native USER limit on every phase.
    def activate_bounded(self, native: HandoverEvidence, hardware: str) -> None:
        native.require_closed(self.journal.identity, hardware, 6, 32)
        self._activate(native.physical)

    # Share only the empty-ledger and preservation checks after the selected physical admission guard has passed.
    def _activate(self, evidence: Evidence) -> None:
        self.owned_present(evidence)
        if self.journal.active() is not None or self.journal.next_delivery() is not None:
            raise RecoveryRequiredError("trial lifecycle requires reconciliation before activation")
        if self.journal.get("trial_phase") not in ("prepared", "active"):
            raise RecoveryRequiredError("rollback has started or completed; trial cannot automatically rearm")
        self.journal.put("trial_phase", "active")

    # Persist intent immediately before send; only the bounded constant-profile trial subset can create resources.
    def before_send(self, action: str, payload: dict[str, Any]) -> None:
        phase = self.journal.get("trial_phase")
        reads = ("GetConfiguration", "GetCompositeSchedule", "TriggerMessage")
        reading = action in reads or action == "DataTransfer" and is_safe_transaction_query(payload)
        if not reading:
            safety = self._safety_write(action, payload)
            if (
                self.journal.get("trial_preservation_fault") or self.journal.get("trial_inventory_fault")
            ) and not safety:
                raise DeniedError("native resource audit failed; mutations remain frozen")
            if self.original_profile_count and (
                action == "ClearChargingProfile" or action == "SetChargingProfile" and not safety
            ):
                raise DeniedError("original profiles prevent new profile writes and automatic clears")
        if phase != "active" and not reading:
            key = "profile:" + str(payload.get("id"))
            if (
                phase in ("blocked", "recovering", "rollback", "backend_restore_pending")
                and action == "RemoteStopTransaction"
                and safety
            ):
                return
            if phase in ("blocked", "recovering") and action == "SetChargingProfile":
                projected = profile_projection(payload)
                if projected["id"] == HUB_PROFILE_ID and projected["limit"] == 0 and safety:
                    phase = "active"  # Only this recorded zero-current intent proceeds through normal ownership checks.
            if phase == "rollback" and action == "ClearChargingProfile" and set(payload) == {"id"}:
                receipt = self.journal.db.execute("SELECT state FROM trial_resources WHERE key=?", (key,)).fetchone()
                if receipt is not None and receipt["state"] != "absent_verified":
                    return
            if phase != "active":
                raise DeniedError("trial is not accepting mutations")
        if action == "SetChargingProfile":
            canonical = profile_projection(payload)
            ceiling = 6
            if self.journal.get("managed_current_capability") is not None:
                # Imports stay local because the immutable native specification also uses the baseline data shape.
                from .managed_current import managed_current_ceiling
                from .native_bounded import NativeBoundedSpec

                spec = NativeBoundedSpec.parse(self.journal.get("native_bounded_handover", {}).get("spec", {}))
                ceiling = managed_current_ceiling(self.journal, spec)
            if canonical["id"] == HUB_PROFILE_ID and canonical["limit"] > 0 and self.before_positive_release:
                self.before_positive_release()
            if canonical["limit"] > ceiling:
                raise DeniedError("trial resource exceeds its approved current ceiling")
            key = "profile:" + str(canonical["id"])
            for original in self.baseline.resources:
                if original.key == key:
                    raise DeniedError("trial cannot overwrite an original resource")
                if original.kind == "profile":
                    body = original.body
                    scope = (body.get("connnectroId"), body.get("purpose"))
                    if scope == tuple(canonical[k] for k in ("connector", "purpose")):
                        raise DeniedError("trial profile would replace an original profile scope")
            with self.journal.atomic():
                if self.journal.db.execute("SELECT 1 FROM trial_resources WHERE key=?", (key,)).fetchone() is None:
                    if self.journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0] >= 128:
                        raise RecoveryRequiredError("trial resource receipt capacity reached")
                    self.journal.db.execute(
                        "INSERT INTO trial_resources VALUES(?,?,NULL,'pending',NULL)", (key, encoded(canonical))
                    )
                else:
                    self.journal.db.execute(
                        "UPDATE trial_resources SET desired=?,state='pending',observed_digest=NULL WHERE key=?",
                        (encoded(canonical), key),
                    )
        elif action == "ClearChargingProfile":
            key = "profile:" + str(payload.get("id"))
            receipt = self.journal.db.execute("SELECT state FROM trial_resources WHERE key=?", (key,)).fetchone()
            if set(payload) != {"id"} or receipt is None or receipt["state"] == "absent_verified":
                raise DeniedError("trial clear does not name an owned resource")
        elif action == "DataTransfer":
            if payload is self.temporary_plan_write:
                self.temporary_plan_write = None
                return
            if payload.get("vendorId") != "IoCharger" or payload.get("messageId") not in (
                "getTransaction",
                "setTransaction",
                "delayStop",
            ):
                raise DeniedError("native plan editing is disabled in the first commissioning trial")
            if payload.get("messageId") == "setTransaction" and json.loads(payload["data"]).get("eco") != "Fast":
                raise DeniedError("first commissioning trial preserves the existing Fast mode")
        elif action not in ("RemoteStartTransaction", "RemoteStopTransaction", *reads):
            raise DeniedError("command is outside the narrow commissioning trial")

    # A real refusal remains distinct from confirmation; resource verification can reconcile either saved intent.
    def after_result(self, action: str, payload: dict[str, Any], answer: Frame) -> None:
        if action not in ("SetChargingProfile", "ClearChargingProfile"):
            return
        identifier = (
            payload["id"] if action == "ClearChargingProfile" else payload["csChargingProfiles"]["chargingProfileId"]
        )
        key = "profile:" + str(identifier)
        if answer[0] == 3 and answer[2].get("status") == "Accepted":
            if action == "SetChargingProfile":
                self.journal.db.execute(
                    "UPDATE trial_resources SET confirmed=desired,state='confirmed',observed_digest=NULL WHERE key=?",
                    (key,),
                )
            else:
                self.journal.db.execute("UPDATE trial_resources SET state='clear_acknowledged' WHERE key=?", (key,))
        else:
            self.journal.db.execute("UPDATE trial_resources SET state='uncertain' WHERE key=?", (key,))

    # Independent complete inventory reconciles a lost ACK without blindly replaying its physical mutation.
    def owned_present(self, evidence: Evidence) -> list[Resource]:
        evidence.require_fresh(self.journal.identity)
        current = {r.key: r for r in evidence.resources}
        originals = {r.key for r in self.baseline.resources}
        rows = self.journal.db.execute("SELECT * FROM trial_resources").fetchall()
        # Record observed owned absence even when a separate original-resource loss keeps rollback incomplete.
        for row in rows:
            if row["key"] not in current:
                self.journal.db.execute(
                    "UPDATE trial_resources SET state='absent_verified',observed_digest=NULL WHERE key=?", (row["key"],)
                )
        self.check_originals(evidence)
        if self.journal.get("trial_inventory_fault"):
            raise SecurityError("native inventory reconciliation is still required")
        temporary: set[str] = set()
        lease = self.journal.get("managed_temporary_schedule", {})
        if lease.get("state") in ("installing", "armed", "clearing"):
            expected = lease["body"]
            key = "plan:" + str(expected["planId"])
            if key in current and current[key].digest == fingerprint(expected) and key not in originals:
                temporary.add(key)
        if set(current) - originals - {row["key"] for row in rows} - temporary:
            self._inventory_failure("unexpected_resources")
        result = []
        for row in rows:
            resource = current.get(row["key"])
            if resource is None:
                continue
            if row["state"] == "absent_verified":
                self._inventory_failure("resource_reappeared", resource.key)
            possible = [json.loads(row[key]) for key in ("desired", "confirmed") if row[key] is not None]
            if resource.canonical not in possible:
                self._inventory_failure("owned_profile_changed", resource.key)
            if row["observed_digest"] is not None and row["observed_digest"] != resource.digest:
                self._inventory_failure("owned_profile_bytes_changed", resource.key)
            self.journal.db.execute(
                "UPDATE trial_resources SET observed_digest=? WHERE key=?", (resource.digest, resource.key)
            )
            result.append(resource)
        return sorted(result, key=lambda r: (r.identifier == HUB_PROFILE_ID, r.key))
