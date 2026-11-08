"""Write-ahead ownership survives transaction retirement and distinguishes original native resources."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .policy import DeniedError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, encoded, fingerprint
from .trial_evidence import Evidence, Resource, profile_projection


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
        return {
            "phase": self.journal.get("trial_phase"),
            "resources": self.journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0],
            "uncertain_resources": self.journal.db.execute(
                "SELECT COUNT(*) FROM trial_resources WHERE state='uncertain'"
            ).fetchone()[0],
            "transaction_open": self.journal.active() is not None,
        }

    # Preserve every original resource, including adopted plans and inactive transaction-specific profiles.
    def check_originals(self, evidence: Evidence) -> None:
        if evidence.identity_digest != self.baseline.identity_digest:
            raise SecurityError("native evidence belongs to another charger")
        current = {r.key: r for r in evidence.resources}
        for original in self.baseline.resources:
            present = current.get(original.key)
            if present is None or present.digest != original.digest:
                raise SecurityError("an original native resource changed; independent reconciliation is required")

    # Arm only a fresh independently observed station with a genuinely empty lifecycle and outbox.
    def activate(self, evidence: Evidence) -> None:
        evidence.require_unplugged(self.journal.identity)
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
        if phase != "active" and action not in reads:
            key = "profile:" + str(payload.get("id"))
            if (
                phase in ("blocked", "recovering")
                and action == "RemoteStopTransaction"
                and payload.get("transactionId") == self.journal.active()
            ):
                return
            if phase in ("blocked", "recovering") and action == "SetChargingProfile":
                projected = profile_projection(payload)
                if (
                    projected["id"] == HUB_PROFILE_ID
                    and projected["limit"] == 0
                    and self.journal.db.execute(
                        "SELECT 1 FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
                    ).fetchone()
                ):
                    phase = "active"  # Only this recorded zero-current intent proceeds through normal ownership checks.
            if phase == "rollback" and action == "ClearChargingProfile" and set(payload) == {"id"}:
                if self.journal.db.execute("SELECT 1 FROM trial_resources WHERE key=?", (key,)).fetchone():
                    return
            if phase != "active":
                raise DeniedError("trial is not accepting mutations")
        if action == "SetChargingProfile":
            canonical = profile_projection(payload)
            if canonical["limit"] > 6:
                raise DeniedError("trial resource exceeds its six-ampere ceiling")
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
            if (
                set(payload) != {"id"}
                or not self.journal.db.execute("SELECT 1 FROM trial_resources WHERE key=?", (key,)).fetchone()
            ):
                raise DeniedError("trial clear does not name an owned resource")
        elif action == "DataTransfer":
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
                    "UPDATE trial_resources SET confirmed=desired,state='confirmed' WHERE key=?", (key,)
                )
            else:
                self.journal.db.execute("UPDATE trial_resources SET state='clear_acknowledged' WHERE key=?", (key,))
        else:
            self.journal.db.execute("UPDATE trial_resources SET state='uncertain' WHERE key=?", (key,))

    # Independent complete inventory reconciles a lost ACK without blindly replaying its physical mutation.
    def owned_present(self, evidence: Evidence) -> list[Resource]:
        self.check_originals(evidence)
        current = {r.key: r for r in evidence.resources}
        originals = {r.key for r in self.baseline.resources}
        rows = self.journal.db.execute("SELECT * FROM trial_resources").fetchall()
        if set(current) - originals - {row["key"] for row in rows}:
            raise SecurityError("unexpected native resources require independent reconciliation")
        result = []
        for row in rows:
            resource = current.get(row["key"])
            if resource is None:
                self.journal.db.execute(
                    "UPDATE trial_resources SET state='absent_verified',observed_digest=NULL WHERE key=?", (row["key"],)
                )
                continue
            if row["state"] == "absent_verified":
                raise SecurityError("a resource reappeared after verified absence without a new trial intent")
            possible = [json.loads(row[key]) for key in ("desired", "confirmed") if row[key] is not None]
            if resource.canonical not in possible:
                raise SecurityError("owned profile differs from every recorded trial intent")
            if row["observed_digest"] is not None and row["observed_digest"] != resource.digest:
                raise SecurityError("owned profile changed after rollback verification")
            self.journal.db.execute(
                "UPDATE trial_resources SET observed_digest=? WHERE key=?", (resource.digest, resource.key)
            )
            result.append(resource)
        return sorted(result, key=lambda r: (r.identifier == HUB_PROFILE_ID, r.key))
