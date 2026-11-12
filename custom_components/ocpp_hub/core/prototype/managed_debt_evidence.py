"""Strict private operator statements identifying one debt and independent evidence of its settled outcome."""

from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass
from typing import Any, Self

from .security import SecurityError


@dataclass(frozen=True)
class DebtAttestation:
    """The evidence digest binds an operator decision; it does not itself prove the remote command's outcome."""

    identity_digest: str
    spec_digest: str
    candidate_digest: str
    evidence_sha256: str
    verified_at_epoch: float

    # Exact types and recent independent verification prevent accepting partial or accidentally reused statements.
    @classmethod
    def parse(cls, value: Any, *, recent: bool = True) -> Self:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise SecurityError("operator debt evidence has an invalid shape")
        for key, item in value.items():
            if key.endswith("digest") or key == "evidence_sha256":
                if not isinstance(item, str) or re.fullmatch(r"[a-f0-9]{64}", item) is None:
                    raise SecurityError("operator debt evidence has an invalid digest")
        at = value["verified_at_epoch"]
        if (
            type(at) not in (int, float)
            or not math.isfinite(at)
            or at <= 0
            or time.time() - at < -2
            or recent
            and time.time() - at > 3600
        ):
            raise SecurityError("operator debt evidence needs recent independent verification")
        for key in ("command_quiescent", "original_state_independently_verified", "command_no_longer_pending"):
            if key in value and value[key] is not True:
                raise SecurityError(
                    "operator must independently establish that this exact command cannot execute later"
                )
        for key in ("mutation_token", "request_id"):
            if key in value and (not isinstance(value[key], str) or re.fullmatch(r"[a-f0-9]{32}", value[key]) is None):
                raise SecurityError("operator evidence has an invalid command identity")
        if "observation_token" in value and (
            not isinstance(value["observation_token"], str)
            or re.fullmatch(r"e-[a-f0-9]{32}", value["observation_token"]) is None
        ):
            raise SecurityError("operator evidence has an invalid report identity")
        if "action" in value and (not isinstance(value["action"], str) or not 1 <= len(value["action"]) <= 80):
            raise SecurityError("operator evidence has an invalid action")
        return cls(**value)


@dataclass(frozen=True)
class VendorWriteAttestation(DebtAttestation):
    """Independently establish the named vendor write is settled and original resources are intact."""

    mutation_token: str
    action: str
    payload_digest: str
    command_quiescent: bool
    original_state_independently_verified: bool


@dataclass(frozen=True)
class LocalStartAttestation(DebtAttestation):
    """Independent proof that this particular accepted owner Start is consumed or cancelled, never session counting."""

    request_id: str
    request_digest: str
    state_digest: str
    receipt_digest: str
    command_no_longer_pending: bool


@dataclass(frozen=True)
class ObservationAttestation(DebtAttestation):
    """Identify exactly the legacy rejected routine report to retire; no command uncertainty is waived."""

    observation_token: str
    action: str
    payload_digest: str


ATTESTATIONS: dict[str, type[DebtAttestation]] = {
    "vendor-write": VendorWriteAttestation,
    "local-start": LocalStartAttestation,
    "observation": ObservationAttestation,
}
