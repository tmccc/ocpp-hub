"""Pure selection for the Start lifecycle owner's failure settlement."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


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
    record_id: str | None
    record_source: str | None
    record_state: str | None
    request_action: str | None
    request_state: str | None
    lease_current: bool
    effects_started: bool
    tag_digest: str | None


@dataclass(frozen=True)
class StartReceiptFacts:
    request_id: str | None
    tag_digest: str | None
    stage: str | None
    local: int | None


@dataclass(frozen=True)
class StartFailureDecision:
    action: StartFailureAction
    retire_prepared: bool = False


# Select settlement from exact current identities; initial zero work counts as effects even before RemoteStart.
def settle_start(facts: StartFailureFacts, receipt: StartReceiptFacts | None = None) -> StartFailureDecision:
    if (
        facts.record_id != facts.request_id
        or facts.record_source != "local"
        or facts.request_action != facts.expected_action
    ):
        return StartFailureDecision(StartFailureAction.UNRELATED)
    retire = bool(
        receipt is not None
        and receipt.request_id == facts.request_id
        and receipt.tag_digest == facts.tag_digest
        and receipt.stage == "prepared"
        and receipt.local is None
    )
    if (
        not facts.lease_current
        or facts.record_state not in ("armed", "starting", "awaiting_power", "charging", "running")
        or facts.request_state in ("completed", "failed")
    ):
        return StartFailureDecision(StartFailureAction.DIAGNOSTIC_ONLY, retire)
    action = StartFailureAction.STOP_REQUIRED if facts.effects_started else StartFailureAction.FAIL_PREFLIGHT
    return StartFailureDecision(action, retire)
