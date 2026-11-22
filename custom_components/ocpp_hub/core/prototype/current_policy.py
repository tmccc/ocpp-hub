"""Current selection prerequisites and independent session evidence."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .control_state import ControlState
from .managed_facts import ManagedFacts
from .recovery_contracts import ALLOW, GateDecision, Pending, Prerequisite
from .store import fingerprint

if TYPE_CHECKING:
    from .connection_scope import ControlScope
    from .gateway import Gateway
    from .managed_current import ManagedCurrentLimits
    from .managed_intent import ControlIntent
    from .trial_handover_evidence import HandoverEvidence


@dataclass
class CurrentSelection:
    """The admitted request and its existing claim, updated only by the owned wire preparation."""

    identifier: str
    value: int
    previous: int
    intent: ControlIntent
    connection: ControlScope
    ticket: str


# Identify only the current native marker on the exact original schedule.
def original_current_plan(n: HandoverEvidence, schedule_digest: str) -> dict[str, Any] | None:
    if n.session_plan_id is None or n.saved_schedule_digest != schedule_digest or n.physical.connection == "Unplug":
        return None
    return next((p for p in (n.saved_schedule or {}).get("planList", []) if p["planId"] == n.session_plan_id), None)


# A retained occurrence can gain metadata without becoming a new native attempt.
def attempt_key(record: dict[str, Any]) -> str:
    return fingerprint([record["filename"], record["startTime"], record["startMs"]])


# Report the first failed prerequisite, preserving the different authority required in app and Pause modes.
def current_refusal(current: ManagedCurrentLimits, native: HandoverEvidence | None) -> str | None:
    c, g, j = current.controller, current.controller.session.gateway, current.journal
    facts = ManagedFacts(c)
    if current.installed_ceiling() != 32:
        return "current_selection_not_commissioned"
    if c.stopping:
        return "service_stopping"
    if j.current_change.read().unresolved:
        return "current_update_pending"
    if j.fault.latched():
        return "fault_latched"
    if not j.phase.read().active:
        return "installation_not_active"
    if not g.controls.ready:
        return "station_ceiling_unconfirmed"
    if not facts.boot_current():
        return "boot_unconfirmed"
    if not facts.introduction_current():
        return "introduction_unconfirmed"
    if j.get("control", {}).get("uncertain"):
        return "control_uncertain"
    if j.get("control", {}).get("start_pending"):
        return "start_pending"
    if g.controls._operations:
        return "control_operation_pending"
    if g.charger._pending:
        return "charger_rpc_pending"

    shared = (
        c.requests.intent().mode == "app"
        and j.get("control", {}).get("owner") == "shared"
        and not j.get("control", {}).get("inhibit")
    )
    if not shared and not (
        c.requests.intent().mode == "paused" and ControlState(j.get("control", {})).explicit_local_hold
    ):
        return "current_selection_authority_unconfirmed"
    if j.get("vendor_start_grant"):
        return "vendor_start_pending"
    if g.vendor_write_evidence().unresolved:
        return "vendor_command_uncertain"
    if j.active() is not None:
        return "transaction_open"
    if not facts.cloud_queue_empty():
        return "cloud_delivery_pending"
    if not facts.mutations_confirmed():
        return "mutation_unconfirmed"
    if c.requests.return_in_progress():
        return "return_pending"
    if c.charging.lease is not None:
        return "managed_start_active"
    if not c.requests.has_capacity():
        return "request_capacity_exhausted"
    if native is None or c.native.last_verified_at == 0 or not 0 <= time.time() - native.physical.observed_at <= 15:
        return "native_observation_stale"
    if native.physical.transaction_open:
        return "native_transaction_open"
    if native.physical.power_w != 0:
        return "native_power_nonzero"
    if not all(value == 0 for value in native.currents_a + native.powers_w):
        return "native_phase_nonzero"
    return None


# Current source prerequisites are ordinary data at both preflight and the final transport boundary.
def current_source_decision(g: Gateway, local: int | None) -> GateDecision:
    j = g.journal
    change = j.current_change.read().value
    intent = j.get("managed_control_intent", {})
    if (
        change.get("state") != "recovering"
        or intent != {"mode": "app", "revision": change.get("recovery_revision")}
        or g.boot_observation is None
        or change.get("source_boot") != g.boot_observation
        or change.get("continuing_local") != local
        or local is None
        and change.get("native_open") is not False
        or j.active() != local
    ):
        return Pending(Prerequisite.SOURCE, "current recovery awaits the independent original session on this Boot")
    return ALLOW


# Actual native and journal identities must agree before continuing this current operation's original session.
def current_session_decision(g: Gateway, n: HandoverEvidence, local: int) -> GateDecision:
    decision = current_source_decision(g, local)
    if isinstance(decision, Pending):
        return decision
    if not n.physical.transaction_open or n.physical.native_transaction_id != local:
        return Pending(Prerequisite.LIFECYCLE, "current recovery awaits matching native and OCPP lifecycle evidence")
    return ALLOW
