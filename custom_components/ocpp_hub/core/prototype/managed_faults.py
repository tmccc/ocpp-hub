"""Persist strict managed refusals before introduction or ordinary control can proceed."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .command_processor import CommandPriority
from .connection_scope import control_scope_matches, current_control_scope
from .gateway import Gateway
from .native_bounded import readiness_decision
from .native_faults import STATION_CEILING_LOST, _NativeMeasurementError
from .policy import DeniedError
from .recovery_contracts import require_allowed
from .rpc import UnavailableError
from .security import SecurityError
from .store import RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController


@dataclass(frozen=True)
class ManagedRefusal:
    """A proved introduction refusal retained until its diagnostic and restrictive latch commit together."""

    classification: str
    diagnostic: dict[str, Any]


# Keep the first proved refusal and its diagnostic atomic, including when introduction precedes the first poll.
def latch_managed_fault(
    gateway: Gateway, reason: str, *, diagnostic: dict[str, Any] | None = None, forwarded_timeout: str | None = None
) -> None:
    # Save and withdraw authority at the same restrictive turn.
    def latch() -> None:
        journal = gateway.journal
        if not journal.fault.latched():
            with journal.atomic():
                fault: dict[str, Any] = {"classification": reason, "at_epoch": time.time()}
                if forwarded_timeout is not None:
                    fault["forwarded_timeout"] = forwarded_timeout
                if diagnostic is not None:
                    journal.put("managed_native_failure", diagnostic)
                    fault["native_failure_digest"] = fingerprint(diagnostic)
                journal.fault.replace(fault)
                control = journal.get("control", {})
                journal.phase.fault(control)
            gateway.controls.invalidate()

    gateway.decide(latch, priority=CommandPriority.RESTRICT)


# An independently absent ceiling allows Stop of the same known charge and cannot establish a zero hold.
async def stop_for_missing_station(controller: ManagedController) -> None:
    session, native = controller.session, controller.native
    gateway, journal = session.gateway, session.journal
    local, connection = journal.active(), current_control_scope(gateway)
    if (
        local is None
        or connection is None
        or not journal.phase.read().supervised
        or not (session.book.station_ceiling_lost() or journal.phase.read().held)
        or controller.stopping
        or gateway.controls._operations
        or gateway.charger._pending
        or controller.pause_task is not None
        and not controller.pause_task.done()
        or gateway.controls.stop_acknowledged(local)
    ):
        return
    intent = controller.requests.intent()
    try:
        try:
            sample = await native.read(owned=True, closing_local=local)
        except _NativeMeasurementError as error:
            if error.authority != native.authority() or error.sequence != native.last_resolved_read:
                return
            sample = error.native
            require_allowed(readiness_decision(sample, False, native.allow_unplugged, local))
        sequence = native.last_resolved_read

        # Only this observation may withdraw authority before taking its scoped Stop claim.
        def accept_absence() -> bool:
            nonlocal connection
            if native.last_resolved_read != sequence:
                return False
            if (
                not control_scope_matches(gateway, connection)
                or controller.requests.intent() != intent
                or controller.stopping
                or journal.active() != local
                or not sample.physical.transaction_open
                or sample.physical.native_transaction_id != local
                or sample.physical.history is None
                or any(resource.key == f"profile:{HUB_PROFILE_ID}" for resource in sample.physical.resources)
                or not session.book.station_ceiling_lost()
                or not native.commissioned_binding_matches(sample)
            ):
                return False

            controller.fault(STATION_CEILING_LOST)
            # Fault publication withdraws old positive authority. The restrictive claim has its own ticket.
            connection = current_control_scope(gateway)
            return True

        if not controller.commands.decide(accept_absence, priority=CommandPriority.RESTRICT):
            return
        fault, sequence = journal.fault.read(), native.last_resolved_read
        control = journal.control.read()
        stop_epoch, prior_ticket = control.values.get("stop_epoch", 0), control.values.get("ticket")

        # Revalidate the matched live session and the exact Stop claim immediately before dispatch.
        def guard() -> None:
            sample.physical.require_fresh(journal.identity, 5)
            current = journal.control.read()
            if (
                controller.stopping
                or not control_scope_matches(gateway, connection)
                or controller.requests.intent() != intent
                or journal.active() != local
                or native.last_resolved_read != sequence
                or journal.fault.read() != fault
                or not current.explicit_local_hold
                or not native.commissioned_binding_matches(sample)
                or journal.get("trial_inventory_fault")
                or journal.get("trial_preservation_fault")
                or not session.book.station_ceiling_lost()
            ):
                raise RecoveryRequiredError("missing station Stop lost its current session proof")
            if (current.values.get("stop_epoch", 0), current.values.get("ticket")) not in (
                (stop_epoch, prior_ticket),
                (stop_epoch + 1, gateway.controls._stop_ticket),
            ):
                raise RecoveryRequiredError("missing station Stop claim was superseded")

        await gateway.controls.stop(expected_local=local, guard=guard, station_absent=True)
    except DeniedError, RecoveryRequiredError, SecurityError, UnavailableError, TimeoutError, sqlite3.Error:
        return  # Any acknowledgement remains scoped. Actual closure and a verified zero are separate facts.
