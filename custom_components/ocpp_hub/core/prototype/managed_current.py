"""Explicit installed-ceiling authority and idle current selection, without starting a charging session."""

from __future__ import annotations

import asyncio
import sqlite3
from functools import partial
from typing import TYPE_CHECKING, Any

from .command_processor import CommandPriority
from .current_policy import (
    CurrentSelection,
)
from .gateway import NativeStartReport, timestamp
from .managed_intent import ControlIntent
from .managed_records import CurrentFailure
from .native_bounded import NativeBoundedSpec
from .policy import DeniedError
from .recovery_contracts import AuthorityStamp, Pending, Prerequisite
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .managed import ManagedController


# Bind the separately commissioned native32 capability without replacing the original admission or its history.
def capability_value(journal: Journal, spec: NativeBoundedSpec) -> dict[str, Any]:
    return {
        "version": 1,
        "identity_digest": fingerprint(journal.identity),
        "spec_digest": spec.digest,
        "native_ceiling_a": 32,
        "max_current_a": 32,
    }


# Legacy commissioning stays at six; only an exact capability in the existing managed journal permits thirty-two.
def managed_current_ceiling(journal: Journal, spec: NativeBoundedSpec) -> int:
    value = journal.get("managed_current_capability")
    if value is None:
        return 6
    if (
        value != capability_value(journal, spec)
        or journal.get("managed_mode") != {"mode": "app_schedule", "spec_digest": spec.digest, "local_start": False}
        or journal.get("native_bounded_handover", {}).get("spec") != spec.value()
        or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
    ):
        raise SecurityError("managed current capability is not bound to this admitted installation")
    return 32


class ManagedCurrentLimits:
    """A current request persists its generation before a guarded profile write; Pause always takes precedence."""

    # Interrupted updates retain their previous selection; no unverified target becomes future permission.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.worker: asyncio.Task[None] | None = None
        self._settling_overlap: tuple[str, AuthorityStamp, int | None] | None = None
        self._closing = False
        self.journal.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_current_requests(id TEXT PRIMARY KEY,current_a INTEGER)"
        )
        controller.session.gateway.controls.decisions.restore_current_request(self)

    # All current receipts and authority use the session's existing exclusive journal owner.
    @property
    def journal(self) -> Journal:
        return self.controller.session.journal

    # The selected future current remains separate from the effective zero ceiling of an intentional pause.
    def selected(self) -> int:
        ceiling = self.installed_ceiling()
        change = self.journal.current_change.read()
        value = (
            change.value["current_a"] if change.selecting else self.journal.get("control", {}).get("resume_limit", 6)
        )
        if type(value) not in (int, float) or not 6 <= value <= ceiling or int(value) != value:
            raise SecurityError("persisted selected current is malformed")
        return int(value)

    # Read the commissioned ceiling that bounds user selections.
    def installed_ceiling(self) -> int:
        return managed_current_ceiling(self.journal, self.controller.native.spec)

    # A pending update blocks Start, Return and another update until its physical outcome has been established.
    def pending(self) -> bool:
        return self.journal.current_change.read().awaiting_outcome

    # Share the same recovery eligibility with status and Pause supervision.
    def failed_idle_hold(self) -> bool:
        return self.controller.session.gateway.controls.decisions.failed_idle_current(self)

    # Report the restored current through the same decision owner.
    def returned_to_previous(self, *, recovery: dict[str, Any] | None = None) -> None:
        self.controller.session.gateway.controls.decisions.restore_previous_current(self, recovery=recovery)

    # A correlated live current worker owns native verification until its accepted revision reaches an outcome.
    def applying(self) -> bool:
        change = self.journal.current_change.read()
        intent = self.controller.requests.intent()
        worker = self.worker
        row = self.journal.db.execute(
            "SELECT action,revision,state FROM managed_requests WHERE id=?", (change.identifier,)
        ).fetchone()
        return bool(
            change.selecting
            and change.value.get("mode") == intent.mode
            and worker is not None
            and not worker.done()
            and row is not None
            and row["action"] == "set_current"
            and row["state"] in ("accepted", "running")
            and row["revision"] + 1 == intent.revision
        )

    # Changing current requires a closed idle session; an open car session cannot be silently resumed.
    def available(self, native: HandoverEvidence | None) -> bool:
        return self.current_refusal(native) is None

    # Status uses the same prerequisite decision as current admission.
    def current_refusal(self, native: HandoverEvidence | None) -> str | None:
        return self.controller.session.gateway.controls.decisions.current_refusal(self, native)

    # Start the worker after commit even if reading the receipt subsequently fails.
    def accept(self, identifier: str, revision: int, current_a: int) -> dict[str, Any]:
        c = self.controller
        decision = c.session.gateway.controls.decisions.accept_current(self, identifier, revision, current_a)
        if isinstance(decision, dict):
            return decision
        self.worker = c.commands.start(lambda: self._apply(decision))
        return c.requests.receipt(identifier)

    # Keep the worker alive independently of the caller and supply the real charger observations.
    async def _apply(self, selection: CurrentSelection) -> None:
        c = self.controller
        controls = c.session.gateway.controls
        decisions = controls.decisions
        guard = partial(decisions.require_current_selection, self, selection)
        try:
            guard()
            if selection.intent.mode == "app":
                await c.native.read(closed=True)
                guard()
                await controls.set_limit(
                    selection.value,
                    guard=guard,
                    shared=True,
                    on_claim=partial(decisions.bind_current_selection, selection),
                    request_id=selection.identifier,
                )
            guard()
            native = await c.native.read(closed=True)
            c.commands.decide(lambda: decisions.complete_current(self, selection, native))
        except (
            DeniedError,
            SecurityError,
            RecoveryRequiredError,
            UnavailableError,
            TimeoutError,
            sqlite3.Error,
        ) as error:
            await self._failed_apply(selection.identifier, selection.value, selection.previous, selection.intent, error)

    # Keep exact failed work owned while SQLite is unavailable; repair cannot replay a positive profile.
    async def _failed_apply(
        self, identifier: str, value: int, previous_value: int, expected: ControlIntent, error: Exception
    ) -> None:
        c = self.controller
        decisions = c.session.gateway.controls.decisions
        cause = {
            "id": identifier,
            "revision": expected.revision - 1,
            "current_a": value,
            "type": type(error).__name__,
            "detail": str(error)[:1024],
        }
        hold_ticket = None
        while not self._closing and not c.stopping:
            try:
                outcome = c.commands.decide(
                    lambda: decisions.settle_failed_current(self, identifier, value, previous_value, expected, cause)
                )
                if outcome is None:
                    return
                if outcome is CurrentFailure.PROTECT_OPEN_SESSION:
                    try:
                        await self.protect_overlap()
                    except DeniedError, UnavailableError, RecoveryRequiredError, TimeoutError:
                        pass  # Durable operation recovery retries zero; a lost acknowledgement never grants Stop.
                else:
                    try:
                        await c.ensure_pause_zero(
                            request_id=identifier if outcome is CurrentFailure.HOLD_IDLE else None
                        )
                    except DeniedError, UnavailableError, RecoveryRequiredError, TimeoutError, sqlite3.Error:
                        pass  # The durable hold remains pending; ordinary Pause supervision retries its zero.
                    c.schedule_pause()
                c.commands.decide(lambda: self.record_failure_diagnostic(cause))
                return
            except sqlite3.Error:
                if not self.journal.db.in_transaction:
                    hold_ticket = await decisions.protect_unsettled_current(
                        self, identifier, previous_value, expected, hold_ticket
                    )
                await asyncio.sleep(0.1)

    # The cause is already durable with the restriction; a legacy diagnostic cannot delay zero or worker retirement.
    def record_failure_diagnostic(self, cause: dict[str, Any]) -> None:
        try:
            self.journal.put("managed_current_failure", {key: cause[key] for key in ("type", "detail")})
        except sqlite3.Error:
            pass  # Optional compatibility metadata cannot undo the exact request-bound failure record.

    # Apply and verify only this current recovery's existing zero.
    async def protect_overlap(self) -> None:
        await self.controller.session.gateway.controls.decisions.protect_current_overlap(self)

    # Reconcile the failed selection under zero, then restore only the previous verified current without any Start.
    async def recover_overlap(self) -> Pending | None:
        c = self.controller
        g = c.session.gateway
        decisions = g.controls.decisions
        prepared = c.commands.decide(
            lambda: decisions.prepare_current_recovery(self), priority=CommandPriority.RESTRICT
        )
        if prepared is None:
            return None
        change, expected = prepared
        settled, pending = await decisions.retry_current_recovery(self, change, expected)
        if settled:
            return pending

        if not c.commands.decide(
            lambda: decisions.reclaim_current_recovery(self, change, expected), priority=CommandPriority.RESTRICT
        ):
            return Pending(Prerequisite.SUPERSEDED, "current recovery yielded to newer control")
        await self.protect_overlap()
        native = await c.native.read_result()
        if isinstance(native, Pending):
            return native
        native_authority, sequence = c.native.authority(), c.native.last_resolved_read

        restore, outcome = c.commands.decide(
            lambda: decisions.assess_current_recovery(self, change, expected, native, native_authority, sequence)
        )
        if not restore:
            return outcome if isinstance(outcome, Pending) else None
        assert not isinstance(outcome, Pending)
        local = outcome
        progress = await c.recovery.restore(continuing_local=local)
        if not progress.shared:
            return progress.pending
        published = c.authority()
        if published is None:
            return Pending(Prerequisite.SUPERSEDED, "current recovery lost its confirmed authority")
        return decisions.complete_current_recovery(self, change, published, local)

    # Hand the real Start report to the current recovery's decision owner.
    async def prepare_native_report(self, payload: dict[str, Any]) -> NativeStartReport | None:
        return await self.controller.session.gateway.controls.decisions.prepare_current_report(self, payload, timestamp)

    # Supervision observes the completed update instead of sampling through a legitimate profile replacement.
    async def settle(self) -> None:
        if self.worker is not None and not self.worker.cancelled():
            await asyncio.shield(self.worker)

    # Publish pending work promptly without auditing through an unfinished physical or restrictive operation.
    async def settle_for_poll(self) -> bool:
        worker = self.worker
        if worker is None:
            return True
        if not worker.done():
            done, _ = await asyncio.wait((worker,), timeout=0.1)
            if not done:
                return False
        if self.worker is not worker:
            return False
        if worker.cancelled():
            raise RuntimeError("managed current worker was cancelled before its outcome was settled")
        worker.result()
        return True

    # Retire all owned current work before the single journal owner closes its database.
    async def close(self) -> None:
        self._closing = True
        if self.worker is not None:
            await asyncio.gather(self.worker, return_exceptions=True)
