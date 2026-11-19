"""Explicit installed-ceiling authority and idle current selection, without starting a charging session."""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from typing import TYPE_CHECKING, Any, cast

from ..control import HUB_PROFILE_ID
from .control_state import ControlState
from .gateway import NativeStartReport
from .local_control import local_start_receipt
from .managed_facts import ManagedFacts
from .managed_intent import ControlIntent, write_intent
from .managed_settlement import claim_internal_pause
from .managed_sources import recorded_station_profile
from .native_bounded import NativeBoundedSpec
from .policy import DeniedError, integer
from .recovery_contracts import ALLOW, GateDecision, Pending, Prerequisite
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .gateway import Gateway
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


# Publish only validated native evidence; a later read must revoke an earlier independent-session identification.
def record_current_source(j: Journal, n: HandoverEvidence, boot: str | None) -> None:
    change = j.current_change.read()
    if not change.awaiting_outcome:
        return
    local = n.physical.native_transaction_id
    plan = next((p for p in (n.saved_schedule or {}).get("planList", []) if p["planId"] == n.session_plan_id), None)
    row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
    original = j.get("native_bounded_handover", {}).get("spec", {}).get("saved_schedule_digest")
    independent = bool(
        n.physical.transaction_open
        and local == j.active()
        and n.physical.connection != "Unplug"
        and plan is not None
        and n.saved_schedule_digest == original
        and row is not None
        and json.loads(row["start"])["idTag"].casefold() == plan["idTag"].casefold()
    )
    j.current_change.replace(
        {
            **change.value,
            "observed_open": change.value.get("observed_open", False) or n.physical.transaction_open,
            "native_open": n.physical.transaction_open,
            "source_boot": boot,
            "continuing_local": local if independent else None,
        },
    )


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


# Prefer the exact modern request's cause; a malformed binding cannot borrow an older global diagnostic.
def current_failure(j: Journal, change: dict[str, Any]) -> dict[str, Any] | None:
    if "failure" not in change:
        legacy = j.get("managed_current_failure")
        return legacy if isinstance(legacy, dict) else None
    cause = change["failure"]
    if not isinstance(cause, dict) or set(cause) != {"id", "revision", "current_a", "type", "detail"}:
        return None
    row = j.db.execute(
        "SELECT r.action,r.revision,c.current_a FROM managed_requests r "
        "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
        (change.get("id"),),
    ).fetchone()
    if (
        row is None
        or row["action"] != "set_current"
        or cause["id"] != change.get("id")
        or type(cause["revision"]) is not int
        or cause["revision"] != row["revision"]
        or type(cause["current_a"]) is not int
        or cause["current_a"] != row["current_a"]
        or cause["current_a"] != change.get("current_a")
        or not isinstance(cause["type"], str)
        or not 1 <= len(cause["type"]) <= 128
        or not isinstance(cause["detail"], str)
        or len(cause["detail"]) > 1024
    ):
        return None
    return dict(cause)


class ManagedCurrentLimits:
    """A current request persists its generation before a guarded profile write; Pause always takes precedence."""

    # Interrupted updates retain their previous selection; no unverified target becomes future permission.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.worker: asyncio.Task[None] | None = None
        self._closing = False
        self.journal.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_current_requests(id TEXT PRIMARY KEY,current_a INTEGER)"
        )
        change = self.journal.current_change.read().value
        if change.get("state") == "pending":
            previous = change.get("previous_a")
            if previous is None:
                # Earlier pending receipts stored the target only. Recover the most recent completed selection.
                row = self.journal.db.execute(
                    "SELECT c.current_a FROM managed_current_requests c JOIN managed_requests r ON r.id=c.id "
                    "WHERE r.state='completed' ORDER BY r.rowid DESC LIMIT 1"
                ).fetchone()
                previous = row["current_a"] if row else 6
            previous = integer(previous, 6, 32)
            change = {**change, "previous_a": previous}
            ownership = controller.requests.pause_ownership()
            intent = ownership.intent
            if intent.mode == "paused" and (change.get("mode") == "app" or ownership.requested):
                # Current admission advances intent, so a matching owner Pause was accepted after this selection.
                with self.journal.atomic():
                    self.journal.control.project(self.journal.get("control"), resume_limit=previous)
                    self.journal.current_change.cancel(change)
                return
            if intent.mode == "app":
                self.cancel_overlap(change["id"], previous, "process_replaced")
                return
            with self.journal.atomic():
                write_intent(self.journal, intent, "paused")
                claim_internal_pause(controller, "current", change["id"])
                control = self.journal.get("control")
                self.journal.control.project(control, resume_limit=previous)
                state = "uncertain" if change.get("mode") == "paused" else "interrupted"
                self.journal.current_change.replace({**change, "state": state})

    # All current receipts and authority use the session's existing exclusive journal owner.
    @property
    def journal(self) -> Journal:
        return self.controller.session.journal

    # The selected future current remains separate from the effective zero ceiling of an intentional pause.
    def selected(self) -> int:
        ceiling = managed_current_ceiling(self.journal, self.controller.native.spec)
        change = self.journal.current_change.read()
        value = (
            change.value["current_a"] if change.selecting else self.journal.get("control", {}).get("resume_limit", 6)
        )
        if type(value) not in (int, float) or not 6 <= value <= ceiling or int(value) != value:
            raise SecurityError("persisted selected current is malformed")
        return int(value)

    # A pending update blocks Start, Return and another update until its physical outcome has been established.
    def pending(self) -> bool:
        return self.journal.current_change.read().awaiting_outcome

    # Retry only the closed zero hold of this exact failed idle update; its current receipt stays uncertain.
    def failed_idle_hold(self) -> bool:
        c, j = self.controller, self.journal
        g, change, control = c.session.gateway, j.current_change.read().value, j.get("control", {})
        row = j.db.execute(
            "SELECT r.action,r.state,r.revision,r.result,c.current_a FROM managed_requests r "
            "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
            (change.get("id"),),
        ).fetchone()
        receipt = local_start_receipt(j)
        return bool(
            not c.stopping
            and (self.worker is None or self.worker.done())
            and change.get("state") == "uncertain"
            and change.get("mode") == "app"
            and change.get("native_open") is False
            and change.get("observed_open") is False
            and "failure" in change
            and current_failure(j, change) is not None
            and row is not None
            and row["action"] == "set_current"
            and row["state"] == "uncertain"
            and row["current_a"] == change.get("current_a")
            and json.loads(row["result"])
            == {
                "classification": "current_unconfirmed",
                "requested_current_a": change.get("current_a"),
                "previous_current_a": change.get("previous_a"),
            }
            and j.get("managed_control_intent") == {"mode": "paused", "revision": row["revision"] + 2}
            and type(change.get("previous_a")) is int
            and change["previous_a"] == self.selected() == control.get("resume_limit")
            and g.boot_observation is not None
            and change.get("source_boot") == g.boot_observation
            and g.current_boot_seen(g._charger_epoch)
            and j.active() is None
            and ControlState(control).explicit_local_hold
            and control.get("uncertain") is True
            and isinstance(control.get("ticket"), str)
            and re.fullmatch(r"[a-f0-9]{32}", control["ticket"]) is not None
            and not control.get("start_pending")
            and not j.fault.latched()
            and not j.get("vendor_start_grant")
            and not j.get("uncertain_vendor_command")
            and not c.requests.return_in_progress()
            and c.charging.lease is None
            and c.charging.state().get("state") in (None, "completed", "failed", "cancelled")
            and receipt.get("stage") not in ("prepared", "issued")
            and (
                receipt.get("stage") != "accepted"
                or j.db.execute(
                    "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (receipt.get("local"),)
                ).fetchone()
            )
            and not j.db.execute(
                "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') LIMIT 1"
            ).fetchone()
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        )

    # Verified restoration retires only its exact abandoned target, in the caller's authority transaction.
    def returned_to_previous(self, *, recovery: dict[str, Any] | None = None) -> None:
        j = self.journal
        change = j.current_change.read().value
        row = j.db.execute(
            "SELECT r.action,r.state,r.revision,r.result,c.current_a FROM managed_requests r "
            "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
            (change.get("id"),),
        ).fetchone()
        automatic = recovery is not None
        eligible = change.get("state") in ("uncertain", "interrupted", "cancelled")
        if recovery is not None:
            # Native observations may refresh during awaits; only immutable request/authority fields are compared.
            keys = ("id", "current_a", "previous_a", "mode", "recovery_revision")
            eligible = (
                change.get("state") == "recovering"
                and all(change.get(key) == recovery.get(key) for key in keys)
                and change.get("mode") == "app"
                and not self.controller.stopping
                and self.controller.requests.intent().mode == "app"
                and self.controller.requests.intent().revision == change.get("recovery_revision")
                and row is not None
                and row["revision"] + 1 == change.get("recovery_revision")
                and (row["state"] != "uncertain" or json.loads(row["result"]) == {"classification": "process_replaced"})
            )
        if (
            eligible
            and change.get("previous_a") == self.selected()
            and row is not None
            and row["action"] == "set_current"
            and row["state"] in ("failed", "uncertain")
            and row["current_a"] == change.get("current_a")
        ):
            j.current_change.replace(
                {
                    **change,
                    "state": "cancelled",
                    "recovery_reason": "verified_overlap_recovery"
                    if automatic
                    else "explicit_return_previous_selection",
                },
            )
            # Preserve terminal failed results byte for byte; only this exact uncertain request can be retired.
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? "
                "WHERE id=? AND action='set_current' AND revision=? AND state='uncertain'",
                (
                    json.dumps(
                        {
                            "classification": (
                                "superseded_by_verified_overlap_recovery"
                                if automatic
                                else "superseded_by_verified_return"
                            ),
                            "current_a": self.selected(),
                        }
                    ),
                    change["id"],
                    row["revision"],
                ),
            )

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

    # Report the first failed prerequisite, preserving the different authority required in app and Pause modes.
    def current_refusal(self, native: HandoverEvidence | None) -> str | None:
        c, g, j = self.controller, self.controller.session.gateway, self.journal
        facts = ManagedFacts(c)
        if managed_current_ceiling(j, c.native.spec) != 32:
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
        if j.get("uncertain_vendor_command"):
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
        if native is None or not 0 <= time.time() - native.physical.observed_at <= 15:
            return "native_observation_stale"
        if native.physical.transaction_open:
            return "native_transaction_open"
        if native.physical.power_w != 0:
            return "native_power_nonzero"
        if not all(value == 0 for value in native.currents_a + native.powers_w):
            return "native_phase_nonzero"
        return None

    # Idempotency binds the complete current argument as well as the request identifier, action and observed revision.
    def accept(self, identifier: str, revision: int, current_a: int) -> dict[str, Any]:
        value = integer(current_a, 6, 32)
        c, j = self.controller, self.journal
        previous = c.requests.existing(identifier, "set_current", revision)
        if previous is not None:
            bound = j.db.execute("SELECT current_a FROM managed_current_requests WHERE id=?", (identifier,)).fetchone()
            if bound is None or bound[0] != value:
                raise SecurityError("request identifier already binds a different current")
            return previous
        intent = c.requests.intent()
        if intent.revision != revision or not self.available(c.latest_native):
            raise RecoveryRequiredError("current selection requires fresh idle authority and the current revision")
        previous_value = self.selected()
        with j.atomic():
            expected = write_intent(j, intent, intent.mode)
            j.current_change.begin(identifier, value, previous_value, intent.mode, j.get("next_transaction"))
            j.db.execute("INSERT INTO managed_current_requests VALUES(?,?)", (identifier, value))
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'accepted',?)",
                (identifier, "set_current", revision, json.dumps({"accepted_at": time.time()})),
            )
            control = j.get("control", {})
            j.control.select(control, value, app=intent.mode == "app")
        self.worker = asyncio.create_task(self._apply(identifier, value, previous_value, expected))
        return c.requests.receipt(identifier)

    # One service-owned worker survives a disappearing HA client and confirms native profile bytes after the ACK.
    async def _apply(self, identifier: str, value: int, previous_value: int, expected: ControlIntent) -> None:
        c, g, j = self.controller, self.controller.session.gateway, self.journal
        epoch, control_revision = g._charger_epoch, g.controls._revision

        # Every actual outgoing RPC rechecks this request's intent and transport generation after awaited evidence.
        def guard() -> None:
            if (
                c.stopping
                or c.requests.intent() != expected
                or epoch != g._charger_epoch
                or control_revision != g.controls._revision
                or not g.current_boot_seen(epoch)
                or j.fault.latched()
                or not j.phase.read().active
                or j.active() is not None
                or j.get("control", {}).get("start_pending")
                or j.get("vendor_start_grant")
                or self.selected() != value
                or j.current_change.read().identifier != identifier
            ):
                raise UnavailableError("current update was superseded by another control operation")

        try:
            guard()
            if expected.mode == "app":
                await c.native.read(closed=True)
                guard()
                await g.controls.set_limit(value, guard=guard, shared=True)
            guard()
            native = await c.native.read(closed=True)
            guard()
            present = c.session.book.owned_present(native.physical)
            target = 0 if expected.mode == "paused" else value
            if not g.controls.ready or not any(
                r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == target for r in present
            ):
                raise RecoveryRequiredError("selected current has not been independently verified")
            with j.atomic():
                guard()
                j.current_change.complete(identifier, value)
                c.requests.finish(identifier, "completed", {"current_a": value, "charging_started": False})
        except (
            DeniedError,
            SecurityError,
            RecoveryRequiredError,
            UnavailableError,
            TimeoutError,
            sqlite3.Error,
        ) as error:
            await self._failed_apply(identifier, value, previous_value, expected, error)

    # Keep exact failed work owned while SQLite is unavailable; repair cannot replay a positive profile.
    async def _failed_apply(
        self, identifier: str, value: int, previous_value: int, expected: ControlIntent, error: Exception
    ) -> None:
        c, j = self.controller, self.journal
        cause = {
            "id": identifier,
            "revision": expected.revision - 1,
            "current_a": value,
            "type": type(error).__name__,
            "detail": str(error)[:1024],
        }
        while not self._closing and not c.stopping:
            try:
                change = j.current_change.read().value
                if change.get("id") != identifier or change.get("state") != "pending":
                    return
                intent = c.requests.intent()
                superseded = intent != expected
                active = j.active()
                overlap = active is not None or j.current_change.read().value.get("observed_open")
                if not superseded and expected.mode == "app" and overlap:
                    self.cancel_overlap(identifier, previous_value, "session_opened", failure=cause)
                    try:
                        await self.protect_overlap()
                    except DeniedError, UnavailableError, RecoveryRequiredError, TimeoutError:
                        pass  # Durable operation recovery retries zero; a lost acknowledgement never grants Stop.
                    self.record_failure_diagnostic(cause)
                    return
                with j.atomic():
                    if not superseded:
                        write_intent(j, intent, "paused")
                        claim_internal_pause(c, "current", identifier)
                    control = j.get("control", {})
                    if control.get("inhibit") is True and j.current_change.read().identifier == identifier:
                        j.control.project(control, resume_limit=previous_value)
                    j.current_change.replace(
                        {
                            **j.current_change.read().value,
                            "id": identifier,
                            "current_a": value,
                            "previous_a": previous_value,
                            "mode": expected.mode,
                            "state": "uncertain",
                            "failure": cause,
                        },
                    )
                    c.requests.finish(
                        identifier,
                        "failed" if superseded else "uncertain",
                        {
                            "classification": "current_unconfirmed",
                            "requested_current_a": value,
                            "previous_current_a": previous_value,
                        },
                    )
                try:
                    await c.ensure_pause_zero()
                except DeniedError, UnavailableError, RecoveryRequiredError, TimeoutError:
                    pass  # The durable hold remains pending; ordinary Pause supervision retries its zero.
                if c.pause_task is None or c.pause_task.done():
                    c.pause_task = asyncio.create_task(c._stop_requested())
                self.record_failure_diagnostic(cause)
                return
            except sqlite3.Error:
                # A later Pause is read again on every retry and retains its own revision and control ticket.
                await asyncio.sleep(0.1)

    # The cause is already durable with the restriction; a legacy diagnostic cannot delay zero or worker retirement.
    def record_failure_diagnostic(self, cause: dict[str, Any]) -> None:
        try:
            self.journal.put("managed_current_failure", {key: cause[key] for key in ("type", "detail")})
        except sqlite3.Error:
            pass  # Optional compatibility metadata cannot undo the exact request-bound failure record.

    # Persist the failed operation's zero hold without manufacturing durable owner Pause intent or Stop authority.
    def cancel_overlap(
        self, identifier: str, previous: int, reason: str, *, failure: dict[str, Any] | None = None
    ) -> None:
        c, j = self.controller, self.journal
        with j.atomic():
            change = cast("dict[str, Any]", j.current_change.stored())
            j.current_change.replace(
                {
                    **change,
                    "state": "recovering",
                    "previous_a": previous,
                    "recovery_revision": c.requests.intent().revision,
                    "recovery_reason": reason,
                    **({"failure": failure} if failure is not None else {}),
                },
            )
            j.control.project(j.get("control", {}), resume_limit=previous)
            c.session.gateway.controls._claim(True, stopping=True)
            c.released_epoch = None
            c.requests.finish(identifier, "failed", {"classification": reason})

    # Apply only the already owned station zero, including before native source identification or reintroduction.
    async def protect_overlap(self) -> None:
        c, j = self.controller, self.journal
        g = c.session.gateway
        change = j.current_change.read().value
        epoch = g._charger_epoch

        # A newer owner Pause or another Boot revokes this operation's restrictive ticket too.
        def guard() -> None:
            if (
                c.stopping
                or change.get("state") != "recovering"
                or j.current_change.read().identifier != change["id"]
                or c.requests.intent().mode != "app"
                or c.requests.intent().revision != change["recovery_revision"]
                or g._charger_epoch != epoch
                or not g.current_boot_seen(epoch)
                or fingerprint(j.get("boot")) != c.native.spec.boot_digest
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
                or g.controls.desired() != 0
            ):
                raise RecoveryRequiredError("current recovery zero was superseded")

        guard()
        await g.controls.ensure_ceiling(guard=guard)
        guard()
        if j.phase.read().active:
            g.controls._complete(j.get("control")["ticket"], uncertain=False)

    # Reconcile the failed selection under zero, then restore only the previous verified current without any Start.
    async def recover_overlap(self) -> Pending | None:
        c, j = self.controller, self.journal
        change = j.current_change.read().value
        if change.get("state") != "recovering":
            return None
        if c.requests.intent().mode != "app" or c.requests.intent().revision != change["recovery_revision"]:
            j.current_change.cancel(change)
            return None
        if not c.stopping and c.session.gateway.controls.desired() != 0:
            # Restoration may have succeeded before settlement storage failed; retry from a new restrictive ticket.
            with j.atomic():
                c.session.gateway.controls._claim(True, stopping=True)
                c.released_epoch = None
        await self.protect_overlap()
        native = await c.native.read_result()
        if isinstance(native, Pending):
            return native
        decision = c.native.session_decision(native)
        if isinstance(decision, Pending):
            return decision
        if (
            change.get("recovery_reason") == "process_replaced"
            and not cast("dict[str, Any]", j.current_change.stored())["observed_open"]
        ):
            # Preserve the existing fail-closed policy when replacement confirms there was no overlapping session.
            with j.atomic():
                intent = c.requests.intent()
                write_intent(j, intent, "paused")
                claim_internal_pause(c, "current", change["id"])
                j.control.project(j.get("control"), resume_limit=change["previous_a"])
                j.current_change.replace({**cast("dict[str, Any]", j.current_change.stored()), "state": "interrupted"})
            return None
        local = j.active()
        if local is not None:
            decision = current_session_decision(c.session.gateway, native, local)
            if isinstance(decision, Pending):
                return decision
        progress = await c.recovery.restore(continuing_local=local)
        if not progress.shared:
            return progress.pending
        with j.atomic():
            self.returned_to_previous(recovery=change)
        return None

    # Accept a delayed genuine original-plan report under this operation's zero, without inventing its lifecycle.
    async def prepare_native_report(self, payload: dict[str, Any]) -> NativeStartReport | None:
        c, j = self.controller, self.journal
        change = j.current_change.read().value
        g = c.session.gateway
        if (
            change.get("state") != "recovering"
            or c.requests.intent().mode != "app"
            or c.requests.intent().revision != change["recovery_revision"]
            or j.active() is not None
            or not g.controls.ready
            or g.controls.desired() != 0
        ):
            return None
        native = await c.native.read()
        local = native.physical.native_transaction_id
        plan = next(
            (p for p in (native.saved_schedule or {}).get("planList", []) if p["planId"] == native.session_plan_id),
            None,
        )
        if (
            not native.physical.transaction_open
            or native.physical.connection == "Unplug"
            or native.saved_schedule_digest != c.native.spec.saved_schedule_digest
            or plan is None
            or plan["idTag"].casefold() != str(payload.get("idTag")).casefold()
            or local is None
            or local <= change.get("floor", 0)
            or j.db.execute("SELECT 1 FROM transactions WHERE local=?", (local,)).fetchone()
            or any(v != 0 for v in native.currents_a + native.powers_w)
        ):
            return None

        # The ordinary gateway commits the supplied Start; only a later read may identify its local transaction.
        def accepted(local: int) -> None:
            j.current_change.replace({**cast("dict[str, Any]", j.current_change.stored()), "reported_local": local})

        return NativeStartReport(local, plan["planId"], None, accepted)

    # Restrict an interrupted selection without giving it owner Pause authority over a concurrent transaction.

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
