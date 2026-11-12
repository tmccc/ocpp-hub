"""Explicit installed-ceiling authority and idle current selection, without starting a charging session."""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .gateway import NativeStartReport
from .managed_schedule import KEY, OBSERVABLE, recorded_station_profile
from .native_bounded import NativeBoundedSpec
from .policy import DeniedError, integer
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, fingerprint
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
    change = j.get("managed_current_change", {})
    if change.get("state") not in ("pending", "recovering"):
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
    j.put(
        "managed_current_change",
        {
            **change,
            "observed_open": change.get("observed_open", False) or n.physical.transaction_open,
            "native_open": n.physical.transaction_open,
            "source_boot": boot,
            "continuing_local": local if independent else None,
        },
    )


# Current recovery has no Stop authority and can continue only its freshly identified independent original session.
def require_current_source(g: Gateway, local: int | None) -> None:
    j = g.journal
    change = j.get("managed_current_change", {})
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
        raise ReleaseDeferredError("current recovery awaits the independent original session on this Boot")


# Every recovery read must still describe the same open original transaction before its positive ceiling is sent.
def require_current_session(g: Gateway, n: HandoverEvidence, local: int) -> None:
    require_current_source(g, local)
    if not n.physical.transaction_open or n.physical.native_transaction_id != local:
        raise ReleaseDeferredError("current recovery awaits matching native and OCPP lifecycle evidence")


class ManagedCurrentLimits:
    """A current request persists its generation before a guarded profile write; Pause always takes precedence."""

    # Interrupted updates retain their previous selection; no unverified target becomes future permission.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.worker: asyncio.Task[None] | None = None
        self.journal.db.execute(
            "CREATE TABLE IF NOT EXISTS managed_current_requests(id TEXT PRIMARY KEY,current_a INTEGER)"
        )
        change = self.journal.get("managed_current_change", {})
        if change.get("state") == "pending":
            timer = self.journal.get(KEY, {})
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
            if change.get("mode") == "app" and controller.requests.intent().mode == "paused":
                # A durable explicit Pause already owns its ticket; the interrupted increase cannot replace it.
                with self.journal.atomic():
                    self.journal.put("control", {**self.journal.get("control"), "resume_limit": previous})
                    self.journal.put("managed_current_change", {**change, "state": "cancelled"})
                return
            if controller.requests.intent().mode == "app":
                if timer.get("state") in OBSERVABLE:
                    self.cancel_under_timer(change["id"], change["current_a"], previous, "process_replaced")
                else:
                    self.cancel_overlap(change["id"], previous, "process_replaced")
                return
            with self.journal.atomic():
                intent = controller.requests.intent()
                self.journal.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                controller.session.gateway.controls._claim(True, stopping=True)
                control = self.journal.get("control")
                self.journal.put("control", {**control, "resume_limit": previous})
                state = "uncertain" if change.get("mode") == "paused" else "interrupted"
                self.journal.put("managed_current_change", {**change, "state": state})

    # All current receipts and authority use the session's existing exclusive journal owner.
    @property
    def journal(self) -> Journal:
        return self.controller.session.journal

    # The selected future current remains separate from the effective zero ceiling of an intentional pause.
    def selected(self) -> int:
        ceiling = managed_current_ceiling(self.journal, self.controller.native.spec)
        change = self.journal.get("managed_current_change", {})
        value = (
            change["current_a"]
            if change.get("state") == "pending"
            else self.journal.get("control", {}).get("resume_limit", 6)
        )
        if type(value) not in (int, float) or not 6 <= value <= ceiling or int(value) != value:
            raise SecurityError("persisted selected current is malformed")
        return int(value)

    # A pending update blocks Start, Return and another update until its physical outcome has been established.
    def pending(self) -> bool:
        return bool(self.journal.get("managed_current_change", {}).get("state") in ("pending", "recovering"))

    # A completed explicit Return retires the failed target once fresh zero and the previous selection are verified.
    def returned_to_previous(self) -> None:
        j = self.journal
        change = j.get("managed_current_change", {})
        row = j.db.execute(
            "SELECT r.action,r.state,c.current_a FROM managed_requests r "
            "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
            (change.get("id"),),
        ).fetchone()
        if (
            change.get("state") == "uncertain"
            and change.get("previous_a") == self.selected()
            and row is not None
            and row["action"] == "set_current"
            and row["state"] in ("failed", "uncertain")
            and row["current_a"] == change.get("current_a")
        ):
            j.put(
                "managed_current_change",
                {**change, "state": "cancelled", "recovery_reason": "explicit_return_previous_selection"},
            )

    # A correlated live current worker owns native verification until its accepted revision reaches an outcome.
    def applying(self) -> bool:
        change = self.journal.get("managed_current_change", {})
        intent = self.controller.requests.intent()
        worker = self.worker
        row = self.journal.db.execute(
            "SELECT action,revision,state FROM managed_requests WHERE id=?", (change.get("id"),)
        ).fetchone()
        return bool(
            change.get("state") == "pending"
            and change.get("mode") == intent.mode
            and worker is not None
            and not worker.done()
            and row is not None
            and row["action"] == "set_current"
            and row["state"] in ("accepted", "running")
            and row["revision"] + 1 == intent.revision
        )

    # The first version changes current only while closed and idle; an open car session cannot be silently resumed.
    def available(self, native: HandoverEvidence | None, *, for_schedule: bool = False) -> bool:
        c, g, j = self.controller, self.controller.session.gateway, self.journal
        timer = j.get(KEY, {})
        return bool(
            (
                for_schedule
                or timer.get("state") in (None, "complete")
                or timer.get("state") == "armed"
                and not timer.get("retire_reason")
                and "hold_since" not in timer
            )
            and managed_current_ceiling(j, c.native.spec) == 32
            and not c.stopping
            and not self.pending()
            and not j.get("managed_control_fault")
            and j.get("trial_phase") == "active"
            and g.controls.ready
            and g.current_boot_seen(g._charger_epoch)
            and c.native.introduced_epoch == g._charger_epoch
            and c.native.introduced_revision == g.controls._revision
            and not j.get("control", {}).get("uncertain")
            and not j.get("control", {}).get("start_pending")
            and not g.controls._operations
            and not g.charger._pending
            and (
                c.requests.intent().mode == "app"
                and j.get("control", {}).get("owner") == "shared"
                and not j.get("control", {}).get("inhibit")
                or for_schedule
                and c.requests.intent().mode == "app"
                and timer.get("idle_cutoff_hold") is True
                and g.controls.desired() == 0
                and j.get("control", {}).get("owner") == "local"
                and j.get("control", {}).get("inhibit") is True
                or c.requests.intent().mode == "paused"
                and j.get("control", {}).get("owner") == "local"
                and j.get("control", {}).get("inhibit") is True
            )
            and not j.get("vendor_start_grant")
            and not j.get("uncertain_vendor_command")
            and j.active() is None
            and not j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            and not c.requests.return_in_progress()
            and c.tests.lease is None
            and c.requests.has_capacity()
            and native is not None
            and 0 <= time.time() - native.physical.observed_at <= 15
            and not native.physical.transaction_open
            and native.physical.power_w == 0
            and all(value == 0 for value in native.currents_a + native.powers_w)
        )

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
            j.put("managed_control_intent", {"mode": intent.mode, "revision": revision + 1})
            timer = j.get(KEY, {})
            if (
                timer.get("state") == "armed"
                and timer.get("control_revision", timer["revision"]) == revision
                and intent.mode == "app"
            ):
                j.put(KEY, {**timer, "control_revision": revision + 1})
            j.put(
                "managed_current_change",
                {
                    "id": identifier,
                    "current_a": value,
                    "previous_a": previous_value,
                    "mode": intent.mode,
                    "floor": j.get("next_transaction"),
                    "state": "pending",
                },
            )
            j.db.execute("INSERT INTO managed_current_requests VALUES(?,?)", (identifier, value))
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'accepted',?)",
                (identifier, "set_current", revision, json.dumps({"accepted_at": time.time()})),
            )
            control = j.get("control", {})
            j.put(
                "control",
                {
                    **control,
                    # Only the guarded worker may make a pending increase available to a live station.
                    **({"owner": "local", "uncertain": True} if intent.mode == "app" else {"resume_limit": value}),
                },
            )
        expected = c.requests.intent()
        self.worker = asyncio.create_task(
            self._apply(identifier, value, previous_value, expected.mode, expected.revision)
        )
        return c.requests.receipt(identifier)

    # One service-owned worker survives a disappearing HA client and confirms native profile bytes after the ACK.
    async def _apply(self, identifier: str, value: int, previous_value: int, mode: str, revision: int) -> None:
        c, g, j = self.controller, self.controller.session.gateway, self.journal
        epoch, control_revision = g._charger_epoch, g.controls._revision

        # Every actual outgoing RPC rechecks this request's intent and transport generation after awaited evidence.
        def guard() -> None:
            intent = c.requests.intent()
            if (
                c.stopping
                or intent.mode != mode
                or intent.revision != revision
                or epoch != g._charger_epoch
                or control_revision != g.controls._revision
                or not g.current_boot_seen(epoch)
                or j.get("managed_control_fault")
                or j.get("trial_phase") != "active"
                or j.active() is not None
                or j.get("control", {}).get("start_pending")
                or j.get("vendor_start_grant")
                or self.selected() != value
                or j.get("managed_current_change", {}).get("id") != identifier
            ):
                raise UnavailableError("current update was superseded by another control operation")
            if mode == "app" and c.schedule.revoked(j.get(KEY, {})):
                raise ReleaseDeferredError("selected current awaits retirement of the native timer")

        try:
            guard()
            if mode == "app":
                await c.native.read(closed=True)
                guard()
                await g.controls.set_limit(value, guard=guard, shared=True)
            guard()
            native = await c.native.read(closed=True)
            guard()
            present = c.session.book.owned_present(native.physical)
            target = 0 if mode == "paused" else value
            if not g.controls.ready or not any(
                r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == target for r in present
            ):
                raise RecoveryRequiredError("selected current has not been independently verified")
            with j.atomic():
                guard()
                j.put("managed_current_change", {"id": identifier, "current_a": value, "state": "completed"})
                c.requests.finish(identifier, "completed", {"current_a": value, "charging_started": False})
        except (DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError) as error:
            j.put("managed_current_failure", {"type": type(error).__name__, "detail": str(error)[:1024]})
            intent = c.requests.intent()
            superseded = intent.mode != mode or intent.revision != revision
            timer = j.get(KEY, {})
            active = j.active()
            retired = c.schedule.revoked(timer)
            overlap = active is not None or j.get("managed_current_change", {}).get("observed_open")
            if not superseded and mode == "app" and timer.get("state") in OBSERVABLE:
                reason = "current_unconfirmed"
                if retired:
                    reason = "timer_retired"
                elif active is not None or timer.get("unreported_native"):
                    reason = "session_opened"
                self.cancel_under_timer(identifier, value, previous_value, reason)
                await c.schedule.protect_station(j.get(KEY))
                return
            if not superseded and mode == "app" and overlap:
                self.cancel_overlap(identifier, previous_value, "session_opened")
                try:
                    await self.protect_overlap()
                except DeniedError, UnavailableError, RecoveryRequiredError, TimeoutError:
                    pass  # Durable operation recovery retries zero; a lost acknowledgement never grants Stop.
                return
            with j.atomic():
                if not superseded:
                    j.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                    g.controls._claim(True, stopping=True)
                control = j.get("control", {})
                if control.get("inhibit") is True and j.get("managed_current_change", {}).get("id") == identifier:
                    j.put("control", {**control, "resume_limit": previous_value})
                j.put(
                    "managed_current_change",
                    {
                        **j.get("managed_current_change", {}),
                        "id": identifier,
                        "current_a": value,
                        "previous_a": previous_value,
                        "mode": mode,
                        "state": "uncertain",
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
            if c.pause_task is None or c.pause_task.done():
                c.pause_task = asyncio.create_task(c._stop_requested())

    # Persist the failed operation's zero hold without manufacturing durable owner Pause intent or Stop authority.
    def cancel_overlap(self, identifier: str, previous: int, reason: str) -> None:
        c, j = self.controller, self.journal
        with j.atomic():
            change = j.get("managed_current_change")
            j.put(
                "managed_current_change",
                {
                    **change,
                    "state": "recovering",
                    "previous_a": previous,
                    "recovery_revision": c.requests.intent().revision,
                    "recovery_reason": reason,
                },
            )
            j.put("control", {**j.get("control", {}), "resume_limit": previous})
            c.session.gateway.controls._claim(True, stopping=True)
            c.released_epoch = None
            c.requests.finish(identifier, "failed", {"classification": reason})

    # Apply only the already owned station zero, including before native source identification or reintroduction.
    async def protect_overlap(self) -> None:
        c, j = self.controller, self.journal
        g = c.session.gateway
        change = j.get("managed_current_change", {})
        epoch = g._charger_epoch

        # A newer owner Pause or another Boot revokes this operation's restrictive ticket too.
        def guard() -> None:
            if (
                c.stopping
                or change.get("state") != "recovering"
                or j.get("managed_current_change", {}).get("id") != change["id"]
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
        if j.get("trial_phase") == "active":
            g.controls._complete(j.get("control")["ticket"], uncertain=False)

    # Reconcile the failed selection under zero, then restore only the previous verified current without any Start.
    async def recover_overlap(self) -> None:
        c, j = self.controller, self.journal
        change = j.get("managed_current_change", {})
        if change.get("state") != "recovering":
            return
        if c.requests.intent().mode != "app" or c.requests.intent().revision != change["recovery_revision"]:
            j.put("managed_current_change", {**change, "state": "cancelled"})
            return
        await self.protect_overlap()
        native = await c.native.read()
        c.native.require_known_session(native)
        if change.get("recovery_reason") == "process_replaced" and not j.get("managed_current_change")["observed_open"]:
            # Preserve the existing fail-closed policy when replacement confirms there was no overlapping session.
            with j.atomic():
                intent = c.requests.intent()
                j.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                c.session.gateway.controls._claim(True, stopping=True)
                j.put("control", {**j.get("control"), "resume_limit": change["previous_a"]})
                j.put("managed_current_change", {**j.get("managed_current_change"), "state": "interrupted"})
            return
        local = j.active()
        if local is not None:
            require_current_session(c.session.gateway, native, local)
        await c.restore_owner_control(continuing_local=local)
        j.put("managed_current_change", {**j.get("managed_current_change"), "state": "cancelled"})

    # Accept a delayed genuine original-plan report under this operation's zero, without inventing its lifecycle.
    async def prepare_native_report(self, payload: dict[str, Any]) -> NativeStartReport | None:
        c, j = self.controller, self.journal
        change = j.get("managed_current_change", {})
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
            j.put("managed_current_change", {**j.get("managed_current_change"), "reported_local": local})

        return NativeStartReport(local, plan["planId"], None, accepted)

    # Restrict an interrupted selection without giving it owner Pause authority over a concurrent transaction.
    def cancel_under_timer(self, identifier: str, value: int, previous: int, reason: str) -> None:
        c, j = self.controller, self.journal
        with j.atomic():
            timer, active = j.get(KEY), j.active()
            if reason in ("process_replaced", "current_unconfirmed") and not timer.get("retire_reason"):
                timer = {**timer, "retire_reason": "current_update_interrupted"}
                j.put(KEY, timer)
            if active is not None:
                timer = {**timer, "unidentified_local": active}
                j.put(KEY, timer)
            c.schedule.begin_hold(timer)
            j.put("control", {**j.get("control", {}), "resume_limit": previous})
            c.session.gateway.controls._claim(True, stopping=True)
            j.put("managed_current_change", {"id": identifier, "current_a": value, "state": "cancelled"})
            c.requests.finish(identifier, "failed", {"classification": reason})

    # Supervision observes the completed update instead of sampling through a legitimate profile replacement.
    async def settle(self) -> None:
        if self.worker is not None and not self.worker.done():
            await asyncio.shield(self.worker)

    # Retire all owned current work before the single journal owner closes its database.
    async def close(self) -> None:
        if self.worker is not None:
            await asyncio.gather(self.worker, return_exceptions=True)
