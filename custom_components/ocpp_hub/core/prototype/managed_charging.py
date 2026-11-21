"""Scoped normal and timed charging share one-shot Start guards; restart never repeats Start."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .command_processor import CommandPriority
from .connection_scope import CloudAttemptScope, control_scope_matches, current_scope
from .control_state import ControlState
from .gateway import NativeStartReport
from .local_control import local_start_receipt, retire_unsent_local_start
from .managed_facts import ManagedFacts
from .managed_intent import ControlIntent, request_pending, write_intent
from .managed_settlement import claim_internal_pause, complete_record, current_candidate, require_record
from .managed_sources import confirmed_vendor_start, recorded_station_profile
from .managed_start_binding import known_plan_source
from .managed_start_cancellation import CancellationRetirement, retire_cancelled_start
from .managed_start_debt import preserved_start_debt
from .policy import DeniedError
from .recovery_contracts import ALLOW, AuthorityStamp, GateDecision, Pending, Prerequisite
from .rpc import UnavailableError
from .security import SecurityError
from .start_handoff import StartHandoff, StartHandoffContext, StartPublication
from .start_settlement import (
    RetirementRule,
    StartFailureAction,
    StartFailureFacts,
    settle_start,
)
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .managed import ManagedController

DURATION = 120
STARTUP_BUDGET = 60
ACTIVE = frozenset({"armed", "starting", "awaiting_power", "charging", "running", "stopping"})


@dataclass(frozen=True)
class TestFlow:
    """The first verified power sample fixes the nominal Stop time even if publication must retry."""

    observed_at: float
    deadline: float


@dataclass
class ChargeLease:
    """A live request belongs to one actual introduction, revision, Stop generation and monotonic budget."""

    identifier: str
    source: str
    revision: int
    epoch: int
    control_revision: int
    identity: str
    spec: str
    stop_epoch: int
    deadline: float
    hard_deadline: float
    first_mutation: int
    mode: str = "app"
    normal: bool = False
    current_a: int = 6
    initial_zero_ticket: str | None = None
    initial_zero_boot: str | None = None
    initial_zero_stop_epoch: int | None = None
    start_scope: CloudAttemptScope | None = None
    first_flow: TestFlow | None = None
    accepted_boot: str | None = None


class ManagedCharging:
    """One owner supervises normal charging and bounded local/cloud tests without replaying Start."""

    # Construction may retire an interrupted permission, but never sends a Start or renews a persisted deadline.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.journal.starts.set_lease(None)
        self.worker: asyncio.Task[None] | None = None
        self._closing = False
        self.timer: asyncio.Task[None] | None = None
        self.changed = asyncio.Event()
        self.source: tuple[HandoverEvidence, str | None, int] | None = None
        saved = self.state()
        if saved.get("state") in ACTIVE:
            with self.journal.atomic():
                ownership = controller.requests.pause_ownership()
                intent = ownership.intent
                receipt = self.journal.starts.receipt()
                refused = (
                    receipt.get("request_id") == saved["id"]
                    and receipt.get("stage") == "rejected"
                    and receipt.get("local") is None
                )
                preserve = (
                    saved.get("kind") == "normal"
                    and intent.mode == "app"
                    and self.journal.get("control", {}).get("start_pending") is True
                    and receipt.get("local") is None
                    and receipt.get("stage") in ("issued", "accepted")
                )
                if not ownership.supersedes(saved.get("revision")):
                    if not preserve:
                        write_intent(self.journal, intent, "paused", coalesce=refused)
                    claim_internal_pause(controller, "test", saved["id"])
                self.journal.starts.interrupted(saved, preserve)
        self.retire_refused_unsent()
        controller.native.observation_listener = self.record_source
        controller.native.positive_guard = self.positive_decision

    # Keep one journal owner and the same durable management receipt table as Pause/Return.
    @property
    def journal(self) -> Journal:
        return self.controller.session.journal

    # The shared lifecycle owner holds the only active lease slot; callers retain their exact object identity.
    @property
    def lease(self) -> ChargeLease | None:
        return self.journal.starts.lease

    # Compatibility callers publish or retire memory only after their owning transaction commits.
    @lease.setter
    def lease(self, value: ChargeLease | None) -> None:
        self.journal.starts.set_lease(value)

    # Private provenance includes identifiers; the public summary below exposes only source, state and timing.
    def state(self) -> dict[str, Any]:
        value: dict[str, Any] = self.journal.get("managed_test_charge", {})
        return value

    # Timed tests retain Pause through settlement; normal Start has its own deferred Return lifecycle.
    def return_blocked(self) -> bool:
        lease, state = self.lease, self.state()
        return bool(
            lease is not None and not lease.normal or state.get("kind") == "test" and state.get("state") == "stopping"
        )

    # A durable failed receipt proves this request had no effects; it cannot own a later ordinary session.
    def retire_refused_unsent(self) -> None:
        candidate = self.journal.starts.retirement(RetirementRule.REFUSED)
        if candidate is not None:
            self.journal.starts.retire(candidate)

    # Select once, then require the caller's live Pause proof before this exact unissued retirement.
    def retire_unissued_at_zero(self, native: HandoverEvidence) -> bool:
        candidate = self.journal.starts.retirement(RetirementRule.UNISSUED)
        if candidate is None:
            return False
        try:
            self.controller.require_paused(native)
        except SecurityError, DeniedError, RecoveryRequiredError:
            return False
        self.journal.starts.retire(candidate)
        return True

    # Idle expiry can use an ended historical receipt, or a separately verified unissued command.
    def ended_at_idle(self, native: HandoverEvidence, *, unissued: bool = False) -> bool:
        j = self.journal
        return bool(
            not j.get("control", {}).get("start_pending")
            and j.active() is None
            and not native.physical.transaction_open
            and (unissued or j.starts.receipt_ended())
        )

    # Exact local closure can finish its request at verified Pause zero while separate cloud delivery stays retained.
    def retire_closed_at_zero(self, native: HandoverEvidence) -> None:
        j = self.journal
        candidate = j.starts.retirement(RetirementRule.CLOSED)
        if candidate is None:
            return
        try:
            self.controller.require_paused(native)
        except SecurityError, DeniedError, RecoveryRequiredError:
            return
        try:
            with j.atomic():
                j.starts.retire(candidate)
        except sqlite3.Error as error:
            if j.db.in_transaction:
                raise
            raise RecoveryRequiredError("verified normal Start closure awaits retirement") from error
        j.starts.set_lease(None)
        self.changed.set()

    # Fault supervision still settles exact local closure; cloud delivery retains its independent recovery gate.
    async def retire_closed_after_fault(self) -> None:
        state = self.state()
        request = self.journal.db.execute(
            "SELECT state FROM managed_requests WHERE id=?", (state.get("id"),)
        ).fetchone()
        if (
            self.controller.stopping
            or self.controller.requests.intent().mode != "paused"
            or state.get("kind") != "normal"
            or state.get("source") != "local"
            or state.get("state") == "completed"
            and (request is None or request["state"] not in ("accepted", "running", "uncertain"))
            or local_start_receipt(self.journal).get("local") is None
        ):
            return
        try:
            native = await self.controller.native.read()

            # Apply the exact closure while the controller still owns supervision.
            def settle_closure() -> None:
                if not self.controller.stopping:
                    self.retire_closed_at_zero(native)

            self.controller.commands.decide(settle_closure)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            return

    # Explain authorization separately from physical health, without granting permission from a public snapshot.
    def authorization_status(self) -> str:
        g = self.controller.session.gateway
        if not self.journal.phase.read().legacy_active or self.journal.phase.starting:
            return "not_ready"
        if any(g._local_start_allowed(tag) for tag in g.policy.allowed_tags):
            return "ready"
        if any(g.local_start_expired(tag) for tag in g.policy.allowed_tags):
            return (
                "renewal_required"
                if self.controller.session.runtime.status.vendor_connected
                and g.vendor.sender is not None
                and g.vendor_registration == "Accepted"
                else "expired_cloud_unavailable"
            )
        return "denied" if g.policy.allowed_tags else "no_approved_tag"

    # Admission may offer a normal online renewal; execution still rechecks approval and fresh physical zero.
    def available(self, native: HandoverEvidence | None = None, *, local: bool = True, normal: bool = False) -> bool:
        return self.start_refusal(native, local=local, normal=normal) is None

    # Report the first failed prerequisite in admission order.
    def start_refusal(
        self, native: HandoverEvidence | None = None, *, local: bool = True, normal: bool = False
    ) -> str | None:
        c, j = self.controller, self.journal
        g = c.session.gateway
        facts = ManagedFacts(c)
        if c.stopping:
            return "service_stopping"
        if not (c.requests.intent().mode == "app" or normal and c.requests.intent().mode == "paused"):
            return "intent_not_startable"
        if j.fault.latched():
            return "fault_latched"
        if not j.phase.read().active:
            return "installation_not_active"
        if not g.controls.ready:
            return "station_ceiling_unconfirmed"
        if g.controls.desired() != (0 if c.requests.intent().mode == "paused" else c.current.selected()):
            return "station_ceiling_differs_from_selection"
        if not normal and c.current.selected() != 6:
            return "timed_test_requires_six_amperes"
        if c.current.pending():
            return "current_update_pending"
        if g.policy.allow_local_start:
            return "legacy_start_enabled"
        if local and not any(g._local_start_allowed(tag) for tag in g.policy.allowed_tags):
            if not normal or self.authorization_status() != "renewal_required":
                return "start_authorization_unavailable"
        if j.get("control", {}).get("uncertain"):
            return "control_uncertain"
        if j.get("control", {}).get("start_pending"):
            return "start_pending"
        if j.active() is not None:
            return "transaction_open"
        if j.get("vendor_start_grant"):
            return "vendor_start_pending"
        if not facts.cloud_queue_empty(lifecycle_only=normal):
            return "cloud_delivery_pending"
        if not facts.mutations_confirmed():
            return "mutation_unconfirmed"
        if c.requests.return_in_progress():
            return "return_pending"
        if not c.requests.has_capacity():
            return "request_capacity_exhausted"
        if self.state().get("state") in ACTIVE:
            return "managed_start_active"
        if not facts.boot_current():
            return "boot_unconfirmed"
        if not normal and not (c.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted"):
            return "timed_test_requires_cloud"
        if native is not None:
            if c.native.last_verified_at == 0:
                return "native_observation_stale"
            if native.physical.connection != "Plug":
                return "vehicle_not_plugged_in"
            if native.physical.transaction_open:
                return "native_transaction_open"
            if native.physical.power_w != 0:
                return "native_power_nonzero"
        return None

    # Acceptance and its one-shot deadline commit before any positive effect; duplicate identifiers return receipts.
    def accept(self, identifier: str, action: str, revision: int) -> dict[str, Any]:
        c, j = self.controller, self.journal
        previous = c.requests.existing(identifier, action, revision)
        if previous is not None:
            return previous
        native = c.current_native_sample()
        if (
            revision != c.requests.intent().revision
            or native is None
            or not 0 <= time.time() - native.physical.observed_at <= 15
            or not self.available(native, local=action in ("start", "start_test"), normal=action == "start")
        ):
            raise RecoveryRequiredError(
                "Start requires fresh idle authority, current approval or online renewal, and no pending work"
                if action == "start"
                else "test requires current shared six-ampere authority and no pending work"
            )
        g = c.session.gateway
        source = "local" if action in ("start", "start_test") else "cloud"
        lease = ChargeLease(
            identifier,
            source,
            revision,
            g._charger_epoch,
            g.controls._revision,
            fingerprint(j.identity),
            c.native.spec.digest,
            j.get("control", {}).get("stop_epoch", 0),
            time.monotonic() + STARTUP_BUDGET,
            time.monotonic() + STARTUP_BUDGET + (0 if action == "start" else DURATION),
            j.db.execute("SELECT COALESCE(MAX(rowid),0) FROM mutations").fetchone()[0],
            mode=c.requests.intent().mode,
            normal=action == "start",
            current_a=c.current.selected(),
            accepted_boot=g.boot_observation,
        )
        j.starts.admit(lease, action, STARTUP_BUDGET, DURATION)
        self.timer = asyncio.create_task(self._deadline(lease))
        if source == "local":
            self.worker = c.commands.start(lambda: self._start(lease))
        return c.requests.receipt(identifier)

    # Pause, deadline, shutdown, introduction, permission changes and a replaced request all revoke positive sends.
    def guard(self, lease: ChargeLease, *, stopping: bool = False) -> None:
        c, j = self.controller, self.journal
        g = c.session.gateway
        state = self.state()
        if (
            self.lease is not lease
            or c.stopping
            or time.monotonic() >= lease.deadline
            or c.requests.intent() != ControlIntent(lease.mode, lease.revision)
            or g._charger_epoch != lease.epoch
            or g.controls._revision != lease.control_revision
            or not g.current_boot_seen(lease.epoch)
            or lease.accepted_boot is not None
            and g.boot_observation != lease.accepted_boot
            or fingerprint(j.identity) != lease.identity
            or c.native.spec.digest != lease.spec
            or g.policy.allow_local_start
            or not 6 <= g.policy.max_current_a <= 32
            or c.current.selected() != lease.current_a
            or c.current.pending()
            or j.fault.latched()
            or not j.phase.read().active
            or state.get("id") != lease.identifier
            or state.get("state") not in ACTIVE
            or state.get("state") == "stopping"
            or lease.source == "local"
            and state.get("state") == "starting"
            and not request_pending(j, lease.identifier, "start" if lease.normal else "start_test", lease.revision)
            or c.requests.return_in_progress()
            or not stopping
            and j.get("control", {}).get("stop_epoch", 0) != lease.stop_epoch
            or lease.source == "local"
            and j.db.execute(
                "SELECT 1 FROM mutations WHERE rowid>? AND origin='vendor' AND action='RemoteStartTransaction' LIMIT 1",
                (lease.first_mutation,),
            ).fetchone()
        ):
            raise DeniedError("managed test Start permission expired or was superseded")

    # A normal Start can request one fresh cloud decision before any charger effect, under exact current authority.
    async def _renew_start_tag(self, lease: ChargeLease) -> str | None:
        c, g = self.controller, self.controller.session.gateway
        if any(g._local_start_allowed(tag) for tag in g.policy.allowed_tags):
            return None
        if self.authorization_status() != "renewal_required":
            raise DeniedError("a current local Start approval is unavailable")
        tag = next(tag for tag in sorted(g.policy.allowed_tags) if g.local_start_expired(tag))
        authority, stored = c.authority(), g.auth_info.stored(tag)
        if authority is None:
            raise DeniedError("Start approval renewal requires current connection authority")

        # A newer owner operation, Boot metadata or tag decision must win over this pending cloud reply.
        def guard() -> None:
            self.guard(lease)
            if c.authority() != authority or g.auth_info.stored(tag) != stored:
                raise DeniedError("Start approval renewal was superseded")

        guard()
        await g._cloud_authorize(tag, guard=guard)
        self.guard(lease)
        if c.authority() != authority or not g._local_start_allowed(tag):
            raise DeniedError("the manufacturer did not provide a current local Start approval")
        return tag

    # The service owns the worker after client disappearance; failures revoke only this Start's authority.
    async def _start(self, lease: ChargeLease) -> None:
        c, j = self.controller, self.journal
        effects = False
        try:
            previous_current = current_candidate(c) if lease.normal else None
            self.guard(lease)
            renewed_tag = await self._renew_start_tag(lease) if lease.normal else None
            self.guard(lease)
            initial = await c.settled_native()
            c.native.require(initial, closed=True)
            if initial.physical.connection != "Plug":
                raise DeniedError("test requires a freshly connected idle vehicle")
            self.guard(lease)
            if j.active() is not None:
                raise DeniedError("another session began before the Start's idle hold")
            effects = True

            # Capture this exact restrictive claim even if the outer worker is cancelled while its child finishes.
            def initial_zero_guard() -> None:
                nonlocal effects
                self.guard(lease, stopping=True)
                if j.active() is not None:
                    if lease.initial_zero_ticket is None:
                        effects = False
                    raise DeniedError("another session began during the Start's idle hold")
                control, g = j.get("control", {}), c.session.gateway
                if lease.initial_zero_ticket is None and control.get("stop_epoch", 0) == lease.stop_epoch:
                    return  # LocalControl checks once before synchronously making its own claim.
                if (
                    control.get("stop_epoch") != lease.stop_epoch + 1
                    or not ControlState(control).explicit_local_hold
                    or not isinstance(control.get("ticket"), str)
                    or lease.initial_zero_ticket not in (None, control["ticket"])
                    or lease.initial_zero_boot not in (None, g.boot_observation)
                ):
                    raise DeniedError("initial Start hold was superseded")
                lease.initial_zero_ticket, lease.initial_zero_boot = control["ticket"], g.boot_observation
                lease.initial_zero_stop_epoch = control["stop_epoch"]

            await c.session.gateway.controls.stop(guard=initial_zero_guard)
            self.guard(lease, stopping=True)
            lease.stop_epoch = j.get("control", {}).get("stop_epoch", 0)
            async with asyncio.timeout(15):
                while True:
                    self.guard(lease)
                    native = await c.native.read(closed=True)
                    if not isinstance(c.zero_decision(native, allow_offline=lease.normal), Pending):
                        break
                    await asyncio.sleep(0.1)
            if native.physical.connection != "Plug":
                raise DeniedError("test requires a connected vehicle")
            self.guard(lease)
            tags = sorted(
                tag for tag in c.session.gateway.policy.allowed_tags if c.session.gateway._local_start_allowed(tag)
            )
            if not tags:
                raise DeniedError("no current approved local tag is available for this test")
            tag = renewed_tag if renewed_tag is not None else tags[0]
            if not c.session.gateway._local_start_allowed(tag):
                raise DeniedError("the selected local Start approval was superseded")

            # Commit the attempt marker with LocalControl's receipt, never before its owned task first executes.
            def prepared() -> None:
                self.guard(lease)
                j.starts.prepared_metadata(lease, fingerprint(tag.casefold()))
                lease.start_scope = current_scope(c.session.gateway)

            # This synchronous writer joins the child's transaction; it never opens a nested atomic block.
            def publish(context: StartHandoffContext) -> StartPublication:
                self.guard(lease)
                expected = ControlIntent(lease.mode, lease.revision)
                authority = AuthorityStamp(expected, context.scope, context.control_revision, context.ticket)
                if not c.publication_is_current(authority):
                    raise RecoveryRequiredError("owned Start publication was superseded")
                if lease.normal:
                    c.requests.start_published(lease.identifier, expected, lease.epoch)
                    if previous_current is not None:
                        try:
                            require_record(c, previous_current)
                        except RecoveryRequiredError:
                            pass  # A newer current request retains its own independently owned outcome.
                        else:
                            complete_record(c, previous_current)
                return j.starts.published(lease, context, c.requests.intent())

            # Only successful COMMIT can change these in-memory authorities; no await separates publication.
            def committed(publication: StartPublication) -> None:
                if lease.normal:
                    c.released_epoch = publication.epoch
                j.starts.committed(lease, publication)
                self.changed.set()

            result = await c.session.gateway.start_managed_result(
                tag,
                lambda: self.guard(lease),
                request_id=lease.identifier,
                on_prepared=prepared,
                handoff=StartHandoff(publish, committed),
            )
            if result.failure is not None:
                raise result.failure
            if result.publication is None:
                raise RecoveryRequiredError("managed Start authority publication was not confirmed")
            self.guard(lease)
        except (
            DeniedError,
            SecurityError,
            RecoveryRequiredError,
            UnavailableError,
            TimeoutError,
            sqlite3.Error,
        ) as error:
            await self._failed_start(lease, error, effects)

    # Settle the exact failed operation before optional diagnostics; a newer restrictive winner remains intact.
    async def _failed_start(self, lease: ChargeLease, error: Exception, effects: bool) -> None:
        c = self.controller
        while not self._closing and not c.stopping:
            try:
                c.commands.decide(lambda: self._settle_failed_start(lease, error, effects))
                return
            except sqlite3.Error:
                # Genuine critical storage failure retains the separate deadline's existing zero/Stop authority.
                lease.deadline = min(lease.deadline, time.monotonic())
                self.changed.set()
                await asyncio.sleep(0.1)

    # Use the failed reply's lease and actual recorded effects before settling Start.
    def _settle_failed_start(self, lease: ChargeLease, error: Exception, effects: bool) -> None:
        j = self.journal
        record = j.starts.load(wire=False, request_id=lease.identifier)
        facts = StartFailureFacts(
            lease.identifier, "start" if lease.normal else "start_test", self.lease is lease, effects
        )
        decision = settle_start(record, facts)
        if decision.action is StartFailureAction.UNRELATED:
            return
        assert record is not None and record.request is not None
        cause = {
            "id": lease.identifier,
            "action": record.request.action,
            "revision": record.request.revision,
            "type": type(error).__name__,
            "detail": str(error)[:1024],
            "at_epoch": time.time(),
            "effects_started": effects,
        }
        record = j.starts.load(ledger=False)
        decision = settle_start(record, facts)
        if decision.retire_prepared:
            assert record is not None
            retire_unsent_local_start(j, record.receipt.ticket)
        if decision.action is StartFailureAction.DIAGNOSTIC_ONLY:
            self.record_failure_diagnostic(cause)
            return  # A deadline or later proof already owns the state and immutable terminal receipt.
        if decision.action is StartFailureAction.FAIL_PREFLIGHT:
            j.starts.fail_preflight(lease.identifier, cause)
            self.changed.set()
            self.retire_refused_unsent()
        else:
            self.stop(lease, "start_not_confirmed", failure=cause)
        self.record_failure_diagnostic(cause)

    # Compatibility diagnostics cannot keep a settled worker alive or undo its request-bound durable cause.
    def record_failure_diagnostic(self, cause: dict[str, Any]) -> None:
        try:
            self.journal.put(
                "managed_test_failure",
                {key: cause[key] for key in ("type", "detail", "at_epoch", "effects_started")},
            )
        except sqlite3.Error:
            pass  # The critical state already records this operation's failure, or a newer outcome won.

    # Bind an actual accepted Start inside its ledger transaction, including sessions ending between native polls.
    def started(self, local: int, payload: dict[str, Any]) -> None:
        lease = self.lease
        if lease is not None:
            try:
                self.journal.starts.managed_bound(lease, local, payload.get("idTag"), lambda: self.guard(lease))
            except DeniedError:
                return

    # A real bound Start/Stop pair completes without changing current authority or stopping a later app session.
    def finish_closed_normal(self, lease: ChargeLease) -> bool:
        finished = self.journal.starts.finish_closed(lease)
        if finished:
            self.changed.set()
        return finished

    # Keep verified source evidence on its actual Boot; an ACK never identifies an unrelated native transaction.
    def record_source(self, native: HandoverEvidence) -> None:
        g = self.controller.session.gateway
        self.source = (native, g.boot_observation, g.controls._revision)
        state, lease = self.state(), self.lease
        if state.get("state") == "completed" or state.get("independent_seen"):
            return
        if (
            not (lease is not None and lease.normal and lease.identifier == state.get("id"))
            and not self.scoped_normal_stop()
        ):
            return
        local = self.journal.active()
        if local is not None and self.independent_daily(native, local):
            self.journal.starts.independent_seen(state)

    # The verified original plan and matching ledger identify the daily even when the owner uses its tag.
    def independent_daily(self, native: HandoverEvidence, local: int) -> bool:
        state, j = self.state(), self.journal
        if (
            state.get("kind") != "normal"
            or not isinstance(state.get("tag_digest"), str)
            or not native.physical.transaction_open
            or native.physical.native_transaction_id != local
            or j.active() != local
            or native.physical.connection == "Unplug"
            or native.saved_schedule is None
            or local_start_receipt(j).get("local") == local
        ):
            return False
        row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
        if row is None:
            return False
        tag = json.loads(row["start"])["idTag"].casefold()
        return bool(
            any(
                plan["planId"] == native.session_plan_id
                and isinstance(plan.get("idTag"), str)
                and plan["idTag"].casefold() == tag
                and plan.get("kindType") == "Recurring"
                for plan in native.saved_schedule["planList"]
            )
        )

    # Cached evidence authorizes only this same transaction on the same fresh, authenticated introduction.
    def current_daily(self, local: int) -> bool:
        c, g = self.controller, self.controller.session.gateway
        if self.source is None:
            return False
        native, boot, revision = self.source
        return bool(
            boot is not None
            and boot == g.boot_observation
            and revision == g.controls._revision
            and c.native.last_verified_at == native.physical.observed_at
            and 0 <= c.native.observation_age() <= 5
            and self.independent_daily(native, local)
        )

    # A newer explicit Pause owns unrestricted Stop; an expired normal Start owns only its exact receipt.
    def scoped_normal_stop(self) -> bool:
        state = self.state()
        if not state.get("expired_normal") or state.get("state") == "completed":
            return False
        ownership = self.controller.requests.pause_ownership()
        return ownership.pause.revision != ownership.intent.revision

    # Preserve only this expired local command's debt after fresh closure, without granting a new Start or Stop.
    def idle_debt(self, native: HandoverEvidence) -> bool:
        return bool(
            self.scoped_normal_stop()
            and preserved_start_debt(self.journal)
            and not native.physical.transaction_open
            and native.physical.native_transaction_id is None
            and all(value == 0 for value in native.currents_a + native.powers_w)
        )

    # Only fresh proof of this exact daily can preserve the separate owner's debt while its lifecycle stays open.
    def preserve_debt(self, native: HandoverEvidence, continuing_local: int | None = None) -> bool:
        if continuing_local is None:
            return self.idle_debt(native)
        return bool(
            self.scoped_normal_stop()
            and preserved_start_debt(self.journal, continuing_local=continuing_local)
            and self.source is not None
            and self.source[0] is native
            and self.current_daily(continuing_local)
            and all(value == 0 for value in native.currents_a + native.powers_w)
        )

    # Expired Start debt is an independent prerequisite, never permission inferred from zero or task completion.
    def positive_decision(self, native: HandoverEvidence) -> GateDecision:
        if not self.scoped_normal_stop():
            return ALLOW
        j, c = self.journal, self.controller
        if self.idle_debt(native):
            return ALLOW
        owner = local_start_receipt(j)
        local = j.active()
        if c.requests.intent().mode == "app" and local is not None:
            if self.independent_daily(native, local):
                return ALLOW
            return Pending(Prerequisite.SOURCE, "expired Start cannot release an unidentified or owner transaction")
        if j.get("control", {}).get("start_pending") or native.physical.transaction_open:
            return Pending(Prerequisite.LIFECYCLE, "expired Start awaits its real owner lifecycle")
        if (
            owner.get("local") is not None
            and not j.db.execute(
                "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (owner["local"],)
            ).fetchone()
        ):
            return Pending(Prerequisite.LIFECYCLE, "expired Start awaits actual owner closure")
        return ALLOW

    # Zero authority does not borrow Stop authority from whichever daily transaction happens to be open.
    async def hold_expired(self) -> None:
        c, j = self.controller, self.journal
        g, expected = c.session.gateway, c.requests.intent()
        epoch, identifier = g._charger_epoch, self.state().get("id")

        # Restrictive writes remain bound to this request, current intent, commissioned device and actual Boot.
        def guard() -> None:
            if (
                c.stopping
                or self.state().get("id") != identifier
                or not self.scoped_normal_stop()
                or c.requests.intent() != expected
                or g._charger_epoch != epoch
                or not g.current_boot_seen(epoch)
                or fingerprint(j.get("boot")) != c.native.spec.boot_digest
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
            ):
                raise RecoveryRequiredError("expired Start zero authority was superseded")

        guard()
        if not j.get("control", {}).get("inhibit"):
            g.controls._claim(True, stopping=True)
        ticket = j.get("control", {}).get("ticket")
        await g.controls.ensure_ceiling(guard=guard)
        guard()
        # Confirm only the station zero. The separate Start receipt and real lifecycle remain outstanding.
        g.controls._complete(ticket, uncertain=False)

    # Each ordinary recovery owner validates its own delayed original-plan report under its existing zero.
    async def prepare_native_report(self, payload: dict[str, Any]) -> NativeStartReport | None:
        c = self.controller
        if self.journal.current_change.read().recovering:
            return await c.current.prepare_native_report(payload)
        if c.ordinary.active():
            return await c.ordinary.prepare_native_report(payload)
        return None

    # Match an already issued owner command without replaying its Start or guessing a lifecycle number.
    def matches_pending_start(self, tag: str) -> bool:
        j = self.journal
        digest = fingerprint(tag.casefold())
        prior = local_start_receipt(j)
        return bool(
            j.get("control", {}).get("start_pending")
            and prior.get("local") is None
            and prior.get("stage") in ("issued", "accepted")
            and prior.get("digest") == digest
            or confirmed_vendor_start(j, digest, None)
        )

    # A held owner report must use this authenticated current introduction.
    def report_hold_authorized(self) -> bool:
        c, j = self.controller, self.journal
        g = c.session.gateway
        return bool(
            g.current_boot_seen(g._charger_epoch)
            and fingerprint(j.get("boot")) == c.native.spec.boot_digest
            and c.native.introduced_epoch == g._charger_epoch
            and c.native.introduced_revision == g.controls._revision
        )

    # Join the real owner Pause zero before admitting the delayed report of its already issued Start.
    async def prepare_owner_report(self, payload: dict[str, Any], *, _reclassified: bool = False) -> None:
        c, j = self.controller, self.journal
        g, tag = c.session.gateway, payload.get("idTag")
        if j.active() is not None or not isinstance(tag, str) or not self.matches_pending_start(tag):
            return
        if c.stopping:
            # Join the retiring service's zero attempt. This report cannot acquire new positive authority.
            stopping = g.controls._stopping
            if stopping is not None and not stopping.done():
                await asyncio.wait((stopping,), timeout=min(1.0, g.charger.timeout))
            if not self.can_record_owner(tag):
                raise UnavailableError("the already issued Start report awaits verified shutdown zero")
            return
        if self.scoped_normal_stop() and c.requests.intent().mode == "app":
            await self.hold_expired()
        if c.requests.intent().mode == "paused" and (not g.controls.ready or g.controls.desired() != 0):
            operations = set(g.controls._operations)
            if c.pause_task is not None and not c.pause_task.done():
                operations.add(c.pause_task)
            if operations:
                await asyncio.wait(operations, timeout=min(1.0, g.charger.timeout))
            if c.requests.intent().mode == "app":
                if _reclassified:
                    raise UnavailableError("Start report authority changed again during its control handoff")
                return await self.prepare_owner_report(payload, _reclassified=True)
            if not self.can_record_owner(tag):
                raise UnavailableError("the acknowledged owner report awaits verified Pause zero")

    # The held listener consumes an existing receipt under verified zero and cannot authorize another Start.
    def can_record_owner(self, tag: str) -> bool:
        c, g = self.controller, self.controller.session.gateway
        return bool(
            (
                c.stopping
                or c.requests.intent().mode == "paused"
                or self.scoped_normal_stop()
                and c.requests.intent().mode == "app"
            )
            and self.matches_pending_start(tag)
            and self.journal.active() is None
            and not self.journal.fault.latched()
            and g.controls.ready
            and g.controls.desired() == 0
            and self.report_hold_authorized()
        )

    # Reconcile expired authority independently of the short lease and retain the original Start reporting debt.
    async def reconcile_expired(self) -> bool:
        if self.controller.stopping or not self.scoped_normal_stop():
            return False
        c, j, state = self.controller, self.journal, self.state()
        g, expected = c.session.gateway, c.requests.intent()
        try:
            active = j.active()
            if expected.mode == "paused" or active is not None and not self.current_daily(active):
                await self.hold_expired()
            owner = local_start_receipt(j)
            closing = (
                active if owner.get("local") == active and owner.get("digest") == state.get("tag_digest") else None
            )
            native = await c.native.read(closing_local=closing, stop_only=closing is not None)

            # Apply the native closure to the original outstanding owner Start.
            def settle_at_zero() -> bool:
                if c.requests.intent() != expected:
                    raise RecoveryRequiredError("expired Start observation was superseded")
                unissued = self.retire_unissued_at_zero(native)
                self.retire_closed_at_zero(native)
                return unissued

            unissued = c.commands.decide(settle_at_zero)
            local, owner = j.active(), local_start_receipt(j)
            if expected.mode == "app" and local is not None and self.independent_daily(native, local):
                if (
                    g.controls.ready
                    and g.controls.desired() == state["current_a"]
                    and c.released_epoch == g._charger_epoch
                ):
                    return False
                await self.hold_expired()
                # Reuse the ordinary guarded release; this raises the ceiling and never sends RemoteStart.
                progress = await c.recovery.restore(expected, continuing_local=local)
                return not progress.shared
            owner_closed = self.ended_at_idle(native, unissued=unissued)
            if owner_closed:
                if expected.mode == "app" and (not g.controls.ready or g.controls.desired() == 0):
                    if not self.scoped_normal_stop():
                        # If the Start is settled, connection recovery owns any retry while cloud Stop is pending.
                        c.released_epoch = None
                    progress = await c.recovery.restore(expected)
                    if not progress.shared:
                        return True

                # A later restore or Pause can leave a different request as the current row.
                def complete_expired_start() -> None:
                    current = self.state()
                    if current.get("id") == state.get("id"):
                        j.starts.complete_metadata(current)

                c.commands.decide(complete_expired_start)
                return False
            if expected.mode == "app" and local is None and not native.physical.transaction_open:
                if (
                    g.controls.ready
                    and g.controls.desired() == state["current_a"]
                    and c.released_epoch == g._charger_epoch
                ):
                    return False
                if self.idle_debt(native):
                    # A retired connection release still needs this domain's own verified zero before restoration.
                    await self.hold_expired()
                    progress = await c.recovery.restore(expected)
                    return not progress.shared
            await self.hold_expired()
            c.native.require_known_session(native)
            if local is not None and owner.get("local") == local and owner.get("digest") == state.get("tag_digest"):
                epoch, boot = g._charger_epoch, g.boot_observation

                # Recheck receipt, lifecycle and intent after every awaited native or profile verification.
                def guard() -> None:
                    if (
                        c.stopping
                        or c.requests.intent() != expected
                        or not self.scoped_normal_stop()
                        or g._charger_epoch != epoch
                        or g.boot_observation != boot
                        or local_start_receipt(j).get("local") != local
                        or j.active() not in (None, local)
                    ):
                        raise RecoveryRequiredError("expired owner Stop was superseded")

                if not g.controls.stop_acknowledged(local):
                    await g.controls.stop(expected_local=local, guard=guard)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            try:
                await self.hold_expired()
            except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
                pass  # Keep the debt and retry zero; no unknown transaction receives a guessed Stop.
        return True

    # Polling waits for the short native/start sequence; its independent Stop timer continues to run meanwhile.
    async def settle(self) -> None:
        if self.worker is not None and not self.worker.cancelled():
            await asyncio.shield(self.worker)

    # A poll leaves physical work owned; cancelled-task recovery still needs fresh source and exact outcome evidence.
    async def settle_for_poll(self) -> bool:
        self.controller.commands.decide(self.journal.starts.publish_pending_reply)
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
            g = self.controller.session.gateway
            if g.controls._operations or g.charger._pending:
                return False  # A shielded child still owns physical work even after its outer task disappeared.
        if worker.cancelled() and self.lease is not None:
            try:
                if retire_cancelled_start(self, self.lease) is CancellationRetirement.PENDING:
                    return False
            except sqlite3.Error, RecoveryRequiredError:
                return False  # Required settlement is pending; no old deadline gains authority over another session.
        elif not worker.cancelled():
            worker.result()
        return True

    # Positive flow must belong to the known transaction and the actually accepted local or vendor Start path.
    def observed(self, native: HandoverEvidence) -> None:
        self.retire_refused_unsent()
        self.retire_unissued_at_zero(native)
        self.retire_closed_at_zero(native)
        lease, c = self.lease, self.controller
        state = self.state()
        if self.journal.starts.observe_normal(native):
            self.changed.set()
            return
        if lease is None:
            if state.get("state") != "completed" and state.get("expired_normal") and self.ended_at_idle(native):
                # An explicit Pause can finish this expired debt too, without reviving it on the next Return.
                self.journal.starts.complete_metadata(state)
            return
        if state.get("state") == "stopping":
            if (
                c.requests.intent().mode == "paused"
                and native.physical.power_w == 0
                and not native.physical.transaction_open
            ):
                c.require_paused(native)
                self.journal.starts.complete_metadata(state, stopped_at=time.time())
                self.journal.starts.set_lease(None)
                self.changed.set()
            return
        if state.get("state") == "charging" and not native.physical.transaction_open and native.physical.power_w == 0:
            self.stop(lease, "charger_closed")
            return
        try:
            self.guard(lease)
        except DeniedError:
            self.stop(lease, "superseded")
            return
        if lease.normal and state.get("start_rpc_accepted"):
            self.journal.starts.observe_normal(
                native, allow_open=True, known_source=known_plan_source(self.journal, native)
            )
            self.changed.set()
            return
        if state.get("state") != "charging" and native.physical.power_w > 10:
            active = self.journal.active()
            if active is None or native.physical.native_transaction_id != active:
                self.stop(lease, "unknown_session")
                return
            grant = self.journal.get("vendor_start_grant", {})
            cloud = (
                grant.get("local") == active
                and self.journal.db.execute(
                    "SELECT 1 FROM mutations WHERE rowid>? AND token=? AND origin='vendor' "
                    "AND action='RemoteStartTransaction' AND state='confirmed' LIMIT 1",
                    (lease.first_mutation, grant.get("mutation")),
                ).fetchone()
            )
            local = local_start_receipt(self.journal)
            if (
                lease.source == "local"
                and (cloud or not state.get("start_rpc_accepted") or local.get("local") != active)
            ) or (lease.source == "cloud" and not cloud):
                return
            if lease.first_flow is None:
                lease.first_flow = TestFlow(time.time(), time.monotonic() + DURATION)
            flow = lease.first_flow
            try:
                with self.journal.atomic():
                    self.journal.starts.confirm_test_flow(
                        lease,
                        state,
                        active,
                        flow.observed_at,
                        DURATION,
                        cloud_start=(grant["token"], grant["mutation"]) if cloud else None,
                    )
            except sqlite3.Error as error:
                if self.journal.db.in_transaction:
                    raise
                raise RecoveryRequiredError("verified test flow awaits receipt publication") from error
            lease.deadline = min(lease.hard_deadline, flow.deadline + (15 if lease.source == "cloud" else 0))
            self.changed.set()

    # A failed normal Start can reclaim only zero from its own unchanged ticket beneath the original owner Pause.
    def failed_start_zero_needed(self, lease: ChargeLease, reason: str, failure: dict[str, Any] | None) -> bool:
        c, j, g = self.controller, self.journal, self.controller.session.gateway
        intent, state, receipt, control = (
            c.requests.intent(),
            self.state(),
            local_start_receipt(j),
            j.get("control", {}),
        )
        return bool(
            self.lease is lease
            and lease.normal
            and reason == "start_not_confirmed"
            and failure is not None
            and failure.get("id") == lease.identifier
            and failure.get("effects_started") is True
            and lease.mode == intent.mode == "paused"
            and lease.revision == intent.revision
            and lease.start_scope is not None
            and control_scope_matches(g, (lease.start_scope, lease.control_revision))
            and state.get("id") == lease.identifier
            and state.get("source") == "local"
            and state.get("start_attempted") is True
            and receipt.get("request_id") == lease.identifier
            and receipt.get("digest") == state.get("tag_digest")
            and receipt.get("stage") in ("issued", "accepted")
            and receipt.get("ticket")
            and control.get("ticket") == receipt["ticket"]
            and control.get("owner") in ("local", "shared")
            and control.get("inhibit") is False
            and j.phase.read().active
            and not j.fault.latched()
            and not c.stopping
        )

    # An internal timed-test Pause can hold zero without stopping a different charging session.
    def timed_pause_leaves_session(self, local: int | None) -> bool:
        requests = self.controller.requests
        if local is None or requests.pause_request() is not None:
            return False
        intent = requests.intent()
        test = self.state()
        internal = self.journal.get("managed_internal_pause", {})
        return (
            intent.mode == "paused"
            and internal.get("source") == "test"
            and internal.get("revision") == intent.revision
            and internal.get("id") == test.get("id")
            and test.get("kind") != "normal"
            and self.timed_stop_target(test) != local
        )

    # A replacement introduction retains the same limits on which session the previous request can Stop.
    def recovery_zero_only(self) -> bool:
        return self.scoped_normal_stop() or self.timed_pause_leaves_session(self.journal.active())

    # Resolve the exact timed test transaction, including a genuine report received before first power.
    def timed_stop_target(self, state: dict[str, Any]) -> int | None:
        for key in ("stop_local", "local_transaction"):
            recorded = state.get(key)
            if isinstance(recorded, int) and not isinstance(recorded, bool):
                return recorded
        j = self.journal
        if state.get("source") == "local":
            receipt = local_start_receipt(j)
            local = receipt.get("local")
            if receipt.get("request_id") == state.get("id") and type(local) is int:
                return local
        elif type(state.get("mutation_floor")) is int:
            grant = j.get("vendor_start_grant", {})
            local = grant.get("local")
            if (
                type(local) is int
                and j.db.execute(
                    "SELECT 1 FROM mutations WHERE rowid>? AND token=? AND origin='vendor' "
                    "AND action='RemoteStartTransaction' AND state='confirmed'",
                    (state["mutation_floor"], grant.get("mutation")),
                ).fetchone()
            ):
                return local
        return None

    # An independently identified daily remains with the app when an unrelated test expires.
    def retire_test_beside_daily(self, lease: ChargeLease, state: dict[str, Any], target: int | None) -> bool:
        c, j = self.controller, self.journal
        active, current = j.active(), self.source
        if (
            lease.normal
            or active is None
            or active == target
            or current is None
            or c.requests.intent() != ControlIntent("app", lease.revision)
            or j.get("control", {}).get("start_pending")
            or j.get("vendor_start_grant")
        ):
            return False
        native, boot, revision = current
        g = c.session.gateway
        if (
            (boot, revision) != (g.boot_observation, g.controls._revision)
            or not 0 <= time.time() - native.physical.observed_at <= 15
            or (c.ordinary.source(native) or {}).get("kind") != "daily"
        ):
            return False
        j.starts.retire_independent_test(lease, state)
        j.starts.set_lease(None)
        self.changed.set()
        return True

    # Retire only the timed test's target, or the matching normal Start's own pending evidence.
    def stop(self, lease: ChargeLease, reason: str, *, failure: dict[str, Any] | None = None) -> None:
        state = self.state()
        if self.lease is not lease or state.get("state") == "stopping":
            return
        c, j = self.controller, self.journal
        if c.stopping:
            return
        target = None if lease.normal else self.timed_stop_target(state)
        if self.retire_test_beside_daily(lease, state, target):
            return
        with j.atomic():
            intent = c.requests.intent()
            preserve = (
                lease.normal
                and intent.mode == "app"
                and intent.revision == lease.revision
                and self.state().get("independent_seen") is True
            )
            if intent.mode != "paused" and not preserve:
                write_intent(j, intent, "paused")
                claim_internal_pause(c, "test", lease.identifier)
                c.requests.charge_stopped()
            elif self.failed_start_zero_needed(lease, reason, failure):
                c.session.gateway.controls._claim(True)
            j.starts.expire(lease, reason, preserve, failure, target=target)
        if lease.normal:
            self.journal.starts.set_lease(None)
        c.schedule_pause()
        self.changed.set()

    # A monotonic deadline owns Stop independently of HA, the initiating socket, native reads and service polling.
    async def _deadline(self, lease: ChargeLease) -> None:
        while self.lease is lease:
            try:
                if self.state().get("state") == "stopping":
                    return
                self.changed.clear()
                remaining = lease.deadline - time.monotonic()
                if remaining <= 0:
                    if await self.controller.commands.run(
                        lambda: self._expire(lease), priority=CommandPriority.RESTRICT
                    ):
                        return
                    await asyncio.sleep(0.1)
                    continue
                await asyncio.wait_for(self.changed.wait(), remaining)
            except TimeoutError:
                continue
            except sqlite3.Error, RecoveryRequiredError, DeniedError:
                # Failed no-dispatch settlement and restrictive claims retry without inventing another outcome.
                await asyncio.sleep(0.1)

    # Decide expiry in one restrictive turn and keep the existing work bound to the same lease.
    def _expire(self, lease: ChargeLease) -> bool:
        if self.lease is not lease or self.state().get("state") == "stopping":
            return True
        if time.monotonic() < lease.deadline:
            return False  # A fresh physical confirmation can extend the exact pending publication.
        cancellation = retire_cancelled_start(self, lease)
        if cancellation is CancellationRetirement.PENDING:
            return False
        if cancellation is not CancellationRetirement.RETIRED and not self.finish_closed_normal(lease):
            self.stop(lease, "deadline")
        return True

    # Public timing is descriptive and redacted; it is never authority to repeat a historical Start.
    def summary(self) -> dict[str, Any] | None:
        state = self.state()
        if state.get("kind") == "normal":
            return None
        remaining = 0.0
        if self.lease is not None:
            deadline = self.lease.deadline
            if state.get("state") == "charging" and self.lease.first_flow is not None:
                deadline = min(deadline, self.lease.first_flow.deadline)
            remaining = deadline - time.monotonic()
        return {
            "source": state.get("source"),
            "state": state.get("state"),
            "duration_seconds": DURATION,
            "seconds_remaining": min(DURATION, max(0, round(remaining))),
        }

    # Ordinary sessions separate measured charging from known transaction existence, without a countdown.
    def normal_summary(self, native: HandoverEvidence | None = None) -> dict[str, Any] | None:
        state = self.state()
        if state.get("kind") != "normal":
            return None
        name = "awaiting_session" if state.get("state") == "awaiting_power" else state.get("state")
        charging = bool(
            name == "running"
            and native is not None
            and native.physical.transaction_open
            and native.physical.native_transaction_id == state.get("local_transaction")
            and native.physical.power_w > 10
        )
        return {"state": name, "charging": charging}

    # Shutdown retires timers and awaits owned workers before their journal can be closed.
    async def close(self) -> None:
        self._closing = True
        self.journal.starts.set_lease(None)
        self.changed.set()
        if self.timer is not None:
            self.timer.cancel()
            await asyncio.gather(self.timer, return_exceptions=True)
        if self.worker is not None:
            await asyncio.gather(self.worker, return_exceptions=True)
