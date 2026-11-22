"""Scoped normal and timed charging share one-shot Start guards; restart never repeats Start."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

from .command_processor import CommandPriority
from .connection_scope import CloudAttemptScope, control_scope_matches, current_control_scope
from .control_decisions import ExpiredStartStop
from .local_control import local_start_receipt
from .managed_settlement import current_candidate
from .managed_sources import confirmed_vendor_start, known_plan_source
from .managed_start_cancellation import CancellationRetirement, retire_cancelled_start
from .managed_start_debt import preserved_start_debt
from .native_bounded import readiness_decision
from .native_faults import _NativeMeasurementError
from .policy import DeniedError
from .recovery_contracts import ALLOW, GateDecision, Pending, Prerequisite, require_allowed
from .rpc import UnavailableError
from .security import SecurityError
from .start_handoff import StartHandoff, StartHandoffContext, StartPublication
from .store import Journal, RecoveryRequiredError, StationZeroStorageError, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .managed import ManagedController

DURATION = 120
STARTUP_BUDGET = 60


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
        controller.session.gateway.controls.decisions.restore_charging_request(self)
        controller.native.observation_listener = partial(
            controller.session.gateway.controls.decisions.schedule.record_start_source, self
        )
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

    # Start owns the settlement; only live observation and waking the timer belong here.
    def settle_local_zero(self, native: HandoverEvidence, *, closed_only: bool = False) -> bool:
        unissued, closed = self.journal.starts.retire_local_zero(
            lambda: self.controller.require_paused(native), closed_only=closed_only
        )
        if closed:
            self.changed.set()
        return unissued

    # Exact local closure can finish its request at verified Pause zero while separate cloud delivery stays retained.
    def retire_closed_at_zero(self, native: HandoverEvidence) -> None:
        self.settle_local_zero(native, closed_only=True)

    # Fault supervision still settles exact local closure; cloud delivery retains its independent recovery gate.
    async def retire_closed_after_fault(self) -> None:
        if (
            self.controller.stopping
            or self.controller.requests.intent().mode != "paused"
            or not self.journal.starts.fault_closure_pending()
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

    # Display the same Start decision used when accepting a request.
    def start_refusal(
        self, native: HandoverEvidence | None = None, *, local: bool = True, normal: bool = False
    ) -> str | None:
        return self.controller.session.gateway.controls.decisions.start_refusal(
            self, native, local=local, normal=normal
        )

    # The decision owner accepts the request and launches work only after its journal commit.
    def accept(self, identifier: str, action: str, revision: int) -> dict[str, Any]:
        return self.controller.session.gateway.controls.decisions.accept_start(
            self, identifier, action, revision, STARTUP_BUDGET, DURATION
        )

    # Capture the existing data and time budget for the accepted Start.
    def _start_lease(self, identifier: str, action: str, revision: int) -> ChargeLease:
        c, j = self.controller, self.journal
        g = c.session.gateway
        source = "local" if action in ("start", "start_test") else "cloud"
        return ChargeLease(
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

    # The worker and deadline keep their existing lifetimes after the request is recorded.
    def _launch_start(self, lease: ChargeLease) -> None:
        c = self.controller
        self.timer = asyncio.create_task(self._deadline(lease))
        if lease.source == "local":
            self.worker = c.commands.start(lambda: self._start(lease))

    # All preparation, dispatch and lifecycle users recheck the same live lease.
    def guard(self, lease: ChargeLease, *, stopping: bool = False) -> None:
        self.controller.session.gateway.controls.decisions.require_start_lease(
            self, lease, time.monotonic, stopping=stopping
        )

    # The service owns the worker after client disappearance; failures revoke only this Start's authority.
    async def _start(self, lease: ChargeLease) -> None:
        c, j = self.controller, self.journal
        effects = False
        try:
            previous_current = current_candidate(c) if lease.normal else None
            self.guard(lease)
            decisions = c.session.gateway.controls.decisions
            renewed_tag = await decisions.renew_local_start(self, lease) if lease.normal else None
            self.guard(lease)
            initial = await c.settled_native()
            decisions.require_idle_start(self, lease, initial)
            effects = True

            # Capture this exact restrictive claim even if the outer worker is cancelled while its child finishes.
            def initial_zero_guard() -> None:
                nonlocal effects
                self.guard(lease, stopping=True)
                if j.active() is not None:
                    if lease.initial_zero_ticket is None:
                        effects = False
                    raise DeniedError("another session began during the Start's idle hold")
                decisions.bind_initial_start_zero(lease)

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
            tag = decisions.choose_start_tag(c.session.gateway, renewed_tag)

            # Commit the attempt marker with LocalControl's receipt, never before its owned task first executes.
            def prepared() -> None:
                decisions.prepare_start_publication(self, lease, tag)

            # This synchronous writer joins the child's transaction; it never opens a nested atomic block.
            def publish(context: StartHandoffContext) -> StartPublication:
                return decisions.publish_start(self, lease, context, previous_current)

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

    # Retry lifecycle settlement through the same owner as admission and publication.
    def _settle_failed_start(self, lease: ChargeLease, error: Exception, effects: bool) -> None:
        self.controller.session.gateway.controls.decisions.settle_failed_start(self, lease, error, effects)

    # Compatibility diagnostics cannot keep a settled worker alive or undo its request-bound durable cause.
    def record_failure_diagnostic(self, cause: dict[str, Any]) -> None:
        try:
            self.journal.put(
                "managed_test_failure",
                {key: cause[key] for key in ("type", "detail", "at_epoch", "effects_started")},
            )
        except sqlite3.Error:
            pass  # The critical state already records this operation's failure, or a newer outcome won.

    # The report caller supplies the actual allocated session to the authority owner.
    def started(self, local: int, payload: dict[str, Any]) -> None:
        self.controller.session.gateway.controls.decisions.bind_managed_start(self, local, payload)

    # A real bound Start/Stop pair completes without changing current authority or stopping a later app session.
    def finish_closed_normal(self, lease: ChargeLease) -> bool:
        finished = self.journal.starts.finish_closed(lease)
        if finished:
            self.changed.set()
        return finished

    # Ask the shared owner whether this exact sample identifies an independent daily.
    def independent_daily(self, native: HandoverEvidence, local: int) -> bool:
        return self.controller.session.gateway.controls.decisions.schedule.independent_daily(self, native, local)

    # Continue a daily only while its verified source still belongs to this Boot and session.
    def current_daily(self, local: int) -> bool:
        return self.controller.session.gateway.controls.decisions.schedule.current_daily(self, local)

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

    # The decision owner also protects the exact Start after its permission has expired.
    async def hold_expired(self) -> None:
        await self.controller.session.gateway.controls.decisions.hold_expired_start(self)

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

    # Ask the owner whether an already-issued Start may report under the existing hold.
    def can_record_owner(self, tag: str) -> bool:
        return self.controller.session.gateway.controls.decisions.can_record_owner_start(self, tag)

    # Reconcile expired authority independently of the short lease and retain the original Start reporting debt.
    async def reconcile_expired(self) -> bool:
        if self.controller.stopping or not self.scoped_normal_stop():
            return False
        c, j, state = self.controller, self.journal, self.state()
        g, expected = c.session.gateway, c.requests.intent()
        try:
            active = j.active()
            owner = local_start_receipt(j)
            closing = (
                active if owner.get("local") == active and owner.get("digest") == state.get("tag_digest") else None
            )
            connection = current_control_scope(g)
            if expected.mode == "paused" or active is not None and not self.current_daily(active):
                try:
                    await self.hold_expired()
                except UnavailableError, TimeoutError, StationZeroStorageError:
                    if closing is None or j.db.in_transaction or not control_scope_matches(g, connection):
                        raise
                    # The known owner session can still receive a guarded Stop after fresh native proof.

            # An exact restrictive Stop can use a full sample that failed only the measured current bound.
            async def read_session(local: int | None) -> HandoverEvidence:
                try:
                    return await c.native.read(closing_local=local, stop_only=local is not None)
                except _NativeMeasurementError as error:
                    if (
                        local is None
                        or error.authority != c.native.authority()
                        or error.sequence != c.native.last_resolved_read
                    ):
                        raise
                    require_allowed(readiness_decision(error.native, False, c.native.allow_unplugged, local))
                    return error.native

            native = await read_session(closing)
            sequence = c.native.last_resolved_read

            # Apply the native closure to the original outstanding owner Start.
            def settle_at_zero() -> bool:
                if c.requests.intent() != expected:
                    raise RecoveryRequiredError("expired Start observation was superseded")
                return self.settle_local_zero(native)

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
            owner_closed = j.starts.ended_at_idle(native, unissued=unissued)
            if owner_closed:
                if expected.mode == "app" and (not g.controls.ready or g.controls.desired() == 0):
                    if not self.scoped_normal_stop():
                        # If the Start is settled, connection recovery owns any retry while cloud Stop is pending.
                        c.released_epoch = None
                    progress = await c.recovery.restore(expected)
                    if not progress.shared:
                        return True

                c.commands.decide(lambda: j.starts.finish_expired(state.get("id"), native, unissued=unissued))
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
            if closing is None or local != closing:
                await self.hold_expired()
                return True
            c.native.require_known_session(native)

            await g.controls.decisions.stop_expired_start(
                self,
                ExpiredStartStop(
                    closing, expected, state.get("id"), state.get("tag_digest"), connection, native, sequence
                ),
                read_session,
            )
        except (
            DeniedError,
            SecurityError,
            RecoveryRequiredError,
            UnavailableError,
            TimeoutError,
            StationZeroStorageError,
        ):
            if j.db.in_transaction:
                raise
            try:
                await self.hold_expired()
            except (
                DeniedError,
                SecurityError,
                RecoveryRequiredError,
                UnavailableError,
                TimeoutError,
                StationZeroStorageError,
            ):
                if j.db.in_transaction:
                    raise
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
        lease = self.lease
        if worker.cancelled() and lease is not None:
            try:
                retirement = self.controller.commands.decide(
                    lambda: retire_cancelled_start(self, lease), priority=CommandPriority.RESTRICT
                )
                if retirement is CancellationRetirement.PENDING:
                    return False
            except sqlite3.Error, RecoveryRequiredError:
                return False  # Required settlement is pending; no old deadline gains authority over another session.
        elif not worker.cancelled():
            worker.result()
        return True

    # Positive flow must belong to the known transaction and the actually accepted local or vendor Start path.
    def observed(self, native: HandoverEvidence) -> None:
        self.journal.starts.retire_refused()
        self.settle_local_zero(native)
        lease, c = self.lease, self.controller
        state = self.state()
        if self.journal.starts.observe_normal(native):
            self.changed.set()
            return
        if lease is None:
            self.journal.starts.finish_expired(state.get("id"), native)
            return
        if state.get("state") == "stopping":
            if (
                c.requests.intent().mode == "paused"
                and native.physical.power_w == 0
                and not native.physical.transaction_open
            ):
                c.require_paused(native)
                if self.journal.starts.finish_timed_hold(lease, time.time()):
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
            source = self.journal.starts.timed_flow_source(lease, active)
            if source is None:
                return
            if lease.first_flow is None:
                lease.first_flow = TestFlow(time.time(), time.monotonic() + DURATION)
            flow = lease.first_flow
            try:
                with self.journal.atomic():
                    self.journal.starts.confirm_test_flow(
                        lease,
                        state,
                        source.local,
                        flow.observed_at,
                        DURATION,
                        cloud_start=source.cloud_start,
                    )
            except sqlite3.Error as error:
                if self.journal.db.in_transaction:
                    raise
                raise RecoveryRequiredError("verified test flow awaits receipt publication") from error
            lease.deadline = min(lease.hard_deadline, flow.deadline + (15 if lease.source == "cloud" else 0))
            self.changed.set()

    # Let the owner check whether this timed Pause may Stop the current session.
    def timed_pause_leaves_session(self, local: int | None) -> bool:
        return self.controller.session.gateway.controls.decisions.timed_pause_leaves_session(self, local)

    # A replacement introduction retains the same limits on which session the previous request can Stop.
    def recovery_zero_only(self) -> bool:
        return self.scoped_normal_stop() or self.timed_pause_leaves_session(self.journal.active())

    # Schedule restrictive execution after the shared owner commits the exact expiry.
    def stop(self, lease: ChargeLease, reason: str, *, failure: dict[str, Any] | None = None) -> None:
        c = self.controller
        if c.session.gateway.controls.decisions.expire_start(self, lease, reason, failure):
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
            except sqlite3.Error:
                await self._hold_timed_cutoff(lease)
                await asyncio.sleep(0.1)
            except RecoveryRequiredError, DeniedError:
                # Failed no-dispatch settlement and restrictive claims retry without inventing another outcome.
                await asyncio.sleep(0.1)

    # Try the guarded zero at the deadline while the exact timed Stop record is still waiting to be saved.
    async def _hold_timed_cutoff(self, lease: ChargeLease) -> None:
        c = self.controller

        try:
            if await c.commands.run(
                lambda: c.session.gateway.controls.decisions.claim_timed_cutoff(self, lease, time.monotonic),
                priority=CommandPriority.RESTRICT,
            ):
                await c.ensure_pause_zero()
        except sqlite3.Error, RecoveryRequiredError, DeniedError:
            pass  # The deadline retries the exact hold and bookkeeping on its next turn.

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
