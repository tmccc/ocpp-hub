"""Explicit connected commissioning under an independently verified persistent native six-ampere limit."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from .connection_scope import ConnectionLineage, control_scope_matches, current_scope
from .control_state import ControlState
from .gateway import Gateway
from .ics import is_safe_transaction_query
from .local_control import local_start_receipt
from .managed_current import current_session_decision, managed_current_ceiling, record_current_source
from .managed_native_repair import NativeRepairAudits
from .managed_recovery import local_recovery_binding
from .managed_sources import recorded_station_profile
from .native_bounded import NativeBoundedSpec, NativeReadinessError, _require_ready, readiness_decision
from .native_faults import MeasuredValues, NativeContentError, _NativeMeasurementError
from .native_history import MissingOriginalHistoryError, audit_history, inspect_history
from .native_http import NativeTransportUnavailableError
from .policy import DeniedError
from .recovery_contracts import ALLOW, GateDecision, Pending, Prerequisite, require_allowed
from .rpc import IntroductionDeferredError, UnavailableError
from .security import SecurityError, StaleEvidenceError
from .station import StationChoice
from .store import RecoveryRequiredError, StationZeroStorageError, fingerprint
from .trial_allocation import initialize_allocation, verify_initial_history
from .trial_evidence import Resource, profile_projection
from .trial_handover_evidence import HandoverEvidence, ObserveHandover
from .trial_resources import ResourceBook
from .trial_rollback import lifecycle_decision, require_lifecycle_idle

NativePatch = Callable[[dict[str, Any]], Awaitable[None]]


class AuditSupersededError(UnavailableError):
    """The independently read snapshot straddled a change to the journal's control or lifecycle authority."""


class NativeSessionPendingError(RecoveryRequiredError):
    """Native and local lifecycle evidence must converge without adopting or stopping an unknown session."""


class NativeBoundedHandover:
    """This mode permits bounded charging; it never infers vehicle pause or manufactures transaction closure."""

    # The explicit immutable specification and ordinary ledger bind this mode independently of vehicle permission.
    def __init__(
        self,
        gateway: Gateway,
        book: ResourceBook,
        observe: ObserveHandover,
        spec: NativeBoundedSpec,
        choice: StationChoice,
        *,
        observe_profiles: Callable[[], Awaitable[tuple[Resource, ...]]],
        allow_unplugged: bool = False,
    ) -> None:
        if type(allow_unplugged) is not bool or allow_unplugged and gateway.policy.allow_local_start:
            raise SecurityError("unplugged native monitoring requires disabled local Start")
        self.gateway, self.book, self.observe, self.spec, self.choice = gateway, book, observe, spec, choice
        self.observe_profiles = observe_profiles
        self.command_source_observer: Callable[[HandoverEvidence], None] | None = None
        self.ordinary_source_observer: Callable[[HandoverEvidence], None] | None = None
        self.allow_unplugged = allow_unplugged
        self.repair_audits: NativeRepairAudits | None = None
        self.lock = asyncio.Lock()
        self.last_verified_at = 0.0
        self.read_sequence = 0
        self.last_resolved_read = 0
        self.observation_listener: Callable[[HandoverEvidence], None] | None = None
        self.continuation_guard: Callable[[HandoverEvidence, int], GateDecision] | None = None
        self.recovery_zero_only: Callable[[], bool] | None = None
        self.positive_guard: Callable[[HandoverEvidence], GateDecision] | None = None
        self.audited_epoch: int | None = None
        self._introductions: set[object] = set()
        self.introduction_service_pending: Callable[[], bool] | None = None
        self.introduced_epoch: int | None = None
        self.introduced_revision: int | None = None
        self.introduction_lineage: ConnectionLineage | None = None
        self.acknowledged_lineage: ConnectionLineage | None = None
        if fingerprint(spec.original_configuration) != book.baseline.configuration_digest:
            raise SecurityError("native bounded mode requires the exact original full configuration")
        saved = gateway.journal.get("native_bounded_handover")
        if saved and saved.get("spec") != spec.value():
            raise SecurityError("native bounded specification changed across process replacement")
        if not saved and gateway.journal.phase.read().value != "prepared":
            raise RecoveryRequiredError("an existing ordinary journal cannot acquire connected admission")

    # The last verified native snapshot expires on the same clock and within the same limit as its admission audit.
    def observation_age(self) -> float:
        return time.time() - self.last_verified_at

    # Report an observation's age honestly rather than treating a persisted desired limit as a live guarantee.
    def status(self) -> dict[str, Any]:
        state = self.gateway.journal.get("native_bounded_handover", {})
        return {
            "configured": True,
            "stage": state.get("stage", "not_admitted"),
            "target_native_limit_a": managed_current_ceiling(self.gateway.journal, self.spec),
            "native_limit_freshly_verified": 0 <= self.observation_age() <= 5,
            "vehicle_pause_asserted": False,
            "restoration_pending": state.get("stage") != "complete",
            "complete": state.get("stage") == "complete",
            "fault": state.get("fault"),
        }

    # Save each stage before its external effect; restart never turns an old observation into fresh authority.
    def save(self, stage: str, **values: Any) -> None:
        journal = self.gateway.journal
        state = journal.get("native_bounded_handover", {})
        journal.put(
            "native_bounded_handover",
            {
                **state,
                **values,
                "stage": stage,
                "spec": self.spec.value(),
                "generation": state.get("generation", 0) + 1,
            },
        )

    # Local changes invalidate a native read even when the charger returned a complete, individually valid body.
    def authority(self) -> str:
        gateway, journal = self.gateway, self.gateway.journal
        return fingerprint(
            {
                "epoch": gateway._charger_epoch,
                "boot_observation": gateway.boot_observation,
                "revision": gateway.controls._revision,
                "phase": journal.phase.read().value,
                "control": journal.get("control"),
                "active": journal.active(),
                "boot": journal.get("boot"),
                "binding": journal.get("trial_binding"),
                "grant": journal.get("vendor_start_grant"),
                "state": journal.get("native_bounded_handover"),
                "current_capability": journal.get("managed_current_capability"),
                "ordinary_recovery": journal.get("managed_ordinary_recovery"),
                "resources": [tuple(row) for row in journal.db.execute("SELECT * FROM trial_resources ORDER BY key")],
            }
        )

    # Read rich native evidence under current authority; eligible repair audits are fenced before observation.
    async def read(
        self,
        route: str = "trial",
        closed: bool = False,
        *,
        owned: bool = False,
        initial: bool = False,
        closing_local: int | None = None,
        selected_current: int | None = None,
        stop_only: bool = False,
    ) -> HandoverEvidence:
        result = await self._read(
            route,
            closed,
            owned=owned,
            initial=initial,
            closing_local=closing_local,
            selected_current=selected_current,
            stop_only=stop_only,
        )
        if isinstance(result, Pending):
            raise AssertionError("legacy native read returned a managed prerequisite")
        return result

    # Managed recovery receives native availability directly after the same full audit and durable publication.
    async def read_result(self, *, closed: bool = False) -> HandoverEvidence | Pending:
        return await self._read(closed=closed, as_result=True)

    # One audit implementation owns strict verdicts, supersession, resource reconciliation and optional availability.
    async def _read(
        self,
        route: str = "trial",
        closed: bool = False,
        *,
        owned: bool = False,
        initial: bool = False,
        as_result: bool = False,
        closing_local: int | None = None,
        selected_current: int | None = None,
        stop_only: bool = False,
    ) -> HandoverEvidence | Pending:
        self.read_sequence += 1
        sequence, before = self.read_sequence, self.authority()
        repair, gateway = None if stop_only else self.repair_audits, self.gateway
        attempt = (
            repair.begin(before, sequence, gateway._charger_epoch, gateway.boot_observation, gateway.controls._revision)
            if repair is not None
            else None
        )
        content_current = False
        strict_complete = False
        resource_error: NativeContentError | None = None

        # Only the newest resolved read may invalidate freshness; an older outcome cannot erase its successor.
        def invalidate() -> None:
            if sequence >= self.last_resolved_read:
                self.last_verified_at = 0

        # Resolve failed or complete reads without letting an older HTTP result publish newer authority.
        def current() -> bool:
            if sequence < self.last_resolved_read:
                return False
            self.last_resolved_read = sequence
            return before == self.authority()

        try:
            try:
                async with asyncio.timeout(15):
                    native = await self.observe()
            except TimeoutError as error:
                raise NativeTransportUnavailableError("complete native observation exceeded its deadline") from error
            if not current():
                raise AuditSupersededError("native bounded audit was superseded by local authority")
            content_current = True
            measured = self._inspect(native, route=route)
            if (
                selected_current is not None
                and gateway.journal.current_change.read().settled_or_missing
                and managed_current_ceiling(gateway.journal, self.spec) == 32
            ):
                measured = measured.selected(selected_current)
            try:
                inspect_history(
                    gateway.journal, self.spec.digest, list(native.history), gateway.policy.allow_local_start
                )
            except MissingOriginalHistoryError as error:
                # A repairable omission must not hide a simultaneous contradiction in the same native sample.
                if resource_error := self.book.native_conflict(native.physical, owned=True):
                    raise resource_error from error
                raise
            if initial:
                if native.physical.history is None:
                    raise SecurityError("actual Boot introduction requires complete current history")
                verify_initial_history(
                    gateway.journal, native.physical.history, gateway.journal.identity, self.spec.hardware_identity
                )
            if attempt is not None and (gateway.controls._operations or gateway.charger._pending):
                raise RecoveryRequiredError("native repair awaits settled resource operations")
            resource_error = self.book.native_conflict(native.physical, owned=owned or stop_only or attempt is not None)
            strict_complete = True
            self.book.check_originals(native.physical)
            if not stop_only and (owned or attempt is not None):
                self.book.owned_present(native.physical)
            if resource_error is not None:
                raise resource_error
            # Restrictive Stop only needs the independent comparisons above and its own command record.
            if not stop_only:
                audit_history(gateway.journal, self.spec.digest, list(native.history), gateway.policy.allow_local_start)
                if self.command_source_observer is not None:
                    self.command_source_observer(native)
                record_current_source(self.gateway.journal, native, self.gateway.boot_observation)
                if self.ordinary_source_observer is not None:
                    self.ordinary_source_observer(native)
            if measured.exceeded:
                raise _NativeMeasurementError(native, measured, self.authority(), sequence)
            _require_ready(native, False, self.allow_unplugged, closing_local)
        except NativeContentError as error:
            invalidate()
            # An adapter refusal may arrive after a newer Boot; validate its authority before freezing it as strict.
            if not content_current and not current():
                if repair is not None:
                    repair.finish(attempt, "superseded")
                raise AuditSupersededError("native content refusal was superseded by local authority") from error
            if repair is not None:
                repair.finish(attempt, "strict", error)
            raise
        except _NativeMeasurementError:
            invalidate()
            if repair is not None:
                repair.finish(attempt, "measurement")
            raise
        except (NativeTransportUnavailableError, StaleEvidenceError) as error:
            invalidate()
            applicable = current()
            if repair is not None:
                repair.finish(attempt, "availability" if applicable else "superseded")
            if not applicable:
                raise AuditSupersededError("native availability result was superseded") from error
            if as_result:
                return Pending(Prerequisite.NATIVE, "recovery awaits fresh complete native evidence", error)
            if self.gateway.journal.current_change.read().recovering:
                raise UnavailableError("current selection hold awaits fresh native evidence") from error
            ordinary = self.gateway.journal.get("managed_ordinary_recovery", {})
            intent = self.gateway.journal.get("managed_control_intent", {})
            if (
                ordinary.get("state") == "holding"
                and intent == {"mode": "app", "revision": ordinary.get("revision")}
                and not (gateway.controls.ready and gateway.controls.desired() > 0)
            ):
                raise UnavailableError("ordinary recovery awaits fresh native evidence") from error
            raise
        except (SecurityError, DeniedError, RecoveryRequiredError, UnavailableError, TimeoutError) as error:
            invalidate()
            applicable = current()
            if repair is not None:
                repair.finish(attempt, "readiness" if applicable else "superseded")
            if (
                applicable
                and as_result
                and isinstance(error, NativeReadinessError)
                and error.unplugged_at_zero
                and error.native.physical.native_transaction_id == gateway.journal.active()
            ):
                return Pending(Prerequisite.LIFECYCLE, str(error), error)
            raise
        except sqlite3.Error as error:
            invalidate()
            # Pure content comparisons precede publication; a proved resource refusal cannot become a storage wait.
            if repair is not None and resource_error is not None:
                repair.defer(attempt, "strict", resource_error)
                raise resource_error from error
            if repair is not None and strict_complete:
                repair.defer(attempt, "readiness", storage_error=error)
            # A read failure that prevented comparison leaves an honest unknown fence, never an invented verdict.
            raise
        except asyncio.CancelledError:
            invalidate()
            if repair is not None:
                repair.finish(attempt, "availability")
            raise
        if repair is not None:
            repair.finish(attempt, "validated")
        self.last_verified_at = native.physical.observed_at
        if not stop_only and self.observation_listener is not None:
            self.observation_listener(native)
        # A valid open source remains fresh while this particular operation awaits a closed station.
        if closed:
            if as_result:
                self._inspect(native, route=route).require()
                decision = readiness_decision(native, True, self.allow_unplugged, None)
                if isinstance(decision, Pending):
                    return decision
            else:
                self.require(native, route=route, closed=True)
        return native

    # Compose immutable checks before a private measured verdict; it is never usable positive authority.
    def _inspect(self, native: HandoverEvidence, *, route: str = "trial") -> MeasuredValues:
        j = self.gateway.journal
        if managed_current_ceiling(j, self.spec) == 32:
            if route != "trial":
                raise NativeContentError(
                    "configuration", "managed current capability must be retired before backend restoration"
                )
            return self.spec._inspect_managed(
                native,
                j.identity,
                allow_unplugged=self.allow_unplugged,
            )
        return self.spec._inspect(native, j.identity, route, allow_unplugged=self.allow_unplugged)

    # Every public native validator still enforces all bounds and its original closed-zero readiness.
    def require(self, native: HandoverEvidence, *, route: str = "trial", closed: bool = False) -> None:
        self._inspect(native, route=route).require()
        _require_ready(native, closed, self.allow_unplugged, None)

    # A private verdict retains selected-meter facts while the controller completes nonmeasurement checks.
    def _selected_measurements(self, native: HandoverEvidence, measured: MeasuredValues) -> MeasuredValues:
        j = self.gateway.journal
        if managed_current_ceiling(j, self.spec) == 32:
            selected = j.get("control", {}).get("resume_limit", self.gateway.controls.policy.max_current_a)
            if type(selected) not in (int, float) or not 6 <= selected <= 32:
                raise SecurityError("managed selected current is malformed")
            return measured.selected(selected)
        return measured

    # Selected-limit enforcement gates every existing positive caller without any opt-out flag.
    def require_selected(self, native: HandoverEvidence) -> None:
        measured = self._selected_measurements(native, self._inspect(native))
        if measured.exceeded:
            raise SecurityError("actual native power exceeds the selected managed current")

    # Renew a retained ceiling after Boot without rewriting it or creating a missing station profile.
    async def require_retained_station(self, limit: float) -> None:
        try:
            native = await self.read(owned=True)
        except _NativeMeasurementError as error:
            raise UnavailableError("retained station awaits a settled measurement") from error
        self.require_known_session(native)
        if self._selected_measurements(native, self._inspect(native)).exceeded:
            raise UnavailableError("retained station awaits a settled measurement")
        if not any(
            resource.identifier == HUB_PROFILE_ID and resource.canonical.get("limit") == limit
            for resource in native.physical.resources
        ):
            raise SecurityError("charger did not retain the commissioned station ceiling across Boot")

    # Only a copy of the already validated USER scalar is normalized; original admission bytes are never rewritten.
    def binding_digest(self, native: HandoverEvidence) -> str:
        if managed_current_ceiling(self.gateway.journal, self.spec) == 32:
            return fingerprint(native.configuration | {"AmpLmt": "6"})
        return native.physical.configuration_digest

    # Restrictive recovery still belongs to the already commissioned installation and its exact configuration.
    def commissioned_binding_matches(self, native: HandoverEvidence) -> bool:
        journal, spec = self.gateway.journal, self.spec
        handover = journal.get("native_bounded_handover", {})
        return (
            journal.phase.read().supervised
            and not journal.phase.starting
            and journal.get("managed_mode")
            == {"mode": "app_schedule", "spec_digest": spec.digest, "local_start": False}
            and handover.get("stage") == "admitted"
            and handover.get("spec") == spec.value()
            and not handover.get("fault")
            and fingerprint(journal.get("boot")) == spec.boot_digest
            and journal.get("trial_binding")
            == {
                "configuration_digest": self.binding_digest(native),
                "station": self.choice.value(),
                "boot_digest": spec.boot_digest,
            }
        )

    # Initial connected admission has its own guard; allocation is reserved before adding any mode metadata.
    async def admit(self) -> None:
        journal = self.gateway.journal
        if journal.phase.read().value != "prepared":
            raise RecoveryRequiredError("native bounded admission requires a fresh prepared journal")
        native = await self.observe()
        self.spec.require_admission(native, journal.identity, route="original")
        self.book.require_profile_free_baseline()
        if native.physical.history is None:
            raise SecurityError("native bounded admission requires complete actual history")
        initialize_allocation(journal, native.physical.history, journal.identity, self.spec.hardware_identity)
        with journal.atomic():
            self.book.activate_bounded(native, self.spec.hardware_identity)
            self.save("admitted", original_history=list(native.history))
            journal.control.admit()
            journal.phase.begin_introduction()

    # Live introduction bodies retain their pending restrictive effects through all awaited cleanup.
    @property
    def introduction_pending(self) -> bool:
        return bool(self._introductions) or (
            self.introduction_service_pending is not None and self.introduction_service_pending()
        )

    # Every invocation owns its own token, so finishing one cannot hide another live introducer.
    @contextmanager
    def _introduction_work(self) -> Iterator[None]:
        token = object()
        self._introductions.add(token)
        try:
            yield
        finally:
            self._introductions.discard(token)

    # Bind only an actual fresh Boot after routing under six amps; reconnects return to a zero hold.
    async def introduce(self, *, selected_current: int | None = None) -> HandoverEvidence:
        with self._introduction_work():
            gateway, journal = self.gateway, self.gateway.journal
            epoch = gateway._charger_epoch
            if not journal.phase.read().supervised:
                raise RecoveryRequiredError("native bounded introduction cannot rearm frozen cleanup")
            if not gateway.current_boot_seen(epoch):
                raise IntroductionDeferredError("native bounded introduction awaits a saved Boot")
            if fingerprint(journal.get("boot")) != self.spec.boot_digest:
                raise SecurityError("native bounded introduction requires the commissioned actual Boot")
            binding = journal.get("trial_binding")
            scope = current_scope(gateway)
            if binding is not None and scope is not None:
                self.introduction_lineage = scope.lineage
            first_zero = binding is None or bool(journal.phase.starting)
            try:
                native = await self.read(
                    closed=first_zero,
                    owned=True,
                    initial=binding is None,
                    selected_current=selected_current,
                    closing_local=None if first_zero else journal.active(),
                )
            except (
                NativeReadinessError,
                NativeTransportUnavailableError,
                StaleEvidenceError,
                UnavailableError,
                TimeoutError,
                sqlite3.Error,
            ) as error:
                # Retrying this initial observation cannot replay any Stop, ceiling change or lifecycle event.
                raise IntroductionDeferredError("introduction awaits fresh native evidence") from error
            try:
                gateway._require_epoch("charger", epoch)
                if binding is None:
                    if (
                        not journal.phase.read().active
                        or journal.db.execute("SELECT 1 FROM trial_resources LIMIT 1").fetchone()
                    ):
                        raise RecoveryRequiredError(
                            "initial native bounded introduction cannot adopt existing resources"
                        )
                    if native.physical.history is None:
                        raise SecurityError("actual Boot introduction requires complete current history")
                    verify_initial_history(
                        journal, native.physical.history, journal.identity, self.spec.hardware_identity
                    )
                    journal.put(
                        "trial_binding",
                        {
                            "configuration_digest": self.binding_digest(native),
                            "station": self.choice.value(),
                            "boot_digest": self.spec.boot_digest,
                        },
                    )
                elif binding != {
                    "configuration_digest": self.binding_digest(native),
                    "station": self.choice.value(),
                    "boot_digest": self.spec.boot_digest,
                }:
                    raise SecurityError("native bounded introduction binding changed")
                try:
                    if binding is None:
                        self.require_known_session(native)
                    else:
                        self.require_recovery_session(native)
                except NativeSessionPendingError as error:
                    raise IntroductionDeferredError("introduction awaits native lifecycle reconciliation") from error
                self.book.owned_present(native.physical)
                if binding is not None or journal.get("control", {}).get("uncertain"):
                    control = journal.get("control", {})
                    journal.control.hold(control)
                    if first_zero:
                        # Stay in initial admission until its first zero succeeds,
                        # including a genuinely unapplied refusal.
                        ticket = journal.get("control", {}).get("ticket")
                        intent = journal.get("managed_control_intent", {})

                        # Initial admission must prove zero against this exact Boot before it can finish.
                        def same_boot() -> None:
                            if current_scope(gateway) != scope:
                                raise UnavailableError("initial zero belongs to a superseded Boot")

                        try:
                            await gateway.controls.ensure_ceiling(guard=same_boot)
                            same_boot()
                        except (UnavailableError, TimeoutError) as error:
                            # Only this zero may have been sent; reread the binding and owned receipt before retrying.
                            raise IntroductionDeferredError(
                                "initial introduction awaits its restrictive zero"
                            ) from error
                        except DeniedError as error:
                            latest, control = journal.get("managed_control_intent", {}), journal.get("control", {})
                            if (
                                latest.get("mode") != "paused"
                                or latest.get("revision", 0) <= intent.get("revision", 0)
                                or control.get("ticket") == ticket
                                or not ControlState(control).explicit_local_hold
                                or gateway.controls.desired() != 0
                            ):
                                raise
                            raise IntroductionDeferredError(
                                "initial zero was superseded by a newer owner Pause"
                            ) from error
                        gateway._require_epoch("charger", epoch)
                    journal.phase.recover()
                self.audited_epoch = epoch
                verified_revision = gateway.controls._revision
                journal.phase.introduced()
            except sqlite3.Error as error:
                # This prefix owns only metadata and restrictive zero; later Stop-capable work is not replayed.
                raise IntroductionDeferredError("introduction awaits required journal storage") from error
            if (
                binding is not None
                and native.physical.transaction_open
                and not journal.current_change.read().recovering
            ):
                zero_only = (
                    journal.get("managed_mode")
                    and journal.get("managed_control_intent", {}).get("mode") == "app"
                    or self.recovery_zero_only is not None
                    and self.recovery_zero_only()
                )
                if zero_only:
                    try:
                        await gateway.controls.ensure_ceiling()
                    except UnavailableError, TimeoutError, StationZeroStorageError:
                        # Runtime can retry the exact-Boot zero proof without rerunning this audit or Stop.
                        pass
                elif journal.active() is not None:
                    await gateway.stop()
            gateway._require_epoch("charger", epoch)
            if verified_revision != gateway.controls._revision:
                raise UnavailableError("the verified charger introduction was superseded")
            self.introduced_epoch, self.introduced_revision = epoch, verified_revision
            return native

    # Finish a paused introduction only from a fresh complete closed-zero proof, never a failed RPC or marker alone.
    def confirm_paused_introduction(self, native: HandoverEvidence) -> None:
        g, j = self.gateway, self.gateway.journal
        epoch, revision = g._charger_epoch, g.controls._revision
        self.require(native, closed=True)
        self.require_known_session(native)
        present = self.book.owned_present(native.physical)
        binding = {
            "configuration_digest": self.binding_digest(native),
            "station": self.choice.value(),
            "boot_digest": self.spec.boot_digest,
        }
        if (
            self.audited_epoch != epoch
            or self.introduction_pending
            or j.phase.starting
            or not j.phase.read().operating
            or j.get("native_bounded_handover", {}).get("stage") != "admitted"
            or j.get("trial_binding") != binding
            or j.get("managed_control_intent", {}).get("mode") != "paused"
            or j.fault.latched()
            or not g.current_boot_seen(epoch)
            or fingerprint(j.get("boot")) != self.spec.boot_digest
            or g.charger.sender is None
            or not g.controls.ready
            or g.controls.desired() != 0
            or not ControlState(j.get("control", {})).local_hold
            or j.get("control", {}).get("start_pending")
            or j.get("uncertain_vendor_command")
            or not any(r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0 for r in present)
            or g.charger._pending
            or g.controls._operations
        ):
            raise RecoveryRequiredError("paused introduction awaits current independently confirmed closed zero")
        # An earlier HTTP audit must finish its own initial claim; only its later failed restrictive RPC is resumable.
        self.introduced_epoch, self.introduced_revision = epoch, revision

    # An unknown native transaction is external activity, never a proxy-created Start or inferred Stop.
    def require_known_session(self, native: HandoverEvidence) -> None:
        decision = self.session_decision(native)
        if isinstance(decision, Pending):
            raise NativeSessionPendingError(decision.detail)

    # A physical/ledger closure gap is ordinary pending evidence and never authority to adopt or stop a session.
    def session_decision(self, native: HandoverEvidence) -> GateDecision:
        known = self.gateway.journal.active()
        if native.physical.transaction_open:
            if known is None or native.physical.native_transaction_id != known:
                return Pending(
                    Prerequisite.LIFECYCLE, "bounded external native activity awaits lifecycle reconciliation"
                )
        elif native.physical.native_transaction_id is not None or known is not None:
            return Pending(Prerequisite.LIFECYCLE, "native and local transaction closure disagree")
        return ALLOW

    # A genuine Stop may have occurred while the process was absent; retain the ledger debt until its real replay.
    def require_recovery_session(self, native: HandoverEvidence) -> None:
        if native.physical.transaction_open:
            j = self.gateway.journal
            control = j.get("control", {})
            if (
                j.current_change.read().recovering
                and recorded_station_profile(j, self.spec.digest, self.spec.boot_digest)
                and control.get("inhibit")
            ):
                return  # Introduce only the restricted listener; no native ID is adopted or given Stop authority.
            if (
                j.active() is None
                and j.get("managed_control_intent", {}).get("mode") == "paused"
                and control.get("start_pending")
                and control.get("inhibit")
                and control.get("owner") == "local"
                and local_start_receipt(j).get("stage") in ("issued", "accepted")
                and recorded_station_profile(j, self.spec.digest, self.spec.boot_digest)
                and all(v == 0 for v in native.currents_a + native.powers_w)
                and any(
                    r.identifier == HUB_PROFILE_ID and r.canonical.get("limit") == 0
                    for r in self.book.owned_present(native.physical)
                )
            ):
                return  # The verified zero listener can receive the real lifecycle without adopting this native ID.
            self.require_known_session(native)
        elif native.physical.native_transaction_id is not None:
            raise RecoveryRequiredError("restricted recovery cannot adopt contradictory native session evidence")

    # Default recovery requires an empty queue; managed local recovery retains queued observations and the zero hold.
    async def resume(
        self,
        *,
        local_only: bool = False,
        continuing_local: int | None = None,
        preserve_expired_start: Callable[[HandoverEvidence, int | None], bool] | None = None,
    ) -> dict[str, Any]:
        result = await self.resume_result(
            local_only=local_only, continuing_local=continuing_local, preserve_expired_start=preserve_expired_start
        )
        if isinstance(result, Pending):
            require_allowed(result)
            raise AssertionError("pending resume did not stop the legacy caller")
        return result

    # Zero acknowledgement shares one transaction and returns ordinary prerequisites without location-dependent catches.
    async def resume_result(
        self,
        *,
        local_only: bool = False,
        continuing_local: int | None = None,
        preserve_expired_start: Callable[[HandoverEvidence, int | None], bool] | None = None,
    ) -> dict[str, Any] | Pending:
        if type(local_only) is not bool:
            raise SecurityError("local recovery permission must be explicit")
        async with self.lock:
            gateway, journal = self.gateway, self.gateway.journal
            local_binding = local_recovery_binding(gateway, self.spec.digest) if local_only else None
            epoch, revision = gateway._charger_epoch, gateway.controls._revision
            scope = current_scope(gateway)
            ticket = journal.get("control", {}).get("ticket")
            if (
                journal.phase.read().value != "recovering"
                or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
                or gateway.charger.sender is None
                or not gateway.current_boot_seen(epoch)
                or scope is None
                or self.introduced_epoch != epoch
                or self.introduced_revision != revision
            ):
                raise RecoveryRequiredError("bounded resume requires the current restricted recovery introduction")
            native = await self.read_result(closed=continuing_local is None)
            if isinstance(native, Pending):
                return native
            if continuing_local is not None:
                if self.continuation_guard is None:
                    decision: GateDecision = Pending(
                        Prerequisite.SOURCE, "continuation requires a managed source guard"
                    )
                else:
                    decision = self.continuation_guard(native, continuing_local)
                if isinstance(decision, Pending):
                    return decision
                if local_only or any(v != 0 for v in native.currents_a + native.powers_w):
                    raise RecoveryRequiredError("original session continuation requires online verified zero")
            gateway._require_epoch("charger", epoch)
            self.spec.require_boot(journal.get("boot"))
            if journal.get("trial_binding") != {
                "configuration_digest": self.binding_digest(native),
                "station": self.choice.value(),
                "boot_digest": self.spec.boot_digest,
            }:
                raise SecurityError("bounded recovery binding changed before acknowledgement")
            present = self.book.owned_present(native.physical)
            receipt = journal.db.execute(
                "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
            ).fetchone()
            station = [r for r in present if r.identifier == HUB_PROFILE_ID]
            control = journal.get("control", {})
            # Evaluate managed debt against this read, after any waits; an earlier source cannot permit recovery.
            decision = lifecycle_decision(
                gateway,
                continuing_local=continuing_local,
                preserve_expired_start=not local_only
                and preserve_expired_start is not None
                and preserve_expired_start(native, continuing_local),
            )
            if isinstance(decision, Pending):
                return decision
            if (
                journal.phase.read().value != "recovering"
                or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
                or not control_scope_matches(gateway, (scope, revision))
                or ticket != control.get("ticket")
                or not gateway.current_boot_seen(epoch)
                or not control.get("inhibit")
                or gateway.controls.desired() != 0
                or not gateway.controls.ready
                or not self.book.confirmed_station_zero(station, receipt)
                or not local_only
                and journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
                or gateway.controls._operations
                or gateway.charger._pending
                or gateway.vendor._pending
                or journal.get("uncertain_vendor_command")
            ):
                return Pending(
                    Prerequisite.ZERO_PROOF, "bounded recovery awaits confirmed zero and history reconciliation"
                )
            if local_only and local_recovery_binding(gateway, self.spec.digest) != local_binding:
                raise RecoveryRequiredError("local recovery intent changed during native verification")
            journal.require_allocation()
            with journal.atomic():
                journal.phase.resume(control)
            self.acknowledged_lineage = scope.lineage
            return {"recovery_acknowledged": True, "zero_hold_retained": True, "charging_resumed": False}

    # Read the station profile independently of an incoming report's still-unassigned transaction number.
    async def require_reported_start_ceiling(self) -> None:
        try:
            profiles = await self.observe_profiles()
        except TimeoutError as error:
            raise UnavailableError("the native station profile read timed out") from error
        self.book.require_confirmed_station(profiles, self.gateway.controls.desired())

    # This check runs inside the actual outgoing RPC gate, after waits and immediately before the send guard.
    async def validate_send(self, action: str, payload: dict[str, Any]) -> GateDecision | None:
        if action == "DataTransfer" and is_safe_transaction_query(payload):
            return None
        if action in ("GetConfiguration", "GetCompositeSchedule", "TriggerMessage", "RemoteStopTransaction"):
            return None
        zero = action == "SetChargingProfile" and profile_projection(payload)["limit"] == 0
        if zero:
            return None  # Existing ledger guards still forbid recreating a station profile after verified absence.
        selecting = self.gateway.journal.current_change.read().selecting
        # Managed release uses the common typed prerequisite path, irrespective of its source or call location.
        if self.book.before_positive_release is not None:
            observed = await self.read_result(
                closed=action == "ClearChargingProfile" or selecting and action == "SetChargingProfile"
            )
            if isinstance(observed, Pending):
                return observed
            native = observed
        else:
            native = await self.read(
                closed=action == "ClearChargingProfile" or selecting and action == "SetChargingProfile"
            )
        if action in (
            "RemoteStartTransaction",
            "SetChargingProfile",
            "DataTransfer",
        ) and self.book.record_missing_station(native.physical):
            raise DeniedError("the managed station ceiling is missing from the fresh native inventory")
        if action == "RemoteStartTransaction" and self.gateway.journal.get("managed_mode"):
            self.book.require_confirmed_station(native.physical.resources, self.gateway.controls.desired())
        self.require_selected(native)
        change = self.gateway.journal.current_change.read().value
        if action == "SetChargingProfile" and change.get("state") == "recovering":
            local = self.gateway.journal.active()
            if local is not None:
                decision = current_session_decision(self.gateway, native, local)
                if isinstance(decision, Pending):
                    return decision
            elif native.physical.transaction_open:
                return Pending(Prerequisite.LIFECYCLE, "current recovery awaits actual lifecycle reconciliation")
        if self.positive_guard is not None:
            decision = self.positive_guard(native)
            if isinstance(decision, Pending):
                return decision
        if action == "ClearChargingProfile":
            require_lifecycle_idle(self.gateway)
            present = self.book.owned_present(native.physical)
            identifier = payload.get("id")
            if set(payload) != {"id"}:
                raise SecurityError("native bounded clear requires an exact owned profile")
            if self.gateway.journal.phase.read().active:
                if identifier == HUB_PROFILE_ID or not any(
                    resource.kind == "profile" and resource.identifier == identifier for resource in present
                ):
                    raise SecurityError("native bounded clear requires an exact cloud-owned profile")
            elif not present or present[0].identifier != identifier:
                raise SecurityError("native bounded cleanup requires the exact next owned resource")
        else:
            self.require_known_session(native)
            if (
                not self.gateway.journal.phase.read().active
                or self.gateway.journal.phase.starting
                or self.gateway.journal.get("native_bounded_handover", {}).get("stage") != "admitted"
            ):
                raise RecoveryRequiredError("native bounded positive controls remain frozen")
        return None

    # During cleanup, wait only for the two independent reports of the session this cleanup started with.
    def _cleanup_closure_pending(self, native: HandoverEvidence, closing_local: int | None) -> bool:
        journal = self.gateway.journal
        if closing_local is None:
            return False
        active = journal.active()
        if not native.physical.transaction_open:
            return active == closing_local and native.physical.native_transaction_id is None
        return (
            active is None
            and native.physical.native_transaction_id == closing_local
            and journal.db.execute(
                "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (closing_local,)
            ).fetchone()
            is not None
        )

    # Stop actual known work, drain real lifecycle delivery, then clear exact owned profiles with station last.
    async def cleanup(self, quiesce: Callable[[], Awaitable[None]]) -> dict[str, Any]:
        if managed_current_ceiling(self.gateway.journal, self.spec) != 6:
            raise SecurityError("retire the managed current capability before legacy cleanup")
        async with self.lock:
            gateway, journal = self.gateway, self.gateway.journal
            if journal.phase.read().value == "backend_restore_pending":
                native = await self.read(closed=True)
                if self.book.owned_present(native.physical):
                    raise RecoveryRequiredError("native bounded cleanup awaits independently verified profile absence")
                return {"profiles_restored": True, "native_limit_a": 6, "complete": False}
            if journal.phase.read().value not in ("active", "recovering", "blocked", "rollback"):
                raise RecoveryRequiredError("native bounded cleanup is not available in this phase")
            if journal.phase.read().value != "rollback":
                self.save("closing")
                journal.phase.block()
                initial = await self.read()
                present = self.book.owned_present(initial.physical)
                self.require_known_session(initial)
                closing_local = journal.active()
                if present or initial.physical.transaction_open or journal.get("control", {}).get("start_pending"):
                    await gateway.stop()
                async with asyncio.timeout(30):
                    while True:
                        try:
                            native = await self.read()
                        except AuditSupersededError:
                            await asyncio.sleep(0.1)
                            continue  # Retry a snapshot crossed by Stop within the same recovery deadline.
                        if isinstance(self.session_decision(native), Pending):
                            if not self._cleanup_closure_pending(native, closing_local):
                                self.require_known_session(native)
                            await asyncio.sleep(0.1)
                            continue
                        if not native.physical.transaction_open:
                            try:
                                require_lifecycle_idle(gateway)
                                break
                            except RecoveryRequiredError:
                                pass
                        await asyncio.sleep(0.1)
            native = await self.read(closed=True)
            require_lifecycle_idle(gateway)
            self.book.owned_present(native.physical)
            journal.phase.begin_cleanup()
            self.save("clearing")
            gateway.controls.invalidate()
            await quiesce()
            epoch = gateway._charger_epoch
            async with gateway.charger.sequence(0, lambda: gateway._require_epoch("charger", epoch)) as call:
                while True:
                    native = await self.read(closed=True)
                    require_lifecycle_idle(gateway)
                    present = self.book.owned_present(native.physical)
                    if not present:
                        break
                    resource = present[0]
                    if resource.kind != "profile":
                        raise SecurityError("native bounded cleanup cannot automatically delete native plans")
                    self.save("clearing", clear_intent={"key": resource.key, "digest": resource.digest})
                    try:
                        answer = await call(
                            "ClearChargingProfile", commands.clear_charging_profile(resource.identifier)
                        )
                        if answer[0] != 3 or answer[2].get("status") not in ("Accepted", "Unknown"):
                            raise UnavailableError("native bounded cleanup was not acknowledged")
                    except UnavailableError:
                        # Only independently verified absence resolves a lost reply; never infer a command retry.
                        pass
                    verification = await self.read(closed=True)
                    if any(row.key == resource.key for row in self.book.owned_present(verification.physical)):
                        raise UnavailableError("native bounded cleanup has not established physical profile absence")
            self.save("awaiting_backend", clear_intent={})
            journal.phase.await_backend()
            return {"profiles_restored": True, "native_limit_a": 6, "complete": False}

    # A route intent is durable and narrowly scoped; a native adapter performs only this exact four-field patch.
    async def restore_route(self, patch: NativePatch, retire: Callable[[], Awaitable[None]]) -> None:
        journal = self.gateway.journal
        if journal.phase.read().value != "backend_restore_pending":
            raise RecoveryRequiredError("native bounded resources are not yet restored")
        state = journal.get("native_bounded_handover", {})
        current = await self.observe()
        if state.get("stage") == "route_intent" and current.configuration == self.spec.configuration("original"):
            await self.read(route="original", closed=True)
            self.save("awaiting_backend", route_restored=True)
            return
        native = await self.read(closed=True)
        require_lifecycle_idle(self.gateway)
        if self.book.owned_present(native.physical):
            raise SecurityError("native bounded resources remain installed")
        self.save("route_intent")
        await retire()
        disconnected = await self.read(closed=True)
        if disconnected.physical.upstream_connected:
            raise UnavailableError("retired proxy's native connection must be observed offline before routing")
        self.save("route_intent", backend_outage_at=disconnected.physical.observed_at)
        await patch({key: self.spec.original_configuration[key] for key in self.spec.trial_route})
        await self.read(route="original", closed=True)
        self.save("awaiting_backend", route_restored=True)

    # Reconnection requires an actual fresh offline view followed by online, after the original route was restored.
    async def observe_backend(self) -> dict[str, Any]:
        journal = self.gateway.journal
        state = journal.get("native_bounded_handover", {})
        if journal.phase.read().value != "backend_restore_pending" or not state.get("route_restored"):
            raise RecoveryRequiredError("native bounded original route has not been independently verified")
        if self.gateway.charger.sender is not None:
            raise SecurityError("proxy transport remains attached during original-backend verification")
        native = await self.read(route="original")
        if not native.physical.upstream_connected:
            self.save("awaiting_backend", backend_outage_at=native.physical.observed_at)
            return {"original_backend_connected": False, "native_limit_a": 6, "complete": False}
        if state.get("backend_outage_at") is None or native.physical.observed_at <= state["backend_outage_at"]:
            raise UnavailableError("original backend needs a fresh observed offline-to-online transition")
        self.save("backend_verified", backend_verified_at=native.physical.observed_at)
        return {"original_backend_connected": True, "native_limit_a": 6, "complete": False}

    # Restore the normal current limit only after independent native closure and original-backend verification.
    async def restore_settings(self, patch: NativePatch) -> dict[str, Any]:
        journal = self.gateway.journal
        state = journal.get("native_bounded_handover", {})
        if state.get("stage") not in ("backend_verified", "settings_intent"):
            raise RecoveryRequiredError("native bounded original backend has not been verified")
        if self.gateway.charger.sender is not None:
            raise SecurityError("proxy is still attached during current-limit restoration")
        native = await self.observe()
        # A lost write reply is reconciled from actual full native state before any possible resend.
        if native.configuration == self.spec.original_configuration and state.get("stage") != "settings_intent":
            raise RecoveryRequiredError("normal current settings changed without the recorded restoration intent")
        if native.configuration != self.spec.original_configuration:
            idle = await self.read(route="original", closed=True)
            self.require_restore_idle(idle)
            self.save("settings_intent")
            idle = await self.read(route="original", closed=True)
            self.require_restore_idle(idle)
            await patch({"AmpLmt": self.spec.original_configuration["AmpLmt"]})
            native = await self.observe()
        self.require_normal_restoration(native)
        require_lifecycle_idle(self.gateway)
        with journal.atomic():
            self.save("complete", clear_intent={}, fault=None)
            journal.phase.complete()
        self.last_verified_at = 0
        return {"original_configuration_restored": True, "native_backend_connected": True, "complete": True}

    # No unknown native profile or newly active session may release the independent limit during restoration.
    def require_restore_idle(self, native: HandoverEvidence) -> None:
        self.spec.require(native, self.gateway.journal.identity, "original", closed=True)
        require_lifecycle_idle(self.gateway)
        if not native.physical.upstream_connected:
            raise UnavailableError("original backend disconnected before native current restoration")
        originals = {resource.key for resource in self.book.baseline.resources}
        if any(resource.kind == "profile" or resource.key not in originals for resource in native.physical.resources):
            raise SecurityError("original backend has unreconciled resources before native current restoration")

    # Ordinary manufacturer charging may begin after the authorized restore; retain its real session and history.
    def require_normal_restoration(self, native: HandoverEvidence) -> None:
        journal = self.gateway.journal
        self.spec.require_normal(native, journal.identity)
        if not native.physical.upstream_connected:
            raise SecurityError("normal manufacturer settings and native connection remain unverified")
        self.book.check_originals(native.physical)
        original = journal.get("native_bounded_handover", {}).get("original_history", [])
        audit_history(journal, self.spec.digest, list(native.history), self.gateway.policy.allow_local_start)
        owned = {row[0] for row in journal.db.execute("SELECT key FROM trial_resources")}
        original_keys = {resource.key for resource in self.book.baseline.resources}
        original_ids = {row["transactionId"] for row in original}
        local_ids = {row[0] for row in journal.db.execute("SELECT local FROM transactions")}
        ordinary_ids = {native.physical.native_transaction_id} if native.physical.transaction_open else set()
        ordinary_ids.update(
            row["transactionId"]
            for row in native.history
            if row["transactionId"] not in original_ids | local_ids
            and type(row.get("startTime")) is int
            and type(row.get("stopTime")) is int
            and row["stopTime"] > row["startTime"] > 0
            and row.get("connectorId") == 1
        )
        for resource in native.physical.resources:
            if resource.key in owned:
                raise SecurityError("an owned trial profile reappeared after restoration")
            if resource.key in original_keys:
                continue
            if resource.kind != "profile" or (
                resource.canonical.get("purpose") != "TxProfile"
                or resource.canonical.get("transaction") not in ordinary_ids
                or resource.canonical.get("connector") != 1
                or resource.canonical.get("kind") != "Relative"
                or resource.canonical.get("stack") != 1
                or resource.canonical.get("unit") != "A"
                or resource.canonical.get("limit", 100) > 32
                or resource.canonical.get("phases") != 1
            ):
                raise SecurityError("normal restoration contains an unexplained native profile")
