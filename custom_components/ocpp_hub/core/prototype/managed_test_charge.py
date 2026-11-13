"""Scoped normal and timed charging share one-shot Start guards; restart never repeats Start."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .local_control import local_start_receipt
from .managed_schedule import KEY, OBSERVABLE, recorded_station_profile
from .managed_start_binding import known_plan_source
from .managed_start_debt import preserved_start_debt
from .policy import DeniedError
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .managed import ManagedController

DURATION = 120
STARTUP_BUDGET = 60
ACTIVE = frozenset({"armed", "starting", "awaiting_power", "charging", "running", "stopping"})


@dataclass
class TestLease:
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


class ManagedChargeTests:
    """The generic Start policy stays disabled; this one-shot test has no caller-selected current/tag/duration."""

    # Construction may retire an interrupted permission, but never sends a Start or renews a persisted deadline.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.lease: TestLease | None = None
        self.worker: asyncio.Task[None] | None = None
        self.timer: asyncio.Task[None] | None = None
        self.changed = asyncio.Event()
        self.source: tuple[HandoverEvidence, str | None, int] | None = None
        saved = self.state()
        if saved.get("state") in ACTIVE:
            with self.journal.atomic():
                intent = controller.requests.intent()
                preserve = (
                    saved.get("kind") == "normal"
                    and intent.mode == "app"
                    and self.journal.get("control", {}).get("start_pending") is True
                    and local_start_receipt(self.journal).get("local") is None
                    and local_start_receipt(self.journal).get("stage") in ("issued", "accepted")
                )
                if not preserve:
                    self.journal.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                controller.session.gateway.controls._claim(True, stopping=True)
                self.journal.put(
                    "managed_test_charge",
                    {
                        **saved,
                        "state": "expired" if preserve else "interrupted",
                        "expired_normal": bool(preserve),
                        "charging": False,
                    },
                )
        self.retire_refused_unsent()
        controller.session.gateway.start_observer = self.started
        controller.native.observation_listener = self.record_source
        controller.native.positive_guard = self.check_positive
        self.prepare_report_hold = controller.session.gateway.prepare_report_hold
        self.held_start_report = controller.session.gateway.held_start_report
        controller.session.gateway.prepare_report_hold = self.prepare_owner_report
        controller.session.gateway.held_start_report = self.can_record_owner

    # Keep one journal owner and the same durable management receipt table as Pause/Return.
    @property
    def journal(self) -> Journal:
        return self.controller.session.journal

    # Private provenance includes identifiers; the public summary below exposes only source, state and timing.
    def state(self) -> dict[str, Any]:
        value: dict[str, Any] = self.journal.get("managed_test_charge", {})
        return value

    # A durable failed receipt proves this request had no effects; it cannot own a later ordinary session.
    def retire_refused_unsent(self) -> None:
        state, j = self.state(), self.journal
        if state.get("state") != "failed" or self.lease is not None:
            return
        row = j.db.execute("SELECT action,state,result FROM managed_requests WHERE id=?", (state.get("id"),)).fetchone()
        result = json.loads(row["result"]) if row is not None else None
        receipt = local_start_receipt(j)
        if (
            row is None
            or row["action"] not in ("start", "start_test")
            or row["state"] != "failed"
            or not isinstance(result, dict)
            or result.get("classification") != "preflight_refused_unsent"
            or state.get("start_rpc_accepted") is not False
            or j.get("control", {}).get("start_pending")
            or receipt.get("local") is None
            and receipt.get("stage") in ("prepared", "issued", "accepted")
            or j.get("uncertain_vendor_command")
        ):
            return
        j.put(
            "managed_test_charge",
            {**state, "state": "completed", "charging": False, "recovery_reason": "preflight_refused_unsent"},
        )

    # Only explicit request-specific provenance can retire a failure before entering the Start command path.
    def unissued_at_zero(self, native: HandoverEvidence) -> bool:
        state, j = self.state(), self.journal
        receipt = local_start_receipt(j)
        unissued = (
            state.get("start_attempted") is False
            or receipt.get("request_id") == state.get("id")
            and receipt.get("digest") == state.get("tag_digest")
            and receipt.get("local") is None
            and receipt.get("stage") in ("unsent", "rejected")
        )
        row = j.db.execute(
            "SELECT action,revision,state FROM managed_requests WHERE id=?", (state.get("id"),)
        ).fetchone()
        if (
            self.lease is not None
            or state.get("kind") not in ("normal", "test")
            or state.get("source") != "local"
            or not unissued
            or state.get("start_rpc_accepted") is not False
            or state.get("local_transaction") is not None
            or row is None
            or row["action"] != ("start" if state.get("kind") == "normal" else "start_test")
            or row["revision"] != state.get("revision")
            or row["state"] not in ("failed", "uncertain")
        ):
            return False
        try:
            self.controller.require_paused(native)
        except SecurityError, DeniedError, RecoveryRequiredError:
            return False
        return True

    # Exact not-accepted provenance and fresh physical zero settle only this request and its metadata together.
    def retire_unissued_at_zero(self, native: HandoverEvidence) -> None:
        if not self.unissued_at_zero(native):
            return
        state, j = self.state(), self.journal
        with j.atomic():
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? WHERE id=? AND revision=? "
                "AND action=? AND state='uncertain'",
                (
                    json.dumps({"classification": "start_not_accepted_at_verified_zero"}),
                    state["id"],
                    state["revision"],
                    "start" if state["kind"] == "normal" else "start_test",
                ),
            )
            j.put(
                "managed_test_charge",
                {
                    **state,
                    "state": "completed",
                    "charging": False,
                    "recovery_reason": "start_not_accepted_at_verified_zero",
                },
            )

    # Exact local closure can finish its request at verified Pause zero while separate cloud delivery stays retained.
    def retire_closed_at_zero(self, native: HandoverEvidence) -> None:
        state, j = self.state(), self.journal
        local, receipt = state.get("local_transaction"), local_start_receipt(j)
        # A real Accepted reply can commit before publication; a later exact lifecycle binds its durable receipt.
        if local is None and receipt.get("request_id") == state.get("id"):
            local = receipt.get("local")
        if (
            self.lease is not None
            and (not self.lease.normal or self.lease.source != "local" or self.lease.identifier != state.get("id"))
            or state.get("kind") != "normal"
            or state.get("source") != "local"
            or type(local) is not int
            or not (state.get("start_rpc_accepted") is True or receipt.get("request_id") == state.get("id"))
            or receipt.get("local") != local
            or receipt.get("stage") not in ("issued", "accepted")
            or receipt.get("stage") == "issued"
            and (
                state.get("start_rpc_accepted") is not False
                or receipt.get("request_id") != state.get("id")
                or state.get("start_attempted") is not True
            )
            or receipt.get("digest") != state.get("tag_digest")
            or j.get("control", {}).get("start_pending")
        ):
            return
        row = j.db.execute("SELECT start,stop,vendor FROM transactions WHERE local=?", (local,)).fetchone()
        if row is None or row["stop"] is None:
            return
        if fingerprint(json.loads(row["start"])["idTag"].casefold()) != state.get("tag_digest"):
            return
        request = j.db.execute(
            "SELECT action,revision,state FROM managed_requests WHERE id=?", (state.get("id"),)
        ).fetchone()
        if (
            request is None
            or request["action"] != "start"
            or request["revision"] + int(state.get("start_rpc_accepted") is True) != state.get("revision")
            or request["state"] not in ("accepted", "running", "uncertain", "completed", "failed")
            or receipt.get("request_id") not in (None, state.get("id"))
        ):
            return
        try:
            self.controller.require_paused(native)
        except SecurityError, DeniedError, RecoveryRequiredError:
            return
        with j.atomic():
            j.db.execute(
                "UPDATE managed_requests SET state='completed',result=? WHERE id=? AND action='start' AND revision=? "
                "AND state IN ('accepted','running','uncertain')",
                (
                    json.dumps({"local_session_verified": True, "charging": False, "timed": False}, sort_keys=True),
                    state["id"],
                    request["revision"],
                ),
            )
            j.put(
                "managed_test_charge",
                {
                    **state,
                    "state": "completed",
                    "charging": False,
                    "local_transaction": local,
                    "recovery_reason": "owned_session_closed_at_verified_zero",
                },
            )
        # Local command completion is separate from unresolved cloud delivery; its fault and FIFO remain intact.
        self.lease = None
        self.changed.set()

    # Fault supervision still settles exact local closure; cloud delivery retains its independent recovery gate.
    async def retire_closed_after_fault(self) -> None:
        state = self.state()
        if (
            self.controller.stopping
            or self.controller.requests.intent().mode != "paused"
            or state.get("kind") != "normal"
            or state.get("source") != "local"
            or state.get("state") == "completed"
            or local_start_receipt(self.journal).get("local") is None
        ):
            return
        try:
            native = await self.controller.native.read()
            if not self.controller.stopping:
                self.retire_closed_at_zero(native)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            return

    # Admission is conservative; the worker independently rechecks physical zero before Start.
    def available(self, native: HandoverEvidence | None = None, *, local: bool = True, normal: bool = False) -> bool:
        c, j = self.controller, self.journal
        g = c.session.gateway
        return bool(
            not c.stopping
            and (c.requests.intent().mode == "app" or normal and c.requests.intent().mode == "paused")
            and not (c.requests.intent().mode == "paused" and j.get(KEY, {}).get("state") in OBSERVABLE)
            and not j.get("managed_control_fault")
            and j.get("trial_phase") == "active"
            and g.controls.ready
            and g.controls.desired() == (0 if c.requests.intent().mode == "paused" else c.current.selected())
            and (normal or c.current.selected() == 6)
            and not c.current.pending()
            and not g.policy.allow_local_start
            and (g.remote_start_guard is None or g.remote_start_guard())
            and (not local or any(g._local_start_allowed(tag) for tag in g.policy.allowed_tags))
            and not j.get("control", {}).get("uncertain")
            and not j.get("control", {}).get("start_pending")
            and j.active() is None
            and not j.get("vendor_start_grant")
            and not j.db.execute(
                "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') LIMIT 1"
                if normal
                else "SELECT 1 FROM outbox LIMIT 1"
            ).fetchone()
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            and not c.requests.return_in_progress()
            and c.requests.has_capacity()
            and self.state().get("state") not in ACTIVE
            and g.current_boot_seen(g._charger_epoch)
            and (normal or c.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted")
            and (
                native is None
                or native.physical.connection == "Plug"
                and not native.physical.transaction_open
                and native.physical.power_w == 0
            )
        )

    # Acceptance and its one-shot deadline commit before any positive effect; duplicate identifiers return receipts.
    def accept(self, identifier: str, action: str, revision: int) -> dict[str, Any]:
        c, j = self.controller, self.journal
        previous = c.requests.existing(identifier, action, revision)
        if previous is not None:
            return previous
        native = c.latest_native
        if (
            revision != c.requests.intent().revision
            or native is None
            or not 0 <= time.time() - native.physical.observed_at <= 15
            or not self.available(native, local=action in ("start", "start_test"), normal=action == "start")
        ):
            raise RecoveryRequiredError("test requires current shared six-ampere authority and no pending work")
        if not c.requests.has_capacity():
            raise RecoveryRequiredError("management receipt capacity requires maintenance")
        g = c.session.gateway
        source = "local" if action in ("start", "start_test") else "cloud"
        lease = TestLease(
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
        )
        with j.atomic():
            j.put(
                "managed_test_charge",
                {
                    "id": identifier,
                    "source": source,
                    "kind": "normal" if lease.normal else "test",
                    "revision": revision,
                    "state": "starting" if source == "local" else "armed",
                    "accepted_at": time.time(),
                    "hard_deadline_epoch": time.time() + STARTUP_BUDGET + (0 if lease.normal else DURATION),
                    "duration_seconds": None if lease.normal else DURATION,
                    "start_rpc_accepted": False,
                    "start_attempted": False,
                    "current_a": lease.current_a,
                },
            )
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'accepted',?)",
                (identifier, action, revision, json.dumps({"accepted_at": time.time()})),
            )
        self.lease = lease
        self.timer = asyncio.create_task(self._deadline(lease))
        if source == "local":
            self.worker = asyncio.create_task(self._start(lease))
        else:
            c.requests.finish(identifier, "completed", {"armed": True, "charging_started": False})
        return c.requests.receipt(identifier)

    # Pause, deadline, shutdown, introduction, permission changes and a replaced request all revoke positive sends.
    def guard(self, lease: TestLease, *, stopping: bool = False) -> None:
        c, j = self.controller, self.journal
        g, intent = c.session.gateway, c.requests.intent()
        if (
            self.lease is not lease
            or c.stopping
            or time.monotonic() >= lease.deadline
            or intent.mode != lease.mode
            or intent.revision != lease.revision
            or g._charger_epoch != lease.epoch
            or g.controls._revision != lease.control_revision
            or not g.current_boot_seen(lease.epoch)
            or fingerprint(j.identity) != lease.identity
            or c.native.spec.digest != lease.spec
            or g.policy.allow_local_start
            or not 6 <= g.policy.max_current_a <= 32
            or c.current.selected() != lease.current_a
            or c.current.pending()
            or j.get("managed_control_fault")
            or j.get("trial_phase") != "active"
            or self.state().get("id") != lease.identifier
            or self.state().get("state") not in ACTIVE
            or self.state().get("state") == "stopping"
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

    # The service owns the worker after client disappearance; failures revoke only this Start's authority.
    async def _start(self, lease: TestLease) -> None:
        c, j = self.controller, self.journal
        effects = False
        try:
            self.guard(lease)
            initial = await c.settled_native()
            c.native.require(initial, closed=True)
            if initial.physical.connection != "Plug":
                raise DeniedError("test requires a freshly connected idle vehicle")
            self.guard(lease)
            effects = True
            await c.session.gateway.stop()
            self.guard(lease, stopping=True)
            lease.stop_epoch = j.get("control", {}).get("stop_epoch", 0)
            async with asyncio.timeout(15):
                while True:
                    self.guard(lease)
                    native = await c.native.read(closed=True)
                    try:
                        c.require_zero(native, allow_offline=lease.normal)
                        break
                    except RecoveryRequiredError:
                        await asyncio.sleep(0.1)
            if native.physical.connection != "Plug":
                raise DeniedError("test requires a connected vehicle")
            self.guard(lease)
            tags = sorted(
                tag for tag in c.session.gateway.policy.allowed_tags if c.session.gateway._local_start_allowed(tag)
            )
            if not tags:
                raise DeniedError("no current approved local tag is available for this test")
            tag = tags[0]

            # Commit the attempt marker with LocalControl's receipt, never before its owned task first executes.
            def prepared() -> None:
                self.guard(lease)
                j.put(
                    "managed_test_charge",
                    {**self.state(), "tag_digest": fingerprint(tag.casefold()), "start_attempted": True},
                )

            await c.session.gateway.start_managed(
                tag, lambda: self.guard(lease), request_id=lease.identifier, on_prepared=prepared
            )
            self.guard(lease)
            with j.atomic():
                self.guard(lease)
                if lease.normal:
                    j.put("managed_control_intent", {"mode": "app", "revision": lease.revision + 1})
                    c.requests.supersede_pauses(lease.identifier)
                j.put(
                    "managed_test_charge",
                    {
                        **self.state(),
                        "state": "awaiting_power",
                        "start_rpc_accepted": True,
                        "revision": lease.revision + int(lease.normal),
                    },
                )
            if lease.normal:
                lease.revision += 1
                lease.mode = "app"
        except (DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError) as error:
            j.put(
                "managed_test_failure",
                {
                    "type": type(error).__name__,
                    "detail": str(error)[:1024],
                    "at_epoch": time.time(),
                    "effects_started": effects,
                },
            )
            if not effects:
                c.requests.finish(lease.identifier, "failed", {"classification": "preflight_refused_unsent"})
                if self.state().get("id") == lease.identifier:
                    j.put("managed_test_charge", {**self.state(), "state": "failed"})
                self.lease = None
                self.retire_refused_unsent()
                self.changed.set()
                return
            c.requests.finish(lease.identifier, "uncertain", {"classification": "start_not_confirmed"})
            self.stop(lease, "start_not_confirmed")

    # Bind an actual accepted Start inside its ledger transaction, including sessions ending between native polls.
    def started(self, local: int, payload: dict[str, Any]) -> None:
        lease, state = self.lease, self.state()
        if lease is None or not lease.normal or state.get("kind") != "normal":
            return
        if state.get("local_transaction") is not None or local_start_receipt(self.journal).get("local") != local:
            return
        tag = payload.get("idTag")
        if not isinstance(tag, str) or state.get("tag_digest") != fingerprint(tag.casefold()):
            return
        try:
            self.guard(lease)
        except DeniedError:
            return
        self.journal.put("managed_test_charge", {**state, "local_transaction": local, "actual_start_at": time.time()})

    # A real bound Start/Stop pair completes without changing current authority or stopping a later app session.
    def finish_closed_normal(self, lease: TestLease) -> bool:
        state = self.state()
        if self.lease is not lease or not lease.normal or not state.get("start_rpc_accepted"):
            return False
        local = state.get("local_transaction")
        row = self.journal.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()
        if row is None or row["stop"] is None:
            return False
        self.journal.put(
            "managed_test_charge", {**state, "state": "completed", "charging": False, "stopped_at": time.time()}
        )
        self.controller.requests.finish(
            lease.identifier, "completed", {"local_session_verified": True, "charging": False, "timed": False}
        )
        self.lease = None
        self.changed.set()
        return True

    # Keep verified source evidence on its actual Boot; an ACK never identifies an unrelated native transaction.
    def record_source(self, native: HandoverEvidence) -> None:
        g = self.controller.session.gateway
        self.source = (native, g.boot_observation, g.controls._revision)
        local, state = self.journal.active(), self.state()
        if local is not None and self.independent_daily(native, local) and not state.get("independent_seen"):
            self.journal.put("managed_test_charge", {**state, "independent_seen": True})

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
        timer = j.get(KEY, {})
        return bool(
            any(
                plan["planId"] == native.session_plan_id
                and isinstance(plan.get("idTag"), str)
                and plan["idTag"].casefold() == tag
                and plan.get("kindType") == "Recurring"
                and plan["planId"] != timer.get("body", {}).get("planId")
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
        c, state = self.controller, self.state()
        return bool(
            state.get("expired_normal")
            and state.get("state") != "completed"
            and self.journal.get("managed_owner_pause", {}).get("revision") != c.requests.intent().revision
        )

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

    # Positive writes under expired Start authority need a verified daily or the genuine owner's closed lifecycle.
    def check_positive(self, native: HandoverEvidence) -> None:
        if not self.scoped_normal_stop():
            return
        j, c = self.journal, self.controller
        if self.idle_debt(native):
            return
        owner = local_start_receipt(j)
        local = j.active()
        if c.requests.intent().mode == "app" and local is not None:
            if self.independent_daily(native, local):
                return
            raise ReleaseDeferredError("expired Start cannot release an unidentified or owner transaction")
        if j.get("control", {}).get("start_pending") or native.physical.transaction_open:
            raise ReleaseDeferredError("expired Start awaits its real owner lifecycle")
        if (
            owner.get("local") is not None
            and not j.db.execute(
                "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (owner["local"],)
            ).fetchone()
        ):
            raise ReleaseDeferredError("expired Start awaits actual owner closure")

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

    # Admit the genuine delayed owner report only after zero, even while unrelated daily authority remains enabled.
    async def prepare_owner_report(self, payload: dict[str, Any]) -> None:
        tag = payload.get("idTag")
        if (
            self.scoped_normal_stop()
            and self.controller.requests.intent().mode == "app"
            and isinstance(tag, str)
            and self.controller.schedule.matches_pending_start(tag)
            and self.journal.active() is None
        ):
            await self.hold_expired()
        if self.prepare_report_hold is not None:
            await self.prepare_report_hold(payload)

    # The held listener consumes only the pending owner receipt and cannot authorize a new Start or guess an ID.
    def can_record_owner(self, tag: str) -> bool:
        c, g = self.controller, self.controller.session.gateway
        return bool(
            self.scoped_normal_stop()
            and c.requests.intent().mode == "app"
            and c.schedule.matches_pending_start(tag)
            and self.journal.active() is None
            and not c.stopping
            and not self.journal.get("managed_control_fault")
            and g.controls.ready
            and g.controls.desired() == 0
            and c.schedule.hold_authorized()
            or self.held_start_report is not None
            and self.held_start_report(tag)
        )

    # Reconcile expired authority independently of the short lease and retain the original Start reporting debt.
    async def reconcile_expired(self) -> bool:
        if not self.scoped_normal_stop():
            return False
        c, j, state = self.controller, self.journal, self.state()
        g, expected = c.session.gateway, c.requests.intent()
        try:
            active = j.active()
            if expected.mode == "paused" or active is not None and not self.current_daily(active):
                await self.hold_expired()
            native = await c.native.read()
            self.retire_unissued_at_zero(native)
            self.retire_closed_at_zero(native)
            local, owner = j.active(), local_start_receipt(j)
            if expected.mode == "app" and local is not None and self.independent_daily(native, local):
                if g.controls.ready and g.controls.desired() == state["current_a"]:
                    return False
                await self.hold_expired()
                # Reuse the ordinary guarded release; this raises the ceiling and never sends RemoteStart.
                await c.restore_owner_control(expected, continuing_local=local)
                return False
            owner_closed = (
                not j.get("control", {}).get("start_pending")
                and local is None
                and not native.physical.transaction_open
                and (
                    self.unissued_at_zero(native)
                    or owner.get("stage") in ("unsent", "rejected")
                    or owner.get("local") is not None
                    and j.db.execute(
                        "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (owner["local"],)
                    ).fetchone()
                )
            )
            if owner_closed:
                if expected.mode == "app" and (not g.controls.ready or g.controls.desired() == 0):
                    await c.restore_owner_control(expected)
                j.put("managed_test_charge", {**self.state(), "state": "completed", "charging": False})
                return False
            if expected.mode == "app" and local is None and not native.physical.transaction_open:
                if g.controls.ready and g.controls.desired() == state["current_a"]:
                    return False
                if self.idle_debt(native):
                    await c.restore_owner_control(expected)
                    return False
            await self.hold_expired()
            c.native.require_known_session(native)
            if local is not None and owner.get("local") == local and owner.get("digest") == state.get("tag_digest"):
                epoch, boot = g._charger_epoch, g.boot_observation

                # Recheck receipt, lifecycle and intent after every awaited native or profile verification.
                def guard() -> None:
                    if (
                        c.requests.intent() != expected
                        or not self.scoped_normal_stop()
                        or g._charger_epoch != epoch
                        or g.boot_observation != boot
                        or local_start_receipt(j).get("local") != local
                        or j.active() not in (None, local)
                    ):
                        raise RecoveryRequiredError("expired owner Stop was superseded")

                await g.controls.stop(expected_local=local, guard=guard)
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            try:
                await self.hold_expired()
            except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
                pass  # Keep the debt and retry zero; no unknown transaction receives a guessed Stop.
        return True

    # Polling waits for the short native/start sequence; its independent Stop timer continues to run meanwhile.
    async def settle(self) -> None:
        if self.worker is not None and not self.worker.done():
            await asyncio.shield(self.worker)

    # Positive flow must belong to the known transaction and the actually accepted local or vendor Start path.
    def observed(self, native: HandoverEvidence) -> None:
        self.retire_refused_unsent()
        self.retire_unissued_at_zero(native)
        self.retire_closed_at_zero(native)
        lease, c = self.lease, self.controller
        state = self.state()
        if lease is not None and self.finish_closed_normal(lease):
            return
        if state.get("kind") == "normal" and state.get("state") == "running":
            local = state.get("local_transaction")
            row = self.journal.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()
            if row is not None and row["stop"] is not None:
                self.journal.put(
                    "managed_test_charge", {**state, "state": "completed", "charging": False, "stopped_at": time.time()}
                )
            elif native.physical.native_transaction_id == local and self.journal.active() == local:
                self.journal.put("managed_test_charge", {**state, "charging": native.physical.power_w > 10})
            return
        if lease is None:
            owner = local_start_receipt(self.journal)
            if (
                state.get("state") != "completed"
                and not native.physical.transaction_open
                and self.journal.active() is None
                and not self.journal.get("control", {}).get("start_pending")
                and (
                    self.unissued_at_zero(native)
                    or state.get("expired_normal")
                    and (
                        owner.get("stage") in ("unsent", "rejected")
                        or self.journal.db.execute(
                            "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL", (owner.get("local"),)
                        ).fetchone()
                    )
                )
            ):
                # An explicit Pause can finish this expired debt too, without reviving it on the next Return.
                self.journal.put("managed_test_charge", {**state, "state": "completed", "charging": False})
            return
        if state.get("state") == "stopping":
            if (
                c.requests.intent().mode == "paused"
                and native.physical.power_w == 0
                and not native.physical.transaction_open
            ):
                c.require_paused(native)
                self.journal.put("managed_test_charge", {**state, "state": "completed", "stopped_at": time.time()})
                self.lease = None
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
            local = state.get("local_transaction")
            row = self.journal.db.execute("SELECT start,stop FROM transactions WHERE local=?", (local,)).fetchone()
            closed = row is not None and row["stop"] is not None
            opened = (
                local is not None
                and native.physical.transaction_open
                and native.physical.native_transaction_id == local
            )
            if opened or closed:
                self.journal.put(
                    "managed_test_charge",
                    {
                        **state,
                        "state": "completed" if closed else "running",
                        "local_transaction": local,
                        "session_started_at": time.time(),
                        "charging": opened and native.physical.power_w > 10,
                        **(
                            {
                                "native_source": {
                                    "marker": native.session_plan_id,
                                    "start": fingerprint(json.loads(row["start"])),
                                    "schedule": native.saved_schedule_digest,
                                }
                            }
                            if opened and not known_plan_source(self.journal, native)
                            else {}
                        ),
                    },
                )
                c.requests.finish(
                    lease.identifier,
                    "completed",
                    {
                        "local_session_verified": True,
                        "charging": opened and native.physical.power_w > 10,
                        "timed": False,
                    },
                )
                self.lease = None
                self.changed.set()
            return
        if state.get("state") != "charging" and native.physical.power_w > 10:
            if self.journal.active() is None or native.physical.native_transaction_id != self.journal.active():
                self.stop(lease, "unknown_session")
                return
            grant = self.journal.get("vendor_start_grant", {})
            cloud = (
                grant.get("local") == self.journal.active()
                and self.journal.db.execute(
                    "SELECT 1 FROM mutations WHERE rowid>? AND token=? AND origin='vendor' "
                    "AND action='RemoteStartTransaction' AND state='confirmed' LIMIT 1",
                    (lease.first_mutation, grant.get("mutation")),
                ).fetchone()
            )
            local = local_start_receipt(self.journal)
            if (
                lease.source == "local"
                and (cloud or not state.get("start_rpc_accepted") or local.get("local") != self.journal.active())
            ) or (lease.source == "cloud" and not cloud):
                return
            lease.deadline = min(
                lease.hard_deadline, time.monotonic() + DURATION + (15 if lease.source == "cloud" else 0)
            )
            self.journal.put(
                "managed_test_charge",
                {
                    **state,
                    "state": "charging",
                    "flow_started_at": time.time(),
                    "stop_deadline_epoch": time.time() + DURATION,
                    "local_transaction": self.journal.active(),
                    "start_source_verified": True,
                },
            )
            c.requests.finish(
                lease.identifier, "completed", {"positive_power_verified": True, "duration_seconds": DURATION}
            )
            self.changed.set()

    # Normal expiry retains separate reporting debt; only an explicit Pause can Stop an independent daily.
    def stop(self, lease: TestLease, reason: str) -> None:
        if self.lease is not lease or self.state().get("state") == "stopping":
            return
        c, j = self.controller, self.journal
        if c.stopping:
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
                j.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                c.session.gateway.controls._claim(True, stopping=True)
                j.db.execute(
                    "UPDATE managed_requests SET state='failed',result=? WHERE action='app_control' "
                    "AND state IN ('accepted','running')",
                    (json.dumps({"classification": "superseded_by_test_stop"}),),
                )
            j.put(
                "managed_test_charge",
                {
                    **self.state(),
                    "state": "expired" if preserve else "stopping",
                    "stop_reason": reason,
                    "expired_normal": lease.normal,
                },
            )
            c.requests.finish(lease.identifier, "uncertain", {"classification": reason})
        if lease.normal:
            self.lease = None
        if c.pause_task is None or c.pause_task.done():
            c.pause_task = asyncio.create_task(c._stop_requested())
        self.changed.set()

    # A monotonic deadline owns Stop independently of HA, the initiating socket, native reads and service polling.
    async def _deadline(self, lease: TestLease) -> None:
        while self.lease is lease and self.state().get("state") != "stopping":
            self.changed.clear()
            remaining = lease.deadline - time.monotonic()
            if remaining <= 0:
                try:
                    if self.finish_closed_normal(lease):
                        return
                    self.stop(lease, "deadline")
                    return
                except sqlite3.Error, RecoveryRequiredError, DeniedError:
                    # Retain an expired grant and retry the atomic hold if storage/control changes were transient.
                    await asyncio.sleep(0.1)
                    continue
            try:
                await asyncio.wait_for(self.changed.wait(), remaining)
            except TimeoutError:
                continue

    # Public timing is descriptive and redacted; it is never authority to repeat a historical Start.
    def summary(self) -> dict[str, Any] | None:
        state = self.state()
        if state.get("kind") == "normal":
            return None
        grace = 15 if state.get("source") == "cloud" and state.get("state") == "charging" else 0
        return {
            "source": state.get("source"),
            "state": state.get("state"),
            "duration_seconds": DURATION,
            "seconds_remaining": min(DURATION, max(0, round(self.lease.deadline - time.monotonic() - grace)))
            if self.lease
            else 0,
        }

    # Ordinary sessions separate measured charging from known transaction existence, without a countdown.
    def normal_summary(self) -> dict[str, Any] | None:
        state = self.state()
        if state.get("kind") != "normal":
            return None
        name = "awaiting_session" if state.get("state") == "awaiting_power" else state.get("state")
        return {"state": name, "charging": name == "running" and state.get("charging") is True}

    # Shutdown retires timers and awaits owned workers before their journal can be closed.
    async def close(self) -> None:
        self.lease = None
        self.changed.set()
        if self.timer is not None:
            self.timer.cancel()
            await asyncio.gather(self.timer, return_exceptions=True)
        if self.worker is not None:
            await asyncio.gather(self.worker, return_exceptions=True)
