"""Explicit two-minute tests own one Start and an independent Stop deadline; restart never repeats Start."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .policy import DeniedError
from .rpc import UnavailableError
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence

if TYPE_CHECKING:
    from .managed import ManagedController

DURATION = 120
STARTUP_BUDGET = 60
ACTIVE = frozenset({"armed", "starting", "awaiting_power", "charging", "stopping"})


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


class ManagedChargeTests:
    """The generic Start policy stays disabled; this one-shot test has no caller-selected current/tag/duration."""

    # Construction may retire an interrupted permission, but never sends a Start or renews a persisted deadline.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.lease: TestLease | None = None
        self.worker: asyncio.Task[None] | None = None
        self.timer: asyncio.Task[None] | None = None
        self.changed = asyncio.Event()
        saved = self.state()
        if saved.get("state") in ACTIVE:
            with self.journal.atomic():
                intent = controller.requests.intent()
                self.journal.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                controller.session.gateway.controls._claim(True, stopping=True)
                self.journal.put("managed_test_charge", {**saved, "state": "interrupted"})

    # Keep one journal owner and the same durable management receipt table as Pause/Return.
    @property
    def journal(self) -> Journal:
        return self.controller.session.journal

    # Private provenance includes identifiers; the public summary below exposes only source, state and timing.
    def state(self) -> dict[str, Any]:
        value: dict[str, Any] = self.journal.get("managed_test_charge", {})
        return value

    # Admission is conservative; the worker independently rechecks physical zero before Start.
    def available(self, native: HandoverEvidence | None = None) -> bool:
        c, j = self.controller, self.journal
        g = c.session.gateway
        return bool(
            not c.stopping
            and c.requests.intent().mode == "app"
            and not j.get("managed_control_fault")
            and j.get("trial_phase") == "active"
            and g.controls.ready
            and g.controls.desired() == 6
            and not g.policy.allow_local_start
            and not j.get("control", {}).get("uncertain")
            and not j.get("control", {}).get("start_pending")
            and j.active() is None
            and not j.get("vendor_start_grant")
            and not j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            and c.requests.pending_return() is None
            and self.state().get("state") not in ACTIVE
            and g.current_boot_seen(g._charger_epoch)
            and c.session.runtime.status.vendor_connected
            and g.vendor_registration == "Accepted"
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
            or not self.available(native)
        ):
            raise RecoveryRequiredError("test requires current shared six-ampere authority and no pending work")
        if j.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] >= 4096:
            raise RecoveryRequiredError("management receipt capacity requires maintenance")
        g = c.session.gateway
        source = "local" if action == "start_test" else "cloud"
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
            time.monotonic() + STARTUP_BUDGET + DURATION,
            j.db.execute("SELECT COALESCE(MAX(rowid),0) FROM mutations").fetchone()[0],
        )
        with j.atomic():
            j.put(
                "managed_test_charge",
                {
                    "id": identifier,
                    "source": source,
                    "revision": revision,
                    "state": "starting" if source == "local" else "armed",
                    "accepted_at": time.time(),
                    "hard_deadline_epoch": time.time() + STARTUP_BUDGET + DURATION,
                    "duration_seconds": DURATION,
                    "start_rpc_accepted": False,
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
            or intent.mode != "app"
            or intent.revision != lease.revision
            or g._charger_epoch != lease.epoch
            or g.controls._revision != lease.control_revision
            or not g.current_boot_seen(lease.epoch)
            or fingerprint(j.identity) != lease.identity
            or c.native.spec.digest != lease.spec
            or g.policy.allow_local_start
            or g.policy.max_current_a != 6
            or j.get("managed_control_fault")
            or j.get("trial_phase") != "active"
            or self.state().get("id") != lease.identifier
            or self.state().get("state") not in ACTIVE
            or self.state().get("state") == "stopping"
            or c.requests.pending_return() is not None
            or not stopping
            and j.get("control", {}).get("stop_epoch", 0) != lease.stop_epoch
            or lease.source == "local"
            and j.db.execute(
                "SELECT 1 FROM mutations WHERE rowid>? AND origin='vendor' AND action='RemoteStartTransaction' LIMIT 1",
                (lease.first_mutation,),
            ).fetchone()
        ):
            raise DeniedError("managed test Start permission expired or was superseded")

    # The service owns the worker after client disappearance; all failures request a normal durable Pause.
    async def _start(self, lease: TestLease) -> None:
        c, j = self.controller, self.journal
        effects = False
        try:
            self.guard(lease)
            initial = await c.settled_native()
            c.native.spec.require(initial, j.identity, "trial", closed=True, allow_unplugged=True)
            if initial.physical.connection != "Plug":
                raise DeniedError("test requires a freshly connected idle vehicle")
            self.guard(lease)
            effects = True
            await c.session.gateway.stop()
            self.guard(lease, stopping=True)
            lease.stop_epoch = j.get("control", {}).get("stop_epoch", 0)
            native = await c.native.read(closed=True)
            c.require_zero(native)
            if native.physical.connection != "Plug":
                raise DeniedError("test requires a connected vehicle")
            self.guard(lease)
            tag = sorted(c.session.gateway.policy.allowed_tags)[0]
            await c.session.gateway.start_managed(tag, lambda: self.guard(lease))
            self.guard(lease)
            j.put("managed_test_charge", {**self.state(), "state": "awaiting_power", "start_rpc_accepted": True})
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError:
            if not effects:
                c.requests.finish(lease.identifier, "failed", {"classification": "preflight_refused_unsent"})
                if self.state().get("id") == lease.identifier:
                    j.put("managed_test_charge", {**self.state(), "state": "failed"})
                self.lease = None
                self.changed.set()
                return
            c.requests.finish(lease.identifier, "uncertain", {"classification": "start_not_confirmed"})
            self.stop(lease, "start_not_confirmed")

    # Polling waits for the short native/start sequence; its independent Stop timer continues to run meanwhile.
    async def settle(self) -> None:
        if self.worker is not None and not self.worker.done():
            await asyncio.shield(self.worker)

    # Positive flow must belong to the known transaction and the actually accepted local or vendor Start path.
    def observed(self, native: HandoverEvidence) -> None:
        lease, c = self.lease, self.controller
        if lease is None:
            return
        state = self.state()
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
            if (lease.source == "local" and (cloud or not state.get("start_rpc_accepted"))) or (
                lease.source == "cloud" and not cloud
            ):
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

    # Stopping records a normal higher-revision Pause before its physical effects; it never automatically returns.
    def stop(self, lease: TestLease, reason: str) -> None:
        if self.lease is not lease or self.state().get("state") == "stopping":
            return
        c, j = self.controller, self.journal
        if c.stopping:
            return
        with j.atomic():
            intent = c.requests.intent()
            if intent.mode != "paused":
                j.put("managed_control_intent", {"mode": "paused", "revision": intent.revision + 1})
                c.session.gateway.controls._claim(True, stopping=True)
            j.put("managed_test_charge", {**self.state(), "state": "stopping", "stop_reason": reason})
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? WHERE action='app_control' "
                "AND state IN ('accepted','running')",
                (json.dumps({"classification": "superseded_by_test_stop"}),),
            )
            c.requests.finish(lease.identifier, "uncertain", {"classification": reason})
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
    def summary(self) -> dict[str, Any]:
        state = self.state()
        grace = 15 if state.get("source") == "cloud" and state.get("state") == "charging" else 0
        return {
            "source": state.get("source"),
            "state": state.get("state"),
            "duration_seconds": DURATION,
            "seconds_remaining": min(DURATION, max(0, round(self.lease.deadline - time.monotonic() - grace)))
            if self.lease
            else 0,
        }

    # Shutdown retires timers and awaits owned workers before their journal can be closed.
    async def close(self) -> None:
        self.lease = None
        self.changed.set()
        if self.timer is not None:
            self.timer.cancel()
            await asyncio.gather(self.timer, return_exceptions=True)
        if self.worker is not None:
            await asyncio.gather(self.worker, return_exceptions=True)
