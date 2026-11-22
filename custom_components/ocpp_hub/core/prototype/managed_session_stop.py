"""End one verified charger session without changing the requested control mode."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

from ..commands import remote_stop_transaction
from .command_processor import CommandPriority
from .connection_scope import ControlScope, control_scope_matches, current_control_scope
from .managed_intent import ControlIntent, insert_request, request_pending
from .native_http import NativeTransportUnavailableError
from .policy import DeniedError
from .rpc import UnavailableError
from .security import SecurityError, StaleEvidenceError
from .store import RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence

STOP_SESSION = "stop_session"
CONFIRMATION_SECONDS = 20


@dataclass(frozen=True)
class SessionStop:
    """The identity chosen at admission survives later UI and charger updates."""

    identifier: str
    target: str
    local: int
    intent: ControlIntent
    connection: ControlScope


class ManagedSessionStop:
    """Use the shared Stop decision and settle only against the exact session's closure."""

    # Replacement can retire an unsent request. A dispatch marker still owns its original possible effect.
    def __init__(self, controller: ManagedController) -> None:
        self.controller = controller
        self.worker: asyncio.Task[None] | None = None
        self.expire(replaced=True)

    # Expose an opaque target only for a verified sample of the exact current open session.
    def target(self) -> str | None:
        c, j, g = self.controller, self.controller.session.journal, self.controller.session.gateway
        native, local, connection = c.current_native_sample(), j.active(), current_control_scope(g)
        if (
            c.stopping
            or not j.phase.read().active
            or local is None
            or connection is None
            or native is None
            or not native.physical.transaction_open
            or native.physical.native_transaction_id != local
            or not 0 <= time.time() - native.physical.observed_at <= 15
        ):
            return None
        scope, generation = connection
        return fingerprint((scope.boot, scope.epoch, scope.metadata_revision, generation, local))

    # Persist the exact target and revision before a worker can act, with no change to Pause or Return intent.
    def accept(self, identifier: str, revision: int, target: str) -> dict[str, Any]:
        c, j, g = self.controller, self.controller.session.journal, self.controller.session.gateway
        existing = c.requests.existing(identifier, STOP_SESSION, revision)
        if existing is not None:
            if c.requests.receipt(identifier, include_binding=True).get("target") != target:
                raise SecurityError("request identifier already binds another Stop target")
            return existing
        intent, local, connection = c.requests.intent(), j.active(), current_control_scope(g)
        if revision != intent.revision or local is None or connection is None or self.target() != target:
            raise RecoveryRequiredError("Stop requires the current verified session")
        if not c.requests.has_capacity():
            raise RecoveryRequiredError("management receipt capacity requires operator maintenance")
        if self.worker is not None and not self.worker.done():
            raise RecoveryRequiredError("the current session Stop is still being sent")
        accepted = time.time()
        with j.atomic():
            insert_request(
                j,
                identifier,
                STOP_SESSION,
                revision,
                {"accepted_at": accepted, "target": target, "_session_stop": {"local": local, "dispatched": False}},
            )
        scope = SessionStop(identifier, target, local, intent, connection)
        self.worker = c.commands.start(partial(self._send, scope), priority=CommandPriority.RESTRICT)
        return {"id": identifier, "action": STOP_SESSION, "state": "accepted", "result": {"accepted_at": accepted}}

    # Only the original owner, Boot and session can dispatch this Stop, even after another command enters the gate.
    async def _send(self, scope: SessionStop) -> None:
        c, j, g = self.controller, self.controller.session.journal, self.controller.session.gateway
        boot = scope.connection[0].boot

        # Reject a new target or an expired/replaced request both before and at the actual write boundary.
        def guard() -> None:
            if (
                c.stopping
                or c.requests.intent() != scope.intent
                or not control_scope_matches(g, scope.connection)
                or self.target() != scope.target
                or not request_pending(j, scope.identifier, STOP_SESSION, scope.intent.revision)
            ):
                raise RecoveryRequiredError("Stop lost its current session or request")

        # Commit possible dispatch before the sender can write, including when the wire reply is lost.
        def dispatch(_action: str) -> None:
            if not g.controls.decisions.authorize_local_send(
                scope.local, boot, scope.identifier, protective=False, session_only=True
            ):
                raise StopAlreadyHandledError
            with j.atomic():
                j.db.execute(
                    "UPDATE managed_requests SET state='running', "
                    "result=json_set(result,'$._session_stop.dispatched',1) WHERE id=?",
                    (scope.identifier,),
                )

        wire_scope = j.outcomes.owner_scope(scope.identifier, scope.identifier, boot)
        try:
            async with g.charger.sequence(
                0, guard, on_dispatch=dispatch, owner_scope=wire_scope, late_reply_action="RemoteStopTransaction"
            ) as call:
                answer = await call("RemoteStopTransaction", remote_stop_transaction(scope.local))
            if answer[0] == 3 and answer[2] == {"status": "Rejected"}:
                c.commands.decide(partial(self._fail, scope.identifier, "stop_rejected"))
        except StopAlreadyHandledError:
            pass  # The original session's real closure can also satisfy this waiting request.
        except DeniedError, RecoveryRequiredError, SecurityError, UnavailableError, TimeoutError, sqlite3.Error:
            # The original request stays readable. Supervision retires an unresolved live wait at its deadline.
            pass

    # A known rejection permits another deliberate attempt without forgetting the original wire result.
    def _fail(self, identifier: str, reason: str) -> None:
        self.controller.session.journal.db.execute(
            "UPDATE managed_requests SET state='failed',result=json_set(result,'$.classification',?) "
            "WHERE id=? AND state IN ('accepted','running')",
            (reason, identifier),
        )

    # Retire the client wait without resending or erasing a Stop that might still take effect.
    def expire(self, *, replaced: bool = False) -> None:
        db = self.controller.session.journal.db
        db.execute(
            "UPDATE managed_requests SET state='failed',result=json_set(result,'$.classification','stop_rejected') "
            "WHERE action=? AND state IN ('running','uncertain') AND EXISTS ("
            "SELECT 1 FROM owner_command_wires WHERE request=managed_requests.id AND reply='Rejected')",
            (STOP_SESSION,),
        )
        db.execute(
            "UPDATE managed_requests SET state=CASE WHEN json_extract(result,'$._session_stop.dispatched') "
            "THEN 'uncertain' ELSE 'failed' END, result=json_set(result,'$.classification',?) "
            "WHERE action=? AND state IN ('accepted','running') "
            "AND (? OR json_extract(result,'$.accepted_at')<=?)",
            (
                "process_replaced" if replaced else "stop_not_confirmed",
                STOP_SESSION,
                replaced,
                time.time() - CONFIRMATION_SECONDS,
            ),
        )

    # A fresh native observation and the genuine Stop report must agree before a receipt completes.
    def observed(self, native: HandoverEvidence) -> None:
        j = self.controller.session.journal
        active = j.active()
        if (
            native.physical.transaction_open != (active is not None)
            or native.physical.native_transaction_id != active
            or not 0 <= time.time() - native.physical.observed_at <= 5
            or active is None
            and (native.physical.power_w != 0 or any(value != 0 for value in native.currents_a + native.powers_w))
        ):
            return
        j.db.execute(
            "UPDATE managed_requests SET state='completed',result=json_set(result,'$.session_stopped',json('true')) "
            "WHERE action=? AND state IN ('accepted','running','uncertain') AND EXISTS ("
            "SELECT 1 FROM transactions WHERE local=json_extract(managed_requests.result,'$._session_stop.local') "
            "AND stop IS NOT NULL)",
            (STOP_SESSION,),
        )

    # Cloud reconciliation must not erase an independently confirmed local closure.
    async def confirm_while_blocked(self) -> None:
        c, j = self.controller, self.controller.session.journal
        if (
            c.stopping
            or j.db.execute(
                "SELECT 1 FROM managed_requests r JOIN transactions t "
                "ON t.local=json_extract(r.result,'$._session_stop.local') "
                "WHERE r.action=? AND r.state IN ('accepted','running','uncertain') AND t.stop IS NOT NULL LIMIT 1",
                (STOP_SESSION,),
            ).fetchone()
            is None
        ):
            return
        try:
            native = await c.native.read(stop_only=True)
            c.native.require_known_session(native)
            c.commands.decide(partial(self.observed, native), priority=CommandPriority.RESTRICT)
        except (
            DeniedError,
            NativeTransportUnavailableError,
            RecoveryRequiredError,
            SecurityError,
            StaleEvidenceError,
            UnavailableError,
            TimeoutError,
        ):
            pass  # Neither an unavailable local reading nor a cloud fault proves closure.

    # During a real Stop, either of the two observations can confirm closure before the other.
    def closure_pending(self, native: HandoverEvidence) -> bool:
        j, physical = self.controller.session.journal, native.physical
        if physical.power_w != 0 or any(value != 0 for value in native.currents_a + native.powers_w):
            return False
        active = j.active()
        if not physical.transaction_open and physical.native_transaction_id is None and active is not None:
            return j.outcomes.session_stop_dispatched(active)
        local = physical.native_transaction_id
        if not physical.transaction_open or local is None or active is not None:
            return False
        latest = j.db.execute("SELECT local,stop FROM transactions ORDER BY local DESC LIMIT 1").fetchone()
        return bool(
            latest is not None
            and latest["local"] == local
            and latest["stop"] is not None
            and j.outcomes.session_stop_dispatched(local)
        )

    # Retire the wire worker before the journal closes. Its dispatch marker survives cancellation.
    async def close(self) -> None:
        if self.worker is not None:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)


class StopAlreadyHandledError(RecoveryRequiredError):
    """A previous dispatch or real closure already owns this session's Stop."""
