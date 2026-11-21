"""Local authority with durable cloud delivery and explicitly approved vendor passthrough.

This is an offline engine, not a Home Assistant setup path. It reuses OCPP Hub's
frames and charging-profile builders while replacing its volatile mirror boundary.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from ..frames import ERROR_FORMATION_VIOLATION, Frame, build_call_error, build_call_result
from .auth_info import InfoBook, expiry, validate_info
from .authorisation import VendorAuthorisation
from .command_outcomes import (
    TRANSACTION_COMMANDS,
    ForwardedReply,
    VendorProfileReply,
    VendorProfileWrite,
    VendorWriteEvidence,
    VendorWriteReply,
)
from .command_processor import CommandPriority, CommandProcessor
from .connection_scope import ControlScope, control_scope_matches, current_control_scope
from .delivery_reply import DeliveryReply, stop_result_shape_supported
from .events import OBSERVATION_ACTIONS, validate_observation
from .ics import TransactionQuery, is_transaction_query, unique_object
from .ics_writes import IcsWrite, data_value, known_write
from .local_control import LocalControl
from .managed_start_delivery import (
    latch_start_delivery_fault,
    record_start_denial,
    record_start_error,
    record_start_wire_ack,
    retry_deferred,
    supersede_unknown_start_retry,
)
from .managed_stop_delivery import approved_stop_metadata, record_stop_ack, reject_stop_reply
from .observation_receipts import ObservationReceipts
from .plans import PlanBook
from .policy import DeniedError, Policy, integer, number
from .rpc import InboundCache, Peer, RegistrationDeferredError, Sender, UnavailableError, profile_status, reply_as
from .security import SecurityError
from .start_handoff import OwnedStartResult, StartHandoff
from .store import ConflictError, Delivery, Journal, RecoveryRequiredError, encoded, fingerprint
from .vendor_read_recovery import recover_legacy_vendor_read

PROFILE_COMMANDS = frozenset({"SetChargingProfile", "ClearChargingProfile"})


class UnreadableFrameError(DeniedError):
    """An isolated malformed JSON message contains no usable OCPP request or reply."""


class MalformedReplyError(DeniedError):
    """A recognizable reply envelope cannot complete a waiting call."""


class MalformedCallError(DeniedError):
    """A malformed request with a usable correlation ID."""


# Reject malformed or non-finite input before handing it to the upstream frame vocabulary.
def decode(raw: str | bytes) -> Frame:
    if not isinstance(raw, str):
        raise DeniedError("frame must be text")
    try:
        if len(raw.encode()) > 131072:
            raise DeniedError("frame must be bounded text")
        frame = json.loads(raw, object_pairs_hook=unique_object)
        encoded(frame)
    except ValueError, RecursionError:
        raise UnreadableFrameError("invalid JSON frame") from None
    if (
        not isinstance(frame, list)
        or len(frame) < 2
        or type(frame[0]) is not int
        or not isinstance(frame[1], str)
        or not 1 <= len(frame[1]) <= 36
    ):
        raise DeniedError("invalid frame identifier")
    try:
        frame[1].encode("utf-8")
    except UnicodeEncodeError:
        raise UnreadableFrameError("frame identifier is not valid text") from None
    if frame[0] == 2:
        if len(frame) == 4 and isinstance(frame[2], str) and isinstance(frame[3], dict):
            return frame
        raise MalformedCallError(frame[1])
    if frame[0] == 3 and len(frame) == 3 and isinstance(frame[2], dict):
        return frame
    if (
        frame[0] == 4
        and len(frame) == 5
        and all(isinstance(frame[i], str) for i in (2, 3))
        and isinstance(frame[4], dict)
    ):
        return frame
    if frame[0] in (3, 4):
        raise MalformedReplyError("invalid reply shape")
    raise DeniedError("invalid frame shape")


# Validate timestamps without confusing a replayed event with a current observation.
def timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise DeniedError("timestamp must be a timezone-aware string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DeniedError("invalid timestamp") from None
    if parsed.tzinfo is None:
        raise DeniedError("timestamp must have a timezone")
    try:
        return parsed.astimezone(UTC)
    except OverflowError, ValueError:
        raise DeniedError("timestamp is outside the supported UTC range") from None


@dataclass(frozen=True)
class NativeStartReport:
    """A held lifecycle report, with an optional verified native source, committed with the actual transaction."""

    native_id: int | None
    plan_id: int | None
    attempt: tuple[str, int] | None
    record: Callable[[int], None] = field(compare=False)


class StartReports(Protocol):
    """Managed hooks for this one charger lifecycle report, in gateway dispatch order."""

    # Establish any required owner hold before the gateway pins its ticket.
    async def prepare_hold(self, payload: dict[str, Any]) -> None: ...

    # Identify a delayed native or owner report using fresh independent evidence.
    async def identify(self, payload: dict[str, Any]) -> NativeStartReport | None: ...

    async def verify_station(self) -> None: ...

    # Permit a proved owner report under a hold without authorizing any new Start.
    def permits_held(self, tag: str) -> bool: ...

    # Reserve a possible command binding until the actual native source is known.
    def defer_binding(self, local: int, tag: str) -> bool: ...

    # Record the observed Start inside its journal transaction.
    def recorded(self, local: int, payload: dict[str, Any]) -> None: ...


class Gateway:
    """One admitted charger, independent local control, and one fixed vendor mirror."""

    # Nothing connects here; an offline harness must explicitly supply the peer senders.
    def __init__(self, journal: Journal, policy: Policy | None = None, timeout: float = 1.0) -> None:
        self.journal = journal
        recover_legacy_vendor_read(journal)
        self.plans = PlanBook(journal)
        self.policy = policy or Policy()
        self.retiring: Callable[[], bool] = lambda: False
        self.command_processor: CommandProcessor | None = None
        self.charger = Peer("local", timeout)
        self.vendor = Peer("vendor", timeout)
        self.controls = LocalControl(
            journal, self.charger, self.policy, lambda: self._charger_epoch, lambda: self.boot_observation
        )
        self.authorisation = VendorAuthorisation(journal, self.policy.vendor_tag_ttl_s)
        self.auth_info = InfoBook(journal, protected_tags=self.policy.allowed_tags)
        self.start_reports: StartReports | None = None
        if self.policy.strict_cloud_authorization:
            self.controls.authorize = self._local_start_allowed
        self._tasks: set[asyncio.Task[None]] = set()
        self._boot_response: asyncio.Task[None] | None = None
        self._reserved_stops: dict[str, asyncio.Task[None]] = {}
        self._refusals: dict[str, asyncio.Task[None]] = {}
        self._flush_lock = asyncio.Lock()
        self._boot_lock = asyncio.Lock()
        self._start_renewals: dict[tuple[str, int, int, str | None, str | None], asyncio.Future[bool]] = {}
        self._vendor_ready = False
        self.vendor_interval = 60
        self.vendor_registration = "disconnected"
        self._boot_retry_at = 0.0
        self._charger_epoch = 0
        self._boot_charger_epoch = -1
        self.boot_metadata_revision = 0
        self.boot_observation: str | None = None
        self._pending_boot: str | None = None
        self._boot_seen = asyncio.Event()
        self._boot_retry = asyncio.Event()
        self._vendor_epoch = 0
        self._inbound = InboundCache(journal=journal)
        self._observations = ObservationReceipts(journal)
        self._start_retry_waits: dict[str, tuple[str, float]] = {}
        self._late_delivery_token: str | None = None
        self.failures: list[str] = []

    # Install the same decision queue on both peers and the existing local command owner.
    def use_command_processor(self) -> CommandProcessor:
        if self.command_processor is None:
            self.command_processor = CommandProcessor()
        self.charger.command_processor = self.command_processor
        self.vendor.command_processor = self.command_processor
        self.controls.command_processor = self.command_processor
        self._inbound.command_processor = self.command_processor
        return self.command_processor

    # Submit a short proxy decision without acquiring either network gate.
    def decide[T](self, decision: Callable[[], T], *, priority: CommandPriority = CommandPriority.NORMAL) -> T:
        processor = self.command_processor
        return decision() if processor is None else processor.decide(decision, priority=priority)

    # An incoming request awaiting admission can yield to a ready restrictive command.
    async def decide_report[T](self, decision: Callable[[], T]) -> T:
        processor = self.command_processor
        return decision() if processor is None else await processor.run(decision)

    # Retry the original recorded response and its independent connection receipt without sending either command again.
    def retry_reply_writes(self) -> None:
        self.controls.retry_unsent_release()
        self.auth_info.retry_denials()
        self.journal.outcomes.retry()
        self.journal.outcomes.retry_owner_writes()
        self._inbound.retry()

    # The outcome owner uses the current peer only to distinguish a live wait from an abandoned dispatch.
    def vendor_write_evidence(self) -> VendorWriteEvidence:
        return self.journal.outcomes.vendor_evidence(self.charger.has_pending_call)

    # Restrict new transports or changed introductions before they can authorize any charging-capable request.
    def _restrict_trial_introduction(self) -> None:
        if self.journal.phase.read().active and self.journal.get("trial_binding") is not None:
            with self.journal.atomic():
                control = self.journal.get("control", {})
                self.journal.phase.restrict(control)

    # Attach only an already admitted transport; the gateway never opens sockets itself.
    def attach_charger(self, sender: Sender) -> int:
        return self.decide(lambda: self._attach_charger(sender), priority=CommandPriority.RESTRICT)

    # Apply the connection change and retire previous evidence in the same turn.
    def _attach_charger(self, sender: Sender) -> int:
        self._restrict_trial_introduction()
        epoch = self.journal.connection_epoch()
        self.charger.detach()
        self._charger_epoch = epoch
        if self._boot_response is not None:
            self._boot_response.cancel()
            self._boot_response = None
        self._retire_boot_proof()
        self._boot_retry.clear()
        self._inbound.retire("charger", epoch)
        self.charger.sender = sender
        return epoch

    # Reconnecting the vendor requires a fresh BootNotification before queued traffic.
    def attach_vendor(self, sender: Sender) -> int:
        return self.decide(lambda: self._attach_vendor(sender), priority=CommandPriority.RESTRICT)

    # Apply the connection change and retire previous evidence in the same turn.
    def _attach_vendor(self, sender: Sender) -> int:
        self.vendor.detach()
        self._vendor_epoch += 1
        self._inbound.retire("vendor", self._vendor_epoch)
        self.vendor.sender = sender
        self._vendor_ready = False
        self.vendor_registration = "connected"
        return self._vendor_epoch

    # A retired reader cannot detach a newer authenticated charger.
    def detach_charger(self, epoch: int) -> None:
        return self.decide(lambda: self._detach_charger(epoch), priority=CommandPriority.RESTRICT)

    # Apply the connection change and retire previous evidence in the same turn.
    def _detach_charger(self, epoch: int) -> None:
        if epoch == self._charger_epoch:
            self.charger.detach()
            if self._boot_response is not None:
                self._boot_response.cancel()
                self._boot_response = None
            self._retire_boot_proof()
            self._boot_charger_epoch = -1
            self.controls.invalidate()
            self._charger_epoch = epoch + 1
            self._inbound.retire("charger", self._charger_epoch)
            self._charger_epoch = self.journal.connection_epoch()

    # A vendor outage never closes the local charger or cancels its handlers.
    def detach_vendor(self, epoch: int) -> None:
        return self.decide(lambda: self._detach_vendor(epoch), priority=CommandPriority.RESTRICT)

    # Apply the connection change and retire previous evidence in the same turn.
    def _detach_vendor(self, epoch: int) -> None:
        if epoch == self._vendor_epoch:
            self.vendor.detach()
            self._vendor_epoch += 1
            self._vendor_ready = False
            self.vendor_registration = "disconnected"

    # A persisted introduction is useful for history but is not proof of the current charger generation.
    def current_boot_seen(self, epoch: int) -> bool:
        return (
            epoch == self._charger_epoch
            and self._boot_charger_epoch == epoch
            and self.charger.sender is not None
            and self.boot_observation is not None
        )

    # Waiters still check generation after waking, so a replacement's Boot cannot introduce an old connection.
    async def wait_for_boot(self, epoch: int) -> None:
        while not self.current_boot_seen(epoch):
            self._require_epoch("charger", epoch)
            await self._boot_seen.wait()
        self._require_epoch("charger", epoch)

    # An unsaved Boot leaves only a read-only probe pending on the same authenticated transport.
    async def wait_for_boot_retry(self, epoch: int) -> None:
        await self._boot_retry.wait()
        self._require_epoch("charger", epoch)
        self._boot_retry.clear()

    # A new Boot withdraws earlier permission even if it cannot be saved or admitted to the work queue.
    def _retire_boot_proof(self) -> None:
        self.boot_observation = None
        self._pending_boot = None
        self._boot_seen.clear()

    # Stop pending work without delivering another command or discarding durable events.
    async def close(self) -> None:
        # Retire both peers before another pending command can send.
        def disconnect() -> None:
            self.charger.detach()
            self.vendor.detach()

        self.decide(disconnect, priority=CommandPriority.SHUTDOWN)
        await self.controls.close()
        replies = self._tasks.union(self._refusals.values())
        for task in replies:
            task.cancel()
        await asyncio.gather(*replies, return_exceptions=True)
        self.decide(self.retry_reply_writes, priority=CommandPriority.RESTRICT)

    # Drain request handlers and their independent refusal replies in deterministic offline tests.
    async def drain(self) -> None:
        while replies := self._tasks.union(self._refusals.values()):
            await asyncio.gather(*replies, return_exceptions=True)
            # Python 3.14 can return a completed gather without yielding to the
            # task-done callbacks that remove these tasks from the set.
            await asyncio.sleep(0)

    # Resolve replies immediately; process bounded incoming requests in separate tasks.
    async def receive(
        self, side: str, raw: str | bytes, epoch: int | None = None, *, replies_only: bool = False
    ) -> None:
        if side not in ("charger", "vendor"):
            raise DeniedError("unknown peer")
        current_epoch = self._charger_epoch if side == "charger" else self._vendor_epoch
        if epoch is not None and epoch != current_epoch:
            raise UnavailableError("retired connection")
        peer = self.charger if side == "charger" else self.vendor
        try:
            frame = decode(raw)
        except UnreadableFrameError, MalformedReplyError:
            return  # Keep the socket and wait for a valid message or the existing reply deadline.
        except MalformedCallError as error:
            if not replies_only:
                self._refuse_call(side, current_epoch, str(error), ERROR_FORMATION_VIOLATION, "invalid CALL shape")
            return
        if frame[0] != 2:
            peer.receive(frame)
            return
        boot = side == "charger" and frame[2] == "BootNotification"
        if boot:
            self._start_boot_response(current_epoch, str(frame[1]), frame[3])
            return
        reserved = len(self._tasks) >= 32
        stop = frame[2] == ("StopTransaction" if side == "charger" else "RemoteStopTransaction")
        if reserved and (not stop or side in self._reserved_stops):
            self._refuse_call(side, current_epoch, frame[1], "OccurrenceConstraintViolation", "request queue is full")
            return
        # A Boot read after this report must not move it into another observation generation.
        generation = None
        if side == "charger" and frame[2] in OBSERVATION_ACTIONS:
            try:
                generation = self.journal.get("observation_generation", 0)
            except sqlite3.Error:
                self._refuse_call(side, current_epoch, frame[1], "InternalError", "observation generation unavailable")
                return
        pending_start_boot = None
        if side == "charger" and frame[2] == "StartTransaction":
            if self._boot_response is not None and self._pending_boot is not None:
                pending_start_boot = self._boot_response, self._pending_boot
        task = asyncio.create_task(
            self._answer(
                side,
                current_epoch,
                frame,
                current_control_scope(self),
                generation,
                replies_only=replies_only,
                pending_start_boot=pending_start_boot,
            )
        )
        self._tasks.add(task)
        if reserved:
            self._reserved_stops[side] = task
        task.add_done_callback(self._finished)

    # Limit refusals to one waiting send per peer while the reader keeps matching replies.
    def _refuse_call(self, side: str, epoch: int, token: str, code: str, detail: str) -> None:
        if side in self._refusals:
            return
        task = asyncio.create_task(self._send_reply(side, epoch, build_call_error(token, code, detail)))
        self._refusals[side] = task
        task.add_done_callback(self._finished)

    # Deduplicate a peer's requests before execution and never send old replies onto a replacement connection.
    async def _answer(
        self,
        side: str,
        epoch: int,
        frame: Frame,
        charger_scope: ControlScope | None,
        observation_generation: int | None,
        *,
        replies_only: bool = False,
        pending_start_boot: tuple[asyncio.Task[None], str] | None = None,
    ) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        if epoch != current:
            return

        # Core charger observations have their own durable receipts; heartbeat times are never cached.
        async def handle() -> str:
            if replies_only:
                return build_call_error(frame[1], "InternalError", "charger is not ready; command was not sent")
            if side == "charger":
                boot = charger_scope[0].boot if charger_scope is not None else None
                if boot is None and pending_start_boot is not None:
                    boot_response, observation = pending_start_boot
                    try:
                        await asyncio.shield(boot_response)
                    except TimeoutError, UnavailableError, OSError:
                        pass
                    if self._charger_epoch == epoch and self.boot_observation == observation:
                        boot = observation
                return await self._charger_call(frame, epoch, boot, observation_generation)
            return await self._vendor_call(frame, epoch, charger_scope)

        read = False
        if side == "vendor" and (
            frame[2] in ("GetConfiguration", "GetCompositeSchedule")
            or frame[2] == "DataTransfer"
            and is_transaction_query(frame[3])
        ):
            try:
                read = self.policy.kind(frame[2], frame[3]) == "read"
            except DeniedError:
                pass  # Only recognised, approved pure reads use the bounded reply cache.
        text = (
            await handle()
            if side == "charger" and frame[2] != "DataTransfer"
            else await self._inbound.response(
                side, epoch, frame, handle, stop=side == "vendor" and frame[2] == "RemoteStopTransaction", read=read
            )
        )
        await self._send_reply(side, epoch, text)

    # A reply can wait on its own sender without transferring to a replacement connection.
    async def _send_reply(self, side: str, epoch: int, text: str) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        peer = self.charger if side == "charger" else self.vendor
        sender = peer.sender
        if epoch == current and sender is not None:
            async with asyncio.timeout(peer.timeout):
                await sender(text)

    # Retire old proof on arrival and give the bounded send its own task so the reader can receive other replies.
    def _start_boot_response(self, epoch: int, token: str, payload: dict[str, Any]) -> None:
        # Withdrawal, admission and persistence are one turn, even if no handler slot is available.
        def prepare() -> tuple[Sender | None, str, str | None] | None:
            self._require_epoch("charger", epoch)
            self._retire_boot_proof()
            if len(self._tasks) >= 32 or (self._boot_response is not None and not self._boot_response.done()):
                self._boot_retry.set()
                return None
            text = self._boot_reply(token, payload)
            return self.charger.sender, text, self._pending_boot

        prepared = self.decide(prepare, priority=CommandPriority.RESTRICT)
        if prepared is None:
            self._refuse_call("charger", epoch, token, "OccurrenceConstraintViolation", "request queue is full")
            return
        sender, text, observation = prepared
        if sender is None:
            return
        task = asyncio.create_task(self._send_boot_response(epoch, sender, text, observation))
        self._boot_response = task
        self._tasks.add(task)
        task.add_done_callback(self._finished)

    # Make a durable Boot usable only after its reply succeeds on the exact connection that received it.
    async def _send_boot_response(self, epoch: int, sender: Sender, text: str, observation: str | None) -> None:
        try:
            async with asyncio.timeout(self.charger.timeout):
                await sender(text)
        except TimeoutError, UnavailableError:
            # Only the current socket can request a fresh Boot after its send fails.
            def retry() -> None:
                if epoch == self._charger_epoch and self.charger.sender is sender:
                    self._boot_retry.set()

            self.decide(retry, priority=CommandPriority.RESTRICT)
            raise

        # A successful send proves only the exact pending observation for its original connection.
        def publish() -> None:
            if (
                observation is not None
                and observation == self._pending_boot
                and epoch == self._charger_epoch
                and self.charger.sender is sender
            ):
                self._boot_charger_epoch = epoch
                self.boot_observation = observation
                self._pending_boot = None
                self._boot_seen.set()

        self.decide(publish, priority=CommandPriority.RESTRICT)

    # A retired connection cannot execute queued work on a replacement transport.
    def _require_epoch(self, side: str, epoch: int) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        if current != epoch:
            raise UnavailableError("request belongs to a retired connection")

    # Retrieve task exceptions without logging frame bodies, identities or credentials.
    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if self._boot_response is task:
            self._boot_response = None
        for group in (self._reserved_stops, self._refusals):
            for side, answer in group.items():
                if answer is task:
                    del group[side]
                    break
        if not task.cancelled() and (error := task.exception()) is not None:
            self._record_failure(type(error).__name__)

    # Retain only recent handler failures for the life of the process.
    def _record_failure(self, kind: str) -> None:
        self.failures.append(kind)
        del self.failures[:-32]

    # Send a local result only after its storage commit; transport failure leaves the receipt available for retry.
    async def _charger_call(
        self, frame: Frame, epoch: int, boot: str | None, observation_generation: int | None
    ) -> str:
        token, action = str(frame[1]), str(frame[2])
        payload: dict[str, Any] = frame[3]
        mutation: str | None = None
        vendor_epoch = self._vendor_epoch
        try:
            if action == "DataTransfer":
                if is_transaction_query(payload) or known_write(payload):
                    raise DeniedError("this iCS query is supported only from the cloud")
                self._extension_allowed(payload)
                await self._ensure_vendor_boot()

                # Recheck ownership after waiting for the vendor's outgoing request gate.
                def prepare() -> dict[str, Any]:
                    nonlocal mutation
                    self._require_epoch("charger", epoch)
                    self._require_epoch("vendor", vendor_epoch)
                    self._extension_allowed(payload)
                    if self.policy.kind(action, payload) == "write":
                        mutation = self.journal.mutation_intent("charger", action, payload)
                    return payload

                answer = await self.vendor.call(action, prepare)
                if mutation:
                    exact_mutation = mutation
                    self.decide(lambda: self.journal.mutation_finished(exact_mutation, True))
                text = reply_as(answer, token)
            else:
                if action == "Authorize" and self.policy.strict_cloud_authorization:
                    info = await self._authorize_tag(payload.get("idTag"))
                    self._require_epoch("charger", epoch)
                    if info["status"] == "Accepted" and not self._tag_allowed(payload.get("idTag")):
                        info = {"status": "Invalid"}
                    if info["status"] == "Accepted":
                        if (
                            self.authorisation.allows(payload.get("idTag", ""))
                            and self.charger.gate.busy
                            and not self.controls.ready
                        ):
                            raise UnavailableError("nested authorization cannot bypass an unconfirmed ceiling")
                        try:
                            await self.controls.ensure_ceiling()
                        except DeniedError:
                            # A newer Stop still returns the normal denied Authorize result, never a stale grant.
                            if self._tag_allowed(payload.get("idTag")):
                                raise
                            info = {"status": "Invalid"}

                    # Recheck acceptance after the last ceiling or vendor wait.
                    def publish(info: dict[str, Any]) -> dict[str, Any]:
                        self._require_epoch("charger", epoch)
                        if info["status"] == "Accepted":
                            latest = self._tag_info(payload["idTag"])
                            info = (
                                latest
                                if self._tag_allowed(payload["idTag"]) or latest["status"] != "Accepted"
                                else {"status": "Invalid"}
                            )
                        return info

                    return build_call_result(token, {"idTagInfo": self.decide(lambda: publish(info))})
                receipt = f"{epoch}:{token}"
                if action == "StartTransaction":
                    response = await self._reported_start(receipt, payload, epoch, boot)
                else:
                    if action == "Authorize" and self._tag_allowed(payload.get("idTag")):
                        await self.controls.ensure_ceiling()
                        self._require_epoch("charger", epoch)
                    response = await self.decide_report(
                        lambda: self._commit_charger_report(epoch, receipt, action, payload, observation_generation)
                    )
                text = build_call_result(token, response)
        except DeniedError, ConflictError:
            text = build_call_error(
                token, "PropertyConstraintViolation", "request conflicts with local policy or state"
            )
        except UnavailableError, RecoveryRequiredError, SecurityError, sqlite3.Error:
            if mutation:
                try:
                    exact_mutation = mutation
                    self.decide(lambda: self.journal.mutation_finished(exact_mutation, False))
                except sqlite3.Error:
                    self._record_failure("JournalUnavailable")
            text = build_call_error(token, "InternalError", "outcome unavailable; success was not confirmed")
        return text

    # Hold, authorize and fence a reported Start before committing its real lifecycle.
    async def _reported_start(
        self, receipt: str, payload: dict[str, Any], epoch: int, boot: str | None
    ) -> dict[str, Any]:
        occurred = timestamp(payload.get("timestamp"))

        # A replay can return its stored result, while a new Start must retain its original introduction.
        def check() -> None:
            self._require_epoch("charger", epoch)
            if (
                self.journal.phase.read().supervised
                and (boot is None or self.boot_observation != boot)
                and not self.journal.known_start(payload)
            ):
                raise UnavailableError("reported Start belongs to a retired charger Boot")

        # Recheck the report and any captured hold at the one turn that can create a lifecycle entry.
        def commit(native: NativeStartReport | None, ticket: str | None) -> dict[str, Any]:
            check()
            if not self.journal.known_start(payload) and self.journal.get("control", {}).get("ticket") != ticket:
                raise DeniedError("a newer owner operation superseded the reported start")
            return self._record_start(receipt, payload, occurred, native)

        check()
        if self.journal.known_start(payload):
            return await self.decide_report(lambda: commit(None, None))
        if self.start_reports is not None and not self.journal.known_start(payload):
            await self.start_reports.prepare_hold(payload)
            check()
        ticket = self.journal.get("control", {}).get("ticket")
        if self.policy.strict_cloud_authorization:
            try:
                await self._renew_direct_start(payload, epoch, check_origin=check)
            except DeniedError, UnavailableError:
                if not self.journal.known_start(payload):
                    raise
            check()
        if not self.journal.known_start(payload) and self._tag_allowed(payload.get("idTag")):
            await self.controls.ensure_ceiling()
            check()
        if self.journal.get("control", {}).get("ticket") != ticket and not self.journal.known_start(payload):
            raise DeniedError("a newer owner operation superseded the start")
        native_report = None
        if self.start_reports is not None and not self.journal.known_start(payload):
            native_report = await self.start_reports.identify(payload)
            tag = payload.get("idTag")
            if (
                native_report is not None
                and self.policy.strict_cloud_authorization
                and isinstance(tag, str)
                and self._tag_info(tag)["status"] != "Accepted"
                and self.auth_info.accepted_for_delayed_report(tag, occurred.timestamp(), payload["timestamp"]) is None
            ):
                await self._renew_direct_start(payload, epoch, held_native=True, check_origin=check)
                refreshed = await self.start_reports.identify(payload)
                if refreshed is None or refreshed != native_report:
                    raise DeniedError("the native occurrence changed during cloud authorization")
                native_report = refreshed
            if native_report is None and not self.journal.known_start(payload):
                try:
                    await self.start_reports.verify_station()
                except DeniedError, UnavailableError, SecurityError:
                    if not self.journal.known_start(payload):
                        raise
            check()
            if self.journal.get("control", {}).get("ticket") != ticket and not self.journal.known_start(payload):
                raise DeniedError("a newer owner operation superseded the reported start")
        return await self.decide_report(lambda: commit(native_report, ticket))

    # Validate the report and commit its command/native binding with the new transaction.
    def _record_start(
        self, token: str, payload: dict[str, Any], occurred: datetime, native_report: NativeStartReport | None
    ) -> dict[str, Any]:
        integer(payload.get("connectorId"), 1, 1)
        number(payload.get("meterStart"), 1e12)
        tag = payload.get("idTag")
        if not isinstance(tag, str):
            raise DeniedError("a reported start requires a tag")
        known = self.journal.known_start(payload)
        historical = None
        if (
            not known
            and self.policy.strict_cloud_authorization
            and (native_report is not None or self.start_reports is not None and self.start_reports.permits_held(tag))
        ):
            historical = self.auth_info.accepted_for_delayed_report(tag, occurred.timestamp(), payload["timestamp"])
        if not known and not self._tag_allowed(
            tag, lifecycle=True, historical=historical is not None, native_report=native_report is not None
        ):
            raise DeniedError("tag is not approved for local authorization")

        # Permission binding and the pending flag belong to the very same commit as the new Start fact.
        def accepted(local: int) -> None:
            if native_report is not None:
                native_report.record(local)
            elif self.start_reports is None or not self.start_reports.defer_binding(local, payload["idTag"]):
                self.authorisation.consume(payload["idTag"], local)
                self.controls.bind_start(payload["idTag"], local)
            if self.start_reports is not None:
                self.start_reports.recorded(local, payload)

        info = historical or (self._tag_info(tag) if self.policy.strict_cloud_authorization and not known else None)
        if info is not None:
            if info["status"] != "Accepted":
                raise DeniedError("authorization changed before accepting the new session")
            if historical is None and not self._local_tag(tag):
                self.auth_info.require_dynamic(info)
        answer = self.journal.start(token, payload, accepted, info)
        self.journal.outcomes.retain_bound_owner_start()
        return answer

    # Commit the newest Boot in arrival order, with no task or await able to publish older proof over it.
    def _boot_reply(self, token: str, payload: dict[str, Any]) -> str:
        try:
            previous_boot = self.journal.get("boot")
            if previous_boot is not None and previous_boot != payload:
                self._restrict_trial_introduction()
                self.controls.invalidate()
            with self.journal.atomic():
                self._observations.boot(self._charger_epoch)
                self.journal.put("boot", payload)
            if previous_boot != payload:
                # A later return to the same body cannot erase an intervening introduction revocation.
                self.boot_metadata_revision += 1
            self._pending_boot = uuid.uuid4().hex
            self.controls.forget_stop_ack()
            self._boot_retry.clear()
        except DeniedError, ConflictError:
            return build_call_error(
                token, "PropertyConstraintViolation", "request conflicts with local policy or state"
            )
        except (UnavailableError, RecoveryRequiredError, SecurityError, sqlite3.Error) as error:
            if isinstance(error, sqlite3.Error):
                self._boot_retry.set()
            return build_call_error(token, "InternalError", "outcome unavailable; success was not confirmed")
        return build_call_result(
            token,
            {"status": "Accepted", "interval": 60, "currentTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")},
        )

    # Recheck the sender's generation in the turn that writes its report.
    def _commit_charger_report(
        self, epoch: int, token: str, action: str, payload: dict[str, Any], observation_generation: int | None
    ) -> dict[str, Any]:
        self._require_epoch("charger", epoch)
        # Even Heartbeat requires a readable journal.
        self.journal.get("control", {}).get("ticket")
        return self._local_answer(token, action, payload, observation_generation)

    # Accept local lifecycle messages while leaving opaque exchanges to their actual vendor.
    def _local_answer(
        self, token: str, action: str, payload: dict[str, Any], observation_generation: int | None
    ) -> dict[str, Any]:
        if action == "Heartbeat":
            return {"currentTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}
        if action == "Authorize":
            tag = payload.get("idTag")
            allowed = self._tag_allowed(tag)
            return {"idTagInfo": {"status": "Accepted" if allowed else "Invalid"}}
        if action == "StopTransaction":
            integer(payload.get("transactionId"))
            number(payload.get("meterStop"), 1e12)
            timestamp(payload.get("timestamp"))
            tag = payload.get("idTag")
            info = None
            if self.policy.strict_cloud_authorization and isinstance(tag, str):
                try:
                    info = self.auth_info.get(tag)
                except sqlite3.Error:
                    if self.journal.db.in_transaction:
                        raise
                    # Tag information is optional. The known transaction still needs its exact Stop recorded.
            if info is not None and info["status"] == "Accepted" and isinstance(tag, str) and not self._local_tag(tag):
                try:
                    self.auth_info.require_dynamic(info)
                except RecoveryRequiredError:
                    info = None
            response = {"idTagInfo": info} if info is not None else None
            return self.journal.stop(token, payload, self.authorisation.retire, response)
        if action in OBSERVATION_ACTIONS:
            assert observation_generation is not None
            validate_observation(action, payload, self.journal, check_history=False)
            with self.journal.atomic():
                published = self._observations.accept(
                    self._charger_epoch, observation_generation, token.partition(":")[2], action, payload
                )
                if published and action == "StatusNotification":
                    self.journal.put("last_status", payload)
                elif published and action == "MeterValues" and type(payload.get("transactionId")) is int:
                    rows = payload.get("meterValue", [])
                    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                        observed = timestamp(rows[0].get("timestamp"))
                        if abs((datetime.now(UTC) - observed).total_seconds()) < 60:
                            self.journal.db.execute(
                                "UPDATE transactions SET live=1 WHERE local=? AND stop IS NULL",
                                (payload["transactionId"],),
                            )
            return {}
        raise DeniedError("unsupported charger request")

    # Local stop overrides both the static allowlist and an app-generated trusted vendor start grant.
    def _tag_allowed(
        self,
        tag: Any,
        *,
        lifecycle: bool = False,
        historical: bool = False,
        native_report: bool = False,
        issuing_grant: str | None = None,
    ) -> bool:
        if not isinstance(tag, str):
            return False
        inhibited = self.journal.get("control", {}).get("inhibit")
        phase = self.journal.phase.read()
        held_report = (
            lifecycle
            and inhibited
            and (native_report or self.start_reports is not None and self.start_reports.permits_held(tag))
        )
        if (
            self.retiring()
            and not (held_report or lifecycle and native_report)
            or inhibited
            and not held_report
            or self.journal.phase.starting
            or not phase.legacy_active
            and not (phase.value == "recovering" and held_report)
        ):
            return False
        # A confirmed held report consumes its original receipt without reviving a tag for new Starts.
        selected = (
            self._local_tag(tag)
            or self.policy.trust_vendor_start_tags
            and (held_report or self.authorisation.allows(tag, issuing=issuing_grant))
        )
        if not selected or not self.policy.strict_cloud_authorization:
            return bool(selected)
        if held_report and historical:
            return True
        info = self.auth_info.get(tag)
        if info is not None and info["status"] == "Accepted" and not self._local_tag(tag):
            try:
                self.auth_info.require_dynamic(info)
            except RecoveryRequiredError:
                return False
        return (self._local_tag(tag) and info is None) or (info is not None and info["status"] == "Accepted")

    # Lookup is case-insensitive while journal facts and outbound OCPP preserve their original tag text.
    def _local_tag(self, tag: str) -> bool:
        return any(tag.casefold() == approved.casefold() for approved in self.policy.allowed_tags)

    # An explicit owner Start may release a completed hold, but cannot bypass revocation or recovery mode.
    def _local_start_allowed(self, tag: str) -> bool:
        if not self._local_tag(tag) or not self.journal.phase.read().legacy_active or self.journal.phase.starting:
            return False
        info = self.auth_info.get(tag)
        return info is None or info["status"] == "Accepted"

    # Only a locally aged Accepted decision is renewable by owner Start; explicit cloud denials remain binding.
    def local_start_expired(self, tag: str) -> bool:
        if not self._local_tag(tag) or not self.journal.phase.read().legacy_active or self.journal.phase.starting:
            return False
        info = self.auth_info.stored(tag)
        return bool(
            info and info["status"] == "Accepted" and "expiryDate" in info and expiry(info["expiryDate"]) <= time.time()
        )

    # Owner-approved local permissions have explicit local expiry, distinct from actual cloud metadata.
    def _tag_info(self, tag: str) -> dict[str, Any]:
        info = self.auth_info.get(tag)
        if info is not None:
            return info
        return self.auth_info.local() if self._local_tag(tag) else {"status": "Invalid"}

    # A native daily schedule can renew a cloud decision that expired or previously denied permission.
    async def _renew_direct_start(
        self,
        payload: dict[str, Any],
        epoch: int,
        *,
        held_native: bool = False,
        check_origin: Callable[[], None] | None = None,
    ) -> None:
        tag = payload.get("idTag")
        if (
            not isinstance(tag, str)
            or not self._local_tag(tag)
            or self.retiring()
            and not held_native
            or self.vendor.sender is None
            or self.journal.known_start(payload)
            or self.journal.active() is not None
            or self.journal.get("control", {}).get("inhibit")
            and not held_native
            or self.journal.phase.starting
            or not self.journal.phase.read().legacy_active
            and not (held_native and self.journal.phase.read().value == "recovering")
            or self.authorisation.allows(tag)
        ):
            return
        stored = self.auth_info.stored(tag)
        if stored is None or self._tag_info(tag)["status"] == "Accepted":
            return
        integer(payload.get("connectorId"), 1, 1)
        number(payload.get("meterStart"), 1e12)
        ticket = self.journal.get("control", {}).get("ticket")

        # Work from a retired plug connection or superseded owner operation cannot renew current authority.
        def guard() -> None:
            self._require_epoch("charger", epoch)
            if check_origin is not None:
                check_origin()
            if (
                self.journal.get("control", {}).get("ticket") != ticket
                or self.auth_info.stored(tag) != stored
                or self.retiring()
                and not held_native
                or held_native
                and (not self.controls.ready or self.controls.desired() != 0)
            ):
                raise DeniedError("scheduled start authority changed during renewal")

        # Share even a timeout or unchanged denial across this burst of retries. A later burst may retry.
        key = (tag.casefold(), epoch, self._vendor_epoch, ticket, self.boot_observation)
        pending = self._start_renewals.get(key)
        if pending is not None:
            if not await asyncio.shield(pending):
                raise UnavailableError("scheduled start renewal was unavailable")
            return
        pending = asyncio.get_running_loop().create_future()
        self._start_renewals[key] = pending
        received = False
        try:
            try:
                await self._cloud_authorize(tag, guard=guard)
            except DeniedError:
                # An ordinary Authorize can publish a fresh decision while this Start waits for the vendor gate.
                self._require_epoch("charger", epoch)
                self._require_epoch("vendor", key[2])
                current = self.auth_info.get(tag)
                if (
                    self.journal.get("control", {}).get("ticket") != ticket
                    or current is None
                    or current["status"] != "Accepted"
                    or not held_native
                    and not self._tag_allowed(tag)
                ):
                    raise
            received = True
        finally:
            pending.set_result(received)
            del self._start_renewals[key]

    # Preserve the actual correlated backend decision, checking generation after every await.
    async def _cloud_authorize(self, tag: str, guard: Callable[[], None] | None = None) -> dict[str, Any]:
        epoch = self._vendor_epoch
        await self._ensure_vendor_boot()
        self._require_epoch("vendor", epoch)

        # Recheck both transports and any owner operation after waiting for the outbound gate.
        def check() -> None:
            self._require_epoch("vendor", epoch)
            if guard is not None:
                guard()

        info: dict[str, Any] | None = None

        # A complete authorization result contains only the validated decision.
        def decision(answer: Frame) -> dict[str, Any]:
            if answer[0] != 3 or set(answer[2]) != {"idTagInfo"}:
                raise UnavailableError("actual cloud authorization was unavailable")
            return validate_info(answer[2]["idTagInfo"])

        # Capture an actual denial before the sending task can be cancelled or lose authority.
        def capture(_action: str, answer: Frame) -> None:
            nonlocal info
            if self._vendor_epoch != epoch:
                return
            try:
                actual = decision(answer)
            except UnavailableError, DeniedError:
                return  # The caller still receives the ordinary validation failure.
            if actual["status"] != "Accepted":
                info = self.auth_info.deny(tag, actual)

        # Positive authorization still depends on the originating owner and connection.
        def received(_action: str, answer: Frame) -> None:
            nonlocal info
            self._require_epoch("vendor", epoch)
            if info is None:
                actual = decision(answer)
                if actual["status"] == "Accepted":
                    check()
                    info = self.auth_info.put(tag, actual)
                else:
                    info = self.auth_info.deny(tag, actual)
            if info["status"] != "Accepted":
                check()

        await self.vendor.call(
            "Authorize", {"idTag": tag}, priority=5, check=check, on_receive=capture, on_reply=received
        )
        assert info is not None
        return info

    # Nested Authorize during a forwarded RemoteStart uses its preflighted grant without acquiring the vendor gate.
    async def _authorize_tag(self, tag: Any) -> dict[str, Any]:
        if (
            not isinstance(tag, str)
            or self.retiring()
            or self.journal.get("control", {}).get("inhibit")
            or not self.journal.phase.read().legacy_active
        ):
            return {"status": "Invalid"}
        if self.authorisation.allows(tag):
            if not self._local_tag(tag) and not self._tag_allowed(tag):
                info = self._tag_info(tag)
                return info if info.get("status") != "Accepted" else {"status": "Invalid"}
            return self._tag_info(tag)
        if not self._local_tag(tag):
            return {"status": "Invalid"}
        if self.vendor.sender is not None and self.vendor_registration == "Accepted":
            return await self._cloud_authorize(tag)
        return self._tag_info(tag)

    # Opaque writes obey the same owner lock as standard mutations.
    def _extension_allowed(self, payload: dict[str, Any]) -> None:
        kind = self.policy.kind("DataTransfer", payload)
        if kind == "write" and self.journal.get("control", {}).get("owner") == "local":
            raise DeniedError("local owner blocks vendor extension writes")

    # Forward a real vendor request and its exact result/error instead of inventing successful acknowledgements.
    async def _vendor_call(self, frame: Frame, epoch: int, charger_scope: ControlScope | None) -> str:
        token, action = str(frame[1]), str(frame[2])
        original: dict[str, Any] = frame[3]
        mutation: str | None = None
        query: TransactionQuery | None = None
        ics_write: IcsWrite | None = None
        grant: str | None = None
        profile_write: VendorProfileWrite | None = None
        translated: dict[str, Any] | None = None
        profile_reply: VendorProfileReply | None = None
        forwarded_reply: ForwardedReply | None = None
        other_reply: VendorWriteReply | None = None
        other_answer: Frame | None = None
        dispatched = False
        charger_epoch = self._charger_epoch
        wire = "c-" + uuid.uuid4().hex
        priority = CommandPriority.RESTRICT if action == "RemoteStopTransaction" else CommandPriority.NORMAL

        # From this boundary onward even a sender exception can follow a physical transmission.
        def on_dispatch(_action: str) -> None:
            nonlocal dispatched
            if mutation is not None:
                with self.journal.atomic():
                    self.journal.record_vendor_result(mutation, "dispatched")
                    if grant is not None:
                        self.authorisation.issue(grant)
            dispatched = True

        # The peer records the actual answer before another owner or a failed local write can supersede it.
        def received(received_action: str, answer: Frame) -> None:
            nonlocal forwarded_reply, profile_reply, other_reply, other_answer
            if not dispatched or mutation is None:
                return
            if received_action in TRANSACTION_COMMANDS and answer[0] == 3:
                forwarded_reply = self.journal.outcomes.capture(mutation, received_action, wire, answer)
                if forwarded_reply is not None:
                    self.journal.outcomes.publish(forwarded_reply)
                return
            if received_action in PROFILE_COMMANDS and answer[0] == 3:
                status = profile_status(received_action, answer)
                if status is None:
                    return
                if status == "Accepted" and translated is not None:
                    profile_reply = VendorProfileReply(
                        mutation,
                        wire,
                        received_action,
                        translated,
                        answer,
                        profile_write,
                        self.charger.after_result is not None,
                    )
                    self.journal.outcomes.save_profile_reply(profile_reply)
                    return
            try:
                plan_accepted = ics_write is not None and ics_write.accepted(answer)
            except UnavailableError:
                return
            status = "call_error"
            if answer[0] == 3:
                reported = answer[2].get("status")
                status = reported if reported in ("Accepted", "Rejected", "NotSupported", "Unknown") else "reply"
            other_answer = answer
            other_reply = VendorWriteReply(
                mutation,
                received_action,
                wire,
                status,
                ics_write.plans if plan_accepted and ics_write is not None else (),
                ics_write.clearing if ics_write is not None else False,
            )
            self.journal.outcomes.save_vendor_write(other_reply)

        # The outcome owner handles an accepted forwarded profile, including its resource update.
        def owns_profile_reply(_action: str, _answer: Frame) -> bool:
            return profile_reply is not None

        # Finalize captured replies independently of the handler's ability to return on the original socket.
        def finish() -> None:
            if profile_reply is not None:
                self.journal.outcomes.publish_profile(profile_reply)
            elif other_reply is not None:
                if self.journal.outcomes.awaits_storage(other_reply.mutation):
                    self.journal.outcomes.save_vendor_write(other_reply)
            elif forwarded_reply is None:
                self._finish_vendor_dispatch(action, original, mutation, grant, profile_write, dispatched)
            elif not self.journal.outcomes.publish(forwarded_reply):
                self.journal.outcomes.publish(forwarded_reply)

        try:
            self.policy.kind(action, original)
            self.policy.validate(action, original)
            self._check_vendor(action, original, require_cap=False)
            write = self.policy.kind(action, original) == "write"
            supervised = write and self.journal.phase.read().supervised
            connection = charger_scope if supervised else None
            positive = supervised and action != "RemoteStopTransaction"
            intent = self.journal.get("managed_control_intent") if positive else None
            ticket = self.journal.get("control", {}).get("ticket") if positive else None
            vendor_transaction = self._vendor_write_transaction(action, original)
            clear_key = str(original["id"]) if action == "ClearChargingProfile" else None
            clear_target = self.journal.get("vendor_profiles", {}).get(clear_key) if clear_key is not None else None
            local_transaction = None
            if vendor_transaction is not None:
                local_transaction = self.journal.local_id(vendor_transaction)
                if local_transaction is None:
                    raise DeniedError("cloud command does not name an active transaction")

            # A cloud number can be reassigned while this command waits for the charger or native evidence.
            def require_transaction() -> None:
                if vendor_transaction is not None and self.journal.local_id(vendor_transaction) != local_transaction:
                    raise DeniedError("cloud command no longer names its original local transaction")
                if clear_key is not None and self.journal.get("vendor_profiles", {}).get(clear_key) != clear_target:
                    raise DeniedError("cloud clear no longer owns its original profile")

            # Preflight reads and authentication use the originating connections and command target.
            def require_origin() -> None:
                self._require_epoch("vendor", epoch)
                self._require_epoch("charger", charger_epoch)
                if supervised and not control_scope_matches(self, connection):
                    raise UnavailableError("cloud command belongs to a retired charger Boot")
                if positive and (
                    self.retiring()
                    or self.journal.get("managed_control_intent") != intent
                    or self.journal.get("control", {}).get("ticket") != ticket
                ):
                    raise DeniedError("cloud command no longer owns its original control scope")
                require_transaction()

            # The existing mutation owns the bound request and its translated physical target.
            def prepare_mutation(profile: int | None) -> str:
                token_id = self.journal.mutation_intent("vendor", action, original)
                self.journal.bind_vendor_command(
                    token_id,
                    request=token,
                    vendor_epoch=epoch,
                    boot=connection[0].boot if connection else None,
                    wire=wire,
                    local=local_transaction,
                    profile=profile,
                    new_profile=profile_write is not None and profile_write.allocated,
                )
                return token_id

            if supervised and self.command_processor is not None:
                await self.command_processor.run(require_origin, priority=priority)
            if write and action != "RemoteStopTransaction":
                await self.controls.ensure_ceiling(guard=require_origin)
            if action == "RemoteStartTransaction" and self.policy.strict_cloud_authorization:
                require_origin()
                info = await self._cloud_authorize(original["idTag"], guard=require_origin)
                if info["status"] != "Accepted":
                    raise DeniedError("cloud refused the trial start tag")
                if not self._local_tag(original["idTag"]):
                    self.auth_info.require_dynamic(info)

            # Allocate profile ids and mutation intent only after the in-gate policy check succeeds.
            def prepare() -> dict[str, Any]:
                nonlocal mutation, query, ics_write, grant, profile_write, translated
                require_origin()
                self._check_vendor(action, original)
                with self.journal.atomic():
                    if action == "DataTransfer" and is_transaction_query(original):
                        query = TransactionQuery.from_request(original, self.journal.historical_local_id)
                        payload = query.request(original)
                    elif action == "DataTransfer" and known_write(original):
                        ics_write = IcsWrite.prepare(original, self.policy, self.journal, self.plans)
                        payload = ics_write.payload
                    else:
                        old_profiles = self.journal.get("vendor_profiles", {})
                        payload = self._translated_command(action, original)
                        if action == "SetChargingProfile":
                            vendor_profile = str(original["csChargingProfiles"]["chargingProfileId"])
                            actual_profile = payload["csChargingProfiles"]
                            profile_write = VendorProfileWrite(
                                vendor_profile,
                                actual_profile["chargingProfileId"],
                                actual_profile.get("transactionId"),
                                vendor_profile not in old_profiles,
                            )
                    if action == "RemoteStartTransaction":
                        if self.policy.strict_cloud_authorization:
                            info = self.auth_info.get(original["idTag"])
                            if info is None or info["status"] != "Accepted":
                                raise DeniedError("trial authorization expired before forwarding")
                            if not self._local_tag(original["idTag"]):
                                self.auth_info.require_dynamic(info)
                        grant = self.authorisation.reserve(original["idTag"], prepared=True)
                    if self.policy.kind(action, original) == "write" and action != "ClearChargingProfile":
                        mutation = prepare_mutation(profile_write.local if profile_write is not None else None)
                        if grant is not None:
                            current_grant = self.journal.get("vendor_start_grant", {})
                            self.journal.put("vendor_start_grant", {**current_grant, "mutation": mutation})
                translated = payload
                return payload

            # Native validation awaits HTTP after preparation; revoke stale origin, ownership or tag authority then.
            def check() -> None:
                require_origin()
                self._check_vendor(action, original, own_mutation=mutation)
                if mutation is not None:
                    row = self.journal.db.execute(
                        "SELECT origin,action,digest,state FROM mutations WHERE token=?", (mutation,)
                    ).fetchone()
                    if row is None or tuple(row) != ("vendor", action, fingerprint(original), "pending"):
                        raise DeniedError("this vendor request no longer owns its pending mutation")
                if action == "RemoteStartTransaction":
                    if self.policy.strict_cloud_authorization:
                        info = self.auth_info.get(original["idTag"])
                        if info is None or info["status"] != "Accepted":
                            raise DeniedError("cloud authorization expired during native validation")
                        if not self._local_tag(original["idTag"]):
                            self.auth_info.require_dynamic(info)
                    if grant is not None:
                        saved = self.journal.get("vendor_start_grant", {})
                        if (
                            saved.get("token") != grant
                            or saved.get("local") is not None
                            or not self.authorisation.allows(original["idTag"], issuing=grant)
                            or not self._tag_allowed(original["idTag"], issuing_grant=grant)
                        ):
                            raise DeniedError("this vendor start no longer owns its current tag grant")

            # Native clear validation must see the existing lifecycle debt before this clear is recorded.
            def prepare_clear() -> None:
                from .trial_rollback import require_lifecycle_idle

                nonlocal mutation
                if self.journal.get("native_bounded_handover"):
                    require_lifecycle_idle(self)
                with self.journal.atomic():
                    mutation = prepare_mutation(clear_target)

            answer = await self.charger.call(
                action,
                prepare,
                priority=priority,
                check=check,
                token=wire,
                before_send=prepare_clear if action == "ClearChargingProfile" else None,
                on_dispatch=on_dispatch,
                on_reply=owns_profile_reply if action in PROFILE_COMMANDS else None,
                on_receive=received if write else None,
                keep_late_reply=action in TRANSACTION_COMMANDS,
            )
            if write and forwarded_reply is None and profile_reply is None and other_reply is None:
                raise UnavailableError("charger write did not return a recognized outcome")
            return reply_as(query.response(answer) if query is not None else answer, token)
        except DeniedError, ConflictError:
            text = build_call_error(token, "SecurityError", "request is not allowed by current local policy")
        except UnavailableError, RecoveryRequiredError, SecurityError, sqlite3.Error:
            if forwarded_reply is not None:
                return build_call_result(token, {"status": forwarded_reply.status})

            if profile_reply is not None:
                return reply_as(profile_reply.answer, token)
            if other_answer is not None:
                return reply_as(other_answer, token)
            text = build_call_error(token, "InternalError", "charger outcome is unconfirmed; request was not replayed")
        finally:
            self.decide(finish, priority=priority)
        return text

    # Retire only proven-unsent work; cancellation or any error after dispatch retains physical uncertainty.
    def _finish_vendor_dispatch(
        self,
        action: str,
        original: dict[str, Any],
        mutation: str | None,
        grant: str | None,
        profile: VendorProfileWrite | None,
        dispatched: bool,
    ) -> None:
        if mutation is None and grant is None and profile is None:
            return
        if not dispatched:
            saved = self.journal.outcomes.retire_unsent(mutation, grant, profile)
        else:
            saved = mutation is None or self.journal.outcomes.record_unanswered(mutation, action, fingerprint(original))
        if not saved:
            self._record_failure("JournalUnavailable")

    # Recheck priority, owner and current transaction after acquiring the outgoing CALL gate.
    def _check_vendor(
        self, action: str, payload: dict[str, Any], require_cap: bool = True, *, own_mutation: str | None = None
    ) -> None:
        kind = self.policy.kind(action, payload)
        if kind == "write" and self.vendor_registration != "Accepted":
            raise UnavailableError("cloud controls await completed registration")
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (self.retiring() or not self.journal.phase.read().legacy_active or self.journal.phase.starting)
        ):
            raise DeniedError("trial vendor controls are frozen")
        control = self.journal.get("control", {})
        if kind == "write" and action != "RemoteStopTransaction":
            if self.journal.current_change.read().recovering:
                raise DeniedError("cloud writes await settlement of the current selection")
            if own_mutation is None:
                self.journal.require_vendor_writes_reconciled()
            elif self.journal.db.execute(
                "SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed' AND token!=? LIMIT 1",
                (own_mutation,),
            ).fetchone():
                raise RecoveryRequiredError("another vendor write still requires reconciliation")
            if require_cap and not self.controls.ready:
                raise UnavailableError("station ceiling is not confirmed for this charger connection")
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (control.get("owner") == "local" or control.get("inhibit"))
        ):
            raise DeniedError("explicit local control has priority")
        if action == "RemoteStartTransaction" and (
            self.journal.active() is not None or self.journal.get("control", {}).get("start_pending")
        ):
            raise DeniedError("a transaction is already unresolved on this connector")
        if action == "RemoteStartTransaction" and self.journal.current_change.read().selecting:
            raise DeniedError("cloud Start awaits verification of the current selection")

    # Identify only cloud writes that translate a cloud transaction ID to the current local session.
    @staticmethod
    def _vendor_write_transaction(action: str, payload: dict[str, Any]) -> int | None:
        if action == "RemoteStopTransaction":
            return integer(payload["transactionId"])
        if action == "SetChargingProfile":
            vendor = payload["csChargingProfiles"].get("transactionId")
            return None if vendor is None else integer(vendor)
        if (
            action == "DataTransfer"
            and payload.get("vendorId") == "IoCharger"
            and payload.get("messageId") in ("setTransaction", "delayStop")
        ):
            return integer(data_value(payload["data"])["transactionId"])
        return None

    # A retired alias needs a new live slot before an explicit request can recreate its independently absent profile.
    def _vendor_profile_retired(self, local: int) -> bool:
        if not self.journal.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trial_resources'"
        ).fetchone():
            return False
        row = self.journal.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(local),)
        ).fetchone()
        return bool(row is not None and row[0] == "absent_verified")

    # Retain every alias identity while independently absent trial profiles stop consuming the live namespace bound.
    def _vendor_profile_capacity(self, profiles: dict[str, int]) -> bool:
        if not self.journal.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trial_resources'"
        ).fetchone():
            return len(profiles) < self.journal.capacity
        absent = {
            row[0] for row in self.journal.db.execute("SELECT key FROM trial_resources WHERE state='absent_verified'")
        }
        return sum("profile:" + str(local) not in absent for local in profiles.values()) < self.journal.capacity

    # Translate session ids in both control directions and isolate vendor profile ids from local profile 7001.
    def _translated_command(self, action: str, original: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = json.loads(encoded(original))
        if action == "RemoteStopTransaction":
            local = self.journal.local_id(payload["transactionId"])
            if local is None:
                raise DeniedError("unknown cloud transaction")
            payload["transactionId"] = local
        if action in ("SetChargingProfile", "ClearChargingProfile"):
            profiles = self.journal.get("vendor_profiles", {})
            profile = payload.get("csChargingProfiles", {})
            original_id = profile["chargingProfileId"] if action == "SetChargingProfile" else payload["id"]
            key = str(original_id)
            if key not in profiles:
                if action == "ClearChargingProfile":
                    raise DeniedError("cloud does not own this profile")
                if not self._vendor_profile_capacity(profiles):
                    raise DeniedError("vendor profile namespace is full")
                local_profile = int(self.journal.get("next_vendor_profile", 99999)) + 1
                if local_profile >= 2**31:
                    raise RecoveryRequiredError("vendor profile counter exhausted")
                self.journal.put("next_vendor_profile", local_profile)
                profiles[key] = local_profile
                self.journal.put("vendor_profiles", profiles)
            elif (
                action == "SetChargingProfile"
                and self._vendor_profile_retired(profiles[key])
                and not self._vendor_profile_capacity(profiles)
            ):
                raise DeniedError("vendor profile namespace is full")
            if action == "SetChargingProfile":
                profile["chargingProfileId"] = profiles[key]
                if "transactionId" in profile:
                    local = self.journal.local_id(profile["transactionId"])
                    if local is None:
                        raise DeniedError("profile names an unknown cloud transaction")
                    profile["transactionId"] = local
            else:
                payload["id"] = profiles[key]
        return payload

    # The dedicated local service serializes safety-cap and command sequences under one priority gate.
    async def set_limit(self, value: Any) -> Frame:
        return await self.controls.set_limit(value)

    # An explicit local start keeps any previous lower ceiling and restores zero if its outcome is uncertain.
    async def start(self, tag: str = "LOCAL_TEST") -> Frame:
        if self.policy.strict_cloud_authorization and not self._local_start_allowed(tag):
            raise DeniedError("current authorization does not permit a new local start")
        return await self.controls.start(tag)

    # One exact-scoped owned Start publishes authority in its child, while retaining truthful failed wire outcomes.
    async def start_managed_result(
        self,
        tag: str,
        guard: Callable[[], None],
        *,
        request_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
        handoff: StartHandoff | None = None,
    ) -> OwnedStartResult:
        connection = current_control_scope(self)

        # Restrictive rollback retains the exact source even after the positive management grant is revoked.
        def source() -> None:
            if not control_scope_matches(self, connection):
                raise UnavailableError("managed Start belongs to a superseded Boot observation")

        # Revocation or a newer Pause can invalidate authorization while native evidence is awaited.
        def check() -> None:
            guard()
            if not self._local_start_allowed(tag):
                raise DeniedError("managed test tag authorization is no longer current")

        check()
        return await self.controls.start_managed_result(
            tag,
            check,
            request_id=request_id,
            on_prepared=on_prepared,
            scope_guard=source,
            scope=connection[0] if connection is not None else None,
            handoff=handoff,
        )

    # A local stop installs a durable zero-current hold even when there is no known transaction to stop.
    async def stop(self) -> Frame:
        if not self.journal.get("managed_mode"):
            return await self.controls.stop()
        connection, local = current_control_scope(self), self.journal.active()

        # An introduction Stop cannot adopt a different transaction while it waits for its restrictive turn.
        def guard() -> None:
            if (
                self.retiring()
                or not control_scope_matches(self, connection)
                or self.journal.active() not in (None, local)
            ):
                raise UnavailableError("local Stop belongs to a superseded Boot or transaction")

        return await self.controls.stop(expected_local=local, guard=guard)

    # Releasing ownership confirms the physical resume ceiling before permitting vendor writes again.
    async def release(self, acknowledge_uncertainty: bool = False) -> None:
        await self.controls.release(acknowledge_uncertainty)

    # Introduce the virtual charge point once per cloud connection without blocking the local receiver.
    async def _ensure_vendor_boot(self) -> None:
        epoch = self._vendor_epoch
        async with self._boot_lock:
            self._require_epoch("vendor", epoch)
            if self._vendor_ready:
                return
            remaining = self._boot_retry_at - asyncio.get_running_loop().time()
            if remaining > 0:
                raise RegistrationDeferredError(remaining)
            boot = self.journal.get("boot")
            if boot is None:
                raise UnavailableError("charger has not introduced itself")
            answer = await self.vendor.call(
                "BootNotification", boot, priority=5, check=lambda: self._require_epoch("vendor", epoch)
            )
            self.decide(lambda: self._accept_vendor_boot(epoch, answer))

    # Confirm registration against the same vendor connection that sent the request.
    def _accept_vendor_boot(self, epoch: int, answer: Frame) -> None:
        self._require_epoch("vendor", epoch)
        if answer[0] != 3:
            raise UnavailableError("vendor registration returned a protocol error")
        status, interval = answer[2].get("status"), answer[2].get("interval")
        if status not in ("Accepted", "Pending", "Rejected") or type(interval) is not int or not 1 <= interval <= 86400:
            raise UnavailableError("vendor registration response is invalid")
        try:
            timestamp(answer[2].get("currentTime"))
        except DeniedError:
            raise UnavailableError("vendor registration time is invalid") from None
        self.vendor_registration = status
        self.vendor.diagnostics.registration(status)
        self.vendor_interval = interval
        if status != "Accepted":
            self._boot_retry_at = asyncio.get_running_loop().time() + interval
            raise RegistrationDeferredError(float(interval))
        self._vendor_ready = True

    # OCPP heartbeat is independent of charger traffic and even an ambiguous retained transaction.
    async def heartbeat(self) -> None:
        epoch = self._vendor_epoch
        await self._ensure_vendor_boot()
        answer = await self.vendor.call("Heartbeat", {}, priority=5, check=lambda: self._require_epoch("vendor", epoch))
        self.decide(lambda: self._accept_heartbeat(epoch, answer))

    # A heartbeat response applies only to the vendor generation that requested it.
    def _accept_heartbeat(self, epoch: int, answer: Frame) -> None:
        self._require_epoch("vendor", epoch)
        if answer[0] != 3:
            raise UnavailableError("vendor heartbeat failed")
        try:
            timestamp(answer[2].get("currentTime"))
        except DeniedError:
            raise UnavailableError("vendor heartbeat time is invalid") from None

    # Deliver durable events in order; a cloud-accepted Start with a lost reply requires explicit reconciliation.
    async def flush(self, *, can_send: Callable[[], bool] | None = None) -> None:
        epoch = self._vendor_epoch
        async with self._flush_lock:
            self.decide(self.retry_reply_writes)
            self._require_epoch("vendor", epoch)
            await self._ensure_vendor_boot()
            while can_send is None or can_send():
                head = self.decide(self._prepare_delivery)
                if head is None:
                    self.decide(lambda: self.journal.outcomes.retain_delivery(None))
                    return
                item = head
                if self.journal.outcomes.delivery_reply is None and retry_deferred(
                    self.journal, item, self._start_retry_waits
                ):
                    return
                payload = dict(item.payload)
                if item.action in ("MeterValues", "StopTransaction") and "transactionId" in payload:
                    vendor = self.journal.vendor_id(integer(payload["transactionId"]))
                    if vendor is None:
                        raise RecoveryRequiredError("transaction mapping is not known; event retained")
                    payload["transactionId"] = vendor
                if self.journal.outcomes.delivery_reply is None:
                    try:
                        current = item
                        sent = False
                        received_reply: DeliveryReply | None = None
                        await_late = can_send is not None and item.action == "StartTransaction"
                        sender = self.vendor.sender

                        # Persist possible delivery inside the CALL gate, immediately before send.
                        def before_send() -> None:
                            nonlocal sent
                            self._require_epoch("vendor", epoch)
                            if can_send is not None and not can_send():
                                raise RegistrationDeferredError(0)
                            self.journal.mark_sent(current)
                            sent = True

                        # Retain the exact reply and its order before the vendor gate can pass to another caller.
                        def received(_action: str, answer: Frame) -> None:
                            nonlocal received_reply
                            authorization = None
                            if (
                                self.policy.strict_cloud_authorization
                                and isinstance(current.payload.get("idTag"), str)
                                and answer[0] == 3
                                and "idTagInfo" in answer[2]
                            ):
                                authorization = self.auth_info.observe()
                            received_reply = DeliveryReply.received(current, answer, authorization)
                            self.journal.outcomes.capture_delivery(current, received_reply)

                        answer = await self.vendor.call(
                            item.action,
                            payload,
                            token=item.token,
                            before_send=before_send,
                            on_receive=received,
                            keep_late_reply=await_late,
                        )
                    except UnavailableError:
                        if await_late and sent and received_reply is None and self.vendor.sender is sender:
                            self._late_delivery_token = item.token
                            self.decide(lambda: self._interrupted_delivery(item, None, True))
                            raise RecoveryRequiredError("cloud Start is awaiting its original reply") from None
                        self.decide(lambda: self._interrupted_delivery(item, received_reply, sent))
                        raise
                    except asyncio.CancelledError:
                        self.decide(lambda: self._interrupted_delivery(item, received_reply, sent))
                        raise
                    if received_reply is None:
                        reply = DeliveryReply.received(item, answer)
                        self.decide(lambda: self.journal.outcomes.capture_delivery(item, reply))
                try:
                    disposition = self.decide(lambda: self._settle_delivery(item))
                except sqlite3.Error:
                    if item.action == "StartTransaction" and self.delivery_outcome_uncertain:
                        try:
                            self.decide(self._hold_failed_start_delivery, priority=CommandPriority.RESTRICT)
                            await self.controls.ensure_ceiling()
                        except (
                            sqlite3.Error,
                            DeniedError,
                            SecurityError,
                            RecoveryRequiredError,
                            UnavailableError,
                            TimeoutError,
                        ):
                            pass  # Retain the real reply and original storage error even if zero cannot be confirmed.
                    raise  # Keep the actual reply until its local transaction can commit.
                if disposition == "deferred":
                    return
                if disposition == "denied":
                    await self.controls.ensure_ceiling()
                    raise RecoveryRequiredError("cloud denied the mapped local session; zero hold retained")

    # A retiring socket may finish the exact Start already sent, within the original reply timeout.
    async def finish_sent_start(self) -> None:
        item = self.journal.next_delivery()
        if (
            item is None
            or item.action != "StartTransaction"
            or item.state != "inflight"
            or not self._flush_lock.locked()
        ):
            return
        try:
            async with asyncio.timeout(self.vendor.timeout), self._flush_lock:
                pass
        except TimeoutError:
            pass  # Cancellation will leave an unanswered Start in the existing ambiguous state.

    # A pump failure describes availability. The exact retained report and its reply describe its outcome.
    @property
    def delivery_outcome_uncertain(self) -> bool:
        outcome = self.journal.outcomes.delivery()
        return outcome is not None and outcome.uncertain

    # Recover the exact head before deciding whether its original request can be sent.
    def _prepare_delivery(self) -> Delivery | None:
        outcome = self.journal.outcomes.delivery()
        if outcome is None:
            return None
        item = outcome.item
        if outcome.reply is not None:
            self.journal.outcomes.retain_delivery(outcome.reply)
        elif item.state == "inflight" and item.action != "StartTransaction":
            # A failed cleanup write can retain an unanswered retryable head in this process too.
            with self.journal.atomic():
                if (
                    self.journal.next_delivery() != item
                    or self.journal.db.execute("SELECT 1 FROM delivery_replies WHERE token=?", (item.token,)).fetchone()
                ):
                    raise RecoveryRequiredError("cloud retry no longer owns the exact unanswered head")
                self.journal.db.execute("UPDATE outbox SET state='queued' WHERE token=?", (item.token,))
                current_head = self.journal.next_delivery()
                assert current_head is not None
                item = current_head
        elif item.state in ("ambiguous", "inflight", "rejected"):
            if item.action == "StartTransaction" and item.state in ("ambiguous", "inflight"):
                with self.journal.atomic():
                    if item.state == "inflight":
                        self.journal.db.execute("UPDATE outbox SET state='ambiguous' WHERE token=?", (item.token,))
                        current_head = self.journal.next_delivery()
                        assert current_head is not None
                        item = current_head
                    self._latch_late_delivery(item)
                    supersede_unknown_start_retry(self.journal, item)
            elif item.action == "StartTransaction":
                with self.journal.atomic():
                    latch_start_delivery_fault(self.journal)
            raise RecoveryRequiredError("cloud delivery requires explicit reconciliation")
        return item

    # Preserve provenance only for the exact Start that can still receive its original cloud reply.
    def _latch_late_delivery(self, item: Delivery) -> None:
        if item.action == "StartTransaction" and item.token == self._late_delivery_token:
            latch_start_delivery_fault(
                self.journal,
                {
                    "classification": "owner_control_or_delivery_uncertain",
                    "at_epoch": time.time(),
                    "late_delivery_token": item.token,
                },
            )

    # Retain an actual interrupted reply, or distinguish a safely retryable unanswered request.
    def _interrupted_delivery(self, item: Delivery, received_reply: DeliveryReply | None, sent: bool) -> None:
        if received_reply is not None:
            self.journal.outcomes.capture_delivery(item, received_reply)
        elif sent:
            try:
                with self.journal.atomic():
                    self.journal.db.execute(
                        "UPDATE outbox SET state=? WHERE token=?",
                        ("ambiguous" if item.action == "StartTransaction" else "queued", item.token),
                    )
                    current_head = self.journal.next_delivery()
                    if current_head is not None:
                        self._latch_late_delivery(current_head)
                        supersede_unknown_start_retry(self.journal, current_head)
            except sqlite3.Error:
                # Preserve cancellation/peer failure; recover the unanswered head on the next flush.
                pass

    # Persist the actual receipt before applying it so either storage boundary can be retried.
    def _settle_delivery(self, item: Delivery) -> str:
        reply = self.journal.outcomes.delivery_reply
        assert reply is not None
        try:
            reply.save(self.journal, self.journal.next_delivery() or item)
            disposition = self._record_delivery_reply(item, reply)
        except DeniedError, ConflictError, RecoveryRequiredError:
            if item.action == "StartTransaction":
                with self.journal.atomic():
                    head = self.journal.next_delivery()
                    if head is not None and head.token == item.token and head.state == "inflight":
                        self.journal.db.execute("UPDATE outbox SET state='ambiguous' WHERE token=?", (item.token,))
                        current_head = self.journal.next_delivery()
                        assert current_head is not None
                        supersede_unknown_start_retry(self.journal, current_head)
            self.journal.outcomes.retain_delivery(None)
            raise RecoveryRequiredError("cloud reply requires exact delivery reconciliation") from None
        self.journal.outcomes.retain_delivery(None)
        return disposition

    # Keep a failed Start delivery under a durable hold before verifying the physical ceiling.
    def _hold_failed_start_delivery(self) -> None:
        with self.journal.atomic():
            latch_start_delivery_fault(self.journal)

    # Apply only a real correlated reply; storage failures leave its single bounded receipt available for retry.
    def _record_delivery_reply(self, item: Delivery, reply: DeliveryReply) -> str:
        answer = reply.answer
        if answer[0] != 3:
            if item.action in OBSERVATION_ACTIONS:
                self.journal.reject_observation(item, str(answer[2]))
                return "acknowledged"
            if item.action == "StopTransaction":
                reject_stop_reply(self.journal, item, answer, reason="cloud_call_error")
                raise RecoveryRequiredError("cloud rejected a retained lifecycle event")
            if item.action == "StartTransaction" and not record_start_error(self.journal, item, answer):
                return "deferred"
            raise RecoveryRequiredError("cloud rejected a retained lifecycle event")
        if item.action == "StopTransaction" and not stop_result_shape_supported(answer[2]):
            reject_stop_reply(self.journal, item, answer[2], reason="response_shape_invalid")
            raise RecoveryRequiredError("cloud Stop result has unsupported fields")
        metadata: dict[str, Any] | None = None
        tag = item.payload.get("idTag")
        if self.policy.strict_cloud_authorization and "idTagInfo" in answer[2]:
            try:
                metadata = validate_info(answer[2]["idTagInfo"])
            except DeniedError:
                if item.action != "StopTransaction":
                    if item.action != "StartTransaction":
                        self.journal.reject_observation(item, "invalid_authorization_metadata")
                        return "acknowledged"
                    raise
                if not approved_stop_metadata(self.journal, item, answer[2]):
                    reject_stop_reply(self.journal, item, answer[2], reason="authorization_metadata_invalid")
                    raise RecoveryRequiredError(
                        "cloud Stop metadata requires exact independent reconciliation"
                    ) from None
        denied = item.action == "StartTransaction" and metadata is not None and metadata["status"] != "Accepted"

        # Cache updates and deletion of this durable event share its actual acknowledgement commit.
        def acknowledged() -> None:
            if metadata is not None and isinstance(tag, str):
                self.auth_info.put_delivery(tag, metadata, reply.authorization, restored=reply.restored)
            if denied:
                record_start_denial(self.journal, item, answer[2])
            elif item.action == "StartTransaction":
                self.journal.retire_reply_fault("late_delivery_token", item.token)
            record_stop_ack(self.journal, item, answer[2])
            record_start_wire_ack(self.journal, item, answer[2])

        try:
            self.journal.acknowledge(item, answer[2], acknowledged, allow_denied=denied)
        except ConflictError, RecoveryRequiredError:
            if item.action == "StartTransaction":
                with self.journal.atomic():
                    self.journal.db.execute("UPDATE outbox SET state='ambiguous' WHERE token=?", (item.token,))
                    current_head = self.journal.next_delivery()
                    assert current_head is not None
                    supersede_unknown_start_retry(self.journal, current_head)
            raise
        return "denied" if denied else "acknowledged"
