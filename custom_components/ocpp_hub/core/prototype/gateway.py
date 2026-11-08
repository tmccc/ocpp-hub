"""Local authority with durable cloud delivery and explicitly approved vendor passthrough.

This is an offline engine, not a Home Assistant setup path. It reuses OCPP Hub's
frames and charging-profile builders while replacing its volatile mirror boundary.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from ..frames import Frame, build_call_error, build_call_result, parse_frame
from .events import validate_observation
from .policy import DeniedError, Policy, integer, number
from .rpc import InboundCache, Peer, Sender, UnavailableError, reply_as
from .store import ConflictError, Journal, RecoveryRequiredError, encoded, fingerprint


# Reject malformed or non-finite input before handing it to the upstream frame vocabulary.
def decode(raw: str | bytes) -> Frame:
    if not isinstance(raw, str) or len(raw.encode()) > 131072:
        raise DeniedError("frame must be bounded text")
    try:
        encoded(json.loads(raw))
        frame = parse_frame(raw)
    except ValueError, RecursionError:
        raise DeniedError("invalid JSON frame") from None
    if frame is None or type(frame[0]) is not int or not isinstance(frame[1], str) or not 1 <= len(frame[1]) <= 64:
        raise DeniedError("invalid frame identifier")
    if frame[0] == 2 and len(frame) == 4 and isinstance(frame[2], str) and isinstance(frame[3], dict):
        return frame
    if frame[0] == 3 and len(frame) == 3 and isinstance(frame[2], dict):
        return frame
    if (
        frame[0] == 4
        and len(frame) == 5
        and all(isinstance(frame[i], str) for i in (2, 3))
        and isinstance(frame[4], dict)
    ):
        return frame
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
    return parsed.astimezone(UTC)


class Gateway:
    """One admitted charger, independent local control, and one fixed vendor mirror."""

    # Nothing connects here; an offline harness must explicitly supply the peer senders.
    def __init__(self, journal: Journal, policy: Policy | None = None, timeout: float = 1.0) -> None:
        self.journal = journal
        self.policy = policy or Policy()
        self.charger = Peer("local", timeout)
        self.vendor = Peer("vendor", timeout)
        self._tasks: set[asyncio.Task[None]] = set()
        self._flush_lock = asyncio.Lock()
        self._boot_lock = asyncio.Lock()
        self._vendor_ready = False
        self._charger_epoch = 0
        self._vendor_epoch = 0
        self._inbound = InboundCache()
        self.failures: list[str] = []

    # Attach only an already admitted transport; the gateway never opens sockets itself.
    def attach_charger(self, sender: Sender) -> int:
        epoch = self.journal.connection_epoch()
        self.charger.detach()
        self._charger_epoch = epoch
        self._inbound.retire("charger", epoch)
        self.charger.sender = sender
        return epoch

    # Reconnecting the vendor requires a fresh BootNotification before queued traffic.
    def attach_vendor(self, sender: Sender) -> None:
        self.vendor.detach()
        self._vendor_epoch += 1
        self._inbound.retire("vendor", self._vendor_epoch)
        self.vendor.sender = sender
        self._vendor_ready = False

    # Stop pending work without delivering another command or discarding durable events.
    async def close(self) -> None:
        self.charger.detach()
        self.vendor.detach()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # Drain request handlers in deterministic offline tests; receivers themselves never block on a bridge.
    async def drain(self) -> None:
        while self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            # Python 3.14 can return a completed gather without yielding to the
            # task-done callbacks that remove these tasks from the set.
            await asyncio.sleep(0)

    # Resolve replies immediately; process bounded incoming requests in separate tasks.
    async def receive(self, side: str, raw: str | bytes, epoch: int | None = None) -> None:
        if side not in ("charger", "vendor"):
            raise DeniedError("unknown peer")
        current_epoch = self._charger_epoch if side == "charger" else self._vendor_epoch
        if epoch is not None and epoch != current_epoch:
            raise UnavailableError("retired connection")
        frame = decode(raw)
        peer = self.charger if side == "charger" else self.vendor
        if frame[0] != 2:
            peer.receive(frame)
            return
        if len(self._tasks) >= 32:
            if peer.sender:
                await peer.sender(build_call_error(frame[1], "OccurrenceConstraintViolation", "request queue is full"))
            return
        task = asyncio.create_task(self._answer(side, current_epoch, frame))
        self._tasks.add(task)
        task.add_done_callback(self._finished)

    # Deduplicate a peer's requests before execution and never send old replies onto a replacement connection.
    async def _answer(self, side: str, epoch: int, frame: Frame) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        if epoch != current:
            return

        # Core charger observations have their own durable receipts; heartbeat times are never cached.
        async def handle() -> str:
            return (
                await self._charger_call(frame, epoch) if side == "charger" else await self._vendor_call(frame, epoch)
            )

        text = (
            await handle()
            if side == "charger" and frame[2] != "DataTransfer"
            else await self._inbound.response(side, epoch, frame, handle)
        )
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        peer = self.charger if side == "charger" else self.vendor
        if epoch == current and peer.sender:
            await peer.sender(text)

    # A retired connection cannot execute queued work on a replacement transport.
    def _require_epoch(self, side: str, epoch: int) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        if current != epoch:
            raise UnavailableError("request belongs to a retired connection")

    # Retrieve task exceptions without logging frame bodies, identities or credentials.
    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            self.failures.append(type(error).__name__)

    # Send a local result only after its storage commit; transport failure leaves the receipt available for retry.
    async def _charger_call(self, frame: Frame, epoch: int) -> str:
        token, action = str(frame[1]), str(frame[2])
        payload: dict[str, Any] = frame[3]
        mutation: str | None = None
        vendor_epoch = self._vendor_epoch
        try:
            if action == "DataTransfer":
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
                    self.journal.mutation_finished(mutation, True)
                text = reply_as(answer, token)
            else:
                receipt = f"{epoch}:{token}"
                text = build_call_result(token, self._local_answer(receipt, action, payload))
        except DeniedError, ConflictError:
            text = build_call_error(
                token, "PropertyConstraintViolation", "request conflicts with local policy or state"
            )
        except UnavailableError, RecoveryRequiredError, sqlite3.Error:
            if mutation:
                self.journal.mutation_finished(mutation, False)
            text = build_call_error(token, "InternalError", "outcome unavailable; success was not confirmed")
        return text

    # Accept local lifecycle messages while leaving opaque exchanges to their actual vendor.
    def _local_answer(self, token: str, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "BootNotification":
            self.journal.put("boot", payload)
            return {"status": "Accepted", "interval": 60, "currentTime": datetime.now(UTC).isoformat()}
        if action == "Heartbeat":
            return {"currentTime": datetime.now(UTC).isoformat()}
        if action == "Authorize":
            tag = payload.get("idTag")
            allowed = isinstance(tag, str) and tag in self.policy.allowed_tags
            return {"idTagInfo": {"status": "Accepted" if allowed else "Invalid"}}
        if action == "StartTransaction":
            integer(payload.get("connectorId"), 1, 1)
            number(payload.get("meterStart"), 1e12)
            timestamp(payload.get("timestamp"))
            if not isinstance(payload.get("idTag"), str) or payload["idTag"] not in self.policy.allowed_tags:
                raise DeniedError("tag is not approved for local authorization")
            return self.journal.start(token, payload)
        if action == "StopTransaction":
            integer(payload.get("transactionId"))
            number(payload.get("meterStop"), 1e12)
            timestamp(payload.get("timestamp"))
            return self.journal.stop(token, payload)
        if action in (
            "StatusNotification",
            "MeterValues",
            "FirmwareStatusNotification",
            "DiagnosticsStatusNotification",
        ):
            validate_observation(action, payload, self.journal)
            with self.journal.atomic():
                self.journal.enqueue(action, payload, payload.get("transactionId"))
                if action == "StatusNotification":
                    self.journal.put("last_status", payload)
                elif action == "MeterValues" and type(payload.get("transactionId")) is int:
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

    # Opaque writes obey the same owner lock as standard mutations.
    def _extension_allowed(self, payload: dict[str, Any]) -> None:
        kind = self.policy.kind("DataTransfer", payload)
        if kind == "write" and self.journal.get("control", {}).get("owner") == "local":
            raise DeniedError("local owner blocks vendor extension writes")

    # Forward a real vendor request and its exact result/error instead of inventing successful acknowledgements.
    async def _vendor_call(self, frame: Frame, epoch: int) -> str:
        token, action = str(frame[1]), str(frame[2])
        original: dict[str, Any] = frame[3]
        mutation: str | None = None
        charger_epoch = self._charger_epoch
        try:
            self.policy.kind(action, original)
            self.policy.validate(action, original)

            # Allocate profile ids and mutation intent only after the in-gate policy check succeeds.
            def prepare() -> dict[str, Any]:
                nonlocal mutation
                self._require_epoch("vendor", epoch)
                self._require_epoch("charger", charger_epoch)
                self._check_vendor(action, original)
                with self.journal.atomic():
                    payload = self._translated_command(action, original)
                    if self.policy.kind(action, original) == "write":
                        mutation = self.journal.mutation_intent("vendor", action, original)
                return payload

            answer = await self.charger.call(action, prepare)
            if mutation:
                self.journal.mutation_finished(mutation, True)
            text = reply_as(answer, token)
        except DeniedError, ConflictError:
            text = build_call_error(token, "SecurityError", "request is not allowed by current local policy")
        except UnavailableError, RecoveryRequiredError, sqlite3.Error:
            if mutation:
                self.journal.mutation_finished(mutation, False)
            self.journal.put("uncertain_vendor_command", {"action": action, "digest": fingerprint(original)})
            text = build_call_error(token, "InternalError", "charger outcome is unconfirmed; request was not replayed")
        return text

    # Recheck priority, owner and current transaction after acquiring the outgoing CALL gate.
    def _check_vendor(self, action: str, payload: dict[str, Any]) -> None:
        kind = self.policy.kind(action, payload)
        control = self.journal.get("control", {})
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (control.get("owner") == "local" or control.get("inhibit"))
        ):
            raise DeniedError("explicit local control has priority")
        if action == "RemoteStopTransaction" and self.journal.local_id(payload["transactionId"]) is None:
            raise DeniedError("cloud stop no longer names the active transaction")
        if action == "SetChargingProfile":
            vendor = payload["csChargingProfiles"].get("transactionId")
            if vendor is not None and self.journal.local_id(vendor) is None:
                raise DeniedError("cloud profile no longer names the active transaction")

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
                if len(profiles) >= 128:
                    raise DeniedError("vendor profile namespace is full")
                profiles[key] = 100000 + len(profiles)
                self.journal.put("vendor_profiles", profiles)
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

    # Claim local ownership before queueing; an uncertain stop continues to inhibit cloud restart after a crash.
    async def _local_command(self, action: str, payload: dict[str, Any]) -> Frame:
        if action not in ("RemoteStartTransaction", "RemoteStopTransaction", "SetChargingProfile"):
            raise DeniedError("unsupported local control")
        previous = self.journal.get("control", {})
        charger_epoch = self._charger_epoch
        epoch = previous.get("stop_epoch", 0) + (1 if action == "RemoteStopTransaction" else 0)
        ticket = uuid.uuid4().hex
        inhibit = action == "RemoteStopTransaction" or (
            previous.get("inhibit", False) and action != "RemoteStartTransaction"
        )
        self.journal.put(
            "control", {"owner": "local", "inhibit": inhibit, "uncertain": True, "stop_epoch": epoch, "ticket": ticket}
        )

        # A stop cancels older queued local work as well as holding off cloud writes.
        def check() -> None:
            self._require_epoch("charger", charger_epoch)
            current = self.journal.get("control", {})
            if current.get("stop_epoch", 0) != epoch:
                raise DeniedError("a newer local stop superseded this request")
            if current.get("inhibit") and action == "RemoteStartTransaction":
                raise DeniedError("a local stop inhibits this start")

        try:
            answer = await self.charger.call(
                action, payload, priority=0 if action == "RemoteStopTransaction" else 1, check=check
            )
        except UnavailableError:
            raise
        else:
            control = self.journal.get("control")
            if control.get("ticket") == ticket and control.get("stop_epoch", 0) == epoch:
                self.journal.put("control", {**control, "uncertain": False})
            return answer

    # Build the local limit with the upstream command builder under this prototype's explicit ceiling.
    async def set_limit(self, value: Any) -> Frame:
        amps = number(value, self.policy.max_current_a)
        amps = 0.0 if amps < 6 else amps
        return await self._local_command(
            "SetChargingProfile",
            commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, amps, HUB_PROFILE_ID),
        )

    # Start only through an explicitly approved synthetic/local tag; never enable automatic starts.
    async def start(self, tag: str = "LOCAL_TEST") -> Frame:
        if self.journal.get("control", {}).get("uncertain"):
            raise RecoveryRequiredError("previous local command outcome is unconfirmed")
        if tag not in self.policy.allowed_tags or self.journal.active() is not None:
            raise DeniedError("tag is not allowed or a transaction is unresolved")
        return await self._local_command("RemoteStartTransaction", commands.remote_start_transaction(tag, 1))

    # Stop the known session; a missing transaction still leaves an explicit local inhibit.
    async def stop(self) -> Frame:
        local = self.journal.active()
        if local is None:
            previous = self.journal.get("control", {})
            self.journal.put(
                "control",
                {**previous, "owner": "local", "inhibit": True, "stop_epoch": previous.get("stop_epoch", 0) + 1},
            )
            raise DeniedError("no known active transaction")
        return await self._local_command("RemoteStopTransaction", commands.remote_stop_transaction(local))

    # Release ownership only by explicit local intent, with uncertainty acknowledged separately.
    def release(self, acknowledge_uncertainty: bool = False) -> None:
        if self.journal.get("control", {}).get("uncertain") and not acknowledge_uncertainty:
            raise RecoveryRequiredError("previous command outcome is unconfirmed")
        self.journal.put(
            "control", {**self.journal.get("control", {}), "owner": "shared", "inhibit": False, "uncertain": False}
        )

    # Introduce the virtual charge point once per cloud connection without blocking the local receiver.
    async def _ensure_vendor_boot(self) -> None:
        epoch = self._vendor_epoch
        async with self._boot_lock:
            self._require_epoch("vendor", epoch)
            if self._vendor_ready:
                return
            boot = self.journal.get("boot")
            if boot is None:
                raise UnavailableError("charger has not introduced itself")
            answer = await self.vendor.call("BootNotification", boot, priority=5)
            self._require_epoch("vendor", epoch)
            if answer[0] != 3 or answer[2].get("status") != "Accepted":
                raise UnavailableError("vendor has not accepted the charger")
            self._vendor_ready = True

    # Deliver durable events in order; a cloud-accepted Start with a lost reply requires explicit reconciliation.
    async def flush(self) -> None:
        epoch = self._vendor_epoch
        async with self._flush_lock:
            self._require_epoch("vendor", epoch)
            await self._ensure_vendor_boot()
            while (item := self.journal.next_delivery()) is not None:
                if item.state in ("ambiguous", "inflight", "rejected"):
                    raise RecoveryRequiredError("cloud delivery requires explicit reconciliation")
                payload = dict(item.payload)
                if item.action in ("MeterValues", "StopTransaction") and "transactionId" in payload:
                    vendor = self.journal.vendor_id(integer(payload["transactionId"]))
                    if vendor is None:
                        raise RecoveryRequiredError("transaction mapping is not known; event retained")
                    payload["transactionId"] = vendor
                try:
                    current = item
                    sent = False

                    # Persist possible delivery inside the CALL gate, immediately before send.
                    def before_send() -> None:
                        nonlocal sent
                        self._require_epoch("vendor", epoch)
                        self.journal.mark_sent(current)
                        sent = True

                    answer = await self.vendor.call(item.action, payload, token=item.token, before_send=before_send)
                except UnavailableError, asyncio.CancelledError:
                    if sent:
                        self.journal.db.execute(
                            "UPDATE outbox SET state=? WHERE token=?",
                            ("ambiguous" if item.action == "StartTransaction" else "queued", item.token),
                        )
                    raise
                if answer[0] != 3:
                    self.journal.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (item.token,))
                    raise RecoveryRequiredError("cloud rejected a retained lifecycle event")
                self.journal.acknowledge(item, answer[2])
