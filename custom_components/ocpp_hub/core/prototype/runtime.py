"""An independent local listener and reconnecting vendor runner, restricted to offline loopback TLS."""

from __future__ import annotations

import asyncio
import math
import sqlite3
import ssl
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Protocol, Self

from websockets.asyncio.client import ClientConnection
from websockets.asyncio.server import Server, ServerConnection
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from .diagnostics import FaultDiagnostics
from .gateway import Gateway
from .policy import DeniedError
from .rpc import IntroductionDeferredError, RegistrationDeferredError, Sender, UnavailableError
from .security import Admission, SecurityError, offline_server, offline_vendor, validate_offline_vendor
from .store import ConflictError, RecoveryRequiredError


@dataclass(frozen=True)
class RuntimeSettings:
    """Bounded timings; endpoint and credentials are deliberately absent from representations."""

    retry_min: float = 1
    retry_max: float = 30
    pump_interval: float = 0.1
    ping_interval: float = 20
    ping_timeout: float = 20
    close_timeout: float = 1
    boot_probe_after: float = 0

    # Reject invalid timings before opening any socket or starting a task.
    def __post_init__(self) -> None:
        values = (
            self.retry_min,
            self.retry_max,
            self.pump_interval,
            self.ping_interval,
            self.ping_timeout,
            self.close_timeout,
        )
        if any(type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 300 for value in values):
            raise ValueError("runtime timing must be finite, positive and bounded")
        if (
            self.retry_max < self.retry_min
            or type(self.boot_probe_after) not in (int, float)
            or not math.isfinite(self.boot_probe_after)
            or not 0 <= self.boot_probe_after <= 300
        ):
            raise ValueError("invalid retry or boot-probe timing")


@dataclass
class RuntimeStatus:
    """Observability contains counts and classifications, never frames or credentials."""

    charger_connected: bool = False
    vendor_connected: bool = False
    reconnects: int = 0
    heartbeats: int = 0
    delivery_blocked: bool = False
    delivery_blocked_cause: str | None = None
    boot_observed: bool = False
    last_failure: str | None = None
    vendor_attempts: int = 0
    vendor_failed_charger_epoch: int | None = None
    charger_fault: FaultDiagnostics = field(default_factory=FaultDiagnostics)
    vendor_fault: FaultDiagnostics = field(default_factory=FaultDiagnostics)


class Transport(Protocol):
    # Bind only the transport's already validated endpoint.
    def listen(self, handler: Callable[[ServerConnection], Awaitable[None]]) -> Awaitable[Server]: ...

    # Connect only to the transport's fixed, already validated upstream.
    def connect(self) -> AbstractAsyncContextManager[ClientConnection]: ...


class ConnectionRuntime:
    """A cloud outage cannot stop local receive/control or erase durable events."""

    # Eagerly validate the fixed destination before even binding the local loopback listener.
    def __init__(
        self,
        gateway: Gateway,
        transport: Transport,
        settings: RuntimeSettings | None = None,
        before_ceiling: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.gateway = gateway
        self.settings = settings or RuntimeSettings()
        self.transport = transport
        self.before_ceiling = before_ceiling
        self.status = RuntimeStatus()
        self._server: Server | None = None
        self._runner: asyncio.Task[None] | None = None
        self._session_tasks: set[asyncio.Task[None]] = set()
        self._charger: ServerConnection | None = None
        self._charger_lock = asyncio.Lock()
        self._closing = False
        self.vendor_enabled: Callable[[], bool] = lambda: True
        self.vendor_session_enabled: Callable[[], bool] = lambda: self.vendor_enabled()
        self.ceiling_enabled: Callable[[], bool] = lambda: True

    # Expose only the ephemeral test port after startup.
    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("offline runtime is not started")
        return int(self._server.sockets[0].getsockname()[1])

    # Start a literal-loopback, ephemeral listener and a separately owned cloud supervisor.
    async def start(self) -> Self:
        if self._server is not None or self._closing:
            raise RuntimeError("offline runtime cannot be started twice")
        self._server = await self.transport.listen(self._charger_handler)
        self._runner = asyncio.create_task(self._vendor_runner())
        return self

    # Translate closed socket sends into the gateway's durable uncertain-outcome boundary.
    @staticmethod
    def _sender(connection: ClientConnection | ServerConnection) -> Sender:
        # A bounded peer call owns the send timeout; the receiver remains independent.
        async def send(raw: str) -> None:
            try:
                await connection.send(raw)
            except ConnectionClosed:
                raise UnavailableError("transport closed during send") from None

        return send

    # Admission already ran before replacement; every receive and finalizer is generation-scoped.
    async def _charger_handler(self, connection: ServerConnection) -> None:
        async with self._charger_lock:
            previous = self._charger
            epoch = self.gateway.attach_charger(self._sender(connection))
            self._charger = connection
            self.status.charger_connected = True
            self.status.boot_observed = False
        probe: asyncio.Task[None] | None = None
        try:
            if previous is not None:
                await previous.close(code=1012)
            probe = asyncio.create_task(self._boot_probe(epoch))
            async for raw in connection:
                await self.gateway.receive("charger", raw, epoch)
        except (ConnectionClosed, UnavailableError, DeniedError) as error:
            self.status.last_failure = type(error).__name__
            self.status.charger_fault.record(error, "charger_receive", self.gateway.charger.diagnostics.action)
        finally:
            if probe is not None:
                probe.cancel()
                await asyncio.gather(probe, return_exceptions=True)
            try:
                self.gateway.detach_charger(epoch)
            except sqlite3.Error as error:
                self.status.last_failure = type(error).__name__
                self.status.charger_fault.record(error, "charger_retirement", "None")
            finally:
                if self._charger is connection:
                    self._charger = None
                    self.status.charger_connected = False
                    self.status.boot_observed = False

    # Explicitly enabled read-only probing never fabricates a boot or changes charger configuration.
    async def _boot_probe(self, epoch: int) -> None:
        try:
            if self.settings.boot_probe_after:
                try:
                    await asyncio.wait_for(self.gateway.wait_for_boot(epoch), self.settings.boot_probe_after)
                except TimeoutError:
                    try:
                        result = await self.gateway.charger.call(
                            "TriggerMessage",
                            {"requestedMessage": "BootNotification"},
                            check=lambda: self.gateway._require_epoch("charger", epoch),
                        )
                        if result[0] != 3 or result[2].get("status") != "Accepted":
                            raise UnavailableError("charger refused the read-only boot probe")
                        await asyncio.wait_for(self.gateway.wait_for_boot(epoch), self.settings.boot_probe_after)
                    except (UnavailableError, TimeoutError) as error:
                        self.status.last_failure = "BootOrCeilingUnavailable"
                        self.status.charger_fault.record(error, "boot_probe", "TriggerMessage")
                        # A failed read-only probe does not invalidate a later genuine Boot on this same connection.
                        await self.gateway.wait_for_boot(epoch)
            else:
                await self.gateway.wait_for_boot(epoch)
            self.status.boot_observed = self.gateway.current_boot_seen(epoch)
            self.gateway._require_epoch("charger", epoch)
            if self.before_ceiling is not None:
                delay = self.settings.retry_min
                while True:
                    try:
                        await self.before_ceiling()
                        break
                    except IntroductionDeferredError as error:
                        self.status.last_failure = "BootOrCeilingUnavailable"
                        self.status.charger_fault.record(
                            error, "boot_or_ceiling", self.gateway.charger.diagnostics.action
                        )
                        await asyncio.sleep(delay)
                        self.gateway._require_epoch("charger", epoch)
                        delay = min(2 * delay, self.settings.retry_max)
            self.gateway._require_epoch("charger", epoch)
            if self.ceiling_enabled():
                await self._confirm_introduced_ceiling(epoch)
        except (UnavailableError, TimeoutError, DeniedError, RecoveryRequiredError) as error:
            self.status.last_failure = "BootOrCeilingUnavailable"
            self.status.charger_fault.record(error, "boot_or_ceiling", self.gateway.charger.diagnostics.action)

    # Retry only the admitted current Boot's restrictive zero sequence; introduction and Stop are never replayed.
    async def _confirm_introduced_ceiling(self, epoch: int) -> None:
        gateway, journal = self.gateway, self.gateway.journal
        boot = gateway.boot_observation
        metadata_revision = gateway.boot_metadata_revision
        control_revision = gateway.controls._revision
        ticket = journal.get("control", {}).get("ticket")
        delay = self.settings.retry_min

        # Only identical metadata under the same admitted owner can start another complete zero proof.
        def same_authority() -> bool:
            return (
                not self._closing
                and self.ceiling_enabled()
                and gateway.current_boot_seen(epoch)
                and boot is not None
                and gateway.boot_metadata_revision == metadata_revision
                and gateway.controls._revision == control_revision
                and journal.get("control", {}).get("ticket") == ticket
                and journal.get("native_bounded_handover", {}).get("stage") == "admitted"
                and journal.get("trial_phase") in ("active", "recovering")
                and not journal.get("trial_starting")
                and journal.get("control", {}).get("inhibit") is True
                and gateway.controls.desired() == 0
            )

        # Every awaited operation still belongs to one actual Boot observation, never its superseded proof.
        def restricted() -> bool:
            return same_authority() and gateway.boot_observation == boot

        # A new identical Boot discards the old attempt; the next attempt repeats zero and composite verification.
        def replace_observation() -> bool:
            nonlocal boot
            if same_authority() and gateway.boot_observation is not None and gateway.boot_observation != boot:
                boot = gateway.boot_observation
                return True
            return False

        # Capability reads and the CALL gate can await; recheck the same Boot immediately before every zero send.
        def guard() -> None:
            gateway._require_epoch("charger", epoch)
            if not restricted():
                raise UnavailableError("restrictive introduction was superseded")

        retrying = False
        while True:
            try:
                await gateway.controls.ensure_ceiling(guard=guard if retrying or restricted() else None)
                return
            except (UnavailableError, TimeoutError) as error:
                if not restricted():
                    if not replace_observation():
                        raise
                    retrying = True
                    continue
                self.status.last_failure = "BootOrCeilingUnavailable"
                self.status.charger_fault.record(error, "boot_or_ceiling", gateway.charger.diagnostics.action)
                await asyncio.sleep(delay)
                gateway._require_epoch("charger", epoch)
                if not restricted() and not replace_observation():
                    raise UnavailableError("restrictive introduction was superseded") from None
                retrying = True
                delay = min(2 * delay, self.settings.retry_max)

    # Classify the live vendor exchange before detach changes registration and pending RPC state.
    def _vendor_stage(self) -> str:
        if self.gateway.vendor_registration != "Accepted":
            return "registration"
        action = self.gateway.vendor.diagnostics.action
        if action in ("Heartbeat", "Authorize"):
            return "heartbeat" if action == "Heartbeat" else "authorization"
        return (
            "history"
            if action in ("StatusNotification", "MeterValues", "StartTransaction", "StopTransaction")
            else "session"
        )

    # Inspect the already received close frame before our own cancellation or context-manager cleanup sends a close.
    def _vendor_close(self, connection: ClientConnection, fault_count: int) -> None:
        received, sent = connection.protocol.close_rcvd, connection.protocol.close_sent
        if received is not None and self.status.vendor_fault.count == fault_count:
            self.status.vendor_fault.record_close(
                self._vendor_stage(),
                self.gateway.vendor.diagnostics.action,
                int(received.code),
                int(sent.code) if sent is not None else None,
            )

    # Recover transport loss with bounded backoff while the local listener stays alive.
    async def _vendor_runner(self) -> None:
        delay = self.settings.retry_min
        while not self._closing:
            if (
                not self.vendor_enabled()
                or self._charger is None
                or not self.gateway.current_boot_seen(self.gateway._charger_epoch)
            ):
                await asyncio.sleep(self.settings.pump_interval)
                continue
            fault_count = self.status.vendor_fault.count
            attempted_charger_epoch = self.gateway._charger_epoch
            attempted_boot = self.gateway.journal.get("boot")
            attempted_boot_revision = self.gateway.boot_metadata_revision
            stage = "opening"
            self.status.vendor_attempts += 1
            try:
                async with self.transport.connect() as connection:
                    if (
                        not self.vendor_enabled()
                        or not self.gateway.current_boot_seen(attempted_charger_epoch)
                        or self.gateway.journal.get("boot") != attempted_boot
                        or self.gateway.boot_metadata_revision != attempted_boot_revision
                    ):
                        raise UnavailableError("vendor introduction changed while its connection was opening")
                    self.status.vendor_connected = True
                    self.status.reconnects += 1
                    epoch = self.gateway.attach_vendor(self._sender(connection))
                    try:
                        await self._vendor_session(connection, epoch)
                    finally:
                        stage = self._vendor_stage()
                        if self.gateway.vendor_registration == "Accepted":
                            delay = self.settings.retry_min
                        self.gateway.detach_vendor(epoch)
                        self.status.vendor_connected = False
            except (OSError, ConnectionClosed, InvalidHandshake, UnavailableError, DeniedError) as error:
                self.status.last_failure = type(error).__name__
                if self.status.vendor_fault.count == fault_count:
                    self.status.vendor_fault.record(error, stage, self.gateway.vendor.diagnostics.action)
            if self.status.vendor_fault.count > fault_count:
                self.status.vendor_failed_charger_epoch = attempted_charger_epoch
            if not self._closing:
                await asyncio.sleep(delay)
                delay = min(self.settings.retry_max, delay * 2)

    # Stop all session children when any transport/heartbeat child ends; quarantine leaves the pump alive.
    async def _vendor_session(self, connection: ClientConnection, epoch: int) -> None:
        charger_epoch = self.gateway._charger_epoch
        boot = self.gateway.journal.get("boot")
        boot_revision = self.gateway.boot_metadata_revision
        fault_count = self.status.vendor_fault.count

        # Keep the vendor receive path free while outgoing RPCs await their correlated responses.
        async def receive() -> None:
            try:
                async for raw in connection:
                    await self.gateway.receive("vendor", raw, epoch)
            except ConnectionClosed as error:
                self.status.vendor_fault.record(error, self._vendor_stage(), self.gateway.vendor.diagnostics.action)
                raise
            else:
                self._vendor_close(connection, fault_count)

        # A virtual charge point must not stay online after its physical connection has gone away or changed.
        async def charger_presence() -> None:
            while (
                self.vendor_session_enabled()
                and self.gateway.current_boot_seen(charger_epoch)
                and self.gateway.journal.get("boot") == boot
                and self.gateway.boot_metadata_revision == boot_revision
            ):
                await asyncio.sleep(self.settings.pump_interval)

        tasks = [
            asyncio.create_task(receive()),
            asyncio.create_task(self._pump()),
            asyncio.create_task(self._heartbeat()),
            asyncio.create_task(charger_presence()),
        ]
        self._session_tasks.update(tasks)
        for task in tasks:
            task.add_done_callback(self._session_tasks.discard)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        finally:
            self._vendor_close(connection, fault_count)
            await self._retire(tasks)

    # Keep strong references to any uncooperative task and surface failure instead of abandoning it.
    async def _retire(self, tasks: list[asyncio.Task[None]]) -> None:
        for task in tasks:
            task.cancel()
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self.settings.close_timeout + 1)
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                self.status.last_failure = "RetirementTimeout"
                raise RuntimeError("offline runtime task retirement exceeded its deadline")

    # Registration retry and durable delivery are independent of heartbeat and local command handling.
    async def _pump(self) -> None:
        while True:
            try:
                if self.gateway.journal.get("boot") is None:
                    await asyncio.sleep(self.settings.pump_interval)
                    continue
                await self.gateway.flush()
                self.status.delivery_blocked = False
                self.status.delivery_blocked_cause = None
            except RegistrationDeferredError as error:
                await asyncio.sleep(error.retry_after)
            except (RecoveryRequiredError, ConflictError) as error:
                self.status.delivery_blocked = True
                self.status.delivery_blocked_cause = "reconciliation"
                self.status.last_failure = type(error).__name__
            except sqlite3.Error as error:
                self.status.delivery_blocked = True
                self.status.delivery_blocked_cause = "storage"
                self.status.last_failure = type(error).__name__
            await asyncio.sleep(self.settings.pump_interval)

    # OCPP data-frame heartbeats are distinct from websocket Ping/Pong and continue through journal quarantine.
    async def _heartbeat(self) -> None:
        while True:
            if self.gateway.vendor_registration != "Accepted":
                await asyncio.sleep(self.settings.pump_interval)
                continue
            await asyncio.sleep(self.gateway.vendor_interval)
            await self.gateway.heartbeat()
            self.status.heartbeats += 1

    # Bound shutdown and leave journal ownership to the caller; no connection survives its runtime.
    async def close(self) -> None:
        self._closing = True
        if self._server is not None:
            self._server.close()
        try:
            if self._runner is not None:
                await self._retire([self._runner])
            await self._retire(list(self._session_tasks))
            if self._server is not None:
                await asyncio.wait_for(self._server.wait_closed(), self.settings.close_timeout + 2)
        finally:
            await self.gateway.close()

    # Rollback retains the local receiver while retiring the cloud supervisor and all its session children.
    async def stop_vendor(self) -> None:
        if self._runner is not None:
            await self._retire([self._runner])
            self._runner = None
        await self._retire(list(self._session_tasks))


class OfflineTransport:
    """The original literal-loopback boundary remains independently enforced on both legs."""

    # Validate without constructing a connector, binding or resolving a hostname.
    def __init__(
        self,
        admission: Admission,
        vendor_url: str,
        server_tls: ssl.SSLContext,
        client_tls: ssl.SSLContext,
        settings: RuntimeSettings,
    ) -> None:
        validate_offline_vendor(vendor_url, client_tls)
        if server_tls.protocol != ssl.PROTOCOL_TLS_SERVER:
            raise SecurityError("offline listener requires a server TLS context")
        self.admission, self.vendor_url = admission, vendor_url
        self.server_tls, self.client_tls, self.settings = server_tls, client_tls, settings

    # Bind only the existing authenticated ephemeral loopback listener.
    def listen(self, handler: Callable[[ServerConnection], Awaitable[None]]) -> Awaitable[Server]:
        s = self.settings
        listener: Awaitable[Server] = offline_server(
            handler,
            self.admission,
            self.server_tls,
            ping_interval=s.ping_interval,
            ping_timeout=s.ping_timeout,
            close_timeout=s.close_timeout,
        )
        return listener

    # The original adapter checks literal loopback again before returning its dormant connector.
    def connect(self) -> AbstractAsyncContextManager[ClientConnection]:
        s = self.settings
        return offline_vendor(
            self.vendor_url,
            self.admission,
            self.client_tls,
            ping_interval=s.ping_interval,
            ping_timeout=s.ping_timeout,
            close_timeout=s.close_timeout,
        )


class OfflineRuntime(ConnectionRuntime):
    """Public offline API: no production flags, destinations or household credential loading."""

    # Preserve the original construction contract while sharing only transport-independent lifecycle code.
    def __init__(
        self,
        gateway: Gateway,
        admission: Admission,
        vendor_url: str,
        server_tls: ssl.SSLContext,
        client_tls: ssl.SSLContext,
        settings: RuntimeSettings | None = None,
    ) -> None:
        actual = settings or RuntimeSettings()
        self.admission = admission
        super().__init__(gateway, OfflineTransport(admission, vendor_url, server_tls, client_tls, actual), actual)
