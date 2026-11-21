"""The vendor mirror: a read-only copy of what the chargers say, sent on to the vendor's own backend.

The mirror listens to the sessions of the central system. For every connected charger it keeps a link to the vendor
backend where the charger shows up as a virtual charge point. The vendor can look but not touch: its commands are
answered by the mirror and never reach the charger.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Callable
from datetime import datetime
from typing import Any

from .control import ControlPolicy
from .frames import Frame
from .mirror_link import MirrorLink
from .mirror_model import MirrorSettings, MirrorStatus
from .model import ChargerSnapshot, ChargeSession
from .plan_bridge import PlanHandler
from .session import SENDER_CHARGER, ChargerSession
from .txid_map import TransactionIdMap

_LOGGER = logging.getLogger(__name__)


class VendorMirror:
    """A SessionListener that copies the traffic of every charger to the vendor backend."""

    # Keep the settings; no link exists before start() and the first charger.
    # An SSL context can be passed in, for example one Home Assistant already built.
    def __init__(
        self,
        settings: MirrorSettings,
        ssl_context: ssl.SSLContext | None = None,
        clock: Callable[[], datetime] | None = None,
        policy: ControlPolicy | None = None,
        plan_handler: PlanHandler | None = None,
    ) -> None:
        self._settings = settings
        self._ssl_context = ssl_context
        self._clock = clock
        self._policy = policy or ControlPolicy()
        self._plan_handler = plan_handler
        self._started = False
        self._links: dict[str, MirrorLink] = {}
        # Kept per charger for as long as the mirror runs: a running session must still be stopped under the vendor's
        # id after the charger reconnected to the hub and its link was rebuilt.
        self._transactions: dict[str, TransactionIdMap] = {}

    # Get ready to accept chargers.
    # Building the default SSL context reads the system certificates from disk, which must not happen on the event
    # loop, so it is done here in a thread, once, before any link needs it.
    async def start(self) -> None:
        if self._settings.use_tls and self._ssl_context is None:
            self._ssl_context = await asyncio.to_thread(ssl.create_default_context)
        self._started = True
        _LOGGER.info("Vendor mirror ready (commands from the vendor backend are refused)")

    # Close every link to the vendor backend.
    async def stop(self) -> None:
        self._started = False
        links = list(self._links.values())
        self._links.clear()
        await asyncio.gather(*(link.stop() for link in links))

    # The link status of one charger, or None when it has no link right now.
    def status(self, cp_id: str) -> MirrorStatus | None:
        link = self._links.get(cp_id)
        return link.status() if link is not None else None

    # The link status of every charger that has a link.
    def statuses(self) -> dict[str, MirrorStatus]:
        return {cp_id: link.status() for cp_id, link in self._links.items()}

    # A charger connected: follow it with its existing link when one is still delivering, otherwise start a new one.
    def on_connected(self, session: ChargerSession) -> None:
        if not self._started:
            _LOGGER.error("Charger %s connected before the mirror was started; it is not mirrored", session.cp_id)
            return
        existing = self._links.get(session.cp_id)
        if existing is not None and existing.attach(session):
            return
        transactions = self._transactions.setdefault(session.cp_id, TransactionIdMap())
        link = MirrorLink(
            session,
            self._settings,
            transactions,
            self._ssl_context,
            self._link_finished,
            self._clock,
            self._policy,
            self._plan_handler,
        )
        self._links[session.cp_id] = link
        link.start()

    # The mirror reads frames, not snapshots: the link remembers what it needs from the frames themselves.
    def on_snapshot(self, snapshot: ChargerSnapshot) -> None:
        return None

    # Forward charger traffic. Accepted Starts are sent through their separate committed event.
    def on_frame(self, cp_id: str, sender: str, frame: Frame) -> None:
        link = self._links.get(cp_id)
        if link is not None and sender == SENDER_CHARGER:
            link.charger_frame(frame)

    # Mirror the original Start once even if the charger never receives its local transaction id.
    def on_start_committed(self, cp_id: str, transaction_id: int, payload: dict[str, Any]) -> None:
        link = self._links.get(cp_id)
        if link is not None:
            link.start_committed(transaction_id, payload)

    # Finished charges are the entities' business; the mirror forwards the charger's own Stop report.
    def on_session_finished(self, session: ChargeSession) -> None:
        return None

    # A charger session ended. When a newer session of the same charger already took over, the end of the old one is
    # not a reason to stop anything; the old session's disconnect can arrive after the new one connected.
    def on_disconnected(self, cp_id: str) -> None:
        link = self._links.get(cp_id)
        if link is not None and not link.session.snapshot.connected:
            link.charger_gone()

    # Forget a link that has ended, unless a newer link already took its place.
    def _link_finished(self, link: MirrorLink) -> None:
        if self._links.get(link.cp_id) is link:
            del self._links[link.cp_id]
