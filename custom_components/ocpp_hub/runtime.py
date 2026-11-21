"""What runs for one config entry: the central system, the optional vendor mirror and the bridge to the entities."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.translation import async_get_translations

from .const import DOMAIN, PACKAGE_LOGGER, charger_notification_id, new_charger_signal, session_signal, snapshot_signal
from .core import commands
from .core.central import CentralSystem
from .core.frames import Frame
from .core.mirror import VendorMirror
from .core.model import ChargerSnapshot, ChargeSession, LimitMemory
from .core.session import ChargerSession
from .plan_sync import PlanSync
from .session_store import LimitStore, SessionStore

_LOGGER = logging.getLogger(__name__)
# The raw OCPP frames, at debug level only; a frame can hold an id tag or a serial number, so it is off by default.
_FRAME_LOGGER = logging.getLogger(f"{PACKAGE_LOGGER}.frames")


# True once the charger has said who it is (its BootNotification arrived).
# Entities are created from then on, so the device carries the model and firmware from the start.
def _has_booted(snapshot: ChargerSnapshot) -> bool:
    return snapshot.info.vendor is not None or snapshot.info.model is not None


# A string a charger supplied, made safe to show in a notification.
# Vendor, model and firmware come from a device on the network, and the notification is rendered as Markdown, so the
# text is cut to one short line without backticks or control characters (a bidi override could spoof how it reads) and
# put in a code span, where Markdown shows it as it is.
def _as_code(value: str | None) -> str:
    printable = "".join(char for char in value or "" if char.isprintable() or char.isspace())
    text = re.sub(r"[`\s]+", " ", printable).strip()[:64]
    return f"`{text or '-'}`"


class HubListener:
    """Hands what the central system reports to Home Assistant: new chargers and every new snapshot."""

    # Keep what is needed to send signals; nothing is sent before the first charger connects.
    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        build: str,
        sessions: SessionStore,
        limits: LimitStore,
        plan_changed: Callable[[ChargerSnapshot], None] = lambda snapshot: None,
        mirror_enabled: bool = False,
    ) -> None:
        self._hass = hass
        self._sessions = sessions
        self._limits = limits
        # Tells the plan about every snapshot, so it can notice that its profile was replaced.
        self.plans_changed = plan_changed
        # What was last handed to storage per charger, so nothing is written when nothing changed.
        self._saved_limits: dict[str, LimitMemory] = {}
        self._entry_id = entry_id
        self._build = build
        self._mirror_enabled = mirror_enabled
        self._announced: set[str] = set()
        # The chargers that already got a notification for their current connection.
        self._notified: set[str] = set()
        self.snapshots: dict[str, ChargerSnapshot] = {}

    @property
    def announced(self) -> list[str]:
        """The chargers that already have entities, or are about to."""
        return sorted(self._announced)

    # A charger connected; it only gets entities and a notification once it has booted.
    def on_connected(self, session: ChargerSession) -> None:
        self._notified.discard(session.cp_id)
        _LOGGER.info("[build %s] Charger %s connected", self._build, session.cp_id)

    # Remember the newest snapshot and pass it on. The first snapshot after the boot announces the charger, before the
    # snapshot itself, so its entities exist and start from the stored state.
    def on_snapshot(self, snapshot: ChargerSnapshot) -> None:
        self.snapshots[snapshot.cp_id] = snapshot
        self._save_limits(snapshot)
        self.plans_changed(snapshot)
        if snapshot.cp_id not in self._announced and _has_booted(snapshot):
            self._announced.add(snapshot.cp_id)
            _LOGGER.info(
                "[build %s] Charger %s booted: %r %r, firmware %r",
                self._build,
                snapshot.cp_id,
                snapshot.info.vendor,
                snapshot.info.model,
                snapshot.info.firmware,
            )
            async_dispatcher_send(self._hass, new_charger_signal(self._entry_id), snapshot.cp_id)
        if snapshot.cp_id not in self._notified and snapshot.connected and _has_booted(snapshot):
            self._notified.add(snapshot.cp_id)
            self._hass.async_create_task(self._notify_connected(snapshot))
        async_dispatcher_send(self._hass, snapshot_signal(self._entry_id, snapshot.cp_id), snapshot)

    # Save the limits of a charger when they changed, so they survive a restart. Only the lasting ones are kept (the
    # central system filters out a profile that ends with its charge), and nothing is written when nothing changed.
    def _save_limits(self, snapshot: ChargerSnapshot) -> None:
        current = LimitMemory(
            hub_a=snapshot.charge_limit_a,
            hub_purpose=snapshot.limit_purpose,
            vendor_a=snapshot.vendor_limit_a,
            vendor_profile_id=snapshot.vendor_profile_id,
            vendor_purpose=snapshot.vendor_purpose,
            vendor_profile_connector_id=snapshot.vendor_profile_connector_id,
            vendor_profile_stack_level=snapshot.vendor_profile_stack_level,
        )
        if not snapshot.connected or self._saved_limits.get(snapshot.cp_id) == current:
            return
        self._saved_limits[snapshot.cp_id] = current
        lasting = (commands.PURPOSE_CHARGE_POINT_MAX, commands.PURPOSE_TX_DEFAULT)
        vendor = current.vendor_purpose in lasting
        self._limits.update(
            snapshot.cp_id,
            LimitMemory(
                hub_a=current.hub_a if current.hub_purpose in lasting else None,
                hub_purpose=current.hub_purpose if current.hub_purpose in lasting else None,
                vendor_a=current.vendor_a if vendor else None,
                vendor_profile_id=current.vendor_profile_id if vendor else None,
                vendor_purpose=current.vendor_purpose if vendor else None,
                vendor_profile_connector_id=current.vendor_profile_connector_id if vendor else None,
                vendor_profile_stack_level=current.vendor_profile_stack_level if vendor else None,
            ),
        )

    # Tell the owner that a charger connected: who it is and what it speaks, in the language of the instance.
    # The notification has a fixed id per charger, so a reconnect updates it instead of piling up new ones.
    async def _notify_connected(self, snapshot: ChargerSnapshot) -> None:
        texts = await async_get_translations(self._hass, self._hass.config.language, "exceptions", [DOMAIN])

        # Look up one text by its key; the English text is the fallback Home Assistant already merged in.
        def text(key: str) -> str:
            return texts.get(f"component.{DOMAIN}.exceptions.{key}.message", key)

        info = snapshot.info
        message = text("charger_connected").format(
            vendor=_as_code(info.vendor),
            model=_as_code(info.model),
            firmware=_as_code(info.firmware),
            serial=_as_code(info.serial or snapshot.cp_id),
            protocol=_as_code(snapshot.protocol),
            mirror=text("charger_connected_mirror_on" if self._mirror_enabled else "charger_connected_mirror_off"),
        )
        persistent_notification.async_create(
            self._hass,
            message,
            title=text("charger_connected_title"),
            notification_id=charger_notification_id(snapshot.cp_id),
        )

    # Frames are the mirror's business, not the entities'. They are only written to the log, and only at debug level of
    # the "frames" logger, so the owner can see exactly what a charger sends when deciding what else is worth a sensor.
    def on_frame(self, cp_id: str, sender: str, frame: Frame) -> None:
        if _FRAME_LOGGER.isEnabledFor(logging.DEBUG):
            _FRAME_LOGGER.debug("%s %s %s", cp_id, sender, frame)

    # The snapshot already publishes the active charge for Home Assistant.
    def on_start_committed(self, cp_id: str, transaction_id: int, payload: dict[str, Any]) -> None:
        return None

    # Keep a finished charge in the history and tell the entities that show it.
    def on_session_finished(self, session: ChargeSession) -> None:
        self._sessions.add(session)
        _LOGGER.info(
            "[build %s] Charge on %s finished: %s Wh in %s s",
            self._build,
            session.cp_id,
            session.energy_wh,
            int(session.duration_s),
        )
        async_dispatcher_send(self._hass, session_signal(self._entry_id, session.cp_id))

    # The final snapshot with connected=False already told the entities; the log line is for the owner.
    def on_disconnected(self, cp_id: str) -> None:
        _LOGGER.info("[build %s] Charger %s disconnected", self._build, cp_id)


@dataclass
class HubRuntime:
    """The running parts of one config entry."""

    central: CentralSystem
    listener: HubListener
    mirror: VendorMirror | None
    sessions: SessionStore
    limits: LimitStore
    plans: PlanSync


type OcppHubConfigEntry = ConfigEntry[HubRuntime]
