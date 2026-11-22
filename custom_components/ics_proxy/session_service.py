"""Home Assistant action that returns the local proxy's result for one session request."""

from __future__ import annotations

import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse

from .api import ProxyError
from .coordinator import DOMAIN


# A response belongs to this exact command. The client reads its receipt and never repeats the write.
def register_session_service(hass: HomeAssistant) -> None:
    # Only a loaded entry and an exact current target can send a session command.
    async def handle(call: ServiceCall) -> dict[str, bool]:
        entry = next(
            (entry for entry in hass.config_entries.async_entries(DOMAIN) if entry.state is ConfigEntryState.LOADED),
            None,
        )
        if entry is None:
            return {"confirmed": False}
        coordinator = entry.runtime_data
        data = coordinator.data or {}
        action = call.data["action"]
        target = call.data.get("target")
        revision = data.get("control_revision")
        if not coordinator.last_update_success or type(revision) is not int:
            return {"confirmed": False}
        if action == "start":
            if target is not None or data.get("can_start") is not True or not coordinator.fresh_local_healthy:
                return {"confirmed": False}
            fields: dict[str, str] = {}
        else:
            if not isinstance(target, str) or target != data.get("stop_target"):
                return {"confirmed": False}
            fields = {"target": target}
        confirmed = False
        try:
            await coordinator.client.command(action, revision, **fields)
            confirmed = True
        except ProxyError:
            pass  # An unknown or rejected outcome must never appear confirmed to the caller.
        finally:
            await coordinator.async_refresh()
        return {"confirmed": confirmed}

    hass.services.async_register(
        DOMAIN,
        "session_command",
        handle,
        schema=vol.Schema(
            {
                vol.Required("action"): vol.In(("start", "stop_session")),
                vol.Optional("target"): vol.Match(r"[a-f0-9]{64}\Z"),
            }
        ),
        supports_response=SupportsResponse.ONLY,
    )
