"""Configure one authenticated management connection on the same host as Home Assistant."""

from __future__ import annotations

import re
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import ProxyClient, ProxyError
from .coordinator import DOMAIN

SCHEMA = vol.Schema(
    {
        vol.Required("port", default=18779): vol.All(vol.Coerce(int), vol.Range(min=1024, max=65535)),
        vol.Required("token"): selector.TextSelector(
            selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
        ),
    }
)


class ProxyConfigFlow(ConfigFlow, domain=DOMAIN):
    """Connection validation is read-only and cannot change the charger's current mode."""

    VERSION = 1

    # Validate credentials using status alone and register a stable single local-device identity.
    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors = {}
        if user_input is not None:
            if not isinstance(user_input.get("token"), str) or not re.fullmatch(r"[a-f0-9]{64}", user_input["token"]):
                errors["base"] = "invalid_auth"
            else:
                try:
                    client = ProxyClient(
                        async_get_clientsession(self.hass), user_input.get("port"), user_input["token"]
                    )
                    await client.request("status")
                except ProxyError:
                    errors["base"] = "cannot_connect"
                else:
                    await self.async_set_unique_id("local_managed_proxy")
                    self._abort_if_unique_id_configured()
                    return self.async_create_entry(title="iCS local proxy", data=user_input)
        return self.async_show_form(step_id="user", data_schema=SCHEMA, errors=errors)

    # Programmatic installation follows the same authenticated, read-only validation as the user form.
    async def async_step_import(self, user_input: dict[str, Any]) -> ConfigFlowResult:
        return await self.async_step_user(user_input)
