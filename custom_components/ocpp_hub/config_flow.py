"""Config flow: one OCPP Hub per Home Assistant, set up with a port; the vendor mirror is set in the options."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.selector import (
    EntitySelector,
    EntitySelectorConfig,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectSelector,
    SelectSelectorConfig,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    CONF_MAX_CURRENT,
    CONF_PORT,
    CONF_SCHEDULE_ENTITY,
    CONF_SCHEDULE_INSIDE,
    CONF_SCHEDULE_OUTSIDE,
    CONF_VENDOR_COMMANDS,
    CONF_VENDOR_PASSWORD,
    CONF_VENDOR_URL,
    CONF_VENDOR_USER,
    DEFAULT_MAX_CURRENT,
    DEFAULT_PORT,
    DOMAIN,
)
from .core.mirror_model import COMMAND_POLICIES, COMMANDS_REJECT, MirrorSettings, MirrorSettingsError

# The form field each settings error belongs to; the others are shown on the form as a whole.
ERROR_FIELDS = {
    "invalid_url": CONF_VENDOR_URL,
    "credentials_in_url": CONF_VENDOR_URL,
    "credentials_incomplete": CONF_VENDOR_USER,
}


# Whether two vendor addresses use the same scheme, host and port; an address that cannot be read matches nothing.
# A stored password may only follow an address to the same server: it must never be sent to a new one unasked, nor
# over ws:// when it was stored for wss://.
def _same_server(first: str, second: str) -> bool:
    try:
        a, b = urlsplit(first), urlsplit(second)
        return bool(a.hostname) and (a.scheme, a.hostname, a.port) == (b.scheme, b.hostname, b.port)
    except ValueError:
        return False


# Turn the submitted options form into the options to store, or raise MirrorSettingsError when they cannot work.
# An empty URL switches the mirror off and forgets the credentials. A password left empty keeps the stored one, because
# the form never shows it; that only applies while a user name is still set and the address still points at the same
# server, so a password can neither linger without a user nor travel to a new host.
def vendor_options(form: Mapping[str, Any], stored: Mapping[str, Any]) -> dict[str, str]:
    url = (form.get(CONF_VENDOR_URL) or "").strip()
    if not url:
        return {}
    user = (form.get(CONF_VENDOR_USER) or "").strip()
    keeps_stored = bool(user) and _same_server(url, stored.get(CONF_VENDOR_URL, ""))
    password = form.get(CONF_VENDOR_PASSWORD) or (stored.get(CONF_VENDOR_PASSWORD, "") if keeps_stored else "")
    commands = form.get(CONF_VENDOR_COMMANDS) or COMMANDS_REJECT
    MirrorSettings(url=url, user=user or None, password=password or None, commands=commands)
    options = {CONF_VENDOR_URL: url, CONF_VENDOR_COMMANDS: commands}
    if user:
        options[CONF_VENDOR_USER] = user
    if password:
        options[CONF_VENDOR_PASSWORD] = password
    return options


# The schedule options from the form: the helper and the two limits, each only when it was filled in.
def schedule_options(form: Mapping[str, Any]) -> dict[str, Any]:
    options: dict[str, Any] = {}
    if form.get(CONF_SCHEDULE_ENTITY):
        options[CONF_SCHEDULE_ENTITY] = form[CONF_SCHEDULE_ENTITY]
    for key in (CONF_SCHEDULE_INSIDE, CONF_SCHEDULE_OUTSIDE):
        if form.get(key) is not None:
            options[key] = float(form[key])
    return options


class OcppHubConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the setup form."""

    VERSION = 1

    # Give the entry an options flow, where the vendor mirror is set.
    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OcppHubOptionsFlow:
        return OcppHubOptionsFlow()

    # Show the port form and create the entry once it is submitted.
    # The port is the only setting the charger side needs; everything else is configured per charger later.
    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if user_input is not None:
            return self.async_create_entry(title="OCPP Hub", data=user_input)
        schema = vol.Schema(
            {vol.Required(CONF_PORT, default=DEFAULT_PORT): vol.All(int, vol.Range(min=1024, max=65535))}
        )
        return self.async_show_form(step_id="user", data_schema=schema)


class OcppHubOptionsFlow(OptionsFlow):
    """The options form of the vendor mirror."""

    # Show the form, check what was submitted, and store the options when they can work.
    # The password is never filled in on the form; leaving it empty keeps the stored one.
    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                ceiling = int(user_input.get(CONF_MAX_CURRENT, DEFAULT_MAX_CURRENT))
                options = vendor_options(user_input, self.config_entry.options)
                return self.async_create_entry(
                    data={**options, CONF_MAX_CURRENT: ceiling, **schedule_options(user_input)}
                )
            except MirrorSettingsError as error:
                errors[ERROR_FIELDS.get(error.code, "base")] = error.code
        shown = user_input if user_input is not None else self.config_entry.options
        schema = vol.Schema(
            {
                vol.Optional(CONF_VENDOR_URL, description={"suggested_value": shown.get(CONF_VENDOR_URL, "")}): str,
                vol.Optional(CONF_VENDOR_USER, description={"suggested_value": shown.get(CONF_VENDOR_USER, "")}): str,
                vol.Optional(CONF_VENDOR_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
                vol.Optional(
                    CONF_VENDOR_COMMANDS, default=shown.get(CONF_VENDOR_COMMANDS, COMMANDS_REJECT)
                ): SelectSelector(
                    SelectSelectorConfig(options=list(COMMAND_POLICIES), translation_key=CONF_VENDOR_COMMANDS)
                ),
                vol.Optional(
                    CONF_SCHEDULE_ENTITY, description={"suggested_value": shown.get(CONF_SCHEDULE_ENTITY)}
                ): EntitySelector(EntitySelectorConfig(domain="schedule")),
                vol.Optional(
                    CONF_SCHEDULE_INSIDE, description={"suggested_value": shown.get(CONF_SCHEDULE_INSIDE)}
                ): NumberSelector(NumberSelectorConfig(min=0, max=80, step=1, mode=NumberSelectorMode.BOX)),
                vol.Optional(
                    CONF_SCHEDULE_OUTSIDE, description={"suggested_value": shown.get(CONF_SCHEDULE_OUTSIDE)}
                ): NumberSelector(NumberSelectorConfig(min=0, max=80, step=1, mode=NumberSelectorMode.BOX)),
                vol.Optional(
                    CONF_MAX_CURRENT,
                    default=int(shown.get(CONF_MAX_CURRENT, DEFAULT_MAX_CURRENT)),
                ): NumberSelector(
                    NumberSelectorConfig(min=6, max=80, step=1, mode=NumberSelectorMode.BOX, unit_of_measurement="A")
                ),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
