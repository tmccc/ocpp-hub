"""The setup form and the options form of the vendor mirror."""

from __future__ import annotations

from typing import Any

import pytest
from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType, InvalidData
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ocpp_hub.const import (
    CONF_MAX_CURRENT,
    CONF_PORT,
    CONF_SCHEDULE_INSIDE,
    CONF_SCHEDULE_OUTSIDE,
    CONF_VENDOR_COMMANDS,
    CONF_VENDOR_PASSWORD,
    CONF_VENDOR_URL,
    CONF_VENDOR_USER,
    DEFAULT_PORT,
    DOMAIN,
)


# An entry that is not set up, so an options flow can run without starting a server.
def _entry(hass: HomeAssistant, options: dict[str, Any] | None = None) -> MockConfigEntry:
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_PORT: DEFAULT_PORT}, options=options or {})
    entry.add_to_hass(hass)
    return entry


# Submit the options form of an entry and return the result.
async def _submit(hass: HomeAssistant, entry: MockConfigEntry, form: dict[str, Any]) -> Any:
    flow = await hass.config_entries.options.async_init(entry.entry_id)
    assert flow["type"] is FlowResultType.FORM and flow["step_id"] == "init"
    return await hass.config_entries.options.async_configure(flow["flow_id"], user_input=form)


# Intent: the setup form asks for a port, creates the entry, and refuses a second instance.
# Why: there is one server per Home Assistant, and its port is the only thing the charger side needs.
async def test_user_flow_creates_one_entry(hass: HomeAssistant) -> None:
    form = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    assert form["type"] is FlowResultType.FORM and form["step_id"] == "user"

    created = await hass.config_entries.flow.async_configure(form["flow_id"], user_input={CONF_PORT: 9001})
    again = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})

    assert created["type"] is FlowResultType.CREATE_ENTRY and created["data"] == {CONF_PORT: 9001}
    assert again["type"] is FlowResultType.ABORT and again["reason"] == "single_instance_allowed"


# Intent: valid vendor options are stored, with the password in its own field and no credentials in the URL.
# Why: the stored options are what the mirror is built from at the next setup.
async def test_options_are_stored(hass: HomeAssistant) -> None:
    entry = _entry(hass)

    result = await _submit(
        hass,
        entry,
        {
            CONF_VENDOR_URL: " wss://vendor.example:8443/ocpp ",
            CONF_VENDOR_USER: "mirror",
            CONF_VENDOR_PASSWORD: "s3cret",
        },
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        CONF_VENDOR_URL: "wss://vendor.example:8443/ocpp",
        CONF_VENDOR_USER: "mirror",
        CONF_VENDOR_PASSWORD: "s3cret",
        CONF_VENDOR_COMMANDS: "reject",
        CONF_MAX_CURRENT: 16,
    }


# Intent: the options form saves plan limits without a schedule helper.
# Why: the app and the set-plan service also use the saved inside and outside limits.
async def test_plan_limits_are_saved_without_a_helper(hass: HomeAssistant) -> None:
    entry = _entry(hass)
    result = await _submit(hass, entry, {CONF_SCHEDULE_INSIDE: 14, CONF_SCHEDULE_OUTSIDE: 6})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_SCHEDULE_INSIDE] == 14 and entry.options[CONF_SCHEDULE_OUTSIDE] == 6


# Intent: an unusable address or half a credential pair is refused on the field it belongs to, with a message code.
# Why: a typo must show up on the form and not as a mirror that silently never connects.
@pytest.mark.parametrize(
    ("form", "field", "code"),
    [
        ({CONF_VENDOR_URL: "https://vendor.example/ocpp"}, CONF_VENDOR_URL, "invalid_url"),
        ({CONF_VENDOR_URL: "wss://me:secret@vendor.example/ocpp"}, CONF_VENDOR_URL, "credentials_in_url"),
        (
            {CONF_VENDOR_URL: "wss://vendor.example/ocpp", CONF_VENDOR_USER: "me"},
            CONF_VENDOR_USER,
            "credentials_incomplete",
        ),
        (
            {CONF_VENDOR_URL: "wss://vendor.example/ocpp", CONF_VENDOR_PASSWORD: "only-a-password"},
            CONF_VENDOR_USER,
            "credentials_incomplete",
        ),
    ],
)
async def test_invalid_options_are_refused(hass: HomeAssistant, form: dict[str, Any], field: str, code: str) -> None:
    entry = _entry(hass)

    result = await _submit(hass, entry, form)

    assert result["type"] is FlowResultType.FORM and result["errors"] == {field: code}
    assert entry.options == {}


# Intent: an empty password keeps the stored one while the host stays the same, and the form never shows it.
# Why: the owner changes the path without retyping a secret that the form cannot display anyway.
async def test_empty_password_keeps_the_stored_one(hass: HomeAssistant) -> None:
    stored = {CONF_VENDOR_URL: "wss://old.example/ocpp", CONF_VENDOR_USER: "mirror", CONF_VENDOR_PASSWORD: "s3cret"}
    entry = _entry(hass, stored)
    flow = await hass.config_entries.options.async_init(entry.entry_id)
    suggested = {
        str(key): key.description["suggested_value"]
        for key in flow["data_schema"].schema
        if key.description and key.description.get("suggested_value") is not None
    }

    result = await hass.config_entries.options.async_configure(
        flow["flow_id"], user_input={CONF_VENDOR_URL: "wss://old.example/v2/ocpp", CONF_VENDOR_USER: "mirror"}
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        **stored,
        CONF_VENDOR_URL: "wss://old.example/v2/ocpp",
        CONF_VENDOR_COMMANDS: "reject",
        CONF_MAX_CURRENT: 16,
    }
    assert suggested == {CONF_VENDOR_URL: "wss://old.example/ocpp", CONF_VENDOR_USER: "mirror"}


# Intent: an address on another host or port does not inherit the stored password; the form asks for one again.
# Why: the stored password belongs to the old server and must never be sent to a new one without being retyped.
@pytest.mark.parametrize(
    "new_url", ["wss://new.example/ocpp", "wss://old.example:8443/ocpp", "ws://old.example/ocpp", "ws://[::1/ocpp"]
)
async def test_new_server_does_not_inherit_the_stored_password(hass: HomeAssistant, new_url: str) -> None:
    stored = {CONF_VENDOR_URL: "wss://old.example/ocpp", CONF_VENDOR_USER: "mirror", CONF_VENDOR_PASSWORD: "s3cret"}
    entry = _entry(hass, stored)

    result = await _submit(hass, entry, {CONF_VENDOR_URL: new_url, CONF_VENDOR_USER: "mirror"})

    assert result["type"] is FlowResultType.FORM and entry.options == stored
    assert set(result["errors"].values()) <= {"credentials_incomplete", "invalid_url"} and result["errors"]


# Intent: clearing the address switches the mirror off and forgets the credentials.
# Why: a password that stays behind after the mirror was switched off is a secret nobody can see or remove.
async def test_empty_url_switches_the_mirror_off(hass: HomeAssistant) -> None:
    entry = _entry(
        hass, {CONF_VENDOR_URL: "wss://old.example/ocpp", CONF_VENDOR_USER: "mirror", CONF_VENDOR_PASSWORD: "s3cret"}
    )

    result = await _submit(hass, entry, {CONF_VENDOR_URL: "", CONF_VENDOR_USER: "mirror"})

    assert result["type"] is FlowResultType.CREATE_ENTRY and entry.options == {CONF_MAX_CURRENT: 16}


# Intent: removing the user name drops the stored password too, so the pair stays whole.
# Why: a password without a user name would be refused when the mirror starts.
async def test_clearing_the_user_drops_the_stored_password(hass: HomeAssistant) -> None:
    entry = _entry(
        hass, {CONF_VENDOR_URL: "wss://old.example/ocpp", CONF_VENDOR_USER: "mirror", CONF_VENDOR_PASSWORD: "s3cret"}
    )

    result = await _submit(hass, entry, {CONF_VENDOR_URL: "wss://old.example/ocpp", CONF_VENDOR_USER: ""})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        CONF_VENDOR_URL: "wss://old.example/ocpp",
        CONF_VENDOR_COMMANDS: "reject",
        CONF_MAX_CURRENT: 16,
    }


# Intent: forwarding the vendor's commands is stored when chosen, and an unknown policy is refused.
# Why: forwarding hands the manufacturer a hand on the charger; it must be an explicit choice that survives a reload,
# and a value outside the two policies must never be stored.
async def test_forwarding_is_stored_only_when_chosen(hass: HomeAssistant) -> None:
    entry = _entry(hass)

    result = await _submit(hass, entry, {CONF_VENDOR_URL: "wss://vendor.example/ocpp", CONF_VENDOR_COMMANDS: "forward"})
    with pytest.raises(InvalidData):
        await _submit(hass, entry, {CONF_VENDOR_URL: "wss://vendor.example/ocpp", CONF_VENDOR_COMMANDS: "everything"})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_VENDOR_COMMANDS] == "forward"
