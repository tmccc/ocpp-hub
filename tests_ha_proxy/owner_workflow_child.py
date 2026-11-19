"""Run one actual Home Assistant instance for the isolated owner workflow test."""

import asyncio
import json
import sys
from pathlib import Path

from tools.offline_ha_validate import network_guard


# Read actual entity states after a fresh poll and identify which saved entry this process loaded.
async def view(hass, entry):
    await entry.runtime_data.async_refresh()
    return {
        "entry_id": entry.entry_id,
        "mode": hass.states.get("sensor.ics_proxy_control_mode").state,
        "return_available": hass.states.get("button.ics_proxy_app_control").state != "unavailable",
    }


# Press the same registered button that an owner uses and report the actual result from Home Assistant.
async def press(hass, button):
    from homeassistant.exceptions import HomeAssistantError

    assert button in ("pause", "app_control")
    try:
        await hass.services.async_call("button", "press", {"entity_id": f"button.ics_proxy_{button}"}, blocking=True)
    except HomeAssistantError as error:
        return {"error": str(error)}
    return {"ok": True}


# Reopen the saved config entry after replacement and keep commands on the real HA services.
async def run(config, port, restore):
    from homeassistant.config_entries import ConfigEntryState
    from homeassistant.setup import async_setup_component

    from tests_ha_proxy.conftest import actual_core
    from tests_ha_proxy.test_client import setup

    async with actual_core(config, restore=restore) as hass:
        if restore:
            (entry,) = hass.config_entries.async_entries("ics_proxy")
            assert await async_setup_component(hass, "persistent_notification", {})
            assert await hass.config_entries.async_setup(entry.entry_id)
        else:
            assert not hass.config_entries.async_entries("ics_proxy")
            entry = await setup(hass, {"port": port})
        assert entry.state is ConfigEntryState.LOADED
        await hass.async_start()
        print(json.dumps(await view(hass, entry)), flush=True)

        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(limit=1024)
        transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        try:
            while raw := await reader.readline():
                command = raw.strip()
                if command == b"view":
                    result = await view(hass, entry)
                else:
                    result = await press(hass, command.decode())
                print(json.dumps(result), flush=True)
        finally:
            transport.close()


if __name__ == "__main__":
    sys.addaudithook(network_guard)
    config_dir, port, generation = sys.argv[1:]
    asyncio.run(run(Path(config_dir), int(port), generation != "1"))
