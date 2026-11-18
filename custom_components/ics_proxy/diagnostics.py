"""A bounded support download reads cached evidence and public software metadata, never the charger or journal."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from homeassistant.const import __version__ as ha_version
from homeassistant.core import HomeAssistant

from . import ProxyEntry
from .diagnostic_view import PHASES, STATES, diagnostics, explanation, label, number


# Only public installed source/manifest/BUILD files are inspected; private config and raw logs are never opened.
def software() -> dict[str, Any]:
    root = Path(__file__).resolve().parent
    result: dict[str, Any] = {
        "ha_core": ha_version,
        "client_version": None,
        "client_build": None,
        "client_source_sha256": None,
    }
    try:
        version = json.loads((root / "manifest.json").read_text()).get("version")
        if isinstance(version, str) and re.fullmatch(r"\d+\.\d+\.\d+", version):
            result["client_version"] = version
        digest = hashlib.sha256()
        for path in sorted(root.glob("*.py")):
            digest.update(path.name.encode() + b"\0" + path.read_bytes() + b"\0")
        result["client_source_sha256"] = digest.hexdigest()
        if (root / "BUILD").is_file():
            build = (root / "BUILD").read_text().strip()
            if re.fullmatch(r"[a-f0-9]{7,40}(?:[+-]dirty)?", build):
                result["client_build"] = build
    except OSError, ValueError:
        pass  # Missing public version metadata is unknown, never inferred from another checkout.
    return result


# The download never refreshes the coordinator or requests a receipt; failed setup still has a useful empty report.
async def async_get_config_entry_diagnostics(hass: HomeAssistant, entry: ProxyEntry) -> dict[str, Any]:
    metadata = await hass.async_add_executor_job(software)
    coordinator = getattr(entry, "runtime_data", None)
    data = (coordinator.data or {}) if coordinator is not None else {}
    status_age = coordinator.age("status_age_seconds") if coordinator is not None else None
    native_age = coordinator.age("native_age_seconds") if coordinator is not None else None
    fresh = (
        coordinator is not None
        and coordinator.last_update_success
        and coordinator.notice.active
        and coordinator.notice.generation == coordinator.notice_generation
        and data.get("running") is True
        and number(status_age, 15) is not None
    )
    native_fresh = fresh and number(native_age, 15) is not None
    public_software = data.get("software")
    public_software = public_software if isinstance(public_software, dict) else {}
    source, python = public_software.get("proxy_source_sha256"), public_software.get("proxy_python")
    return {
        "software": {
            **metadata,
            "proxy_source_sha256": source
            if isinstance(source, str) and re.fullmatch(r"[a-f0-9]{64}", source)
            else None,
            "proxy_python": python if isinstance(python, str) and re.fullmatch(r"\d+\.\d+\.\d+", python) else None,
        },
        "snapshot_available": bool(data),
        "snapshot_fresh": fresh,
        "native_fresh": native_fresh,
        "status_age_seconds": number(status_age),
        "native_age_seconds": number(native_age),
        "summary": explanation(data, fresh=fresh, native_fresh=native_fresh),
        "last_reported": {
            "state": label(data.get("state"), STATES),
            "phase": label(data.get("phase"), PHASES),
            "desired_control": label(data.get("desired_control"), {"app", "paused"}),
            **{
                key: data.get(key) if type(data.get(key)) is bool else None
                for key in ("healthy", "local_healthy", "cloud_connected", "charger_connected", "local_hold_verified")
            },
        },
        "measurements": {
            key: number(data.get(key)) if native_fresh else None
            for key in ("current_a", "power_w", "station_ceiling_a", "native_limit_a", "requested_current_a")
        },
        "diagnostics": diagnostics(data.get("diagnostics"), timeline=True),
    }
