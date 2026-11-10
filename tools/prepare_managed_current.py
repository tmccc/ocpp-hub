#!/usr/bin/env python3
"""Prepare the approved native32 ceiling under exclusive journal ownership and a verified station zero hold."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.trial_proxy import emit, load_core, native_spec, preflight  # noqa: E402

load_core()
from ocpp_hub_core.prototype.managed_current_prepare import prepare_current_capability  # noqa: E402
from ocpp_hub_core.prototype.native_http import read_native  # noqa: E402
from ocpp_hub_core.prototype.store import Journal  # noqa: E402
from ocpp_hub_core.prototype.trial_resources import ResourceBook  # noqa: E402


# A named action is the only write path; the default parses private inputs without contacting the charger.
async def run(action: str, config_path: Path, spec_path: Path) -> int:
    prepared = preflight(config_path)
    spec = native_spec(spec_path, prepared)
    if action == "check":
        emit({"offline_only": True, "native_ceiling_a": 32, "charger_changed": False})
        return 0
    journal = Journal(prepared.config.journal, prepared.config.identity)
    try:
        book = ResourceBook(journal, prepared.baseline)

        # This adapter permits exactly one previously approved USER change; it has no route, hardware or Start API.
        async def patch_user32() -> None:
            await read_native(
                prepared.config.charger_ip,
                prepared.observer.port,
                prepared.observer.authorization,
                "setConfig",
                {"AmpLmt": "32"},
            )

        emit(await prepare_current_capability(journal, book, spec, prepared.observer.observe_handover, patch_user32))
    finally:
        journal.close()
    return 0


# Explicit preparation authorizes native32 only; service deployment and charging remain separate actions.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", nargs="?", choices=("check", "prepare-native32"), default="check")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--native-bounded-spec", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        return asyncio.run(run(args.action, args.config, args.native_bounded_spec))
    except Exception as error:
        # This operator boundary publishes classifications only, never native bodies, endpoint or credential text.
        emit({"preparation_failed": type(error).__name__, "charging_started": False})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
