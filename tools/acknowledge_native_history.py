#!/usr/bin/env python3
"""Reconcile a supported native history fault under exclusive offline journal ownership at verified zero."""

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

from ocpp_hub_core.prototype.native_history_recovery import (  # noqa: E402
    acknowledge_history_policy,
    acknowledge_start_attempts,
)
from ocpp_hub_core.prototype.store import Journal  # noqa: E402
from ocpp_hub_core.prototype.trial_resources import ResourceBook  # noqa: E402


# No listener, charger writer or service lifecycle is available through this deliberately small operator tool.
async def run(action: str, config_path: Path, spec_path: Path) -> int:
    prepared = preflight(config_path)
    spec = native_spec(spec_path, prepared)
    if action == "check":
        emit({"inputs_valid": True, "offline_only": True, "charger_changed": False})
        return 0
    journal = Journal(prepared.config.journal, prepared.config.identity)
    try:
        book = ResourceBook(journal, prepared.baseline)
        native = await prepared.observer.observe_handover()
        reconcile = acknowledge_start_attempts if action == "reconcile-start-attempts" else acknowledge_history_policy
        emit(reconcile(journal, book, spec, native))
    finally:
        journal.close()
    return 0


# A named acknowledgement is required; the default only validates private inputs without opening the journal.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", nargs="?", choices=("check", "acknowledge", "reconcile-start-attempts"), default="check"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--native-bounded-spec", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        return asyncio.run(run(args.action, args.config, args.native_bounded_spec))
    except Exception as error:
        # This operator boundary never publishes private native bodies, credential paths or peer error strings.
        emit({"acknowledgement_failed": type(error).__name__, "charging_started": False})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
