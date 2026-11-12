#!/usr/bin/env python3
"""Inspect or reconcile one cloud-accepted Start under exclusive offline journal ownership and verified zero."""

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
from ocpp_hub_core.prototype.managed_delivery_recovery import (  # noqa: E402
    DeliveryAttestation,
    delivery_candidate,
    reconcile_delivery_start,
)
from ocpp_hub_core.prototype.security import SecurityError  # noqa: E402
from ocpp_hub_core.prototype.store import Journal  # noqa: E402
from ocpp_hub_core.prototype.trial_config import private_json  # noqa: E402
from ocpp_hub_core.prototype.trial_resources import ResourceBook  # noqa: E402


# The explicit actions have a read-only native observer and journal owner; none has a charger writer or listener.
async def run(action: str, config: Path, strategy: Path, evidence_path: Path | None, native_port: int = 80) -> int:
    if type(native_port) is not int or not 1 <= native_port <= 65535:
        raise SecurityError("native HTTP port is invalid")
    prepared = preflight(config)
    spec = native_spec(strategy, prepared)
    evidence = None
    if evidence_path is not None:
        forbidden = [
            config,
            strategy,
            prepared.config.native_secret,
            prepared.config.certificate,
            prepared.config.private_key,
        ]
        forbidden += [
            p
            for p in (prepared.config.incoming_secret, prepared.config.upstream_secret, prepared.config.ca_file)
            if p is not None
        ]
        if evidence_path.resolve() in {p.resolve() for p in forbidden}:
            raise SecurityError("cloud evidence must not alias a private configuration input")
        prepared.config.check_paths(evidence_path)
        evidence = DeliveryAttestation.parse(private_json(evidence_path), recent=False)
    if action == "check":
        emit({"inputs_valid": True, "evidence_supplied": evidence is not None, "charger_changed": False})
        return 0
    if action == "reconcile-start" and evidence is None:
        raise SecurityError("reconciliation requires explicit independently verified cloud evidence")
    prepared.observer.port = native_port
    journal = Journal(prepared.config.journal, prepared.config.identity)
    try:
        book = ResourceBook(journal, prepared.baseline)
        native = await prepared.observer.observe_handover()
        if action == "inspect":
            emit({"candidate": delivery_candidate(journal, book, spec, native), "charging_started": False})
        else:
            assert evidence is not None
            emit(reconcile_delivery_start(journal, book, spec, native, evidence))
    finally:
        journal.close()
    return 0


# Default check never opens the journal or network; named reconciliation never retrieves or guesses a cloud ID.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", nargs="?", choices=("check", "inspect", "reconcile-start"), default="check")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--native-bounded-spec", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--native-port", type=int, default=80)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        return asyncio.run(run(args.action, args.config, args.native_bounded_spec, args.evidence, args.native_port))
    except Exception as error:
        # Operator files and native error bodies are private; publish only the classified boundary failure.
        emit({"reconciliation_failed": type(error).__name__, "charging_started": False})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
