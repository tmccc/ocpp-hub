#!/usr/bin/env python3
"""Check private access offline, or explicitly verify the reported SEAT pause using one read-only cloud GET."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from trial_proxy import load_core

load_core()
from ocpp_hub_core.prototype.seat_access import configured_verifier  # noqa: E402


# An offline check is the default; explicit verify is the only action that contacts the fixed cloud host.
def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", nargs="?", choices=("check", "verify"), default="check")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    try:
        verifier = configured_verifier(args.config)
        if args.action == "check":
            print(json.dumps({"private_access_valid": True, "network_contacted": False, "handover_authority": False}))
            return 0
        report = asyncio.run(verifier.check())
        print(json.dumps(report.summary(), sort_keys=True))
        return 3
    except Exception:
        # Credential, network and parser failures must not print raw values or credential-bearing tracebacks.
        print(json.dumps({"car_pause_check": "private_access_unavailable", "handover_authority": False}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
