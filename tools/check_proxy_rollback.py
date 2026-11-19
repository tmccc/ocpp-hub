#!/usr/bin/env python3
"""Check the stopped journal and immutable builds for the rehearsed standalone rollback."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import time
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.trial_proxy import load_core  # noqa: E402

load_core()
from ocpp_hub_core.control import HUB_PROFILE_ID  # noqa: E402
from ocpp_hub_core.prototype.control_state import ControlState  # noqa: E402
from ocpp_hub_core.prototype.managed_intent import decode_intent, decode_pause_claim  # noqa: E402
from ocpp_hub_core.prototype.policy import DeniedError  # noqa: E402
from ocpp_hub_core.prototype.security import SecurityError  # noqa: E402
from ocpp_hub_core.prototype.station import StationChoice  # noqa: E402
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint  # noqa: E402
from ocpp_hub_core.prototype.trial_config import TrialConfig  # noqa: E402
from ocpp_hub_core.prototype.trial_evidence import profile_projection  # noqa: E402


@dataclass(frozen=True)
class Build:
    """A source fingerprint for exactly one side of the tested transition."""

    commit: str
    digest: str
    incident: bool


CURRENT = Build(
    "fc2dc0a95585ca19d75384965aacf1d4d51345ab",
    "f459f45dc2375fdb373e53d13aa5a5e3334b908d96e1704874d22821b1455ea0",
    True,
)
PREVIOUS = Build(
    "7a08f2ade3b9e597099aaece2aef2f8542852a41",
    "71141220fceddd3dc0349a84e430a65d56dc908e43fee2d20327b6ba93b42c7d",
    False,
)
REQUIREMENTS = "2773b4a9f9b16cfe90626e328db61ff281810c3f53d9a86f3178c0b54455586d"
REHEARSED_SCHEMA = "1b3d8757ddca8f860ff1d69e9d092839efcf948bda9a70524e04aa361bd9c8a4"

# These include the inactive history labels and markers the two rehearsed builds can retain.
SUPPORTED_METADATA = frozenset(
    """
    boot connection_epoch control identity local_start_receipt managed_control_intent
    managed_current_capability managed_current_change managed_current_prepare_intent managed_mode
    managed_native_failure managed_ordinary_recovery managed_ordinary_source managed_ordinary_zero_introduction
    managed_owner_epoch managed_owner_pause managed_owner_stop managed_schedule_tags managed_test_charge
    native_bounded_handover next_transaction observation_boot_epoch observation_generation station_selection
    trial_allocation trial_baseline trial_binding trial_phase trial_starting vendor_profile_transactions vendor_profiles
    """.split()
)


@dataclass(frozen=True)
class RollbackCheck:
    """A classified snapshot of eligibility for the one rehearsed build pair."""

    allowed: bool
    reason: str
    current_commit: str = CURRENT.commit
    previous_commit: str = PREVIOUS.commit
    pause_revision: int | None = None


class RollbackRefusedError(ValueError):
    """A fixed classification that excludes saved request bodies and private paths."""


# Keep refusal classifications local to the check and reject ambiguous persisted states.
def require(condition: bool, reason: str) -> None:
    if not condition:
        raise RollbackRefusedError(reason)


# Bind the exact core and entry-point bytes using the same fingerprint as each build's reporting.
def build_matches(root: Path, build: Build) -> bool:
    core = root / "custom_components/ocpp_hub/core"
    names = sorted(core.rglob("*.py"))
    tools = ["managed_proxy.py", "managed_reporting.py"]
    if build.incident:
        tools.append("managed_incident.py")
    tools.append("trial_proxy.py")
    names.extend(root / "tools" / filename for filename in tools)
    digest = hashlib.sha256()
    try:
        for path in names:
            if path.is_symlink() or not path.is_file():
                return False
            digest.update(str(path.relative_to(root)).encode() + b"\0" + path.read_bytes() + b"\0")
        pins = (root / "requirements-offline.txt").read_bytes()
    except OSError:
        return False
    return digest.hexdigest() == build.digest and hashlib.sha256(pins).hexdigest() == REQUIREMENTS


# Compare the complete schema, including triggers and index predicates, with the known predecessor's layout.
def schema_digest(db: sqlite3.Connection) -> str:
    version = db.execute("PRAGMA user_version").fetchone()[0]
    application = db.execute("PRAGMA application_id").fetchone()[0]
    objects = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name").fetchall()
    return hashlib.sha256(json.dumps([version, application, objects], separators=(",", ":")).encode()).hexdigest()


# Take the existing single-owner lock without creating it, then read real WAL contents in one SQLite snapshot.
@contextmanager
def stopped_journal(path: Path):
    info = path.lstat()
    require(
        stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077, "journal_unavailable"
    )
    descriptor = os.open(str(path) + ".lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as lock:
        held = os.fstat(lock.fileno())
        require(
            stat.S_ISREG(held.st_mode) and held.st_uid == os.getuid() and not held.st_mode & 0o077,
            "journal_unavailable",
        )
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RollbackRefusedError("journal_owner_running") from None
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.1)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            yield db


# Keep known allocation and both configured identities tied to the preserved transaction ledger.
def require_allocation(db: sqlite3.Connection, config: TrialConfig, meta: dict[str, Any]) -> None:
    proof, counter = meta.get("trial_allocation"), meta.get("next_transaction")
    maximum = db.execute("SELECT max(local) FROM transactions").fetchone()[0]
    require(meta.get("identity") == config.identity and isinstance(proof, dict), "allocation_unverified")
    floor, count, observed = (proof.get(key) for key in ("floor", "count", "observed_at"))
    require(
        set(proof) == {"version", "identity_digest", "hardware_digest", "observed_at", "digest", "count", "floor"}
        and type(proof["version"]) is int
        and proof["version"] == 1
        and proof["identity_digest"] == fingerprint(config.identity)
        and proof["hardware_digest"] == fingerprint(config.hardware_identity)
        and isinstance(proof["digest"], str)
        and re.fullmatch(r"[0-9a-f]{64}", proof["digest"]) is not None
        and type(floor) is type(counter) is int
        and 1000 <= floor <= counter < 2**31 - 1
        and (maximum is None or counter >= maximum)
        and type(count) is int
        and 0 <= count <= 10000
        and type(observed) in (int, float)
        and 0 < observed <= time.time() + 2,
        "allocation_unverified",
    )


# An acknowledged Start remains unresolved until its exact local lifecycle closed.
def require_settled_start(db: sqlite3.Connection, meta: dict[str, Any], control: dict[str, Any]) -> None:
    require(control.get("start_pending", False) is False, "unresolved_start")
    receipt = meta.get("local_start_receipt", {})
    require(isinstance(receipt, dict), "unrehearsed_journal_state")
    if not receipt:
        return
    stage, local, request = (receipt.get(key) for key in ("stage", "local", "request_id"))
    require(stage not in ("prepared", "issued") and not (stage == "accepted" and local is None), "unresolved_start")
    require(
        isinstance(request, str)
        and db.execute(
            "SELECT 1 FROM managed_requests WHERE id=? AND action='start' AND state IN ('completed','failed')",
            (request,),
        ).fetchone()
        is not None,
        "unrehearsed_journal_state",
    )
    if stage == "accepted":
        require(
            type(local) is int
            and db.execute(
                "SELECT 1 FROM transactions WHERE local=? AND stop IS NOT NULL AND vendor IS NOT NULL", (local,)
            ).fetchone()
            is not None,
            "unresolved_start",
        )
    else:
        require(stage in ("rejected", "unsent") and local is None, "unrehearsed_journal_state")


# Require the last commissioning binding and the exact reserved zero profile saved for that station.
def require_installation(db: sqlite3.Connection, meta: dict[str, Any]) -> None:
    mode = meta.get("managed_mode")
    binding = meta.get("trial_binding")
    boot = meta.get("boot")
    require(
        meta.get("trial_phase") == "active"
        and meta.get("trial_starting") is False
        and isinstance(mode, dict)
        and set(mode) == {"mode", "spec_digest", "local_start"}
        and mode["mode"] == "app_schedule"
        and mode["local_start"] is False
        and isinstance(mode["spec_digest"], str)
        and re.fullmatch(r"[0-9a-f]{64}", mode["spec_digest"]) is not None
        and isinstance(binding, dict)
        and set(binding) == {"boot_digest", "configuration_digest", "station"}
        and isinstance(boot, dict)
        and binding["boot_digest"] == fingerprint(boot)
        and isinstance(binding["configuration_digest"], str)
        and re.fullmatch(r"[0-9a-f]{64}", binding["configuration_digest"]) is not None,
        "installation_state_unrehearsed",
    )
    try:
        zero = profile_projection(StationChoice(**binding["station"]).payload(0))
    except TypeError, DeniedError, SecurityError:
        raise RollbackRefusedError("installation_state_unrehearsed") from None
    row = db.execute(
        "SELECT desired,confirmed,state FROM trial_resources WHERE key=?", (f"profile:{HUB_PROFILE_ID}",)
    ).fetchone()
    require(
        row is not None and row[2] == "confirmed" and row[0] == row[1] and json.loads(row[0]) == zero,
        "saved_station_zero_required",
    )


# Evaluate only the paused, settled journal variant proved by the cross-build rehearsal.
def check_snapshot(db: sqlite3.Connection, config: TrialConfig) -> int:
    require(schema_digest(db) == REHEARSED_SCHEMA, "journal_schema_unrehearsed")
    require(
        db.execute("PRAGMA quick_check(1)").fetchone() == ("ok",)
        and db.execute("PRAGMA foreign_key_check").fetchone() is None,
        "journal_integrity_unverified",
    )
    raw = db.execute("SELECT key,value FROM meta").fetchall()
    require(all(key in SUPPORTED_METADATA for key, _ in raw), "unrehearsed_journal_metadata")
    meta = {key: json.loads(value) for key, value in raw}
    control = meta.get("control")
    require(isinstance(control, dict), "unrehearsed_journal_state")
    require_settled_start(db, meta, control)
    require(
        db.execute("SELECT 1 FROM managed_requests WHERE state NOT IN ('completed','failed') LIMIT 1").fetchone()
        is None,
        "unresolved_request",
    )
    require(
        db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone() is None
        and db.execute("SELECT 1 FROM delivery_replies LIMIT 1").fetchone() is None
        and db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone() is None,
        "unresolved_lifecycle_delivery",
    )
    require(
        db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone() is None
        and db.execute(
            "SELECT 1 FROM trial_resources WHERE state NOT IN ('confirmed','absent_verified') LIMIT 1"
        ).fetchone()
        is None,
        "unresolved_device_change",
    )
    for key, states in (
        ("managed_test_charge", ("completed",)),
        ("managed_current_change", ("completed", "cancelled")),
        ("managed_ordinary_recovery", ("complete",)),
    ):
        value = meta.get(key, {})
        require(isinstance(value, dict) and (not value or value.get("state") in states), "unrehearsed_journal_state")
    preparation = meta.get("managed_current_prepare_intent", {})
    capability = meta.get("managed_current_capability")
    require(
        isinstance(preparation, dict)
        and (
            not preparation
            and capability is None
            or preparation.get("stage") == "complete"
            and isinstance(capability, dict)
            and bool(capability)
            and preparation.get("capability") == capability
        ),
        "unresolved_current_preparation",
    )
    require_installation(db, meta)
    intent = decode_intent(meta.get("managed_control_intent"))
    pause = decode_pause_claim(meta.get("managed_owner_pause"))
    resume, stop_epoch, ticket = (control.get(key) for key in ("resume_limit", "stop_epoch", "ticket"))
    # Shutdown takes a new physical ticket. The saved owner Pause still identifies the desired revision.
    require(
        ControlState(control).explicit_local_hold
        and control.get("uncertain") is False
        and type(stop_epoch) is int
        and 0 <= stop_epoch < 2**53
        and isinstance(ticket, str)
        and re.fullmatch(r"[0-9a-f]{32}", ticket) is not None
        and type(resume) in (int, float)
        and 6 <= resume <= 32
        and pause.owns(intent, pause.ticket)
        and isinstance(pause.ticket, str)
        and re.fullmatch(r"[0-9a-f]{32}", pause.ticket) is not None,
        "settled_owner_pause_required",
    )
    require_allocation(db, config, meta)
    return intent.revision


# Give a classified read-only result. The check never launches either build or changes a configuration.
def check_rollback(config: TrialConfig, current: Path, previous: Path) -> RollbackCheck:
    try:
        require(build_matches(current, CURRENT) and build_matches(previous, PREVIOUS), "build_pair_unrehearsed")
        with stopped_journal(config.journal) as db:
            revision = check_snapshot(db, config)
        return RollbackCheck(True, "rehearsed_stopped_journal", pause_revision=revision)
    except RollbackRefusedError as error:
        return RollbackCheck(False, str(error))
    except (
        OSError,
        sqlite3.Error,
        ValueError,
        TypeError,
        RecursionError,
        OverflowError,
        RecoveryRequiredError,
        SecurityError,
    ):
        return RollbackCheck(False, "journal_unavailable_or_malformed")


# Load the private configuration locally and print a result that contains no file paths, IDs or saved payloads.
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--current-root", required=True, type=Path)
    parser.add_argument("--previous-root", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        config = TrialConfig.load(args.config)
    except OSError, ValueError, SecurityError:
        result = RollbackCheck(False, "configuration_unavailable")
    else:
        result = check_rollback(config, args.current_root, args.previous_root)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0 if result.allowed else 1


if __name__ == "__main__":
    raise SystemExit(main())
