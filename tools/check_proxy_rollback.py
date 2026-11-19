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
from ocpp_hub_core.prototype.managed_intent import (  # noqa: E402
    decode_intent,
    decode_pause_claim,
    resolved_timed_starts,
)
from ocpp_hub_core.prototype.native_history import indexed  # noqa: E402
from ocpp_hub_core.prototype.native_start_attempts import split_attempts  # noqa: E402
from ocpp_hub_core.prototype.policy import DeniedError  # noqa: E402
from ocpp_hub_core.prototype.retired_schedules import schedule_history  # noqa: E402
from ocpp_hub_core.prototype.security import SecurityError  # noqa: E402
from ocpp_hub_core.prototype.station import StationChoice  # noqa: E402
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint  # noqa: E402
from ocpp_hub_core.prototype.trial_config import TrialConfig  # noqa: E402
from ocpp_hub_core.prototype.trial_evidence import profile_projection  # noqa: E402


@dataclass(frozen=True)
class Build:
    """A source fingerprint for one side of a tested transition."""

    commit: str
    digest: str
    incident: bool
    migrated_only: bool = False


CURRENT = Build(
    "fc2dc0a95585ca19d75384965aacf1d4d51345ab",
    "f459f45dc2375fdb373e53d13aa5a5e3334b908d96e1704874d22821b1455ea0",
    True,
)
MIGRATED_RELEASES = (
    Build(
        "051500f16ca3915ba50db32bb8aef6fcad0556da",
        "d6bf540b51441b528200be4e373067c93f40857edb9238160d9a80b07c09fc9e",
        True,
        migrated_only=True,
    ),
    Build(
        "3fdace88bbfb7e8f0802b9e295915eb8a4492ce1",
        "2c21808bd8345d9f623b7d459d2ef763176ee78e8f48b1cf2876401f35c7f8cb",
        True,
        migrated_only=True,
    ),
)
PREVIOUS = Build(
    "7a08f2ade3b9e597099aaece2aef2f8542852a41",
    "71141220fceddd3dc0349a84e430a65d56dc908e43fee2d20327b6ba93b42c7d",
    False,
)
REQUIREMENTS = "2773b4a9f9b16cfe90626e328db61ff281810c3f53d9a86f3178c0b54455586d"
REHEARSED_SCHEMA = "1b3d8757ddca8f860ff1d69e9d092839efcf948bda9a70524e04aa361bd9c8a4"
MIGRATED_SCHEMA = "0b08c43048c89c897d8224d2335e43294652dd9ba58fd940030a61437c776f0d"

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
MIGRATED_METADATA = SUPPORTED_METADATA | frozenset(
    """
    last_status managed_release_operator_acknowledgement managed_temporary_occurrences managed_temporary_schedule
    managed_temporary_schedule_head native_history_archive_binding native_history_policy
    native_history_policy_acknowledgement native_history_summary native_start_attempt_recovery vendor_start_grant
    """.split()
)


@dataclass(frozen=True)
class RollbackCheck:
    """A classified snapshot of eligibility for a rehearsed rollback."""

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


# Match the selected ceiling the older build will read after the rollback.
def require_current_preparation(meta: dict[str, Any]) -> int:
    preparation = meta.get("managed_current_prepare_intent", {})
    capability = meta.get("managed_current_capability")
    require(isinstance(preparation, dict), "unresolved_current_preparation")
    if capability is None:
        require(not preparation, "unresolved_current_preparation")
        return 6
    handover = meta.get("native_bounded_handover")
    spec_digest = meta["managed_mode"]["spec_digest"]
    expected = {
        "version": 1,
        "identity_digest": fingerprint(meta.get("identity")),
        "spec_digest": spec_digest,
        "native_ceiling_a": 32,
        "max_current_a": 32,
    }
    require(
        preparation.get("stage") == "complete"
        and preparation.get("capability") == capability == expected
        and isinstance(handover, dict)
        and handover.get("stage") == "admitted"
        and fingerprint(handover.get("spec")) == spec_digest,
        "unresolved_current_preparation",
    )
    return 32


# Verify the admitted archive policy and the original bodies retained in the stopped journal.
def require_migrated_history(db: sqlite3.Connection, meta: dict[str, Any]) -> None:
    schedule_history(db)
    schedule = meta.get("managed_temporary_schedule", {})
    head = meta.get("managed_temporary_schedule_head")
    occurrences = meta.get("managed_temporary_occurrences", {})
    require(
        meta.get("vendor_start_grant", {}) == {}
        and (not schedule or schedule.get("timer_present") is False)
        and (head is None or isinstance(head, str) and head == schedule.get("id") and head in occurrences),
        "unresolved_migrated_control",
    )

    handover = meta.get("native_bounded_handover")
    acknowledgement = meta.get("native_history_policy_acknowledgement")
    require(isinstance(handover, dict) and isinstance(acknowledgement, dict), "native_history_unverified")
    original = handover.get("original_history")
    spec_digest = meta["managed_mode"]["spec_digest"]
    binding = {"identity": fingerprint(meta["identity"]), "original": fingerprint(original)}
    policy = {
        "version": 1,
        "mode": "archive_missing",
        "identity_digest": binding["identity"],
        "spec_digest": spec_digest,
        "original_history_digest": binding["original"],
    }
    at = acknowledgement.get("at_epoch")
    native_digest = acknowledgement.get("native_digest")
    require(
        handover.get("stage") == "admitted"
        and fingerprint(handover.get("spec")) == spec_digest
        and meta.get("native_history_archive_binding") == binding
        and fingerprint(meta.get("native_history_policy")) == fingerprint(policy)
        and fingerprint(acknowledgement.get("policy")) == fingerprint(policy)
        and acknowledgement.get("charging_started") is False
        and type(at) in (int, float)
        and 0 < at <= time.time() + 2
        and isinstance(native_digest, str)
        and re.fullmatch(r"[a-f0-9]{64}", native_digest) is not None,
        "native_history_unverified",
    )
    numbered, attempts = split_attempts(original)
    archived_originals = set()
    for identifier, digest, body, admitted in db.execute(
        "SELECT transaction_id,digest,body,original FROM native_history_records"
    ):
        record = json.loads(body)
        require(
            isinstance(record, dict)
            and record.get("transactionId") == identifier
            and fingerprint(record) == digest
            and admitted in (0, 1),
            "native_history_unverified",
        )
        if admitted:
            archived_originals.add((identifier, digest))
    require(
        archived_originals == {(identifier, fingerprint(body)) for identifier, body in indexed(numbered).items()},
        "native_history_unverified",
    )
    archived_attempts = set()
    for digest, body in db.execute("SELECT digest,body FROM native_start_attempts"):
        require(fingerprint(json.loads(body)) == digest, "native_history_unverified")
        archived_attempts.add(digest)
    require({fingerprint(body) for body in attempts.values()} <= archived_attempts, "native_history_unverified")


# Require every unsettled row to have the same exact historical proof understood by vendor recovery.
def require_settled_requests(db: sqlite3.Connection, meta: dict[str, Any], *, migrated: bool, revision: int) -> None:
    pending = {
        row[0] for row in db.execute("SELECT id FROM managed_requests WHERE state NOT IN ('completed','failed')")
    }
    if not pending:
        return
    require(migrated, "unresolved_request")
    resolved = resolved_timed_starts(
        db, meta.get("native_start_attempt_recovery"), revision, meta.get("managed_test_charge", {}).get("id")
    )
    require(pending == resolved, "unresolved_request")


# Evaluate only the paused, settled journal variant proved by the cross-build rehearsal.
def check_snapshot(db: sqlite3.Connection, config: TrialConfig, *, migrated_only: bool = False) -> int:
    schema = schema_digest(db)
    migrated = schema == MIGRATED_SCHEMA
    require(migrated or not migrated_only and schema == REHEARSED_SCHEMA, "journal_schema_unrehearsed")
    require(
        db.execute("PRAGMA quick_check(1)").fetchone() == ("ok",)
        and db.execute("PRAGMA foreign_key_check").fetchone() is None,
        "journal_integrity_unverified",
    )
    raw = db.execute("SELECT key,value FROM meta").fetchall()
    supported = MIGRATED_METADATA if migrated else SUPPORTED_METADATA
    require(all(key in supported for key, _ in raw), "unrehearsed_journal_metadata")
    meta = {key: json.loads(value) for key, value in raw}
    control = meta.get("control")
    require(isinstance(control, dict), "unrehearsed_journal_state")
    require_settled_start(db, meta, control)
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
    require_installation(db, meta)
    ceiling = require_current_preparation(meta)
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
        and 6 <= resume <= ceiling
        and int(resume) == resume
        and pause.owns(intent, pause.ticket)
        and isinstance(pause.ticket, str)
        and re.fullmatch(r"[0-9a-f]{32}", pause.ticket) is not None,
        "settled_owner_pause_required",
    )
    require_allocation(db, config, meta)
    require_settled_requests(db, meta, migrated=migrated, revision=intent.revision)
    if migrated:
        require_migrated_history(db, meta)
    return intent.revision


# Give a classified read-only result. The check never launches either build or changes a configuration.
def check_rollback(config: TrialConfig, current: Path, previous: Path) -> RollbackCheck:
    build = None
    try:
        build = next((build for build in (CURRENT, *MIGRATED_RELEASES) if build_matches(current, build)), None)
        require(build is not None and build_matches(previous, PREVIOUS), "build_pair_unrehearsed")
        with stopped_journal(config.journal) as db:
            revision = check_snapshot(db, config, migrated_only=build.migrated_only)
        return RollbackCheck(True, "rehearsed_stopped_journal", current_commit=build.commit, pause_revision=revision)
    except RollbackRefusedError as error:
        reason = str(error)
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
        reason = "journal_unavailable_or_malformed"
    return RollbackCheck(False, reason, current_commit=(build or CURRENT).commit)


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
