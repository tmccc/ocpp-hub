"""Durable acceptance, session correlation and cloud delivery for the offline prototype."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


class RecoveryRequiredError(RuntimeError):
    """An uncertain remote outcome needs reconciliation instead of automatic replay."""


class ReleaseDeferredError(RecoveryRequiredError):
    """An owner release was withheld before the positive profile reached the transport."""


class ConflictError(ValueError):
    """A retry contradicts an already accepted fact."""


# Encode deterministically and reject non-finite JSON before anything is accepted.
def encoded(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


# Fingerprint the complete request; a reused CALL id must not silently change its meaning.
def fingerprint(value: Any) -> str:
    return hashlib.sha256(encoded(value).encode()).hexdigest()


@dataclass(frozen=True)
class Delivery:
    """One durable cloud request, still expressed using local transaction ids."""

    token: str
    action: str
    payload: dict[str, Any]
    transaction: int | None
    state: str


class Journal:
    """A private SQLite WAL journal; failures propagate before any successful charger reply."""

    # Open one exclusive prototype owner and recover interrupted sends conservatively.
    def __init__(self, path: Path, identity: str, capacity: int = 10000) -> None:
        import fcntl

        self.path = path
        self.identity = identity
        self.capacity = capacity
        path.parent.mkdir(parents=True, exist_ok=True)
        # Failed construction must release both the private lock and database handle.
        with ExitStack() as cleanup:
            self._lock = open(str(path) + ".lock", "a+b")
            cleanup.callback(self._lock.close)
            os.chmod(str(path) + ".lock", 0o600)
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RecoveryRequiredError("journal already has an active owner") from None
            self.db = sqlite3.connect(path, isolation_level=None)
            cleanup.callback(self.db.close)
            self._initialize(path, identity)
            cleanup.pop_all()

    # Validate the private database and recover only known incomplete operations.
    def _initialize(self, path: Path, identity: str) -> None:
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone():
            old_identity = self.get("identity")
            if old_identity is not None and old_identity != identity:
                raise ConflictError("journal belongs to another charger")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        if self.db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RecoveryRequiredError("journal integrity check failed")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS transactions(
                local INTEGER PRIMARY KEY, connector INTEGER NOT NULL, start TEXT UNIQUE NOT NULL,
                stop TEXT, vendor INTEGER, live INTEGER NOT NULL DEFAULT 0);
            CREATE UNIQUE INDEX IF NOT EXISTS one_open_connector ON transactions(connector) WHERE stop IS NULL;
            CREATE TABLE IF NOT EXISTS receipts(
                token TEXT PRIMARY KEY, action TEXT NOT NULL, digest TEXT NOT NULL, response TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(
                seq INTEGER PRIMARY KEY AUTOINCREMENT, token TEXT UNIQUE NOT NULL, action TEXT NOT NULL,
                payload TEXT NOT NULL, local INTEGER, state TEXT NOT NULL DEFAULT 'queued');
            CREATE TABLE IF NOT EXISTS mutations(
                token TEXT PRIMARY KEY, origin TEXT NOT NULL, action TEXT NOT NULL,
                digest TEXT NOT NULL, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS transaction_clock_discontinuities(
                local INTEGER PRIMARY KEY REFERENCES transactions(local),
                start_timestamp TEXT NOT NULL,stop_timestamp TEXT NOT NULL,observed_at_epoch REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS rejected_observations(
                token TEXT PRIMARY KEY,action TEXT NOT NULL,digest TEXT NOT NULL,local INTEGER,
                code TEXT,prior_state TEXT NOT NULL,recorded_at REAL NOT NULL);
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(transactions)")}
        for name in ("start_info", "stop_info"):
            if name not in columns:
                self.db.execute("ALTER TABLE transactions ADD COLUMN " + name + " TEXT")
        self._migrate_vendor_ids()
        self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_open_vendor ON transactions(vendor) WHERE stop IS NULL")
        with self.atomic():
            old = self.get("identity")
            if old is not None and old != identity:
                raise ConflictError("journal belongs to another charger")
            self.put("identity", identity)
            # An upstream Start may have been accepted before its response was lost.
            self.db.execute("UPDATE outbox SET state='ambiguous' WHERE state='inflight' AND action='StartTransaction'")
            self.db.execute("UPDATE outbox SET state='queued' WHERE state='inflight'")
            self.db.execute("UPDATE transactions SET live=0")
            head = self.next_delivery()
            if head is not None and head.action == "StartTransaction" and head.state == "ambiguous":
                # The same exact proof normalization is used after a live timeout and an interrupted process.
                from .managed_start_delivery import supersede_unknown_start_retry

                supersede_unknown_start_retry(self, head)

    # Preserve history while allowing a cloud ID to recur only after its previous lifecycle has drained.
    def _migrate_vendor_ids(self) -> None:
        unique_vendor = any(
            row["unique"]
            and [column["name"] for column in self.db.execute("PRAGMA index_info(" + row["name"] + ")")] == ["vendor"]
            and not row["partial"]
            for row in self.db.execute("PRAGMA index_list(transactions)")
        )
        if not unique_vendor:
            return
        expected = {"local", "connector", "start", "stop", "vendor", "live", "start_info", "stop_info"}
        if {row["name"] for row in self.db.execute("PRAGMA table_info(transactions)")} != expected or self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE tbl_name='transactions' AND "
            "(type='trigger' OR type='index' AND sql IS NOT NULL "
            "AND name NOT IN ('one_open_connector','one_open_vendor'))"
        ).fetchone():
            raise RecoveryRequiredError("transaction mapping migration cannot discard an unknown schema extension")
        self.db.execute("PRAGMA foreign_keys=OFF")
        try:
            with self.atomic():
                self.db.execute(
                    "CREATE TABLE transactions_new(local INTEGER PRIMARY KEY,connector INTEGER NOT NULL,"
                    "start TEXT UNIQUE NOT NULL,stop TEXT,vendor INTEGER,live INTEGER NOT NULL DEFAULT 0,"
                    "start_info TEXT,stop_info TEXT)"
                )
                self.db.execute(
                    "INSERT INTO transactions_new(local,connector,start,stop,vendor,live,start_info,stop_info) "
                    "SELECT local,connector,start,stop,vendor,live,start_info,stop_info FROM transactions"
                )
                self.db.execute("DROP TABLE transactions")
                self.db.execute("ALTER TABLE transactions_new RENAME TO transactions")
                self.db.execute("CREATE UNIQUE INDEX one_open_connector ON transactions(connector) WHERE stop IS NULL")
                self.db.execute("CREATE UNIQUE INDEX one_open_vendor ON transactions(vendor) WHERE stop IS NULL")
                if self.db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                    raise RecoveryRequiredError("transaction mapping migration failed its history reference check")
        finally:
            self.db.execute("PRAGMA foreign_keys=ON")

    # Commit all related facts together, or publish none of them.
    @contextmanager
    def atomic(self) -> Iterator[None]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    # Close the durable store and release its single-owner lock.
    def close(self) -> None:
        self.db.close()
        self._lock.close()

    # Read a typed JSON setting without carrying authentication material.
    def get(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    # Store a setting inside the caller's transaction when one exists.
    def put(self, key: str, value: Any) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, encoded(value)))

    # New trial sessions need a durable identity-bound allocation boundary; old receipts and Stop remain readable.
    def require_allocation(self) -> None:
        if self.get("trial_phase") is None:
            return
        proof = self.get("trial_allocation")
        counter = self.get("next_transaction")
        maximum = self.db.execute("SELECT MAX(local) FROM transactions").fetchone()[0]
        if (
            not isinstance(proof, dict)
            or type(proof.get("version")) is not int
            or proof.get("version") != 1
            or set(proof)
            != {"version", "identity_digest", "hardware_digest", "observed_at", "digest", "count", "floor"}
            or proof.get("identity_digest") != fingerprint(self.identity)
            or any(
                not isinstance(proof.get(key), str)
                or len(proof[key]) != 64
                or any(character not in "0123456789abcdef" for character in proof[key])
                for key in ("hardware_digest", "digest")
            )
            or type(proof.get("count")) is not int
            or not 0 <= proof["count"] <= 10000
            or type(proof.get("observed_at")) not in (int, float)
            or not math.isfinite(proof["observed_at"])
            or not 0 < proof["observed_at"] <= time.time() + 2
            or type(proof.get("floor")) is not int
            or type(counter) is not int
            or not 1000 <= proof["floor"] <= counter < 2**31 - 1
            or maximum is not None
            and counter < maximum
        ):
            raise RecoveryRequiredError("new sessions require a verified durable allocation boundary")

    # Reserve lifecycle storage independently of observations; every accepted open Start retains room for its Stop.
    def enqueue(
        self, action: str, payload: dict[str, Any], local: int | None = None, *, _reserved_stop: bool = False
    ) -> str:
        if _reserved_stop:
            row = self.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()
            if (
                action != "StopTransaction"
                or row is None
                or row["stop"] != encoded(payload)
                or self.db.execute(
                    "SELECT 1 FROM outbox WHERE action='StopTransaction' AND local=?", (local,)
                ).fetchone()
            ):
                raise ConflictError("Stop delivery does not consume an accepted transaction reservation")
        elif action in ("StartTransaction", "StopTransaction"):
            pending = self.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE action IN ('StartTransaction','StopTransaction')"
            ).fetchone()[0]
            reserved = self.db.execute("SELECT COUNT(*) FROM transactions WHERE stop IS NULL").fetchone()[0]
            if pending + reserved + 1 > max(2, self.capacity):
                raise RecoveryRequiredError("lifecycle journal capacity reached; event was not accepted")
        elif (
            self.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE action NOT IN ('StartTransaction','StopTransaction')"
            ).fetchone()[0]
            >= self.capacity
        ):
            raise RecoveryRequiredError("observation journal capacity reached; event was not accepted")
        token = "e-" + uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO outbox(token,action,payload,local) VALUES(?,?,?,?)", (token, action, encoded(payload), local)
        )
        return token

    # Scope CALL-id reuse to a newly authenticated charger connection, while lifecycle bodies retain their identity.
    def connection_epoch(self) -> int:
        with self.atomic():
            epoch = int(self.get("connection_epoch", 0)) + 1
            self.put("connection_epoch", epoch)
        return epoch

    # Retain only a digest of a mutating RPC, never its possibly secret proprietary body.
    def mutation_intent(self, origin: str, action: str, payload: dict[str, Any]) -> str:
        digest = fingerprint(payload)
        pending = self.db.execute(
            "SELECT token FROM mutations WHERE origin=? AND action=? AND digest=? AND state!='confirmed'",
            (origin, action, digest),
        ).fetchone()
        if pending:
            raise RecoveryRequiredError("a matching mutation has an unconfirmed outcome")
        count = self.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0]
        if count >= self.capacity:
            protected = {
                entry["proof"]["attestation"]["mutation_token"]
                for entry in self.get("managed_operator_debt_reconciliations", {}).values()
                if entry.get("completed") is None and entry["proof"]["kind"] in ("vendor-write", "vendor-start")
            }
            grant = self.get("vendor_start_grant", {})
            if grant.get("mutation"):
                protected.add(grant["mutation"])
            removable = [
                row[0]
                for row in self.db.execute("SELECT token FROM mutations WHERE state='confirmed' ORDER BY rowid")
                if row[0] not in protected
            ][: count - self.capacity + 1]
            self.db.executemany("DELETE FROM mutations WHERE token=?", ((token,) for token in removable))
            if self.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0] >= self.capacity:
                raise RecoveryRequiredError("mutation receipt capacity reached; uncertain writes were retained")
        token = uuid.uuid4().hex
        self.db.execute("INSERT INTO mutations VALUES(?,?,?,?,?)", (token, origin, action, digest, "pending"))
        return token

    # A changed body must not bypass uncertainty about an earlier write to the same remote device.
    def require_vendor_writes_reconciled(self) -> None:
        if self.db.execute("SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed' LIMIT 1").fetchone():
            raise RecoveryRequiredError("an earlier vendor write requires explicit reconciliation")

    # Confirm only an actual peer response; uncertain writes are never automatically replayed after restart.
    def mutation_finished(self, token: str, confirmed: bool) -> None:
        self.db.execute(
            "UPDATE mutations SET state=? WHERE token=?", ("confirmed" if confirmed else "uncertain", token)
        )

    # An explicit operator decision can release a quarantined digest after independently checking the peer.
    def reconcile_mutation(self, token: str) -> None:
        row = self.db.execute("SELECT state FROM mutations WHERE token=?", (token,)).fetchone()
        if row is None:
            raise ConflictError("unknown mutation intent")
        self.mutation_finished(token, True)

    # Return a previous critical response, rejecting a changed body under the same CALL id.
    def receipt(self, token: str, action: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM receipts WHERE token=?", (token,)).fetchone()
        if row is None:
            return None
        if row["action"] != action or row["digest"] != fingerprint(payload):
            raise ConflictError("CALL id was reused with different transaction data")
        value: dict[str, Any] = json.loads(row["response"])
        if not isinstance(value, dict):
            raise RecoveryRequiredError("invalid durable receipt")
        return value

    # Save the critical reply in the same commit as its transaction and cloud event.
    def remember(self, token: str, action: str, payload: dict[str, Any], answer: dict[str, Any]) -> None:
        self.db.execute("INSERT INTO receipts VALUES(?,?,?,?)", (token, action, fingerprint(payload), encoded(answer)))

    # Historical replays retain their accepted fact even after a short-lived authorisation grant expires.
    def known_start(self, payload: dict[str, Any]) -> bool:
        return self.db.execute("SELECT 1 FROM transactions WHERE start=?", (encoded(payload),)).fetchone() is not None

    # Accept a start exactly once locally, including a retry sent under a fresh CALL id.
    def start(
        self,
        token: str,
        payload: dict[str, Any],
        accepted: Callable[[int], None] | None = None,
        info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.atomic():
            cached = self.receipt(token, "StartTransaction", payload)
            if cached is not None:
                return cached
            previous = self.db.execute(
                "SELECT local,start_info FROM transactions WHERE start=?", (encoded(payload),)
            ).fetchone()
            if previous:
                local = int(previous[0])
                info = json.loads(previous[1]) if previous[1] is not None else {"status": "Accepted"}
            else:
                if self.active(int(payload["connectorId"])) is not None:
                    raise ConflictError("connector already has an unresolved transaction")
                self.require_allocation()
                counter = self.get("next_transaction", 1000)
                if type(counter) is not int or not 1000 <= counter < 2**31 - 1:
                    raise RecoveryRequiredError("transaction counter is invalid or exhausted")
                local = counter + 1
                if local >= 2**31:
                    raise RecoveryRequiredError("transaction counter exhausted")
                self.put("next_transaction", local)
                self.db.execute(
                    "INSERT INTO transactions(local,connector,start,live,start_info) VALUES(?,?,?,0,?)",
                    (local, payload["connectorId"], encoded(payload), encoded(info or {"status": "Accepted"})),
                )
                self.enqueue("StartTransaction", payload, local)
                if accepted is not None:
                    accepted(local)
            answer = {"transactionId": local, "idTagInfo": info or {"status": "Accepted"}}
            self.remember(token, "StartTransaction", payload, answer)
            return answer

    # Accept a stop once without ever clearing some other session on the connector.
    def stop(
        self,
        token: str,
        payload: dict[str, Any],
        closed: Callable[[int], None] | None = None,
        info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.atomic():
            cached = self.receipt(token, "StopTransaction", payload)
            if cached is not None:
                return cached
            local = payload["transactionId"]
            row = self.db.execute("SELECT stop,start,stop_info FROM transactions WHERE local=?", (local,)).fetchone()
            if row is None:
                raise ConflictError("stop names an unknown transaction")
            start = json.loads(row["start"])
            if payload["meterStop"] < start["meterStart"]:
                raise ConflictError("stop meter precedes its accepted start")
            if row[0] is not None and row[0] != encoded(payload):
                raise ConflictError("stop contradicts a previously accepted stop")
            if row[0] is None:
                self.db.execute(
                    "UPDATE transactions SET stop=?,live=0,stop_info=? WHERE local=?",
                    (encoded(payload), encoded(info or {}), local),
                )
                if datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00")) < datetime.fromisoformat(
                    start["timestamp"].replace("Z", "+00:00")
                ):
                    self.db.execute(
                        "INSERT INTO transaction_clock_discontinuities VALUES(?,?,?,?)",
                        (local, start["timestamp"], payload["timestamp"], time.time()),
                    )
                self.enqueue("StopTransaction", payload, local, _reserved_stop=True)
                profiles = self.get("vendor_profiles", {})
                transactions = self.get("vendor_profile_transactions", {})
                retired = [key for key, value in transactions.items() if value == local]
                for key in retired:
                    profiles.pop(key, None)
                    transactions.pop(key, None)
                self.put("vendor_profiles", profiles)
                self.put("vendor_profile_transactions", transactions)
                if closed is not None:
                    closed(local)
            answer = (info or {}) if row[0] is None else json.loads(row[2] or "{}")
            self.remember(token, "StopTransaction", payload, answer)
            return answer

    # Look up an open transaction; restored rows remain unconfirmed observations.
    def active(self, connector: int = 1) -> int | None:
        row = self.db.execute(
            "SELECT local FROM transactions WHERE connector=? AND stop IS NULL", (connector,)
        ).fetchone()
        return int(row[0]) if row else None

    # Translate a known local transaction without guessing from connector number.
    def vendor_id(self, local: int) -> int | None:
        row = self.db.execute("SELECT vendor FROM transactions WHERE local=?", (local,)).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    # Translate a cloud transaction only while its corresponding local session is still open.
    def local_id(self, vendor: int) -> int | None:
        row = self.db.execute("SELECT local FROM transactions WHERE vendor=? AND stop IS NULL", (vendor,)).fetchone()
        return int(row[0]) if row else None

    # Read-only historical queries may name a completed session; controls still use active-only local_id.
    def historical_local_id(self, vendor: int) -> int | None:
        rows = self.db.execute("SELECT local FROM transactions WHERE vendor=? LIMIT 2", (vendor,)).fetchall()
        return int(rows[0][0]) if len(rows) == 1 else None

    # Peek rather than delete: a process death must not remove an unacknowledged event.
    def next_delivery(self) -> Delivery | None:
        row = self.db.execute("SELECT * FROM outbox ORDER BY seq LIMIT 1").fetchone()
        if row is None:
            return None
        return Delivery(row["token"], row["action"], json.loads(row["payload"]), row["local"], row["state"])

    # Record possible delivery before putting a Start onto the wire.
    def mark_sent(self, item: Delivery) -> None:
        self.db.execute("UPDATE outbox SET state='inflight' WHERE token=?", (item.token,))

    # A correlated rejection terminates only a routine report; lifecycle events retain their quarantine.
    def reject_observation(self, item: Delivery, code: str | None, *, legacy: bool = False) -> None:
        if item.action not in (
            "StatusNotification",
            "MeterValues",
            "FirmwareStatusNotification",
            "DiagnosticsStatusNotification",
        ):
            raise RecoveryRequiredError("a lifecycle event cannot be discarded as a routine report")
        with self.atomic():
            current = self.next_delivery()
            expected = "rejected" if legacy else "inflight"
            if (
                current is None
                or current.token != item.token
                or current.action != item.action
                or current.payload != item.payload
                or current.transaction != item.transaction
                or current.state != expected
                or legacy != (code is None)
            ):
                raise RecoveryRequiredError("routine rejection no longer identifies the exact delivery head")
            self.db.execute(
                "INSERT INTO rejected_observations VALUES(?,?,?,?,?,?,?)",
                (item.token, item.action, fingerprint(item.payload), item.transaction, code, expected, time.time()),
            )
            self.db.execute("DELETE FROM outbox WHERE token=?", (item.token,))

    # Commit the cloud acknowledgement and its transaction mapping atomically.
    def acknowledge(
        self,
        item: Delivery,
        result: dict[str, Any],
        acknowledged: Callable[[], None] | None = None,
        *,
        allow_denied: bool = False,
    ) -> None:
        with self.atomic():
            if item.action == "StartTransaction":
                if item.transaction is None:
                    raise RecoveryRequiredError("start is missing its local transaction")
                vendor = result.get("transactionId")
                if (
                    type(vendor) is not int
                    or not 0 <= vendor < 2**31
                    or not isinstance(result.get("idTagInfo"), dict)
                    or result["idTagInfo"].get("status")
                    not in (
                        ("Accepted", "Blocked", "Expired", "Invalid", "ConcurrentTx") if allow_denied else ("Accepted",)
                    )
                ):
                    raise RecoveryRequiredError("cloud did not accept the local transaction")
                old = self.vendor_id(int(item.transaction))
                if old is not None and old != vendor:
                    raise ConflictError("cloud attempted to change a transaction mapping")
                if self.db.execute(
                    "SELECT 1 FROM transactions t WHERE vendor=? AND local!=? AND "
                    "(stop IS NULL OR EXISTS(SELECT 1 FROM outbox o WHERE o.local=t.local)) LIMIT 1",
                    (vendor, item.transaction),
                ).fetchone():
                    raise ConflictError("cloud transaction still belongs to another undelivered local lifecycle")
                try:
                    self.db.execute("UPDATE transactions SET vendor=? WHERE local=?", (vendor, item.transaction))
                except sqlite3.IntegrityError:
                    raise ConflictError("cloud transaction already belongs to another local session") from None
            if acknowledged is not None:
                acknowledged()
            self.db.execute("DELETE FROM outbox WHERE token=?", (item.token,))

    # Explicitly reconcile a cloud Start with an independently established vendor id.
    def reconcile_start(self, local: int, vendor: int, acknowledged: Callable[[], None] | None = None) -> None:
        if type(local) is not int or type(vendor) is not int or min(local, vendor) < 0:
            raise ConflictError("invalid reconciliation ids")
        row = self.db.execute("SELECT * FROM outbox WHERE local=? AND action='StartTransaction'", (local,)).fetchone()
        if row is None or row["state"] not in ("ambiguous", "inflight", "rejected"):
            raise ConflictError("transaction is not awaiting explicit reconciliation")
        item = Delivery(row["token"], row["action"], json.loads(row["payload"]), local, row["state"])
        self.acknowledge(item, {"transactionId": vendor, "idTagInfo": {"status": "Accepted"}}, acknowledged)
