"""Remember bounded observation CALL identities without merging distinct same-body reports."""

from __future__ import annotations

from typing import Any

from .events import validate_observation
from .store import ConflictError, Journal, fingerprint


class ObservationReceipts:
    """Publication receipts share the observation commit and survive socket or process replacement."""

    # Preserve recent publication identities separately from the outbox, including already delivered observations.
    def __init__(self, journal: Journal) -> None:
        self.j = journal
        self.j.db.executescript("""
            CREATE TABLE IF NOT EXISTS observation_receipts(
                seq INTEGER PRIMARY KEY AUTOINCREMENT, generation INTEGER NOT NULL,
                token TEXT NOT NULL, digest TEXT NOT NULL, timed INTEGER NOT NULL,
                publication TEXT NOT NULL, UNIQUE(generation,token,digest));
            CREATE TABLE IF NOT EXISTS observation_calls(
                epoch INTEGER NOT NULL,token TEXT NOT NULL,digest TEXT NOT NULL,
                publication TEXT NOT NULL,
                PRIMARY KEY(epoch,token));
        """)

    # A first Boot on a new connection admits new untimed observations even if firmware reuses its CALL counter.
    def boot(self, epoch: int) -> None:
        if not self.j.db.in_transaction:
            raise ConflictError("observation generation must share the accepted Boot transaction")
        if self.j.get("observation_boot_epoch") == epoch:
            return
        self.j.put("observation_generation", self.j.get("observation_generation", 0) + 1)
        self.j.put("observation_boot_epoch", epoch)

    # The caller owns the transaction so a receipt can never survive without its corresponding accepted event.
    def accept(self, epoch: int, token: str, action: str, payload: dict[str, Any]) -> bool:
        if not self.j.db.in_transaction:
            raise ConflictError("observation receipt must share its acceptance transaction")
        digest = fingerprint([action, payload])
        previous = self.j.db.execute(
            "SELECT digest,publication FROM observation_calls WHERE epoch=? AND token=?", (epoch, token)
        ).fetchone()
        if previous is not None and previous["digest"] != digest:
            raise ConflictError("observation CALL id names a different request on this connection")
        generation = self.j.get("observation_generation", 0)
        timed = action == "MeterValues" or action == "StatusNotification" and "timestamp" in payload
        row = self.j.db.execute(
            "SELECT publication FROM observation_receipts WHERE generation=? AND token=? AND digest=?",
            (generation, token, digest),
        ).fetchone()
        if previous is not None:
            row = previous
        if row is None and timed:
            row = self.j.db.execute(
                "SELECT publication FROM observation_receipts WHERE token=? AND digest=? AND timed=1 LIMIT 1",
                (token, digest),
            ).fetchone()
        duplicate = row is not None
        if not duplicate:
            validate_observation(action, payload, self.j)
        publication = (
            row["publication"] if row is not None else self.j.enqueue(action, payload, payload.get("transactionId"))
        )
        self.j.db.execute(
            "INSERT OR IGNORE INTO observation_calls VALUES(?,?,?,?)", (epoch, token, digest, publication)
        )
        if not duplicate:
            self.j.db.execute(
                "INSERT INTO observation_receipts(generation,token,digest,timed,publication) VALUES(?,?,?,?,?)",
                (generation, token, digest, int(timed), publication),
            )
        # Recent receipts provide a bounded retry window; old connection counters cannot grow without limit.
        capacity = max(1, self.j.capacity)
        self.j.db.execute(
            "DELETE FROM observation_receipts WHERE seq IN "
            "(SELECT seq FROM observation_receipts r WHERE NOT EXISTS "
            "(SELECT 1 FROM outbox o WHERE o.token=r.publication) ORDER BY seq DESC LIMIT -1 OFFSET ?)",
            (capacity,),
        )
        self.j.db.execute("DELETE FROM observation_calls WHERE epoch!=?", (epoch,))
        self.j.db.execute(
            "DELETE FROM observation_calls WHERE rowid IN "
            "(SELECT rowid FROM observation_calls c WHERE NOT EXISTS "
            "(SELECT 1 FROM outbox o WHERE o.token=c.publication) ORDER BY rowid DESC LIMIT -1 OFFSET ?)",
            (capacity,),
        )
        return not duplicate
