"""Discard native snapshots overtaken by legitimate local control, lifecycle or cleanup changes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .rpc import UnavailableError
from .store import Journal, fingerprint
from .trial_evidence import Evidence


@dataclass(frozen=True, repr=False)
class Authority:
    """A private generation marker is independent of the native bytes being audited."""

    epoch: int
    revision: int
    phase: str | None
    digest: str


class StableObserver:
    """One aggregate deadline and three reads prevent an older snapshot from poisoning durable ownership receipts."""

    # Both real native HTTP and synthetic TLS benches use the same current-authority comparison.
    def __init__(
        self,
        journal: Journal,
        observe: Callable[[], Awaitable[Evidence]],
        epoch: Callable[[], int],
        revision: Callable[[], int],
        mutating: Callable[[], bool],
    ) -> None:
        self.journal, self.observe, self.epoch, self.revision = journal, observe, epoch, revision
        self.mutating = mutating

    # An acknowledged mutation can legitimately replace the prior confirmed bytes; wait within the same audit budget.
    async def wait_for_mutation(self, deadline: float) -> None:
        loop = asyncio.get_running_loop()
        while self.mutating():
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise UnavailableError("native audit is waiting for a charger mutation outcome")
            await asyncio.sleep(min(0.01, remaining))

    # Capture all local changes that can legitimately make the sampled native resource/session view obsolete.
    def authority(self) -> Authority:
        journal = self.journal
        rows = [tuple(row) for row in journal.db.execute("SELECT * FROM trial_resources ORDER BY key")]
        value = {
            "control": journal.get("control"),
            "active": journal.active(),
            "grant": journal.get("vendor_start_grant"),
            "station": journal.get("station_selection"),
            "binding": journal.get("trial_binding"),
            "resources": rows,
            "connected_handover": journal.get("connected_handover"),
            "native_bounded_handover": journal.get("native_bounded_handover"),
        }
        return Authority(self.epoch(), self.revision(), journal.get("trial_phase"), fingerprint(value))

    # Refresh a raced snapshot before ownership classification; old epochs and frozen cleanup never get rearmed.
    async def __call__(self) -> Evidence:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        for _ in range(3):
            await self.wait_for_mutation(deadline)
            before = self.authority()
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                async with asyncio.timeout(remaining):
                    evidence = await self.observe()
            except TimeoutError:
                raise UnavailableError("native audit did not stabilize before its deadline") from None
            after = self.authority()
            if before.epoch != after.epoch or before.revision != after.revision:
                raise UnavailableError("native audit belongs to a retired charger introduction")
            if self.mutating():
                await self.wait_for_mutation(deadline)
                settled = self.authority()
                if before.epoch != settled.epoch or before.revision != settled.revision:
                    raise UnavailableError("native audit belongs to a retired charger introduction")
                if settled.phase in ("rollback", "backend_restore_pending", "complete"):
                    raise UnavailableError("native audit was superseded by frozen cleanup")
                continue
            if before == after:
                return evidence
            if after.phase in ("rollback", "backend_restore_pending", "complete"):
                raise UnavailableError("native audit was superseded by frozen cleanup")
        raise UnavailableError("local authority kept changing during the native audit")
