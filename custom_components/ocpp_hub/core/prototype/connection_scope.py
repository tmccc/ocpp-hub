"""Process-local recovery origin is distinct from exact Boot-bound attempt evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .gateway import Gateway


@dataclass(frozen=True)
class ConnectionLineage:
    """Matching Boot observations belong to the same originating connection and metadata revision."""

    epoch: int
    metadata_revision: int


@dataclass(frozen=True)
class CloudAttemptScope:
    """A failure or native acknowledgement belongs to one actual Boot observation."""

    epoch: int
    boot: str
    metadata_revision: int

    # Origin bookkeeping cannot be used as a substitute for exact observation proof.
    @property
    def lineage(self) -> ConnectionLineage:
        return ConnectionLineage(self.epoch, self.metadata_revision)


# Missing or retired Boot observations never establish a recovery cause or a failed cloud attempt.
def current_scope(gateway: Gateway) -> CloudAttemptScope | None:
    boot = gateway.boot_observation
    if boot is None or not gateway.current_boot_seen(gateway._charger_epoch):
        return None
    return CloudAttemptScope(gateway._charger_epoch, boot, gateway.boot_metadata_revision)
