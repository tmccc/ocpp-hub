"""Bounded, single-start tag grants from an explicitly trusted authenticated vendor."""

from __future__ import annotations

import time
import uuid

from .store import ConflictError, Journal, fingerprint


class VendorAuthorisation:
    """Changing app-generated idTags do not become an unrestricted offline allowlist."""

    # Persist only a tag digest, one pending start and its expiry; the normal transaction owns its actual tag.
    def __init__(self, journal: Journal, ttl: int) -> None:
        self.journal, self.ttl = journal, ttl

    # Arm one authenticated start before forwarding so a nested Authorize can be answered without deadlock.
    def reserve(self, tag: str) -> str:
        previous = self.journal.get("vendor_start_grant", {})
        if previous and previous.get("local") is None:
            raise ConflictError("an earlier vendor start is still awaiting a transaction")
        token = uuid.uuid4().hex
        self.journal.put(
            "vendor_start_grant", {"digest": fingerprint(tag), "expires": time.time() + self.ttl, "token": token}
        )
        return token

    # A grant authorises only its bounded pending start or the currently open transaction bound to that grant.
    def allows(self, tag: str) -> bool:
        grant = self.journal.get("vendor_start_grant", {})
        if grant.get("digest") != fingerprint(tag):
            return False
        local = grant.get("local")
        return (local is None and grant.get("expires", 0) > time.time()) or (
            local is not None and self.journal.active() == local
        )

    # Bind once; a subsequent transaction cannot reuse the pending grant after its original session stops.
    def consume(self, tag: str, local: int) -> None:
        grant = self.journal.get("vendor_start_grant", {})
        if grant.get("digest") == fingerprint(tag):
            self.journal.put("vendor_start_grant", {**grant, "local": local})

    # An actual refusal revokes this request only, never a replacement's newer grant.
    def revoke(self, token: str) -> None:
        if self.journal.get("vendor_start_grant", {}).get("token") == token:
            self.journal.put("vendor_start_grant", {})

    # A completed transaction cannot leave a reusable historical start permission behind.
    def retire(self, local: int) -> None:
        if self.journal.get("vendor_start_grant", {}).get("local") == local:
            self.journal.put("vendor_start_grant", {})
