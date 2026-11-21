"""Bounded, single-start tag grants from an explicitly trusted authenticated vendor."""

from __future__ import annotations

import time
import uuid

from .auth_info import tag_key
from .store import ConflictError, Journal


class VendorAuthorisation:
    """Changing app-generated idTags do not become an unrestricted offline allowlist."""

    # Persist only a tag digest, one pending start and its expiry; the normal transaction owns its actual tag.
    def __init__(self, journal: Journal, ttl: int) -> None:
        self.journal, self.ttl = journal, ttl

    # Arm one authenticated start before forwarding so a nested Authorize can be answered without deadlock.
    def reserve(self, tag: str, *, prepared: bool = False) -> str:
        previous = self.journal.get("vendor_start_grant", {})
        if previous and previous.get("local") is None:
            raise ConflictError("an earlier vendor start is still awaiting a transaction")
        token = uuid.uuid4().hex
        self.journal.put(
            "vendor_start_grant",
            {
                "digest": tag_key(tag),
                "expires": time.time() + self.ttl,
                "token": token,
                **({"prepared": True} if prepared else {}),
            },
        )
        return token

    # Expiry cannot finish a Start's lifecycle. An actual refusal can.
    def pending(self) -> bool:
        grant = self.journal.get("vendor_start_grant", {})
        return (
            bool(grant)
            and grant.get("local") is None
            and self.journal.outcomes.status(grant.get("mutation")) != "Rejected"
        )

    # A grant authorises only its bounded pending start or the currently open transaction bound to that grant.
    def allows(self, tag: str, *, issuing: str | None = None) -> bool:
        grant = self.journal.get("vendor_start_grant", {})
        if (
            grant.get("digest") != tag_key(tag)
            or (grant.get("prepared") is True and grant.get("token") != issuing)
            or self.journal.outcomes.status(grant.get("mutation")) == "Rejected"
        ):
            return False
        local = grant.get("local")
        return (local is None and grant.get("expires", 0) > time.time()) or (
            local is not None and self.journal.active() == local
        )

    # Only the actual wire owner can make its prepared grant available to incoming charger reports.
    def issue(self, token: str) -> None:
        grant = self.journal.get("vendor_start_grant", {})
        if grant.get("token") != token or grant.get("prepared") is not True:
            raise ConflictError("vendor Start no longer owns the prepared authorization grant")
        grant.pop("prepared")
        self.journal.put("vendor_start_grant", grant)

    # Bind once; a subsequent transaction cannot reuse the pending grant after its original session stops.
    def consume(self, tag: str, local: int) -> None:
        grant = self.journal.get("vendor_start_grant", {})
        if (
            grant.get("digest") == tag_key(tag)
            and grant.get("prepared") is not True
            and grant.get("local") in (None, local)
            and self.journal.outcomes.status(grant.get("mutation")) != "Rejected"
        ):
            self.journal.outcomes.bind_vendor_start(grant.get("mutation"), local)
            self.journal.put("vendor_start_grant", {**grant, "local": local})

    # An actual refusal revokes this request only, never a replacement's newer grant.
    def revoke(self, token: str) -> None:
        if self.journal.get("vendor_start_grant", {}).get("token") == token:
            self.journal.put("vendor_start_grant", {})

    # A completed transaction cannot leave a reusable historical start permission behind.
    def retire(self, local: int) -> None:
        if self.journal.get("vendor_start_grant", {}).get("local") == local:
            self.journal.put("vendor_start_grant", {})
