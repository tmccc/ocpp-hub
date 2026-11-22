"""Match a command to its actual transaction when the native schedule uses the same tag."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .gateway import NativeStartReport
    from .managed_charging import ManagedCharging


class ManagedStartReports:
    """Pass charger Start reports to the shared control owner."""

    # Use the existing managed charging worker and its controller.
    def __init__(self, charging: ManagedCharging) -> None:
        self.charging = charging

    # A protected owner Start may first need a zero hold.
    async def prepare_hold(self, payload: dict[str, Any]) -> None:
        await self.charging.prepare_owner_report(payload)

    # Use the independently observed native occurrence and schedule.
    async def identify(self, payload: dict[str, Any]) -> NativeStartReport | None:
        c = self.charging.controller
        return await c.session.gateway.controls.decisions.schedule.prepare_native_report(c, payload)

    # Check the owned station ceiling in the same decision that accepts the Start report.
    async def commit_at_verified_ceiling(self, commit: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        return await self.charging.controller.native.commit_reported_start(commit)

    # A held report is allowed only for the existing operation.
    def permits_held(self, tag: str) -> bool:
        return self.charging.can_record_owner(tag)

    # Keep the command pending while its source is still ambiguous.
    def defer_binding(self, local: int, tag: str) -> bool:
        return self.charging.controller.session.gateway.controls.decisions.schedule.defer_start_binding(local, tag)

    # Publish the recorded Start to the active charging operation.
    def recorded(self, local: int, payload: dict[str, Any]) -> None:
        self.charging.started(local, payload)
