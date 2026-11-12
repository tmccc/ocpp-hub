"""Settings and status shapes of the vendor mirror."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlsplit

from .frames import basic_auth_header

# What happens to the vendor backend's commands: refused (the default), or the ones in mirror_forward.FORWARDED passed
# on to the charger, under the owner's current ceiling.
COMMANDS_REJECT = "reject"
COMMANDS_FORWARD = "forward"
COMMAND_POLICIES = (COMMANDS_REJECT, COMMANDS_FORWARD)


class MirrorSettingsError(ValueError):
    """Settings that cannot work. The code names the problem so a form can show a translated message for it."""

    # Keep a short machine-readable code next to the human message.
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class MirrorSettings:
    """How the mirror reaches the vendor backend and how patient it is."""

    url: str
    user: str | None = None
    # Kept out of repr so a log line or a diagnostics dump of the settings can never show it.
    password: str | None = field(default=None, repr=False)
    commands: str = COMMANDS_REJECT
    max_queue: int = 5000
    reply_timeout_s: float = 30.0
    connect_timeout_s: float = 10.0
    backoff_min_s: float = 1.0
    backoff_max_s: float = 30.0
    # Used until the vendor has told its own heartbeat interval in the answer to BootNotification.
    fallback_heartbeat_s: int = 300

    # Reject settings that cannot work, at the boundary, so a typo shows up as an error and not as a silent retry loop.
    def __post_init__(self) -> None:
        try:
            parts = urlsplit(self.url)
            # Reading the port is what raises for a port that is not a number.
            _ = parts.port
        except ValueError as error:
            raise MirrorSettingsError("invalid_url", "the vendor URL cannot be read as a web address") from error
        if parts.scheme not in ("ws", "wss") or not parts.hostname:
            raise MirrorSettingsError("invalid_url", "the vendor URL must start with ws:// or wss:// and name a host")
        if parts.username is not None or parts.password is not None:
            raise MirrorSettingsError(
                "credentials_in_url", "the vendor URL must not contain credentials; use the user and password settings"
            )
        if bool(self.user) != bool(self.password):
            raise MirrorSettingsError("credentials_incomplete", "set both the vendor user and password, or neither")
        if self.commands not in COMMAND_POLICIES:
            raise MirrorSettingsError(
                "invalid_settings", f"unsupported command policy {self.commands!r}; use one of {COMMAND_POLICIES}"
            )
        if self.max_queue < 1 or self.fallback_heartbeat_s < 1:
            raise MirrorSettingsError(
                "invalid_settings", "the queue size and the fallback heartbeat must be at least 1"
            )
        delays = (self.reply_timeout_s, self.connect_timeout_s, self.backoff_min_s, self.backoff_max_s)
        if any(not math.isfinite(value) or value <= 0 for value in delays):
            raise MirrorSettingsError("invalid_settings", "timeouts and backoff delays must be finite and positive")
        if self.backoff_min_s > self.backoff_max_s:
            raise MirrorSettingsError("invalid_settings", "the minimum backoff delay cannot exceed the maximum")

    @property
    def use_tls(self) -> bool:
        """True for a wss:// URL, which needs an SSL context."""
        return urlsplit(self.url).scheme == "wss"

    @property
    def auth_header(self) -> tuple[str, str] | None:
        """The Basic authorization header for the websocket upgrade, or None when no credentials are set."""
        if not self.user or not self.password:
            return None
        return basic_auth_header(self.user, self.password)


@dataclass(frozen=True, slots=True)
class MirrorStatus:
    """What Home Assistant shows about the mirror link of one charger."""

    connected: bool
    queue_length: int
    last_success: datetime | None = None
    last_error: str | None = None
    # Frames thrown away because the queue was full (only meter values and status changes are ever dropped).
    dropped_frames: int = 0
    # Stop or meter frames whose transaction the vendor never confirmed, so no vendor id could be given.
    unmapped_transactions: int = 0
