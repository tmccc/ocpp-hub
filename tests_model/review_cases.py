"""Review-driven external failures and bounded observation retry histories, declared before execution."""

from dataclasses import dataclass
from itertools import permutations, product


@dataclass(frozen=True)
class CloudStartCase:
    """One actual cloud refusal crosses closure, explicit Pause and service replacement."""

    fault: str
    source: str
    selected: int
    demand: bool
    events: tuple[str, ...]

    # Stable identifiers contain only independently chosen fault and owner inputs.
    @property
    def identifier(self):
        return f"cloud-start-{self.fault}-{self.source}-{self.selected}-{int(self.demand)}-" + ".".join(self.events)


# All six orders are meaningful: a final new Pause follows the genuine closure and actual queue acknowledgments.
def cloud_start_candidates():
    return tuple(
        CloudStartCase(*seed, events)
        for seed in product(
            ("error-once", "error-twice", "Blocked", "Expired", "Invalid", "ConcurrentTx"),
            ("daily", "local"),
            (6, 16, 32),
            (False, True),
        )
        for events in permutations(("close", "pause", "restart"))
    )


@dataclass(frozen=True)
class ObservationCase:
    """A single actual observation can lose its local reply or already have reached the cloud."""

    payload: str
    cut: str
    boundary: str
    events: tuple[str, ...] = ("accept", "replace", "retry", "drain", "two-lifecycles")

    # The complete message class and external delivery boundary identify a replayable history.
    @property
    def identifier(self):
        return f"observation-{self.payload}-{self.cut}-{self.boundary}"


# An untimed report after a fresh replacement Boot is explicitly a new observation, not an identifiable old retry.
def observation_candidates():
    return tuple(
        ObservationCase(*seed)
        for seed in product(
            ("meter-transaction", "meter-station", "status-timed", "status-untimed", "firmware", "diagnostics"),
            ("reply-lost", "reply-received", "cloud-ack", "cloud-error"),
            ("same", "reconnect", "restart", "reconnect-boot", "restart-boot"),
        )
    )


@dataclass(frozen=True)
class IssuedStartCase:
    """A genuine killed process has an exact command receipt and requires independent cancellation proof."""

    phase: str
    selected: int
    paused: bool
    demand: bool
    events: tuple[str, ...] = (
        "request",
        "durable-read",
        "sigkill",
        "restart",
        "pause",
        "attest",
        "retire",
        "recover",
        "two-lifecycles",
    )

    # Every real kill point and original owner mode has its own independently replayable input identity.
    @property
    def identifier(self):
        return f"issued-proof-{self.phase}-{self.selected}-{int(self.paused)}-{int(self.demand)}"


# All histories use actual process death; ordinary lost replies are separate directed tests.
def issued_start_candidates():
    return tuple(
        IssuedStartCase(*seed)
        for seed in product(
            ("after-issued-commit", "before-accepted-commit", "after-accepted-commit"),
            (6, 16, 32),
            (False, True),
            (False, True),
        )
    )


@dataclass(frozen=True)
class BoundOwnerDeliveryCase:
    """A real unpublished command later closes locally while its distinct cloud delivery remains unknown."""

    phase: str
    cloud: str
    selected: int
    demand: bool
    events: tuple[str, ...] = (
        "request",
        "sigkill",
        "late-start",
        "cloud-quarantine",
        "closed",
        "local-settled",
        "attest",
        "drain",
        "recover",
        "two-lifecycles",
    )

    # The cross-fault inputs remain independent of whichever tool or runtime outcome is observed.
    @property
    def identifier(self):
        return f"bound-owner-{self.phase}-{self.cloud}-{self.selected}-{int(self.demand)}"


# Both real unpublished boundaries cross unknown/explicitly refused cloud delivery and both later demand orders.
def bound_owner_delivery_candidates():
    return tuple(
        BoundOwnerDeliveryCase(*seed)
        for seed in product(
            ("before-accepted-commit", "after-accepted-commit"),
            ("silent", "errors"),
            (6, 16, 32),
            (False, True),
        )
    )
