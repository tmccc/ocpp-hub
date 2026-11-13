"""Explicit finite introduction and current-operation boundary inputs, independent of engine outcomes."""

from dataclasses import dataclass
from itertools import permutations, product


@dataclass(frozen=True)
class BootCase:
    """An actual zero-proof reply or observed native outage is followed by all declared competing orders."""

    selected: int
    intent: str
    boundary: str
    demand: bool
    events: tuple[str, ...]

    # Distinguish genuine native failure from delayed charger acknowledgement controls.
    @property
    def identifier(self):
        return f"boot-{self.selected}-{self.intent}-{self.boundary}-{int(self.demand)}-" + ".".join(self.events)


# Include both superseded and ordinary held-ACK controls, with no inert supersession event.
def boot_reply_candidates():
    orders = (*permutations(("reply", "boot", "pause")), *permutations(("reply", "pause")))
    return tuple(
        BootCase(*seed, order)
        for seed in product((6, 16, 32), ("app", "paused"), ("profile", "composite"), (False, True))
        for order in orders
    )


# A real failed getConfig in the prefix makes every heal event causally meaningful.
def boot_retry_candidates():
    return tuple(
        BootCase(selected, intent, "native503", demand, order)
        for selected, intent, demand in product((6, 16, 32), ("app", "paused"), (False, True))
        for order in permutations(("boot", "pause", "heal"))
    )


# These input grammars contain only realizable independent peer actions, without source-dependent exclusions.
def no_exclusion(case):
    return None


@dataclass(frozen=True)
class CurrentCase:
    """One actual change crosses a real native or charger proof boundary and competing restrictive events."""

    initial: int
    target: int
    boundary: str
    outcome: str
    demand: bool
    events: tuple[str, ...]

    # A named paused boundary represents future selection verification without any positive profile operation.
    @property
    def identifier(self):
        return f"current-{self.initial}-{self.target}-{self.boundary}-{self.outcome}-{int(self.demand)}-" + ".".join(
            self.events
        )


# Cover every directed representative current pair, each real proof boundary and every competing event order.
def current_candidates():
    variants = (
        *product(("native-before", "native-after", "paused-native"), ("valid", "503", "eof", "bad-json")),
        *product(("profile",), ("valid", "rejected", "call-error")),
        *product(("composite",), ("valid", "rejected", "call-error", "bad-schedule")),
    )
    return tuple(
        CurrentCase(initial, target, boundary, outcome, demand, order)
        for initial, target in permutations((6, 16, 32), 2)
        for boundary, outcome in variants
        for demand in (False, True)
        for order in permutations(("resolve", "pause", "boot"))
    )


@dataclass(frozen=True)
class StopCase:
    """A proven retained Stop retry receives genuine wire acknowledgement and process/owner events."""

    origin: str
    refusal: str
    demand: bool
    events: tuple[str, ...]

    # Every retry history retains the original lifecycle identity through all four competing events.
    @property
    def identifier(self):
        return f"stop-{self.origin}-{self.refusal}-{int(self.demand)}-" + ".".join(self.events)


# Exact operator proof is the fixed prefix; no event fabricates a cloud acknowledgement or clears unrelated debt.
def stop_candidates():
    return tuple(
        StopCase(*seed, order)
        for seed in product(("daily", "local", "local-unobserved"), ("invalid-info", "call-error"), (False, True))
        for order in permutations(("ack", "pause", "replace", "observe"))
    )


@dataclass(frozen=True)
class CurrentReplacementCase:
    """Process loss occurs at a real native boundary with no shielded physical operation in flight."""

    initial: int
    target: int
    boundary: str
    pause_before: bool
    demand: bool

    # A process-owned worker is canceled before the old native response can authorize any new generation.
    @property
    def events(self):
        middle = ("pause", "replace") if self.pause_before else ("replace", "pause")
        return ("held-native", *middle, "recover")

    # Each directed current pair and genuine pre/post/paused boundary remains separately auditable.
    @property
    def identifier(self):
        return (
            f"current-replace-{self.initial}-{self.target}-{self.boundary}-{int(self.pause_before)}-{int(self.demand)}"
        )


# Distinct target changes cross actual pre-mutation, applied target and paused future-selection verification.
def current_replacement_candidates():
    return tuple(
        CurrentReplacementCase(initial, target, *seed)
        for initial, target in permutations((6, 16, 32), 2)
        for seed in product(("native-before", "native-after", "paused-native"), (False, True), (False, True))
    )


@dataclass(frozen=True)
class VendorCase:
    """An accepted unbound or uncertain real cloud Start retains debt through owner/process observations."""

    outcome: str
    demand: bool
    events: tuple[str, ...]

    # One cloud mutation is never replayed by a status observation, replacement or repeated restrictive intent.
    @property
    def identifier(self):
        return f"vendor-{self.outcome}-{int(self.demand)}-" + ".".join(self.events)


# Keep different proof obligations as separate seeds and execute every legal three-event order.
def vendor_candidates():
    return tuple(
        VendorCase(outcome, demand, order)
        for outcome, demand in product(("accepted", "uncertain"), (False, True))
        for order in permutations(("pause", "replace", "observe"))
    )
