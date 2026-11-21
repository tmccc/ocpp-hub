"""Legal orders come from the external events, without consulting engine decisions."""

from collections import defaultdict, deque
from itertools import permutations
from random import Random

EVENTS = ("pause", "return", "write_failure", "newer_pause", "replay_return", "reply", "restart")


# Each action occurs at most once under its name. A replacement cannot receive the retired socket's reply.
def admissible(events):
    if not events or events[0] != "pause" or not set(events) <= set(EVENTS) or len(set(events)) != len(events):
        return False
    at = {event: index for index, event in enumerate(events)}
    end = len(events)
    return (
        ("return" not in at or at["return"] < at.get("newer_pause", -1))
        and ("replay_return" not in at or "return" in at and at["newer_pause"] < at["replay_return"])
        and at.get("write_failure", -1) < at.get("newer_pause", end)
        and ("reply" not in at or at["reply"] < at.get("restart", end))
    )


# List every complete history in the held-Start fixture, independent of the implementation or sample seed.
def complete_orders():
    base = tuple(event for event in EVENTS[1:] if event != "reply")
    return tuple(
        ("pause", *suffix)
        for delivered in (False, True)
        for suffix in permutations((*base, "reply") if delivered else base)
        if admissible(("pause", *suffix))
    )


# Distribute a bounded sample across different reply, restart and genuine write-failure positions.
def selected_orders(seed, count):
    groups = defaultdict(list)
    for order in complete_orders():
        delivered = "reply" in order
        restart = order.index("restart")
        returned, newer = order.index("return"), order.index("newer_pause")
        key = (
            delivered,
            (restart > returned) + (restart > newer),
            order.index("write_failure") < returned,
            delivered and order.index("reply") < newer,
        )
        groups[key].append(order)
    rng = Random(seed)
    buckets = list(groups.values())
    rng.shuffle(buckets)
    for bucket in buckets:
        rng.shuffle(bucket)
    active = deque(buckets)
    result = []
    while active and len(result) < count:
        bucket = active.popleft()
        result.append(bucket.pop())
        if bucket:
            active.append(bucket)
    return result
