"""Explore every reachable decision and preserve shortest failure traces."""

from collections import defaultdict, deque
from collections.abc import Callable, Iterable
from typing import NamedTuple


class Exploration[State](NamedTuple):
    paths: dict[State, tuple[str, ...]]
    incoming: dict[State, set[State]]
    transitions: int
    events: frozenset[str]


# Breadth first makes the first invariant failure a shortest reproducible event sequence.
def explore[State, Edge](
    initial: State,
    choices: Callable[[State], Iterable[Edge]],
    check: Callable[[State, Edge], str | None],
    successor: Callable[[Edge], State],
    event: Callable[[Edge], str],
    recovery_event: Callable[[str], bool],
) -> Exploration[State]:
    paths = {initial: ()}
    incoming = defaultdict(set)
    pending = deque([initial])
    transitions = 0
    events: set[str] = set()
    while pending:
        before = pending.popleft()
        for edge in choices(before):
            after, name = successor(edge), event(edge)
            transitions += 1
            events.add(name)
            violation = check(before, edge)
            if violation is not None:
                raise AssertionError(f"{violation}\n{' -> '.join((*paths[before], name))}")
            if after != before and recovery_event(name):
                incoming[after].add(before)
            if after not in paths:
                paths[after] = (*paths[before], name)
                pending.append(after)
    return Exploration(paths, incoming, transitions, frozenset(events))


# After failures stop, required evidence must admit a path back to the ordinary operating state.
def require_recovery[State](graph: Exploration[State], recovered: Callable[[State], bool]) -> None:
    reachable = {state for state in graph.paths if recovered(state)}
    pending = deque(reachable)
    while pending:
        for before in graph.incoming.get(pending.popleft(), ()):
            if before not in reachable:
                reachable.add(before)
                pending.append(before)
    for state, path in graph.paths.items():
        assert state in reachable, f"Cannot recover from {state}\n{' -> '.join(path)}"
