"""Compose the installation gate, timed command and HA receipt at their shared decisions."""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NamedTuple

from .home_interactions import Action, Home, Receipt, home_edges, home_violation
from .timed_interactions import Active, TestPhase, Timed, TimedEdge, admits_test, timed_edges, timed_violation


class Window(StrEnum):
    INTRODUCING = "management has no current installation proof"
    MANAGED = "current installation proof"
    RETIRING = "only restrictive work and cleanup"
    RESTORED = "original backend restored"


@dataclass(frozen=True, slots=True)
class System:
    window: Window = Window.INTRODUCING
    timed: Timed = Timed(introduced=False)
    home: Home = Home()


class SystemEdge(NamedTuple):
    event: str
    state: System
    effect: str = ""


# After a newer owner decision the dashboard may still display its old snapshot.
def supersede(home: Home) -> Home:
    return replace(home, revision_matches=False, admission_eligible=False)


# The installation gate supplies proof and can withdraw it while a request or a dashboard exchange is pending.
def installation_events(system: System) -> Iterable[SystemEdge]:
    window, timed, home = system.window, system.timed, system.home
    if window is Window.INTRODUCING:
        yield SystemEdge(
            "current installation becomes usable",
            replace(
                system,
                window=Window.MANAGED,
                timed=replace(timed, introduced=True),
            ),
        )
    if window in (Window.INTRODUCING, Window.MANAGED):
        (pause,) = (edge.state for edge in timed_edges(timed) if edge.event == "new Pause committed")
        home = supersede(home)
        if home.receipt is Receipt.ACCEPTED and pause.phase is TestPhase.IDLE:
            home = replace(home, receipt=Receipt.FAILED)
        yield SystemEdge("installation closes for restoration", System(Window.RETIRING, pause, home))
    if window is Window.RETIRING and timed.phase is TestPhase.IDLE and timed.active is Active.IDLE and timed.zero:
        yield SystemEdge(
            "original backend verified after cleanup",
            System(
                Window.RESTORED,
                replace(timed, introduced=False, zero=False),
                supersede(home),
            ),
        )


# The timed request's physical result is committed once by the proxy under the same owner.
def commit_timed_edge(system: System, edge: TimedEdge) -> SystemEdge:
    event, timed, effect = edge
    window, home = system.window, system.home
    if event == "local test accepted":
        home = replace(home, receipt=Receipt.ACCEPTED, admission_eligible=False)
    if event in ("Boot invalidates current proof", "process replacement interrupts test") and window is Window.MANAGED:
        window = Window.INTRODUCING
        home = supersede(home)
    if event in ("new Pause committed", "verified Return completed"):
        home = supersede(home)
    if home.receipt is Receipt.ACCEPTED:
        if event == "original power proof saved":
            home = replace(home, receipt=Receipt.COMPLETE)
        elif timed.phase is TestPhase.IDLE:
            home = replace(home, receipt=Receipt.FAILED)
        elif event == "process replacement interrupts test":
            home = replace(home, receipt=Receipt.UNCERTAIN)
    return SystemEdge(event, System(window, timed, home), effect)


# Admission and the matching timed receipt are synchronized. Read-only client work can interleave freely.
def system_edges(system: System) -> Iterable[SystemEdge]:
    window, timed, home = system.window, system.timed, system.home
    yield from installation_events(system)
    for edge in timed_edges(timed):
        name = edge.event
        if name == "replacement introduction verified":
            if window is not Window.RETIRING:
                continue
        if (
            name
            in (
                "local test accepted",
                "cloud test armed",
                "local Start sent",
                "app sent matching cloud Start",
                "verified Return completed",
                "selected current changed",
                "independent daily starts",
            )
            and window is not Window.MANAGED
        ):
            continue
        if name == "local test accepted" and not (
            home.action is Action.TEST and home.receipt is Receipt.NONE and home.admission_eligible
        ):
            continue
        if window is Window.RESTORED:
            continue
        yield commit_timed_edge(system, edge)
    for edge in home_edges(home):
        name = edge.event
        if name.startswith("owner presses ") and name != f"owner presses {Action.TEST}":
            continue  # Other button exchanges are explored by the full client model.
        if name.startswith(("request saved by proxy", "proxy verifies", "proxy records", "proxy retains")):
            continue  # Only the synchronized timed decision can advance this exact stored receipt.
        if name == "current outage notice deadline":
            continue  # Notice timing is checked in the full client graph and cannot affect admission.
        if name.startswith("fresh ") and name != "fresh entry setup begins" and window is not Window.MANAGED:
            continue
        yield SystemEdge(name, replace(system, home=replace(edge.state, notify_due=False, notice=False)), edge.effect)
    if (
        home.action is Action.TEST
        and home.admission_eligible
        and (window is not Window.MANAGED or not admits_test(timed))
    ):
        yield SystemEdge(
            "proxy refuses timed admission",
            replace(
                system,
                home=replace(home, admission_eligible=False),
            ),
        )


# Exact saved outcomes are the only connection between dashboard confirmation and physical charging.
def system_violation(before: System, edge: SystemEdge) -> str | None:
    event, after, effect = edge
    if event == "local test accepted" and not (
        before.window is Window.MANAGED
        and before.home.action is Action.TEST
        and before.home.admission_eligible
        and after.home.receipt is Receipt.ACCEPTED
    ):
        return "the timed test was admitted without current authority or its exact request"
    if (
        before.home.receipt is Receipt.ACCEPTED
        and after.home.receipt is Receipt.COMPLETE
        and (event != "original power proof saved" or not after.timed.flow_saved)
    ):
        return "a timed receipt completed without saved matching power"
    if before.window in (Window.RETIRING, Window.RESTORED) and effect in ("send local Start", "restore app"):
        return "an old request published positive control during restoration"
    if before.timed != after.timed and event in (
        "entry unloaded or disabled",
        "entry removed",
        "first reply lost",
        "retired entry callback",
    ):
        return "a dashboard event changed timed authority"
    if before.timed != after.timed:
        return timed_violation(before.timed, TimedEdge(event, after.timed, effect))
    if before.home != after.home or effect == "send one request":
        return home_violation(before.home, (event, after.home, effect))
    return None
