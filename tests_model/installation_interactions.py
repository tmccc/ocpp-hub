"""Reference decisions for commissioning, replacement and restoration of the original backend."""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import IntEnum, StrEnum
from typing import NamedTuple


class Phase(StrEnum):
    PREPARED = "prepared"
    INTRODUCING = "introducing"
    ACTIVE = "active"
    HELD = "recovering or blocked"
    CLOSING = "closing for cleanup"
    CLEARING = "rollback"
    RESTORING = "backend_restore_pending"
    COMPLETE = "complete"


class Backend(IntEnum):
    TRIAL_ROUTE = 0
    ROUTE_INTENT = 1
    DETACHED_OFFLINE = 2
    ORIGINAL_ROUTE = 3
    OFFLINE_SEEN = 4
    ONLINE_VERIFIED = 5
    SETTINGS_INTENT = 6
    SETTINGS_VERIFIED = 7


@dataclass(frozen=True, slots=True)
class Installation:
    phase: Phase = Phase.PREPARED
    matching_boot: bool = False
    latest_proof: bool = False
    paused: bool = False
    native_open: bool = False
    ledger_open: bool = False
    known_session: bool = False
    cloud_pending: bool = False
    managed_current: bool = False
    profiles: int = 0
    clearing_one: bool = False
    backend: Backend = Backend.TRIAL_ROUTE


class InstallEdge(NamedTuple):
    event: str
    state: Installation
    effect: str = ""


# Finish current introduction before a positive decision. Freshness belongs to its own Boot.
def commissioning(state: Installation) -> Iterable[InstallEdge]:
    if state.phase is Phase.PREPARED:
        yield InstallEdge("original baseline verified and admission committed", replace(state, phase=Phase.INTRODUCING))
    if state.phase in (Phase.INTRODUCING, Phase.HELD, Phase.ACTIVE):
        yield InstallEdge(
            "new Boot or connection",
            replace(
                state,
                phase=Phase.INTRODUCING,
                matching_boot=False,
                latest_proof=False,
            ),
        )
        yield InstallEdge("explicit Pause during service lifetime", replace(state, paused=True))
        yield InstallEdge("native refusal or freshness lost", replace(state, phase=Phase.HELD, latest_proof=False))
        if not state.matching_boot:
            yield InstallEdge("matching current Boot registered", replace(state, matching_boot=True))
        else:
            yield InstallEdge("current native installation proof", replace(state, latest_proof=True))
        if (
            state.latest_proof
            and state.matching_boot
            and not state.cloud_pending
            and (state.known_session or not (state.native_open or state.ledger_open))
        ):
            yield InstallEdge("introduction or recovery complete", replace(state, phase=Phase.ACTIVE, profiles=2))
            if state.phase is Phase.ACTIVE and state.paused and not (state.native_open or state.ledger_open):
                yield InstallEdge("verified Return", replace(state, paused=False))
    if state.phase is Phase.ACTIVE and state.latest_proof and not state.paused and not state.cloud_pending:
        yield InstallEdge("normal or timed positive effect qualifies", state, "permit positive")
        if not (state.native_open or state.ledger_open):
            yield InstallEdge(
                "exact charger session opens",
                replace(
                    state,
                    native_open=True,
                    ledger_open=True,
                    known_session=True,
                    cloud_pending=True,
                ),
            )
            yield InstallEdge(
                "unmatched native activity withdraws permission",
                replace(state, phase=Phase.HELD, latest_proof=False, native_open=True, known_session=False),
            )
    if (
        state.phase is Phase.ACTIVE
        and state.latest_proof
        and state.paused
        and not (state.native_open or state.ledger_open or state.cloud_pending or state.managed_current)
    ):
        yield InstallEdge("managed current capability verified", replace(state, managed_current=True))
    if state.phase in (Phase.INTRODUCING, Phase.ACTIVE, Phase.HELD):
        if state.managed_current:
            yield InstallEdge("legacy cleanup refused with managed current capability", state)
        else:
            yield InstallEdge("cleanup requested", replace(state, phase=Phase.CLOSING, paused=True, latest_proof=False))


# Join independent closing reports before clearing the station. A Stop reply alone is a stutter.
def cleanup(state: Installation) -> Iterable[InstallEdge]:
    if state.phase in (Phase.ACTIVE, Phase.HELD, Phase.INTRODUCING, Phase.CLOSING):
        if state.native_open:
            yield InstallEdge("native session closed", replace(state, native_open=False))
        if state.ledger_open:
            yield InstallEdge("exact Stop report", replace(state, ledger_open=False, cloud_pending=True))
        if state.cloud_pending:
            yield InstallEdge("exact remaining delivery settled", replace(state, cloud_pending=False))
    if state.phase is Phase.CLOSING:
        if state.ledger_open and state.known_session and state.matching_boot:
            yield InstallEdge("Stop exact closing session", state, "stop exact known session")
        if not (state.native_open or state.ledger_open or state.cloud_pending):
            yield InstallEdge("fresh independent idle verified for cleanup", replace(state, latest_proof=True))
            if state.latest_proof:
                yield InstallEdge("cleanup commit", replace(state, phase=Phase.CLEARING))
    if state.phase is Phase.CLEARING:
        if not state.latest_proof:
            yield InstallEdge("fresh cleanup proof reacquired", replace(state, latest_proof=True))
        elif state.profiles and not state.clearing_one:
            yield InstallEdge(
                "owned clear issued",
                replace(state, clearing_one=True),
                ("clear station profile" if state.profiles == 1 else "clear other owned profile"),
            )
        if state.clearing_one and state.latest_proof:
            yield InstallEdge(
                "owned profile independently absent",
                replace(
                    state,
                    profiles=state.profiles - 1,
                    clearing_one=False,
                ),
            )
        if not state.profiles and state.latest_proof:
            yield InstallEdge(
                "profiles absent, restoration pending",
                replace(
                    state,
                    phase=Phase.RESTORING,
                    matching_boot=False,
                    latest_proof=False,
                ),
            )


# Model old-route effects and their proof as different steps, including a saved receipt after replacement.
def restoration(state: Installation) -> Iterable[InstallEdge]:
    if state.phase is not Phase.RESTORING:
        return
    events = {
        Backend.TRIAL_ROUTE: ("route restoration intent saved", Backend.ROUTE_INTENT),
        Backend.ROUTE_INTENT: ("proxy detached and offline verified", Backend.DETACHED_OFFLINE),
        Backend.DETACHED_OFFLINE: ("original route independently observed", Backend.ORIGINAL_ROUTE),
        Backend.ORIGINAL_ROUTE: ("original backend offline observed", Backend.OFFLINE_SEEN),
        Backend.OFFLINE_SEEN: ("later original backend online verified", Backend.ONLINE_VERIFIED),
        Backend.ONLINE_VERIFIED: ("fresh idle and online proof recorded for settings", Backend.SETTINGS_INTENT),
        Backend.SETTINGS_INTENT: ("original settings and history verified", Backend.SETTINGS_VERIFIED),
    }
    if state.backend in events:
        name, milestone = events[state.backend]
        yield InstallEdge(name, replace(state, backend=milestone))
    else:
        yield InstallEdge("restoration completed", replace(state, phase=Phase.COMPLETE))
    if state.backend is Backend.ORIGINAL_ROUTE:
        yield InstallEdge(
            "later original-backend connection verified after route switch",
            replace(state, backend=Backend.ONLINE_VERIFIED),
        )


# Ordinary writes and failed reads cannot reopen cleanup or certify independent evidence.
def install_edges(state: Installation) -> Iterable[InstallEdge]:
    yield InstallEdge("receipt write refused", state)
    yield InstallEdge("unrelated wire reply", state)
    yield InstallEdge("management read", state)
    if state.phase in (Phase.CLOSING, Phase.CLEARING, Phase.RESTORING, Phase.COMPLETE):
        yield InstallEdge("ordinary mutation refused during restoration", state)
        yield InstallEdge("replacement during restoration", replace(state, matching_boot=False, latest_proof=False))
        yield InstallEdge("ordinary fault during restoration", state)
    yield from commissioning(state)
    yield from cleanup(state)
    yield from restoration(state)


# Cleanup needs independent proof of exact closed history before effects may touch owned resources.
def install_violation(before: Installation, edge: InstallEdge) -> str | None:
    event, after, effect = edge
    if effect == "permit positive" and (
        before.phase is not Phase.ACTIVE or not before.matching_boot or not before.latest_proof or before.paused
    ):
        return "a positive effect escaped introduction or Pause"
    if effect.startswith("clear") and (
        before.phase is not Phase.CLEARING
        or not before.latest_proof
        or before.native_open
        or before.ledger_open
        or before.cloud_pending
    ):
        return "an owned profile was cleared before exact closure and cloud settlement"
    if effect == "clear station profile" and before.profiles != 1:
        return "station protection was cleared before other owned profiles"
    if after.phase in (Phase.CLOSING, Phase.CLEARING) and after.managed_current:
        return "legacy restoration acquired control of an installation with managed current capability"
    if after.phase is Phase.COMPLETE and (after.profiles or after.backend is not Backend.SETTINGS_VERIFIED):
        return "restoration completed before proof of original backend and settings"
    if (
        event in ("ordinary fault during restoration", "receipt write refused", "unrelated wire reply")
        and after != before
    ):
        return "an unrelated event changed durable scope or receipt evidence"
    return None
