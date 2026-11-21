"""Reference decisions for the local timed test and the independently armed cloud test."""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NamedTuple


class TestPhase(StrEnum):
    IDLE = "idle"
    PREPARED = "local Start prepared"
    ARMED = "cloud armed without sending"
    ISSUED = "Start possibly sent"
    CONFIRMED = "matching Start reply"
    FLOW_FOUND = "first matching physical power observed"
    CHARGING = "first matching power saved"
    GRACE = "cloud Stop grace"
    STOPPING = "restrictive Stop settlement"


class Source(StrEnum):
    NONE = "none"
    LOCAL = "local"
    CLOUD = "cloud"


class Active(StrEnum):
    IDLE = "no session"
    DAILY = "independent daily"
    TEST = "matching test"
    LEDGER_CLOSED = "test closed in charger ledger"
    NATIVE_CLOSED = "test closed in native report"
    CLOSED = "both matching closures"


@dataclass(frozen=True, slots=True)
class Timed:
    phase: TestPhase = TestPhase.IDLE
    source: Source = Source.NONE
    active: Active = Active.IDLE
    introduced: bool = True
    paused: bool = False
    owner_pause: bool = False
    six_amps: bool = True
    zero: bool = False
    daily_proof: bool = False
    possible_start: bool = False
    stop_issued: bool = False
    flow_saved: bool = False


class TimedEdge(NamedTuple):
    event: str
    state: Timed
    effect: str = ""


# Recognize when a new precisely scoped test can be admitted.
def admits_test(state: Timed) -> bool:
    return (
        state.phase is TestPhase.IDLE
        and state.active is Active.IDLE
        and state.introduced
        and not state.paused
        and state.six_amps
    )


# Expiry can leave a freshly identified daily with the app. Otherwise retain the restrictive hold.
def end_pending(state: Timed, *, expiry: bool = False) -> Timed:
    if expiry and state.active is Active.DAILY and state.daily_proof and not state.possible_start and not state.paused:
        return replace(state, phase=TestPhase.IDLE, source=Source.NONE)
    return replace(state, phase=TestPhase.STOPPING, paused=True, zero=False)


# Admit owner work; keep newer restrictive decisions independent of an unfinished test.
def owner_events(state: Timed) -> Iterable[TimedEdge]:
    if admits_test(state):
        yield TimedEdge(
            "local test accepted",
            replace(
                state,
                phase=TestPhase.PREPARED,
                source=Source.LOCAL,
                flow_saved=False,
            ),
        )
        yield TimedEdge(
            "cloud test armed",
            replace(
                state,
                phase=TestPhase.ARMED,
                source=Source.CLOUD,
                flow_saved=False,
            ),
        )
    if state.phase is not TestPhase.IDLE:
        for command in ("ordinary Start", "another test", "current change", "Return"):
            yield TimedEdge(f"{command} refused during timed work", state)
    elif state.introduced:
        if state.paused and state.zero and state.active is Active.IDLE:
            yield TimedEdge(
                "verified Return completed",
                replace(state, paused=False, owner_pause=False, zero=False),
                "restore app",
            )
        if state.active is Active.IDLE:
            yield TimedEdge("selected current changed", replace(state, six_amps=not state.six_amps))
    next_state = end_pending(state) if state.phase is not TestPhase.IDLE else state
    yield TimedEdge("new Pause committed", replace(next_state, paused=True, owner_pause=True, zero=False))
    if state.introduced:
        replaced = replace(next_state, introduced=False, zero=False, daily_proof=False)
        yield TimedEdge("process replacement interrupts test", replaced)
        yield TimedEdge(
            "Boot invalidates current proof", replace(state, introduced=False, zero=False, daily_proof=False)
        )
    else:
        yield TimedEdge("replacement introduction verified", replace(state, introduced=True))


# The test interval starts on its own power proof, and a saved Start cannot be repeated.
def timing_events(state: Timed) -> Iterable[TimedEdge]:
    if state.phase in (TestPhase.PREPARED, TestPhase.ARMED) and state.introduced and state.active is Active.IDLE:
        local = state.source is Source.LOCAL
        yield TimedEdge(
            "local Start sent" if local else "app sent matching cloud Start",
            replace(state, phase=TestPhase.ISSUED, possible_start=True),
            "send local Start" if local else "",
        )
    if state.phase is TestPhase.ISSUED:
        yield TimedEdge("matching Start accepted", replace(state, phase=TestPhase.CONFIRMED))
        if state.active is Active.IDLE:
            yield TimedEdge(
                "matching Start definitely rejected",
                replace(
                    state,
                    phase=TestPhase.IDLE,
                    source=Source.NONE,
                    possible_start=False,
                ),
            )
        yield TimedEdge("Start reply unavailable", end_pending(state))
    if state.phase in (
        TestPhase.PREPARED,
        TestPhase.ARMED,
        TestPhase.ISSUED,
        TestPhase.CONFIRMED,
        TestPhase.FLOW_FOUND,
    ):
        yield TimedEdge("startup or hard deadline elapsed", end_pending(state, expiry=True))
    if state.phase is TestPhase.CONFIRMED and state.introduced and state.active is Active.TEST:
        yield TimedEdge("matching power first observed", replace(state, phase=TestPhase.FLOW_FOUND))
    if state.phase is TestPhase.FLOW_FOUND and state.introduced and state.active is Active.TEST:
        yield TimedEdge("power receipt write refused", state)
        yield TimedEdge(
            "original power proof saved",
            replace(
                state,
                phase=TestPhase.CHARGING,
                flow_saved=True,
            ),
        )
    if state.phase is TestPhase.CHARGING:
        after = (
            replace(state, phase=TestPhase.GRACE) if state.source is Source.CLOUD else end_pending(state, expiry=True)
        )
        yield TimedEdge("nominal duration elapsed", after)
    if state.phase in (TestPhase.CHARGING, TestPhase.GRACE):
        yield TimedEdge("hard deadline elapsed", end_pending(state, expiry=True))
    if state.phase is TestPhase.GRACE:
        yield TimedEdge("cloud Stop grace elapsed", end_pending(state, expiry=True))


# Match local and native closure independently, then retire the timed claim under physical zero.
def lifecycle_events(state: Timed) -> Iterable[TimedEdge]:
    if state.active is Active.IDLE and state.introduced:
        if not state.paused:
            yield TimedEdge("independent daily starts", replace(state, active=Active.DAILY))
        if state.possible_start:
            yield TimedEdge("exact test lifecycle reported", replace(state, active=Active.TEST, zero=False))
    if state.active is Active.DAILY:
        yield TimedEdge("independent daily closes", replace(state, active=Active.IDLE, daily_proof=False))
        if state.introduced:
            yield TimedEdge("independent daily source verified", replace(state, daily_proof=True))
        if state.owner_pause and state.introduced:
            yield TimedEdge("explicit Pause stops known daily", state, "stop daily")
    if state.active in (Active.TEST, Active.NATIVE_CLOSED):
        closed = Active.CLOSED if state.active is Active.NATIVE_CLOSED else Active.LEDGER_CLOSED
        yield TimedEdge("matching ledger Stop", replace(state, active=closed))
    if state.active in (Active.TEST, Active.LEDGER_CLOSED):
        closed = Active.CLOSED if state.active is Active.LEDGER_CLOSED else Active.NATIVE_CLOSED
        yield TimedEdge("matching native closure", replace(state, active=closed))
    if state.paused and state.introduced:
        yield TimedEdge("fresh station zero", replace(state, zero=True))
    if state.phase is TestPhase.STOPPING and state.introduced:
        if state.active in (Active.TEST, Active.NATIVE_CLOSED) and not state.stop_issued:
            yield TimedEdge("timed Stop sent to matching session", replace(state, stop_issued=True), "stop test")
        if state.stop_issued:
            yield TimedEdge("Stop reply, closure still independent", state)
        if state.zero and (state.active is Active.CLOSED or (state.active is Active.IDLE and not state.possible_start)):
            yield TimedEdge(
                "exact timed settlement saved",
                replace(
                    state,
                    phase=TestPhase.IDLE,
                    source=Source.NONE,
                    active=Active.IDLE,
                    possible_start=False,
                    stop_issued=False,
                ),
            )
    if state.phase in (TestPhase.CHARGING, TestPhase.GRACE) and state.active is Active.CLOSED:
        yield TimedEdge("matching session ended", end_pending(state))


# Join the decisions from the owner, clock and independent charger.
def timed_edges(state: Timed) -> Iterable[TimedEdge]:
    yield TimedEdge("unrelated power or retired reply", state)
    yield from owner_events(state)
    yield from timing_events(state)
    yield from lifecycle_events(state)


# Validate source ownership and separate restrictive output, exact Stop, and verified closure.
def timed_violation(before: Timed, edge: TimedEdge) -> str | None:
    event, after, effect = edge
    if effect == "send local Start" and (
        before.source is not Source.LOCAL
        or before.phase is not TestPhase.PREPARED
        or not before.introduced
        or before.active is not Active.IDLE
    ):
        return "a local Start escaped its own timed request"
    if effect == "stop test" and before.active not in (Active.TEST, Active.NATIVE_CLOSED):
        return "a timed Stop targeted an independent session"
    if effect == "stop daily" and not (before.owner_pause and before.active is Active.DAILY):
        return "the timed test borrowed authority to stop a scheduled session"
    if not before.flow_saved and after.flow_saved and before.phase is not TestPhase.FLOW_FOUND:
        return "power success was recorded without matching evidence"
    if event.endswith("deadline elapsed") and (
        before.active is Active.DAILY and before.daily_proof and not before.possible_start and not before.paused
    ):
        if before.paused != after.paused or before.active is not after.active or effect:
            return "an unused test took authority over the daily charge"
    if event == "exact timed settlement saved" and not (
        before.zero and (before.active is Active.CLOSED or (before.active is Active.IDLE and not before.possible_start))
    ):
        return "a timed Stop was settled without exact closure and zero"
    if event in ("unrelated power or retired reply", "Stop reply, closure still independent") and after != before:
        return "a reply or unrelated sample changed physical evidence"
    return None
