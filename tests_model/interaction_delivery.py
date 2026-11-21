"""Reference rules for forwarding ordered lifecycle reports and settling exact replies."""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NamedTuple


class Phase(StrEnum):
    START = "Start ready"
    START_SENT = "Start possibly delivered"
    START_UNKNOWN = "Start needs independent proof"
    START_SAVED = "Start mapping reply committed"
    STOP = "Stop ready with mapping"
    STOP_SENT = "Stop possibly delivered"
    STOP_SAVED = "Stop acknowledgement committed"


@dataclass(frozen=True, slots=True)
class Delivery:
    # The head keeps one exact identity until retirement. Later sessions cannot borrow its reply or mapping.
    waiting: int = 0
    phase: Phase = Phase.START
    start_errors: int = 0
    connected: bool = True
    late_start_reply: bool = False


class DeliveryDecision(NamedTuple):
    event: str
    state: Delivery
    effect: str = ""


# Three ordered pairs exercise a current head, its successor, and an independently queued tail.
def delivery_decisions(state: Delivery) -> Iterable[DeliveryDecision]:
    yield DeliveryDecision("wrong or retired reply", state)
    if state.waiting < 3:
        yield DeliveryDecision("local closed pair queued", replace(state, waiting=state.waiting + 1))
    if state.connected:
        disconnected_phase = {Phase.START_SENT: Phase.START_UNKNOWN, Phase.STOP_SENT: Phase.STOP}
        yield DeliveryDecision(
            "cloud connection replaced",
            replace(
                state,
                connected=False,
                phase=disconnected_phase.get(state.phase, state.phase),
                late_start_reply=False,
            ),
        )
    else:
        yield DeliveryDecision("cloud becomes ready", replace(state, connected=True))
    if not state.waiting:
        return
    if state.phase is Phase.START:
        if state.connected and state.start_errors < 3:
            yield DeliveryDecision("Start head transmitted", replace(state, phase=Phase.START_SENT), "send head Start")
        if state.start_errors == 3:
            yield DeliveryDecision("exact Start independently reconciled", replace(state, phase=Phase.START_SAVED))
    elif state.phase is Phase.START_SENT:
        yield DeliveryDecision(
            "Start reply unavailable",
            replace(
                state,
                phase=Phase.START_UNKNOWN,
                late_start_reply=True,
            ),
        )
        yield from start_replies(state)
    elif state.phase is Phase.START_UNKNOWN:
        if state.late_start_reply:
            yield from start_replies(state)
        yield DeliveryDecision(
            "exact Start independently reconciled",
            replace(
                state,
                phase=Phase.START_SAVED,
                late_start_reply=False,
            ),
        )
    elif state.phase is Phase.START_SAVED:
        yield DeliveryDecision("mapping commit fails", state)
        yield DeliveryDecision("mapping committed", replace(state, phase=Phase.STOP))
    elif state.phase is Phase.STOP:
        if state.connected:
            yield DeliveryDecision("Stop head transmitted", replace(state, phase=Phase.STOP_SENT), "send mapped Stop")
        yield DeliveryDecision("exact Stop independently reconciled", replace(state, phase=Phase.STOP_SAVED))
    elif state.phase is Phase.STOP_SENT:
        yield DeliveryDecision("Stop reply unavailable or rejected", replace(state, phase=Phase.STOP))
        yield DeliveryDecision("Stop reply commit fails", state)
        yield DeliveryDecision("Stop reply saved", replace(state, phase=Phase.STOP_SAVED))
    elif state.phase is Phase.STOP_SAVED:
        yield DeliveryDecision("Stop retirement commit fails", state)
        yield DeliveryDecision(
            "Stop retirement committed",
            replace(
                state,
                waiting=state.waiting - 1,
                phase=Phase.START,
                start_errors=0,
            ),
            "retire exactly the head",
        )


# A late result can settle this head while its original connection is still the reply source.
def start_replies(state: Delivery) -> Iterable[DeliveryDecision]:
    yield DeliveryDecision("Start reply commit fails", state)
    yield DeliveryDecision(
        "Start mapping reply saved",
        replace(
            state,
            phase=Phase.START_SAVED,
            late_start_reply=False,
        ),
    )
    yield DeliveryDecision(
        "Start error saved",
        replace(
            state,
            phase=Phase.START,
            start_errors=state.start_errors + 1,
            late_start_reply=False,
        ),
    )


# Wire, mapping and retirement are different facts. An older reply or failed commit cannot move the queue.
def delivery_violation(before: Delivery, decision: DeliveryDecision) -> str | None:
    event, after, effect = decision
    if event.endswith("fails") or event == "wrong or retired reply":
        if after != before or effect:
            return "delivery advanced on a failed write or an unrelated reply"
    if effect == "send head Start" and (before.phase is not Phase.START or before.start_errors >= 3):
        return "a possibly delivered or exhausted Start was sent again"
    if effect == "send mapped Stop" and before.phase is not Phase.STOP:
        return "Stop sent before its exact mapping committed"
    if event in ("Start mapping reply saved", "Start error saved") and (
        not before.connected or (before.phase is not Phase.START_SENT and not before.late_start_reply)
    ):
        return "a retired Start reply supplied fresh evidence"
    if after.waiting < before.waiting and not (
        before.phase is Phase.STOP_SAVED and after.waiting == before.waiting - 1 and effect == "retire exactly the head"
    ):
        return "retirement skipped a queued session or lost its proof"
    return None
