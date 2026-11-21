"""Reference decisions for Home Assistant's view and one exact outstanding management request."""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NamedTuple


class Action(StrEnum):
    NONE = "none"
    START = "start"
    TEST = "start_test"
    PAUSE = "pause"
    RETURN = "app_control"
    CURRENT = "set_current"


class Entry(StrEnum):
    LOADING = "setup or retry"
    ACTIVE = "active"
    UNLOADED = "unloaded or disabled"
    REMOVED = "removed"


class View(StrEnum):
    NONE = "no snapshot"
    HEALTHY = "fresh service and charger"
    LOCAL = "fresh local control, cloud unavailable"
    OLD = "snapshot expired"
    FAULT = "fault or unavailable status"


class Exchange(StrEnum):
    IDLE = "no request"
    SENT = "first reply outstanding"
    LOOKUP = "original receipt lookup"
    FOLLOW = "matching newer Pause lookup"
    OK = "confirmed"
    REFUSED = "refused"
    UNKNOWN = "outcome uncertain"
    RECORDED = "recorded; physical confirmation pending"


class Receipt(StrEnum):
    NONE = "no committed request"
    ACCEPTED = "recorded"
    COMPLETE = "verified and saved"
    FAILED = "failed"
    SUPERSEDED = "newer Pause owns protection"
    UNCERTAIN = "unresolved after failure or replacement"


@dataclass(frozen=True, slots=True)
class Home:
    entry: Entry = Entry.LOADING
    view: View = View.NONE
    revision_matches: bool = False
    notify_due: bool = False
    notice: bool = False
    action: Action = Action.NONE
    exchange: Exchange = Exchange.IDLE
    receipt: Receipt = Receipt.NONE
    admission_eligible: bool = False
    known_recorded: bool = False
    newer_pause_verified: bool = False


class HomeEdge(NamedTuple):
    event: str
    state: Home
    effect: str = ""


# Entry replacements cancel local observations, but the proxy still owns any request that reached it.
def view_events(state: Home) -> Iterable[HomeEdge]:
    yield HomeEdge("retired entry callback", state)
    if state.entry in (Entry.ACTIVE, Entry.LOADING):
        yield HomeEdge(
            "active entry or setup loses status",
            replace(
                state,
                view=View.FAULT,
                revision_matches=False,
                notify_due=True,
            ),
        )
        yield HomeEdge(
            "fresh exact full status received",
            replace(
                state,
                entry=Entry.ACTIVE,
                view=View.HEALTHY,
                revision_matches=True,
                notify_due=False,
                notice=False,
            ),
        )
        if state.notify_due:
            yield HomeEdge("current outage notice deadline", replace(state, notify_due=False, notice=True))
        yield HomeEdge(
            "entry unloaded or disabled",
            replace(
                state,
                entry=Entry.UNLOADED,
                view=View.NONE,
                revision_matches=False,
            ),
        )
        yield HomeEdge(
            "entry removed",
            replace(
                state,
                entry=Entry.REMOVED,
                view=View.NONE,
                revision_matches=False,
                notify_due=False,
                notice=False,
            ),
        )
    if state.entry is Entry.UNLOADED:
        yield HomeEdge("fresh entry setup begins", replace(state, entry=Entry.LOADING))
    if state.entry is Entry.ACTIVE:
        yield HomeEdge(
            "fresh local status during cloud outage",
            replace(
                state,
                view=View.LOCAL,
                revision_matches=True,
                notify_due=True,
            ),
        )
        if state.view in (View.HEALTHY, View.LOCAL):
            yield HomeEdge("service or charger reading expires", replace(state, view=View.OLD, notify_due=True))


# Proxy admission and evidence evolve independently of a single Home Assistant exchange.
def proxy_events(state: Home) -> Iterable[HomeEdge]:
    yield HomeEdge(
        "owner revision changes at the proxy",
        replace(
            state,
            revision_matches=False,
            admission_eligible=state.admission_eligible and state.action is Action.PAUSE,
        ),
    )
    if state.action is not Action.NONE and state.receipt is Receipt.NONE and state.admission_eligible:
        yield HomeEdge("original request lost before admission", replace(state, admission_eligible=False))
        yield HomeEdge(
            "request saved by proxy",
            replace(
                state,
                receipt=Receipt.ACCEPTED,
                admission_eligible=False,
            ),
        )
    if state.receipt is Receipt.ACCEPTED:
        yield HomeEdge("proxy verifies and completes original request", replace(state, receipt=Receipt.COMPLETE))
        yield HomeEdge("proxy records failure", replace(state, receipt=Receipt.FAILED))
        yield HomeEdge("proxy retains unknown physical outcome", replace(state, receipt=Receipt.UNCERTAIN))
        if state.action is Action.PAUSE:
            yield HomeEdge(
                "newer Pause supersedes pending Pause",
                replace(
                    state,
                    receipt=Receipt.SUPERSEDED,
                    revision_matches=False,
                ),
            )
    if state.receipt is Receipt.SUPERSEDED:
        yield HomeEdge("newer Pause is verified by the proxy", replace(state, newer_pause_verified=True))


# The only outbound mutation is the initial press. Everything after it observes a receipt or times out.
def exchange_events(state: Home) -> Iterable[HomeEdge]:
    working = (Exchange.SENT, Exchange.LOOKUP, Exchange.FOLLOW)
    if (
        state.entry is Entry.ACTIVE
        and state.exchange not in working
        and state.receipt is not Receipt.ACCEPTED
        and not state.admission_eligible
    ):
        for action in Action:
            if action is Action.NONE:
                continue
            if (
                state.view is View.HEALTHY
                or state.view in (View.LOCAL, View.OLD)
                and action is Action.PAUSE
                or state.view is View.LOCAL
                and action in (Action.START, Action.CURRENT)
            ):
                yield HomeEdge(
                    f"owner presses {action}",
                    replace(
                        state,
                        action=action,
                        exchange=Exchange.SENT,
                        receipt=Receipt.NONE,
                        admission_eligible=state.revision_matches or action is Action.PAUSE,
                        known_recorded=False,
                        newer_pause_verified=False,
                    ),
                    "send one request",
                )
    if state.exchange is Exchange.SENT:
        yield HomeEdge("first reply lost", replace(state, exchange=Exchange.LOOKUP))
        if state.receipt is Receipt.NONE and not state.admission_eligible:
            yield HomeEdge("proxy refuses obsolete revision", replace(state, exchange=Exchange.REFUSED))
    if state.exchange in working:
        yield HomeEdge("receipt temporarily unavailable", state)
        pending = Exchange.RECORDED if state.known_recorded else Exchange.UNKNOWN
        yield HomeEdge("invalid or unrelated receipt", replace(state, exchange=pending))
        yield HomeEdge(
            "receipt deadline expires",
            replace(
                state,
                exchange=pending,
            ),
        )
    if state.exchange in (Exchange.SENT, Exchange.LOOKUP):
        if state.receipt is Receipt.ACCEPTED:
            yield HomeEdge(
                "matching pending receipt read", replace(state, exchange=Exchange.LOOKUP, known_recorded=True)
            )
        elif state.receipt is Receipt.COMPLETE:
            yield HomeEdge("exact completion read", replace(state, exchange=Exchange.OK))
        elif state.receipt is Receipt.FAILED:
            yield HomeEdge("exact failure read", replace(state, exchange=Exchange.REFUSED))
        elif state.receipt is Receipt.UNCERTAIN:
            yield HomeEdge("exact uncertain result read", replace(state, exchange=Exchange.UNKNOWN))
        elif state.receipt is Receipt.SUPERSEDED:
            yield HomeEdge(
                "follow exact replacement Pause", replace(state, exchange=Exchange.FOLLOW, known_recorded=True)
            )
    if state.exchange is Exchange.FOLLOW:
        yield HomeEdge("cyclic replacement Pause refused", replace(state, exchange=Exchange.RECORDED))
        if state.newer_pause_verified:
            yield HomeEdge("exact replacement Pause verified", replace(state, exchange=Exchange.OK))


# Route source events to the independent view, proxy and exchange state.
def home_edges(state: Home) -> Iterable[HomeEdge]:
    yield from view_events(state)
    yield from proxy_events(state)
    yield from exchange_events(state)


# Dashboard lifetime and read failures cannot change a durable request or invent its success.
def home_violation(before: Home, edge: HomeEdge) -> str | None:
    event, after, effect = edge
    if effect and (effect != "send one request" or not event.startswith("owner presses ")):
        return "an automatic retry or view event sent another command"
    if after.exchange is Exchange.OK and before.exchange is not Exchange.OK:
        if before.receipt is not Receipt.COMPLETE and not (
            before.exchange is Exchange.FOLLOW and before.newer_pause_verified and before.action is Action.PAUSE
        ):
            return "the client claimed success without the exact verified receipt"
    if event in (
        "retired entry callback",
        "entry removed",
        "entry unloaded or disabled",
        "first reply lost",
        "service or charger reading expires",
        "active entry or setup loses status",
    ) and (before.receipt is not after.receipt or effect):
        return "a client-side event changed the proxy's command"
    return None
