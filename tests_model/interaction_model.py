"""Independent reference rules for ordered control decisions and evidence at the iCS proxy."""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import NamedTuple


class Mode(StrEnum):
    APP = "app"
    PAUSING = "pause requested"
    PAUSED = "pause verified"


class Start(StrEnum):
    NONE = "no owner Start"
    PREPARED = "prepared"
    ISSUED = "possibly sent"
    UNKNOWN = "reply unavailable"
    ACCEPTED = "wire Accepted"
    PUBLISHED = "own authority published"
    ERROR = "wire error, physical effect unknown"


class Reply(StrEnum):
    NONE = "no correlated reply captured"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    ERROR = "call error"


class Session(StrEnum):
    NONE = "no matching report"
    OPEN = "matching session open"
    LEDGER_CLOSED = "matching ledger closed"
    NATIVE_CLOSED = "matching native closed"
    CLOSED = "both matching reports closed"


class Edit(StrEnum):
    NONE = "no pending edit"
    RETURN = "Return waiting for proof"
    CURRENT = "Current waiting for proof"
    VERIFIED_RETURN = "Return proof acquired"
    VERIFIED_CURRENT = "Current proof acquired"


class Stop(StrEnum):
    NONE = "no matching Stop issued"
    ISSUED = "Stop awaiting reply"
    ACKNOWLEDGED = "Stop acknowledged, closure unproved"
    UNKNOWN = "Stop reply unavailable"


class DailyCheck(StrEnum):
    NONE = "no pending report"
    FIRST = "checking current profiles"
    RETRY = "newer poll requires another profile read"
    SECOND = "checking profiles again"


@dataclass(frozen=True, slots=True)
class Control:
    mode: Mode = Mode.APP
    connected: bool = True
    zero: bool = False
    start: Start = Start.NONE
    start_revoked: bool = False
    reply_eligible: bool = False
    captured_reply: Reply = Reply.NONE
    owner: Session = Session.NONE
    stop: Stop = Stop.NONE
    daily: bool = False
    daily_check: DailyCheck = DailyCheck.NONE
    cloud_block: bool = False
    edit: Edit = Edit.NONE
    selected_alternate: bool = False


class Decision(NamedTuple):
    event: str
    state: Control
    effect: str = ""


# List events enabled by the independent facts in this state, with their committed consequences.
def decisions(state: Control) -> Iterable[Decision]:
    yield Decision("status read fails", state)
    yield Decision("unrelated reply or closed session report", state)
    yield Decision("stale request or exact UUID replay", state)
    yield Decision("Pause receipt write fails", state)
    yield Decision(
        "Pause commits",
        replace(
            state,
            mode=Mode.PAUSING,
            start_revoked=state.start is not Start.NONE,
            edit=Edit.NONE,
            zero=False,
            daily_check=DailyCheck.NONE,
        ),
    )
    if state.connected:
        yield Decision(
            "charger replaced or Boot invalidates proof",
            replace(
                state,
                connected=False,
                zero=False,
                start=Start.UNKNOWN if state.start is Start.ISSUED else state.start,
                start_revoked=state.start is not Start.NONE,
                reply_eligible=False,
                captured_reply=Reply.NONE,
                stop=Stop.UNKNOWN if state.stop is Stop.ISSUED else state.stop,
                edit=Edit.NONE,
                daily_check=DailyCheck.NONE,
            ),
        )
    else:
        yield Decision("charger becomes ready", replace(state, connected=True))
    if state.cloud_block:
        yield Decision("exact cloud debt reconciled", replace(state, cloud_block=False))
    else:
        yield Decision(
            "cloud delivery becomes blocking",
            replace(
                state,
                cloud_block=True,
                zero=False,
                start_revoked=state.start is not Start.NONE,
                edit=Edit.NONE,
                daily_check=DailyCheck.NONE,
            ),
        )

    if state.connected and state.mode is not Mode.APP and not state.zero:
        yield Decision("independent zero verified", replace(state, zero=True))
    if state.zero:
        yield Decision("native evidence expires", replace(state, zero=False, edit=Edit.NONE))
    if state.mode is Mode.PAUSING and state.zero and state.start in (Start.NONE, Start.PREPARED):
        yield Decision("Pause completed", replace(state, mode=Mode.PAUSED))

    yield from daily_reports(state)
    yield from starts(state)
    yield from edits(state)


# A normal scheduled report needs scoped profile evidence, even if a concurrent poll supersedes the first read.
def daily_reports(state: Control) -> Iterable[Decision]:
    if state.daily:
        yield Decision("daily Stop report", replace(state, daily=False))
        return
    if not state.connected:
        return
    if state.mode is not Mode.APP:
        yield Decision("daily Start refused under Pause", state)
        return
    if state.daily_check is DailyCheck.NONE:
        yield Decision("daily report starts ceiling check", replace(state, daily_check=DailyCheck.FIRST))
    if state.daily_check in (DailyCheck.FIRST, DailyCheck.SECOND):
        yield Decision(
            "daily Start admitted",
            replace(
                state,
                daily=True,
                zero=False,
                edit=Edit.NONE,
                daily_check=DailyCheck.NONE,
            ),
        )
    if state.daily_check is DailyCheck.FIRST:
        yield Decision("newer native poll supersedes report read", replace(state, daily_check=DailyCheck.RETRY))
    if state.daily_check is DailyCheck.RETRY:
        yield Decision("daily report rereads ceiling", replace(state, daily_check=DailyCheck.SECOND))
    if state.daily_check is not DailyCheck.NONE:
        yield Decision("daily report verification unavailable", replace(state, daily_check=DailyCheck.NONE))


# Keep the owner Start and scheduled session distinct, including all orders of their closure reports.
def starts(state: Control) -> Iterable[Decision]:
    if state.start is Start.NONE:
        if state.connected and not state.daily and not state.cloud_block and state.edit is Edit.NONE:
            if state.mode is Mode.APP or (state.mode is Mode.PAUSED and state.zero):
                yield Decision("owner Start prepared", replace(state, start=Start.PREPARED))
        return
    if state.start is Start.PREPARED:
        yield Decision("owner Start proven unsent", replace(state, start=Start.NONE, start_revoked=False))
        if state.connected and not (state.start_revoked or state.daily or state.cloud_block):
            yield Decision(
                "owner Start dispatched",
                replace(
                    state,
                    start=Start.ISSUED,
                    reply_eligible=True,
                ),
                "send Start",
            )
        return

    if state.start is Start.ISSUED:
        yield Decision("owner Start reply times out", replace(state, start=Start.UNKNOWN, start_revoked=True))
    if state.reply_eligible and state.captured_reply is Reply.NONE:
        for reply in (Reply.ACCEPTED, Reply.REJECTED, Reply.ERROR):
            yield Decision(
                f"matching {reply} captured",
                replace(
                    state,
                    captured_reply=reply,
                    reply_eligible=False,
                ),
            )
    if state.captured_reply is not Reply.NONE:
        yield Decision("owner Start reply write fails", state)
        cleared = replace(state, captured_reply=Reply.NONE)
        if state.captured_reply is Reply.ACCEPTED:
            yield Decision("matching Accepted saved", replace(cleared, start=Start.ACCEPTED))
        elif state.captured_reply is Reply.ERROR or state.owner is not Session.NONE:
            yield Decision("indeterminate Start reply saved", replace(cleared, start=Start.ERROR))
        else:
            yield Decision("matching rejection saved", replace(cleared, start=Start.NONE, start_revoked=False))
    if (
        state.start is Start.ACCEPTED
        and state.connected
        and not (state.start_revoked or state.cloud_block or state.daily)
    ):
        yield Decision("Start authority commit fails", state)
        yield Decision(
            "Start publishes app",
            replace(
                state,
                mode=Mode.APP,
                start=Start.PUBLISHED,
                zero=False,
                edit=Edit.NONE,
                daily_check=DailyCheck.NONE,
            ),
            "publish Start",
        )
    if state.connected and state.owner is Session.NONE:
        yield Decision("matching owner Start report", replace(state, owner=Session.OPEN, zero=False, edit=Edit.NONE))
    if state.owner in (Session.OPEN, Session.NATIVE_CLOSED):
        owner = Session.CLOSED if state.owner is Session.NATIVE_CLOSED else Session.LEDGER_CLOSED
        yield Decision("matching ledger closes", replace(state, owner=owner))
    if state.owner in (Session.OPEN, Session.LEDGER_CLOSED):
        owner = Session.CLOSED if state.owner is Session.LEDGER_CLOSED else Session.NATIVE_CLOSED
        yield Decision("matching native closes", replace(state, owner=owner))
    if state.owner is Session.CLOSED and state.zero:
        yield Decision(
            "old owner Start settled",
            replace(
                state,
                owner=Session.NONE,
                start=Start.NONE,
                start_revoked=False,
                reply_eligible=False,
                captured_reply=Reply.NONE,
                stop=Stop.NONE,
            ),
        )
    if state.connected and state.mode is not Mode.APP and state.owner is Session.OPEN and state.stop is Stop.NONE:
        yield Decision(
            "restrictive owner Stop dispatched",
            replace(
                state,
                stop=Stop.ISSUED,
            ),
            "stop matching owner",
        )
    if state.stop is Stop.ISSUED:
        yield Decision("matching Stop Acknowledged", replace(state, stop=Stop.ACKNOWLEDGED))
        yield Decision("matching Stop Rejected", replace(state, stop=Stop.NONE))
        yield Decision("Stop reply unavailable", replace(state, stop=Stop.UNKNOWN))


# Verify and publish pending edits separately so intervening decisions get a chance to revoke the proof.
def edits(state: Control) -> Iterable[Decision]:
    if state.edit is Edit.NONE:
        if state.mode is not Mode.APP and state.connected:
            yield Decision("Return requested", replace(state, edit=Edit.RETURN))
        if state.connected:
            yield Decision("Current requested", replace(state, edit=Edit.CURRENT))
        return
    if not state.connected or state.cloud_block or state.start is not Start.NONE:
        return
    if state.edit is Edit.RETURN and state.mode is Mode.PAUSED and state.zero and not state.daily:
        yield Decision("Return proof acquired", replace(state, edit=Edit.VERIFIED_RETURN))
    if state.edit is Edit.CURRENT and (state.mode is Mode.APP or state.zero):
        yield Decision("Current proof acquired", replace(state, edit=Edit.VERIFIED_CURRENT))
    if state.edit is Edit.VERIFIED_RETURN:
        yield Decision("Return commit fails", state)
        yield Decision("Return publishes", replace(state, mode=Mode.APP, zero=False, edit=Edit.NONE), "publish app")
    if state.edit is Edit.VERIFIED_CURRENT:
        yield Decision("Current commit fails", state)
        yield Decision(
            "Current publishes",
            replace(
                state,
                selected_alternate=not state.selected_alternate,
                edit=Edit.NONE,
                daily_check=DailyCheck.NONE,
            ),
            "publish selection",
        )


# Check independently which exact evidence can justify an effect and which inputs leave ownership alone.
def control_violation(before: Control, decision: Decision) -> str | None:
    event, after, effect = decision
    if event.endswith("fails") or event in (
        "unrelated reply or closed session report",
        "stale request or exact UUID replay",
    ):
        if after != before or effect:
            return "a failed or unrelated event took effect"
    if event == "Pause commits" and (after.mode is not Mode.PAUSING or after.edit is not Edit.NONE):
        return "a newer Pause failed to revoke an older edit"
    if event.startswith("matching ") and after.mode is not before.mode:
        return "evidence for an older attempt changed owner mode"
    if event in ("matching Accepted saved", "indeterminate Start reply saved", "matching rejection saved"):
        if before.captured_reply is Reply.NONE:
            return "a reply was saved without actual transport evidence"
    if event == "daily Stop report" and (after.start, after.owner) != (before.start, before.owner):
        return "a daily Stop settled an unrelated owner Start"
    if event == "daily Start admitted" and (
        before.mode is not Mode.APP or before.daily_check not in (DailyCheck.FIRST, DailyCheck.SECOND)
    ):
        return "a scheduled Start was admitted without its own current proof"
    if effect == "send Start" and (not before.connected or before.start_revoked or before.daily or before.cloud_block):
        return "an obsolete or competing Start was sent"
    if effect == "publish Start" and not (
        before.connected
        and before.start is Start.ACCEPTED
        and not (before.start_revoked or before.cloud_block or before.daily)
    ):
        return "Start published without an exact reply under live authority"
    if effect == "stop matching owner" and before.owner is not Session.OPEN:
        return "a Stop targeted a session without matching evidence"
    if event in ("matching Stop Acknowledged", "matching Stop Rejected", "Stop reply unavailable"):
        if before.owner != after.owner or before.zero != after.zero:
            return "a Stop reply substituted for lifecycle or physical evidence"
    if effect == "publish app" and not (
        before.zero
        and before.mode is Mode.PAUSED
        and before.connected
        and before.start is Start.NONE
        and not (before.daily or before.cloud_block)
    ):
        return "Return published without current zero, ownership and closure evidence"
    if effect == "publish selection" and (
        after.mode is not before.mode
        or before.cloud_block
        or before.start is not Start.NONE
        or not before.connected
        or (before.mode is not Mode.APP and not before.zero)
    ):
        return "Current changed without proof or changed Pause authority"
    if (
        before.owner is not Session.NONE
        and after.owner is Session.NONE
        and (before.owner is not Session.CLOSED or not before.zero)
    ):
        return "a started session's obligation retired without both closures and zero"
    if before.selected_alternate != after.selected_alternate and effect != "publish selection":
        return "selected current changed without its own committed edit"
    return None
