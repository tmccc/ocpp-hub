"""A newer Pause stays in charge when an issued command answers late or crosses a charger reconnect."""

import asyncio
import uuid
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.trial_evidence import profile_projection

from tests.charge_rig import actual_stop, owner_rig, positive_flow, report_actual_stop
from tests.current_rig import adjustable
from tests.managed_rig import healthy, introduced, starts
from tests.prototype_rig import BOOT
from tests.recovery_barriers import completed_control, require_zero_held
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Capture the independent charger's actual response without blocking its reader from processing a protective zero.
def hold_issued_reply(test, monkeypatch, action, limit=None):
    send, calls = test.charger.send, test.rig.bench.device_calls
    held, entered = [], asyncio.Event()

    # Keep the real reply bytes for either the original socket or a device retry after connecting again.
    async def hold(raw):
        reply = decode(raw)
        if not held and reply[0] == 3 and calls and calls[-1][1] == reply[1] and calls[-1][2] == action:
            call = calls[-1]
            if limit is None or profile_projection(call[3])["limit"] == limit:
                held.append((call, raw))
                entered.set()
                return
        await send(raw)

    monkeypatch.setattr(test.charger, "send", hold)
    return send, held, entered


# Read Stop frames from the simulated charger's independent receipt log.
def stops_after(test, mark):
    return [frame for frame in test.rig.bench.device_calls[mark:] if frame[2] == "RemoteStopTransaction"]


# Require an actual Stop exchange on this connection while the independent session remains open.
async def require_exact_stop(test, mark, local):
    c, g = test.controller, test.session.gateway
    async with asyncio.timeout(7):
        while True:
            stops = stops_after(test, mark)
            if stops and g.controls.stop_acknowledged(local):
                break
            await c.tick()
            await asyncio.sleep(0.01)
    assert [frame[3] for frame in stops] == [{"transactionId": local}]
    assert test.session.journal.active() == local and test.rig.bench.firmware.active == local
    return stops[0]


# Intent: Pause accepts while a real positive command is awaiting its reply, then protects the charger across reconnect.
# Why: a late reply can settle the original exchange but cannot publish its former authority or select a Stop target.
@pytest.mark.parametrize("command", ["start", "app_control", "set_current"], ids=["start", "return", "current"])
@pytest.mark.parametrize("reply_before_reconnect", [True, False], ids=["late-on-original", "old-reply-on-new-socket"])
async def test_pause_supersedes_issued_command_across_reconnect(
    tmp_path, tls, unused_tcp_port, monkeypatch, command, reply_before_reconnect
):
    starting, selecting = command == "start", command == "set_current"
    factory = adjustable if selecting else owner_rig
    async with factory(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        c, rig, session = test.controller, test.rig, test.session
        g, journal, bench = session.gateway, session.journal, rig.bench
        if not selecting:
            pause, revision = uuid.uuid4().hex, c.requests.intent().revision
            c.command(pause, "pause", revision)
            await completed_control(test, pause, mode="paused", revision=revision + 1, ceiling=0)

        limit = 16 if selecting else 6
        action = "RemoteStartTransaction" if starting else "SetChargingProfile"
        send, held, entered = hold_issued_reply(test, monkeypatch, action, None if starting else limit)
        request, revision = uuid.uuid4().hex, c.requests.intent().revision
        assert c.command(request, command, revision, current_a=limit if selecting else None)["state"] == "accepted"
        returning = asyncio.create_task(c.return_requested(request, revision)) if command == "app_control" else None
        try:
            await asyncio.wait_for(entered.wait(), 3)
            ((call, raw),) = held
            wire = call[1]
            assert decode(raw) == [3, wire, {"status": "Accepted"}]
            assert g.charger.has_pending_call(wire) and bench.firmware.effective_limit() == limit
            assert journal.active() is None

            # Look up this exact call inside the older request, keeping its reply separate from its outcome.
            def saved_reply():
                owner = journal.outcomes.owner(request, include_wires=True)
                assert owner is not None
                return next(row.reply for row in owner.wires if row.wire == wire)

            assert saved_reply() is None
            pause, pause_revision = uuid.uuid4().hex, c.requests.intent().revision
            assert c.command(pause, "pause", pause_revision)["state"] == "accepted"
            winning = c.requests.intent()
            assert winning.mode == "paused" and winning.revision == pause_revision + 1
            mark = len(bench.device_calls)
            epoch, boot = g._charger_epoch, g.boot_observation

            if reply_before_reconnect:
                await until(lambda: bench.firmware.effective_limit() == 0, timeout=8)
                assert not g.charger.has_pending_call(wire) and saved_reply() is None
                require_zero_held(rig, mark)
                await send(raw)
                await until(lambda: saved_reply() == "Accepted")
                assert c.requests.intent() == winning
                require_zero_held(rig, mark)

            await test.charger.close()
            await until(lambda: not session.runtime.status.charger_connected)
            new_wire = len(bench.device_calls)
            async with rig.charger(session) as replacement:
                await introduced(rig, session, replacement, "replacement-after-owner-pause")
                assert g._charger_epoch != epoch and g.boot_observation != boot
                assert bench.received_limits[-1] == 0 and g.controls.ready and g.controls.desired() == 0
                assert any(frame[2] == "SetChargingProfile" for frame in bench.device_calls[new_wire:])
                if not reply_before_reconnect:
                    unmatched = g.charger.diagnostics.unmatched_replies
                    await replacement.send(raw)
                    await until(lambda: g.charger.diagnostics.unmatched_replies == unmatched + 1)
                assert saved_reply() == ("Accepted" if reply_before_reconnect else None)
                require_zero_held(rig, mark)
                assert c.requests.intent() == winning
                assert not bench.remote_stop.is_set()
                if returning is not None:
                    await asyncio.wait_for(asyncio.shield(returning), 5)

                if starting:
                    # The delayed Start now identifies a real session. Only its own lifecycle report can close it.
                    assert c.requests.receipt(pause)["state"] != "completed"
                    connected = replace(test, charger=replacement)
                    local = await positive_flow(connected)
                    await require_exact_stop(connected, new_wire, local)
                    assert c.requests.receipt(pause)["state"] != "completed"
                    assert (await actual_stop(connected, local))["local_hold_verified"]
                else:
                    assert c.requests.receipt(request)["state"] == "failed"
                await completed_control(test, pause, mode="paused", revision=winning.revision, ceiling=0)
                require_zero_held(rig, mark)
                assert c.current.selected() == 6 and journal.active() is None
                assert saved_reply() == ("Accepted" if reply_before_reconnect else None)
                assert starts(rig) == ([call] if starting else [])
                if starting:
                    assert [frame[3] for frame in stops_after(test, mark)] == [{"transactionId": local}]
                    outcome = journal.outcomes.owner(request, include_wires=True)
                    assert outcome is not None and outcome.state == "completed"
                    assert [(effect.local, effect.closed) for effect in outcome.sessions] == [(local, True)]
                else:
                    assert not stops_after(test, mark)
                await session.close()
        finally:
            if returning is not None:
                if not returning.done():
                    returning.cancel()
                await asyncio.gather(returning, return_exceptions=True)


# Intent: a newer Pause gets its own exact Stop after a real reconnect with the previous Stop reply outstanding.
# Why: even a real Accepted for the previous exchange cannot establish closure on the new connection.
@pytest.mark.parametrize("closure_order", ["ack-first", "closure-first", "joined-foreground-stop"])
async def test_newer_pause_stops_open_session_after_charger_reconnect(
    tmp_path, tls, unused_tcp_port, monkeypatch, closure_order
):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        c, rig, session = test.controller, test.rig, test.session
        g, bench = session.gateway, rig.bench
        local = await positive_flow(test, "daily")
        assert (await healthy(c))["current_a"] == 6
        _, held, entered = hold_issued_reply(test, monkeypatch, "RemoteStopTransaction")
        first = uuid.uuid4().hex
        c.command(first, "pause", c.requests.intent().revision)
        await asyncio.wait_for(entered.wait(), 3)
        ((old_stop, raw),) = held
        assert old_stop[3] == {"transactionId": local} and g.charger.has_pending_call(old_stop[1])
        assert bench.firmware.active == local and decode(raw) == [3, old_stop[1], {"status": "Accepted"}]
        current, revision = uuid.uuid4().hex, c.requests.intent().revision
        assert c.command(current, "pause", revision)["state"] == "accepted"
        assert c.requests.receipt(first)["state"] == "failed"
        mark, epoch = len(bench.device_calls), g._charger_epoch
        require_zero_held(rig, mark)

        await test.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        replacement_mark = len(bench.device_calls)
        async with rig.charger(session) as replacement:
            connected = replace(test, charger=replacement)
            send, new_held, new_entered = hold_issued_reply(connected, monkeypatch, "RemoteStopTransaction")
            foreground = closure_order == "joined-foreground-stop"
            audit_waiting, start_audit = asyncio.Event(), asyncio.Event()
            if foreground:
                before_ceiling = session.runtime.before_ceiling
                assert before_ceiling is not None

                # Delay only the start of re-introduction. Its actual audit and resulting Stop still run.
                async def delay_audit():
                    audit_waiting.set()
                    await start_audit.wait()
                    await before_ceiling()

                monkeypatch.setattr(session.runtime, "before_ceiling", delay_audit)
            try:
                boot = await bench.call(replacement, "replacement-before-stop-reply", "BootNotification", BOOT)
                assert boot[0] == 3 and g._charger_epoch != epoch and g.current_boot_seen(g._charger_epoch)
                if foreground:
                    # Let the ordinary Pause worker claim Stop before the new socket's introduction starts.
                    await asyncio.wait_for(audit_waiting.wait(), 2)
                    c.schedule_pause()
                    await asyncio.wait_for(new_entered.wait(), 3)
                    assert c.native.audited_epoch != g._charger_epoch
            finally:
                start_audit.set()
            unmatched = g.charger.diagnostics.unmatched_replies
            await send(raw)
            await until(lambda: g.charger.diagnostics.unmatched_replies == unmatched + 1)
            await asyncio.wait_for(new_entered.wait(), 3)
            ((new_stop, response),) = new_held
            assert new_stop[3] == {"transactionId": local} and g.charger.has_pending_call(new_stop[1])
            if foreground:
                await until(lambda: c.native.audited_epoch == g._charger_epoch)
                assert c.native.introduced_epoch != g._charger_epoch
                assert stops_after(test, replacement_mark) == [new_stop]
            assert c.requests.receipt(current)["state"] != "completed"
            require_zero_held(rig, mark)

            # The new Stop is the newer Pause's own exchange, independently of its saved closure state.
            def owned_stop():
                winner = session.journal.outcomes.owner(current, include_wires=True)
                assert winner is not None
                return next((wire for wire in winner.wires if wire.wire == new_stop[1]), None)

            wire = owned_stop()
            assert wire is not None and wire.local == local and wire.reply is None
            if closure_order != "ack-first":
                await report_actual_stop(connected, local)
            await send(response)
            await until(lambda: (wire := owned_stop()) is not None and wire.reply == "Accepted")
            if closure_order == "ack-first":
                await require_exact_stop(connected, replacement_mark, local)
                await report_actual_stop(connected, local)
            await completed_control(test, current, mode="paused", revision=revision + 1, ceiling=0)
            require_zero_held(rig, mark)
            assert stops_after(test, mark) == [new_stop]
            owner = session.journal.outcomes.owner(first, include_wires=True)
            assert owner is not None and next(wire for wire in owner.wires if wire.wire == old_stop[1]).reply is None
            assert not starts(rig)
            await session.close()
