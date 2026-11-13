"""Actual synthetic cloud responses and later independent charging lifecycles for recovery tests."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode

from tests import test_managed_proxy as mp
from tests.recovery_barriers import fresh_daily_values, returned_to_app, witnessed_native_loss
from tests.test_managed_test_charge import positive_flow
from tests_model.test_start_recovery_sequences import close_daily


class StartReplyBench(mp.OwnerBench):
    """Use the ordinary device/native/cloud bench with an independently configurable backend response plan."""

    # Initialize peer-owned fault plans before ordinary test fixture setup can open a cloud transport.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_replies, self.lifecycle_wires, self.actual_start_replies, self.underlying_start_replies = (
            [],
            [],
            [],
            [],
        )
        self.fixed_vendor = None
        self.backend_starts, self.backend_errors, self.stop_replies = [], [], []
        self.start_reply_gate = None

    # The independent backend refuses before allocation, records real allocation before reply loss, and closes once.
    async def vendor(self, connection):
        self.cloud = connection
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            token, action, body = frame[1:]
            if action in ("StartTransaction", "StopTransaction"):
                self.lifecycle_wires.append(frame)
            answer = {}
            if action == "BootNotification":
                answer = {"status": "Accepted", "interval": 1, "currentTime": "2030-01-01T00:00:00Z"}
            elif action == "Heartbeat":
                answer = {"currentTime": "2030-01-01T00:00:00Z"}
            elif action == "Authorize":
                answer = {"idTagInfo": {"status": "Accepted", "expiryDate": "2030-01-01T00:00:00Z"}}
            elif action == "StartTransaction":
                mode = self.start_replies.pop(0) if self.start_replies else "Accepted"
                if mode == "error":
                    reply = build_call_error(token, "InternalError", "synthetic preallocation refusal")
                    self.actual_start_replies.append(decode(reply))
                    await connection.send(reply)
                    continue
                existing = next((entry for entry in self.backend_starts if entry["token"] == token), None)
                if existing is not None:
                    assert existing["body"] == body
                    answer = existing["answer"]
                else:
                    self.next_vendor += 1
                    vendor = self.next_vendor if self.fixed_vendor is None else self.fixed_vendor
                    answer = {
                        "transactionId": vendor,
                        "idTagInfo": {
                            "status": mode if mode in ("Blocked", "Expired", "Invalid", "ConcurrentTx") else "Accepted"
                        },
                    }
                    self.backend_starts.append(
                        {"token": token, "body": body, "answer": answer, "closed": False, "stop": None}
                    )
                self.underlying_start_replies.append([3, token, answer])
                if self.start_reply_gate is not None:
                    await self.start_reply_gate.wait()
                if mode == "silent":
                    continue
                if mode == "missing-id":
                    answer = {"idTagInfo": {"status": "Accepted"}}
                elif mode == "bad-info":
                    answer = {"transactionId": answer["transactionId"], "idTagInfo": {"status": "invalid"}}
                self.actual_start_replies.append([3, token, answer])
            elif action == "StopTransaction":
                mode = self.stop_replies.pop(0) if self.stop_replies else "accepted"
                if mode == "error":
                    await connection.send(build_call_error(token, "InternalError", "synthetic Stop refusal"))
                    continue
                matches = [
                    entry
                    for entry in self.backend_starts
                    if entry["answer"]["transactionId"] == body["transactionId"] and not entry["closed"]
                ]
                previous = next(
                    (entry for entry in self.backend_starts if entry["stop"] == {"token": token, "body": body}), None
                )
                if previous is None and len(matches) != 1:
                    self.backend_errors.append({"action": action, "body": body})
                    await connection.send(
                        build_call_error(token, "PropertyConstraintViolation", "unknown independent cloud lifecycle")
                    )
                    continue
                if previous is None:
                    matches[0]["closed"], matches[0]["stop"] = True, {"token": token, "body": body}
                if mode == "bad-info":
                    answer = {"idTagInfo": {"status": "invalid"}}
            await connection.send(build_call_result(token, answer))
            self.sent_vendor_replies.append([3, token, answer])


# Finish two new genuine mapped sessions, including independent native outages, without issuing an owner Start.
async def two_later_sessions(b, *, selected, original_starts, first_minute=10):
    c, rig, j = b.controller, b.rig, b.session.journal
    assert (await returned_to_app(b, selected))["station_ceiling_a"] == selected
    locals_seen = []
    for number, demand in enumerate((True, False)):
        beginning, meter_start = fresh_daily_values(
            rig,
            not_before=datetime(2026, 1, 1, 0, first_minute + 2 * number, tzinfo=UTC),
            meter_floor=1200 + 100 * number,
        )
        stamp = beginning.isoformat().replace("+00:00", "Z")
        # A previous explicit tag denial remains effective until an actual new cloud authorization permits it.
        authorized = await rig.bench.call(b.charger, uuid.uuid4().hex, "Authorize", {"idTag": "NATIVE_TIMER"})
        assert authorized[0] == 3 and authorized[2]["idTagInfo"]["status"] == "Accepted"
        local = await positive_flow(
            b,
            "daily",
            call_id=uuid.uuid4().hex,
            meter_start=meter_start,
            timestamp=stamp,
        )
        locals_seen.append(local)
        if not demand:
            rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        await mp.healthy(c)
        assert rig.bench.firmware.current == (selected if demand else 0)
        assert await witnessed_native_loss(b, selected, local) >= 3
        await close_daily(
            b,
            local,
            "ledger",
            timestamp=(beginning + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            meter_stop=meter_start + 100,
        )
        assert j.vendor_id(local) is not None and j.next_delivery() is None
        assert len(mp.starts(rig)) == original_starts
        if isinstance(rig.bench, StartReplyBench):
            assert not rig.bench.backend_errors
            entry = next(entry for entry in rig.bench.backend_starts if entry["body"]["timestamp"] == stamp)
            assert entry["closed"] and entry["stop"]["body"]["transactionId"] == entry["answer"]["transactionId"]
        await asyncio.sleep(0)
    assert locals_seen[0] != locals_seen[1]
