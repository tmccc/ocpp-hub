"""Independent device fault cuts and byte-preserving observations; this support never selects recovery policy."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager


# Match one real outgoing operation without replacing its controller or outcome handling.
def matches_operation(operation, action, payload):
    if operation == "stop":
        return action == "RemoteStopTransaction"
    if operation == "clear":
        return action == "DataTransfer" and payload.get("messageId") == "clearPlans"
    if action != "SetChargingProfile":
        return False
    limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
    return limit == 0 if operation in ("zero", "zero_before_cutoff") else limit > 0


# Refuse before physical application or withhold its actual response after the independent device applied it.
@asynccontextmanager
async def fail_one_operation(b, operation, lost_ack, witness=None):
    answer, before_reply = b.device.answer, b.rig.bench.before_reply
    entered, release = asyncio.Event(), asyncio.Event()
    if witness is None:
        witness = {}  # Existing selective-cleanup callers need no positive-release observations.

    # Capture actual sender-owned facts at the independent peer; no guard, result or authority is manufactured.
    def observe_restore():
        if operation == "restore":
            attempt = b.session.gateway.controls.release_attempt
            assert attempt is not None and attempt.purpose == "automatic" and not attempt.finished
            assert attempt.positive_may_have_dispatched and not attempt.positive_confirmed
            assert attempt.ticket == b.session.journal.get("control")["ticket"]
            witness.update(
                attempt=attempt,
                physical_limit=b.device.effective_limit(),
                device_profiles=copy.deepcopy(b.device.profiles),
            )

    # The peer itself refuses one request and leaves its independently stored physical state intact.
    def refuse_once(action, payload):
        if not lost_ack and not entered.is_set() and matches_operation(operation, action, payload):
            observe_restore()
            entered.set()
            return {"status": "Rejected", "data": ""} if operation == "clear" else {"status": "Rejected"}
        return answer(action, payload)

    # A missing reply is injected only after the firmware's actual application of the matching request.
    async def withhold_once(action, payload):
        if lost_ack and not entered.is_set() and matches_operation(operation, action, payload):
            observe_restore()
            entered.set()
            try:
                await asyncio.wait_for(release.wait(), b.session.gateway.charger.timeout + 0.05)
            except TimeoutError:
                pass  # The lost acknowledgement expires; the peer then remains available for subsequent requests.
        if before_reply is not None:
            await before_reply(action, payload)

    b.device.answer, b.rig.bench.before_reply = refuse_once, withhold_once
    try:
        yield entered
    finally:
        release.set()
        b.device.answer, b.rig.bench.before_reply = answer, before_reply
        await asyncio.sleep(0)


# Record only real replies sent by the independent TLS peer, always forwarding their original bytes unchanged.
def observe_device_replies(b, monkeypatch):
    send, replies = b.sock.send, []

    # Retain the reply only after the real socket send succeeds; no engine callback or verdict is replaced.
    async def observed(raw):
        await send(raw)
        frame = json.loads(raw)
        if frame[0] in (3, 4):
            replies.append(copy.deepcopy(frame))

    monkeypatch.setattr(b.sock, "send", observed)
    return replies
