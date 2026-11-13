"""Peer-owned command receipt and explicit settlement inputs for offline operator-evidence contracts."""


class PeerStartLedger:
    """Received commands remain pending until a distinct independent cancellation event is supplied by the test."""

    # Keep command identities on the device peer, never infer pending state from proxy receipts or measured zero.
    def __init__(self, bench):
        self.bench, self.pending, self.events = bench, {}, []

    # The independent device records the actual correlated command before it sends or loses its response.
    async def observe(self, action, payload):
        if action == "RemoteStartTransaction":
            frame = self.bench.device_calls[-1]
            assert frame[2] == action and frame[3] == payload
            self.pending[frame[1]] = {"token": frame[1], "action": action, "payload": payload}

    # This explicit external input models independent cancellation verification, not a firmware cancellation API.
    def settle(self, request_id, actual_calls):
        assert {frame[1] for frame in actual_calls} == set(self.pending)
        event = {
            "request_id": request_id,
            "received": list(self.pending.values()),
            "outcome": "independently_cancelled" if actual_calls else "independently_verified_absent",
        }
        self.pending.clear()
        self.events.append(event)
        assert not self.pending
        return event
