"""Independent literal wire and declared recovery obligations; no selector or production validator is imported."""

import json
from collections import Counter
from itertools import product
from types import SimpleNamespace


# Boundary classes are an explicit contract, not a classification derived from execution results.
def boundary(value):
    return "lower" if value == 6 else "upper" if value == 32 else "interior"


# Keep same/up/down independently even when current code shares successful implementation paths.
def direction(old, target):
    return "same" if old == target else "up" if old < target else "down"


# Each context has separate old/new literals, same-literal and exact boundary-pair requirements.
def obligations(cases):
    result = set()
    for case in cases:
        old, target, context = case.initial, case.target, (case.intent, case.demand)
        result.add(("old-literal", old, *context))
        result.add(("target-literal", target, *context))
        result.add(("boundary-direction", boundary(old), boundary(target), direction(old, target), *context))
        if old == target:
            result.add(("same-literal", old, *context))
        if old in (6, 32) or target in (6, 32):
            result.add(("endpoint-pair", old, target, *context))
    return result


# Independently declare the full intended domain; never obtain expected obligations from the reduced generator.
def required_obligations():
    return obligations(
        SimpleNamespace(initial=old, target=target, intent=intent, demand=demand)
        for old, target, intent, demand in product(range(6, 33), range(6, 33), ("app", "paused"), (False, True))
    )


# Exact equality prevents both a missing class and silent expansion beyond the stated cover denominator.
def require_cover(cases):
    assert len(cases) == 524
    assert all(
        type(case.initial) is int
        and 6 <= case.initial <= 32
        and type(case.target) is int
        and 6 <= case.target <= 32
        and case.intent in ("app", "paused")
        and type(case.demand) is bool
        for case in cases
    )
    assert len({case.identifier for case in cases}) == 524
    actual, required = obligations(cases), required_obligations()
    assert actual == required, f"missing={sorted(required - actual)!r}; unexpected={sorted(actual - required)!r}"


# Every selected row must own a unique declared obligation; this is minimality only for this explicit projection.
def require_irreducible(cases):
    counts = Counter(token for case in cases for token in obligations((case,)))
    assert all(any(counts[token] == 1 for token in obligations((case,))) for case in cases)


# Literal input expectations are compared with observed API, wire and peer values, not with engine-derived targets.
def require_transport_summary(old, target, intent, observed):
    assert observed["reset_selected"] == old
    assert observed["reset_profile"] == (0 if intent == "paused" else old)
    assert observed["receipt_state"] == "completed"
    assert observed["receipt_current"] == target
    assert observed["selected"] == target
    assert observed["profile"] == (0 if intent == "paused" else target)
    assert observed["new_profile_limits"] == ([] if intent == "paused" else [target])
    assert observed["replayed_receipt_equal"] is True
    assert observed["replay_new_wires"] == 0
    assert observed["replay_durable_equal"] is True


# Successful choices converge before recovery: previous input survives only in terminal historical receipts.
def require_completed_current(b, identifier, revision, target, mode):
    c, j, gateway = b.controller, b.session.journal, b.session.gateway
    assert j.get("managed_current_change") == {"id": identifier, "current_a": target, "state": "completed"}
    control = j.get("control")
    assert control["resume_limit"] == target and control["inhibit"] is (mode == "paused")
    assert control["uncertain"] is False and not j.get("managed_control_fault")
    assert c.current.selected() == target
    assert c.requests.intent().mode == mode and c.requests.intent().revision == revision + 1
    assert gateway.controls.ready
    assert gateway.controls.desired() == b.rig.bench.firmware.effective_limit() == (0 if mode == "paused" else target)
    row = j.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()
    assert row["action"] == "set_current" and row["revision"] == revision and row["state"] == "completed"
    assert json.loads(row["result"]) == {"current_a": target, "charging_started": False}
    arguments = j.db.execute("SELECT current_a FROM managed_current_requests WHERE id=?", (identifier,)).fetchone()
    assert arguments["current_a"] == target
    assert not j.db.execute(
        "SELECT id FROM managed_requests WHERE action='set_current' AND state IN ('accepted','running','uncertain')"
    ).fetchall()
