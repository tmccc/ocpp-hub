"""Bounded search, exact replay and single-event reduction of real recovery histories."""

import json
import time

import pytest

from tests.model_tls_support import model_tls as model_tls
from tests_discovery.overlap import CASE_SECONDS, ViolationError, simulate
from tests_discovery.schedules import admissible, selected_orders

SCHEMA_VERSION = 1
# Reserve part of five minutes for a timed-out child's shutdown and the final summary.
SEARCH_SECONDS = 270
MAX_CASES = 24


# Store only the external event names and classified evidence, without raw receipts or endpoint data.
def save(path, value):
    temporary = path.with_suffix(".new")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


# Replay accepts the documented bounded event alphabet and rejects unknown or impossible orders.
def load_trace(path):
    if not path.is_file() or path.stat().st_size > 16_384:
        raise pytest.UsageError("discovery replay requires a small saved JSON trace")
    body = json.loads(path.read_text())
    if (
        not isinstance(body, dict)
        or body.get("schema_version") != SCHEMA_VERSION
        or not isinstance(body.get("events"), list)
        or not all(isinstance(event, str) for event in body["events"])
        or not admissible(body["events"])
    ):
        raise pytest.UsageError("discovery replay has an unsupported or impossible event order")
    signature = body.get("rule"), body.get("event")
    if signature != (None, None) and not all(isinstance(field, str) and 0 < len(field) < 100 for field in signature):
        raise pytest.UsageError("discovery replay has a malformed failure signature")
    return tuple(body["events"]), signature if signature != (None, None) else None


# Intent: explore new overlaps, then shorten a failing order while preserving the same rule failure.
# Why: a fixed successful recovery can conceal a wrong intermediate receipt or an obsolete owner's control effect.
async def test_recovery_discovery(tmp_path, model_tls, unused_tcp_port_factory, monkeypatch, pytestconfig):
    destination = pytestconfig.getoption("--discovery-output")
    count = pytestconfig.getoption("--discovery-cases")
    seed = pytestconfig.getoption("--discovery-seed")
    replay = pytestconfig.getoption("--discovery-replay")
    if destination is None or not destination.is_absolute() or destination.exists() or not 1 <= count <= MAX_CASES:
        raise pytest.UsageError("use a new absolute discovery output directory and 1–24 cases")
    expected = None
    if replay is not None:
        order, expected = load_trace(replay)
        orders = [order]
    else:
        orders = selected_orders(seed, count)
    destination.mkdir(parents=True)
    started = time.monotonic()
    deadline = started + SEARCH_SECONDS
    runs = []
    attempts = 0

    # Keep the observed steps and outcome of every run, including attempted reductions.
    async def run(order, purpose):
        nonlocal attempts
        attempts += 1
        facts = []
        result = {"purpose": purpose, "events": list(order), "observations": facts}
        runs.append(result)
        try:
            await simulate(
                tmp_path / f"attempt-{attempts}",
                order,
                model_tls,
                unused_tcp_port_factory,
                monkeypatch,
                facts,
                min(CASE_SECONDS, deadline - time.monotonic()),
            )
        except ViolationError as error:
            result.update(status="violation", rule=error.rule, event=error.event)
            return error
        except Exception as error:
            result.update(status="execution_error", error=type(error).__name__)
            save(destination / "execution-error.json", {"schema_version": SCHEMA_VERSION, "seed": seed, **result})
            raise
        result["status"] = "passed"
        return None

    # One-action deletion retains only schedules that independently fail the same rule at the same action.
    async def shorten(order, failure):
        index = 0
        try:
            while index < len(order):
                candidate = order[:index] + order[index + 1 :]
                if admissible(candidate):
                    if deadline - time.monotonic() < CASE_SECONDS:
                        return
                    reduced = await run(candidate, "shorten")
                    if reduced is not None and (reduced.rule, reduced.event) == (failure.rule, failure.event):
                        order, index = candidate, 0
                        continue
                index += 1
        finally:
            # Even when a candidate cannot run, keep the last sequence proved to reproduce the failure.
            save(
                destination / "reduced.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "seed": seed,
                    "events": list(order),
                    "rule": failure.rule,
                    "event": failure.event,
                    "reduction_finished": index == len(order),
                },
            )

    status = "execution_error"
    matched = None
    try:
        for order in orders:
            if deadline - time.monotonic() < CASE_SECONDS:
                status = "budget_exhausted"
                pytest.fail("discovery budget ended before every selected history ran")
            failure = await run(order, "replay" if replay is not None else "search")
            if expected is not None:
                matched = failure is not None and (failure.rule, failure.event) == expected
            if failure is None:
                continue
            status = "different_violation" if matched is False else "violation"
            trace = {"schema_version": SCHEMA_VERSION, "seed": seed, **runs[-1]}
            save(destination / "failure.json", trace)
            if replay is None:
                await shorten(order, failure)
            pytest.fail(f"{failure}. Saved trace in {destination}")
        status = "passed"
    finally:
        save(
            destination / "summary.json",
            {
                "schema_version": SCHEMA_VERSION,
                "seed": seed,
                "planned": len(orders),
                "status": status,
                "elapsed_seconds": round(time.monotonic() - started, 2),
                "reproduced_saved_failure": matched,
                "observed_events": sorted(
                    {observation["event"] for case in runs for observation in case["observations"]}
                ),
                "runs": runs,
            },
        )
