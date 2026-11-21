"""Explore every reachable state and enabled edge in each finite reference model."""

from tests_model.exploration import explore, require_recovery
from tests_model.interaction_delivery import Delivery, delivery_decisions, delivery_violation
from tests_model.interaction_model import Control, Mode, Start, control_violation, decisions


# Given cooperating peers and no further invalidation, an intentional Pause may still be needed to recover.
def control_recovery_event(event: str) -> bool:
    return event not in {
        "charger replaced or Boot invalidates proof",
        "cloud delivery becomes blocking",
        "native evidence expires",
        "daily Start admitted",
        "daily report starts ceiling check",
        "owner Start prepared",
        "owner Start dispatched",
        "owner Start reply times out",
        "indeterminate Start reply saved",
        "Current requested",
    } and not event.endswith("fails")


# Once faults stop and the peer or independent attestation provides proof, no new work is required to drain.
def delivery_recovery_event(event: str) -> bool:
    return event not in {
        "cloud connection replaced",
        "local closed pair queued",
        "Start error saved",
        "Start reply unavailable",
        "Stop reply unavailable or rejected",
    } and not event.endswith("fails")


# Ordinary control can operate again with the previously selected current or its committed replacement.
def control_ready(state: Control) -> bool:
    return state.mode is Mode.APP and state.connected and state.start is Start.NONE and not state.cloud_block


# The cloud has no remaining lifecycle work and can receive a new session.
def delivery_ready(state: Delivery) -> bool:
    return state.waiting == 0 and state.connected


# Intent: explore to closure, including a new Pause at any boundary and both owner closure orders.
# Why: checking a few full sequences can miss an unsafe intermediate decision or a state with no recovery path.
def test_reachable_control_interactions():
    graph = explore(
        Control(),
        decisions,
        control_violation,
        lambda edge: edge.state,
        lambda edge: edge.event,
        control_recovery_event,
    )
    require_recovery(graph, control_ready)
    print(f"control {len(graph.paths):,} states, {graph.transitions:,} transitions")


# Intent: exercise every phase of the three-pair FIFO, including failures and retired replies.
# Why: failure and acknowledgement can affect the next session through head retirement.
def test_reachable_lifecycle_delivery_interactions():
    graph = explore(
        Delivery(),
        delivery_decisions,
        delivery_violation,
        lambda edge: edge.state,
        lambda edge: edge.event,
        delivery_recovery_event,
    )
    require_recovery(graph, delivery_ready)
    print(f"delivery {len(graph.paths):,} states, {graph.transitions:,} transitions")
