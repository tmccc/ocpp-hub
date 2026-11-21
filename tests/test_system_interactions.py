"""Explore timed, commissioning and Home Assistant decisions through their observed boundaries."""

from tests_model.exploration import explore, require_recovery
from tests_model.home_interactions import Entry, Exchange, Home, Receipt, View, home_edges, home_violation
from tests_model.installation_interactions import Installation, Phase, install_edges, install_violation
from tests_model.system_interaction import System, Window, system_edges, system_violation
from tests_model.timed_interactions import Active, Timed, timed_edges, timed_violation
from tests_model.timed_interactions import TestPhase as TimedPhase


# Only new deliberate work and recurring trouble are excluded once cooperation resumes.
def recovering_timed(event):
    return event not in {
        "local test accepted",
        "cloud test armed",
        "local Start sent",
        "app sent matching cloud Start",
        "independent daily starts",
        "Boot invalidates current proof",
        "process replacement interrupts test",
        "power receipt write refused",
    }


# Intent: visit all reachable orders of a timed request, source proof, expiry, owner control and replacement.
# Why: the time limit, verified power and the right to Stop have different owners.
def test_timed_interactions():
    graph = explore(
        Timed(), timed_edges, timed_violation, lambda edge: edge.state, lambda edge: edge.event, recovering_timed
    )
    require_recovery(
        graph,
        lambda state: (
            state.phase is TimedPhase.IDLE and state.active is Active.IDLE and state.introduced and not state.paused
        ),
    )
    assert {
        "cloud test armed",
        "app sent matching cloud Start",
        "original power proof saved",
        "nominal duration elapsed",
        "hard deadline elapsed",
        "timed Stop sent to matching session",
        "explicit Pause stops known daily",
    } <= graph.events
    print(f"timed {len(graph.paths):,} states, {graph.transitions:,} transitions")


# Intent: visit every admissible order of introduction, independent closure and backend restoration.
# Why: verified closure and ownership need to survive a crash without re-adding a listener or a cleared profile.
def test_installation_interactions():
    graph = explore(
        Installation(),
        install_edges,
        install_violation,
        lambda edge: edge.state,
        lambda edge: edge.event,
        lambda event: (
            event
            not in {
                "new Boot or connection",
                "native refusal or freshness lost",
                "exact charger session opens",
                "ordinary fault during restoration",
                "replacement during restoration",
                "receipt write refused",
            }
        ),
    )
    require_recovery(
        graph,
        lambda state: (
            state.phase is Phase.COMPLETE
            or (state.phase is Phase.ACTIVE and state.latest_proof and not state.paused and not state.cloud_pending)
        ),
    )
    assert {
        "normal or timed positive effect qualifies",
        "exact remaining delivery settled",
        "owned profile independently absent",
        "replacement during restoration",
        "restoration completed",
        "legacy cleanup refused with managed current capability",
    } <= graph.events
    print(f"installation {len(graph.paths):,} states, {graph.transitions:,} transitions")


# Intent: exercise proxy receipt changes alongside lost responses, expiry and config-entry replacement.
# Why: a visible success must retain exact meaning when the controller and dashboard advance independently.
def test_home_assistant_interactions():
    graph = explore(
        Home(),
        home_edges,
        home_violation,
        lambda edge: edge.state,
        lambda edge: edge.event,
        lambda event: (
            not event.startswith("owner presses ")
            and event
            not in {
                "active entry or setup loses status",
                "service or charger reading expires",
                "receipt temporarily unavailable",
                "invalid or unrelated receipt",
                "first reply lost",
                "owner revision changes at the proxy",
                "entry removed",
                "entry unloaded or disabled",
                "fresh local status during cloud outage",
            }
        ),
    )
    require_recovery(
        graph,
        lambda state: (
            state.entry in (Entry.ACTIVE, Entry.REMOVED)
            and (state.entry is Entry.REMOVED or state.view is View.HEALTHY)
            and state.receipt is not Receipt.ACCEPTED
            and not state.admission_eligible
            and state.exchange not in (Exchange.SENT, Exchange.LOOKUP, Exchange.FOLLOW)
        ),
    )
    assert {
        "owner presses start",
        "owner presses start_test",
        "owner presses pause",
        "owner presses app_control",
        "owner presses set_current",
        "first reply lost",
        "exact replacement Pause verified",
        "current outage notice deadline",
        "retired entry callback",
    } <= graph.events
    print(f"Home Assistant {len(graph.paths):,} states, {graph.transitions:,} transitions")


# Intent: check all orders of the shared installation gate, timed Start and exact Home Assistant receipt.
# Why: checking the three separately misses loss of authority between admission, power, shutdown and confirmation.
def test_shared_system_boundaries():
    graph = explore(
        System(),
        system_edges,
        system_violation,
        lambda edge: edge.state,
        lambda edge: edge.event,
        lambda event: recovering_timed(event) and not event.startswith("owner presses "),
    )
    require_recovery(
        graph,
        lambda state: (
            state.timed.phase is TimedPhase.IDLE
            and state.timed.active is Active.IDLE
            and (state.window is Window.RESTORED or (state.window is Window.MANAGED and not state.timed.paused))
            and state.home.receipt is not Receipt.ACCEPTED
            and not state.home.admission_eligible
            and state.home.exchange not in (Exchange.SENT, Exchange.LOOKUP, Exchange.FOLLOW)
        ),
    )
    assert {
        "local test accepted",
        "proxy refuses timed admission",
        "original power proof saved",
        "Boot invalidates current proof",
        "process replacement interrupts test",
        "installation closes for restoration",
        "original backend verified after cleanup",
        "entry unloaded or disabled",
        "exact completion read",
        "verified Return completed",
        "selected current changed",
    } <= graph.events
    print(f"shared system {len(graph.paths):,} states, {graph.transitions:,} transitions")
