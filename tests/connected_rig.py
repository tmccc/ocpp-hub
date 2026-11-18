"""Native HTTP, charger TLS and cloud TLS peers for bounded admission."""

from __future__ import annotations

import asyncio
import copy
import json
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec, capture_native_bounded_baseline
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from ocpp_hub_core.prototype.station import StationChoice
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_evidence import NativeObserver
from ocpp_hub_core.prototype.trial_session import PreparedTrial, TrialSession, preflight, save_private
from websockets.asyncio.server import serve

from tests.handover_helpers import ORIGINAL, NativeVehiclePeer
from tests.prototype_rig import BOOT
from tests.runtime_rig import FAST
from tests.trial_helpers import config_files
from tests.trial_peer import Bench


@dataclass
class ConnectedBench:
    """The native peer owns physical state; the engine never supplies its observation outcomes."""

    bench: Bench
    peer: NativeVehiclePeer
    prepared: PreparedTrial
    spec: NativeBoundedSpec
    sessions: list[TrialSession] = field(default_factory=list)

    # Construct the ordinary session unless this one explicitly supplies the separate native6 contract.
    def session(self, bounded: bool = True) -> TrialSession:
        session = TrialSession(self.prepared, FAST, native_bounded=self.spec if bounded else None)
        self.sessions.append(session)
        return session

    # Model the explicit backend switch only after the listener has admitted the original route under USER6.
    def route_to_trial(self) -> None:
        self.peer.configuration.update(self.spec.trial_route)

    # Answer real proxy commands independently while device CALLs and their replies use the encrypted socket.
    @asynccontextmanager
    async def charger(self, session: TrialSession) -> AsyncIterator[Any]:
        async with self.bench.connection_to(session) as charger:
            reader = asyncio.create_task(self.bench.read_charger(charger))
            try:
                yield charger
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)


# Capture the original connected closed baseline over real HTTP before changing the fake device's USER and route.
@asynccontextmanager
async def connected_bench(
    directory: Path,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    listener_port: int,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[Bench] = Bench,
    time_zone: str | None = None,
    initial_history: list[dict[str, Any]] | None = None,
    station: StationChoice | None = None,
    incoming_mode: str = "basic",
) -> AsyncIterator[ConnectedBench]:
    bench = bench_type()
    bench.cp, bench.connection = "B", "Plug"
    bench.history = (
        copy.deepcopy(initial_history)
        if initial_history is not None
        else [{"transactionId": 9000000, "original_record": {"preserve": [1, 2, 3]}}]
    )
    bench.firmware.plans[9] = {"planId": 9, "limitCurrent": 32, "idTag": "NATIVE_TIMER", "original": "preserve"}
    peer = native_peer(bench)
    original = copy.deepcopy(ORIGINAL) | {"connectorNum": "1"}
    if time_zone is not None:
        original["timeZone"] = time_zone
    peer.configuration = copy.deepcopy(original)
    native = await asyncio.start_server(peer.handle, "127.0.0.1", 0)
    rig: ConnectedBench | None = None
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=tls_contexts[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            config, path = config_files(directory, listener_port, cloud.sockets[0].getsockname()[1])
            if station is not None or incoming_mode != "basic":
                value = json.loads(path.read_text())
                if station is not None:
                    value.update(
                        station_profile_kind=station.kind,
                        station_stack_level=station.stack,
                        station_profile_start=station.start,
                    )
                    bench.firmware.configuration["ChargeProfileMaxStackLevel"] = str(station.stack)
                if incoming_mode == "none":
                    value.update(incoming_auth="none", incoming_secret=None)
                save_private(path, value)
            observer = NativeObserver(config, native.sockets[0].getsockname()[1])
            snapshot = await observer.observe_handover()
            baseline = capture_native_bounded_baseline(snapshot, config.identity, config.hardware_identity)
            save_private(config.baseline, baseline.value())
            save_private(config.baseline.with_suffix(".original-config.json"), original)
            route = {
                "sDomain": "127.0.0.1",
                "sPort": str(config.listener_port),
                "sURL": config.charger_path,
                "allowSelfSign": "1",
            }
            spec = NativeBoundedSpec(
                config.hardware_identity,
                original,
                route,
                snapshot.saved_schedule_digest,
                fingerprint("synthetic-native6-reboot-proof"),
                fingerprint(BOOT),
            )
            peer.configuration = original | {"AmpLmt": "6"}
            peer.runtime_user = 60
            prepared = preflight(path)
            prepared.observer.port = native.sockets[0].getsockname()[1]
            rig = ConnectedBench(bench, peer, prepared, spec)
            yield rig
    finally:
        if rig is not None:
            for session in rig.sessions:
                await session.close()
        native.close()
        await native.wait_closed()


# Preserve native strategy binding through CLI preflight without changing the actual production route contract.
def strategy_file(rig, tmp_path):
    value = rig.spec.value()
    value["trial_route"].update(sDomain=rig.prepared.config.listener_name, sURL="/charger/")
    rig.spec = NativeBoundedSpec.parse(value)
    strategy = tmp_path / "native-spec.json"
    save_private(strategy, value)
    return strategy
