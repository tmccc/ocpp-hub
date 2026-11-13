"""The independent process observer must remain bounded throughout genuine multi-minute recovery waits."""

import gc
from pathlib import Path
from types import SimpleNamespace

from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.store import Journal

from tests.process_helpers import ServiceProcess


# Intent: repeated actual read-only SQLite snapshots release their descriptors without relying on garbage collection.
# Why: multi-minute cloud retry waits otherwise exhaust the observer's descriptors and abort valid process histories.
def test_durable_observer_releases_each_sqlite_connection(tmp_path):
    path = tmp_path / "observer.db"
    j = Journal(path, "DURABLE_READER")
    ManagedRequests(j)
    observer = object.__new__(ServiceProcess)
    observer.rig = SimpleNamespace(prepared=SimpleNamespace(config=SimpleNamespace(journal=path)))
    gc.collect()
    enabled = gc.isenabled()
    gc.disable()
    try:
        before = len(list(Path("/proc/self/fd").iterdir()))
        for _ in range(100):
            snapshot = observer.durable()
            assert snapshot["metadata"]["identity"] == "DURABLE_READER"
            assert snapshot["requests"] == snapshot["transactions"] == snapshot["outbox"] == []
        assert len(list(Path("/proc/self/fd").iterdir())) <= before + 2
    finally:
        if enabled:
            gc.enable()
        gc.collect()
        j.close()
