"""Keep planned test listeners outside the client ephemeral range and other workers' pools."""

import socket
from collections.abc import Callable


# An unbound planned listener must not be borrowed by a concurrent client's automatic ephemeral allocation.
def listener_port_factory(worker: int) -> Callable[[], int]:
    if not 0 <= worker < 20:
        raise ValueError("test worker index outside its allocated pool")
    used: set[int] = set()

    # Probe only this worker's non-ephemeral range, and never return the same planned port twice in one fixture.
    def allocate() -> int:
        for port in range(14000 + worker * 500, 14500 + worker * 500):
            if port in used:
                continue
            with socket.socket() as reservation:
                try:
                    reservation.bind(("127.0.0.1", port))
                except OSError:
                    continue
            used.add(port)
            return port
        raise RuntimeError("no free worker listener port")

    return allocate
