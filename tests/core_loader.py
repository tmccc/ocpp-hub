"""Load the standalone engine without running either Home Assistant integration."""

import importlib.util
import sys
from pathlib import Path


# Commission a case with the selected engine before test fixtures import its types.
def load_test_core(root: Path) -> None:
    directory = root / "custom_components/ocpp_hub/core"
    if "ocpp_hub_core" in sys.modules:
        return
    spec = importlib.util.spec_from_file_location(
        "ocpp_hub_core", directory / "__init__.py", submodule_search_locations=[str(directory)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["ocpp_hub_core"] = module
    spec.loader.exec_module(module)
