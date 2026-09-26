"""The NetworkX engine's own behaviour.

What every engine must do is stated in tests/test_engine_contract.py. This file is for
what only the NetworkX engine does, such as the warnings it raises where the others'
MATCH quietly finds nothing.
"""

import pytest

from neontology import NeontologyWarning

pytest.importorskip("grandcypher", reason="needs the [grand] extra")

from neontology.graphengines.networkxengine import NetworkxConfig, NetworkxEngine  # noqa: E402


@pytest.mark.parametrize("end", ["source", "target"])
def test_a_node_not_found_by_another_property_names_its_end(end):
    engine = NetworkxEngine(NetworkxConfig())

    with pytest.warns(NeontologyWarning, match=f"^{end.capitalize()} node with property number=9 not found"):
        engine._swap_prop([{f"{end}_prop": 9}], f"{end}_prop", "number", "pp", "EngineContractNode")
