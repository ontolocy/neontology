import re
from pathlib import Path

import pytest

from neontology.graphengines import MemgraphEngine, Neo4jEngine
from neontology.graphengines.capabilities import Capability, render_capability_matrix

DOCS = Path(__file__).parent.parent / "docs" / "graph-engines.md"
MARKERS = re.compile(
    r"<!-- BEGIN CAPABILITY MATRIX -->\n(.*?)\n<!-- END CAPABILITY MATRIX -->",
    re.DOTALL,
)


def test_capability_matrix_in_docs_is_current():
    match = MARKERS.search(DOCS.read_text())

    assert match, "capability matrix markers missing from docs/graph-engines.md"

    assert match.group(1) == render_capability_matrix(), (
        "The capability matrix in docs/graph-engines.md is out of date. Regenerate it with:\n"
        "  python -c 'from neontology.graphengines.capabilities import render_capability_matrix;"
        " print(render_capability_matrix())'"
    )


def test_supports_rejects_unknown_capability():
    """A typo must be an error, not a silently unsupported feature."""
    with pytest.raises(TypeError):
        Neo4jEngine.supports("graph_mutatoins")


def test_default_engines_support_everything():
    """Capabilities name where engines diverge, so the mainstream engines have them all."""
    for engine in (Neo4jEngine, MemgraphEngine):
        assert engine.supported_capabilities == frozenset(Capability)
