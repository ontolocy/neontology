import re
from pathlib import Path

import pytest

from neontology.graphengines import MemgraphEngine, Neo4jEngine
from neontology.graphengines.capabilities import Capability, render_capability_matrix

try:
    from neontology.graphengines import NetworkxEngine  # noqa: F401

    HAS_GRAND = True

except ImportError:
    HAS_GRAND = False

DOCS = Path(__file__).parent.parent / "docs" / "graph-engines.md"
MARKERS = re.compile(
    r"<!-- BEGIN CAPABILITY MATRIX -->\n(.*?)\n<!-- END CAPABILITY MATRIX -->",
    re.DOTALL,
)


@pytest.mark.skipif(
    not HAS_GRAND,
    reason="the committed matrix includes the networkx column, which needs the [grand] extra",
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


def test_no_duplicate_primary_labels_in_the_suite():
    """Test models must not share a primary label.

    Type discovery is global - `get_node_types()` walks BaseNode.__subclasses__() and
    builds {primary_label: class} - so two test models claiming the same label leave
    the winner decided by traversal order. Queries that rehydrate without explicit
    node_classes could then build the wrong class, which fails confusingly and depends
    on test ordering.
    """
    import collections
    import glob
    import re

    root = Path(__file__).parent

    definitions = collections.defaultdict(list)

    for path in glob.glob(str(root / "**" / "*.py"), recursive=True):
        for lineno, line in enumerate(Path(path).read_text().split("\n"), start=1):
            match = re.search(r'__primarylabel__[^=]*=\s*"([^"]+)"', line)

            if match:
                definitions[match.group(1)].append(f"{Path(path).name}:{lineno}")

    duplicates = {label: sites for label, sites in definitions.items() if len(sites) > 1}

    assert not duplicates, f"primary labels defined more than once: {duplicates}"
