"""Capabilities that a graph engine may or may not support.

Neontology aims for feature parity between engines. Where an engine cannot offer
something - currently only the experimental NetworkX/grand-cypher backend - it is
named here and declared on the engine, so the difference is stated in one place
rather than inferred from test assertions.

Engines declare what they *do* support (see `GraphEngineBase.supported_capabilities`).
An omission therefore means "unsupported", which is the safe direction: a capability
added here is not silently claimed by every engine.
"""

from enum import Enum


class Capability(str, Enum):
    """A named piece of engine behaviour that callers or tests may depend on."""

    # Writing to the graph with a raw query (CREATE, DELETE and friends).
    # grand-cypher is a query language over an existing NetworkX graph and
    # implements no mutation clauses.
    GRAPH_MUTATIONS = "graph_mutations"

    # `RETURN *` projections. grand-cypher's parser rejects the '*'.
    RETURN_STAR = "return_star"

    # create() inserts a node even when one with the same primary property
    # already exists. NetworkX keys nodes by a hash of (primary property,
    # label), so a second create() overwrites the first instead of duplicating.
    DUPLICATE_CREATE = "duplicate_create"

    # Case insensitive filters: name__icontains, name__iexact, name__istartswith.
    CASE_INSENSITIVE_FILTERS = "case_insensitive_filters"

    # Comparing datetime values in filters, e.g. created__gt=some_datetime.
    DATETIME_FILTERS = "datetime_filters"

    # Datetime accessors inside a query, e.g. RETURN n.created.year. grand-cypher
    # returns the datetime itself rather than evaluating the accessor.
    DATETIME_FUNCTIONS = "datetime_functions"

    # Filtering on list-valued properties.
    LIST_PROPERTY_FILTERS = "list_property_filters"

    # Filtering on properties holding complex (non-scalar) types.
    COMPLEX_PROPERTY_TYPES = "complex_property_types"

    # Aggregating values into a list with COLLECT, which @related_property uses
    # to return several values from one query.
    COLLECTED_VALUES = "collected_values"

    # Querying relationship properties via get_related().
    RELATIONSHIP_PROPERTY_QUERIES = "relationship_property_queries"


def render_capability_matrix() -> str:
    """Render the engine capability matrix as a markdown table.

    The table is committed into docs/graph-engines.md between marker comments and
    checked by a test, so the documented matrix cannot drift from what the engines
    declare.

    Returns:
        str: a markdown table, one row per capability and one column per engine.
    """
    from . import MemgraphEngine, Neo4jEngine

    engines: list = [("Neo4j", Neo4jEngine), ("Memgraph", MemgraphEngine)]

    try:
        from .networkxengine import NetworkxEngine

        engines.append(("NetworkX", NetworkxEngine))

    except ImportError:  # pragma: no cover - depends on the optional grand extra
        pass

    lines = [
        "| Capability | " + " | ".join(name for name, _ in engines) + " |",
        "| --- | " + " | ".join("---" for _ in engines) + " |",
    ]

    for capability in Capability:
        marks = ["Yes" if engine.supports(capability) else "No" for _, engine in engines]
        lines.append(f"| `{capability.value}` | " + " | ".join(marks) + " |")

    return "\n".join(lines)
