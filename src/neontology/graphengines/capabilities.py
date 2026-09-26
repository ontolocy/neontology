"""Capabilities that a graph engine may or may not support.

Neontology aims for feature parity between engines. Where an engine cannot offer
something - the experimental NetworkX/grand-cypher backend, and the embedded
LadybugDB backend - it is named here and declared on the engine, so the difference
is stated in one place rather than inferred from test assertions.

Engines declare what they *do* support (see `GraphEngineBase.supported_capabilities`).
An omission therefore means "unsupported", which is the safe direction: a capability
added here is not silently claimed by every engine.
"""

from enum import Enum


class CapabilityNotSupportedError(NotImplementedError):
    """Raised when an engine is asked for something its backend cannot do.

    Subclasses NotImplementedError, which is what the engine methods used to raise
    bare, so existing `except NotImplementedError` callers keep working.
    """


class Capability(str, Enum):
    """A named piece of engine behaviour that callers or tests may depend on."""

    # A node carrying more than one label, so a subclass node also matches its
    # parent's label. LadybugDB stores each label as its own table and a node
    # belongs to exactly one of them, so secondary and inheritable labels are not
    # written and a query naming one directly finds nothing. Class scoped queries
    # still find subclasses - see GraphEngineBase.label_pattern().
    SECONDARY_LABELS = "secondary_labels"

    # Naming a label, relationship type or property the database has not been told
    # about. LadybugDB is schema first - every label is a table with typed columns - so
    # a query naming one that does not exist is an error, where the other engines treat
    # it as a pattern or a filter that simply matches nothing. Neontology's own writes
    # declare what they need; this is about queries you write yourself.
    UNDECLARED_SCHEMA = "undeclared_schema"

    # A datetime keeping its timezone through a round trip. LadybugDB's TIMESTAMP holds
    # no offset, so an aware datetime is read back naive. Its TIMESTAMP_TZ is not the
    # answer either: it reports every value as UTC, which would make a naive datetime
    # come back aware and an offset other than UTC come back wrong.
    TIMEZONE_AWARE_DATETIMES = "timezone_aware_datetimes"

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

    # Comparing datetime values in filters, e.g. created__gt=some_datetime.
    DATETIME_FILTERS = "datetime_filters"

    # Datetime accessors inside a query, e.g. RETURN n.created.year. grand-cypher
    # returns the datetime itself rather than evaluating the accessor.
    DATETIME_FUNCTIONS = "datetime_functions"

    # Filtering on list-valued properties.
    LIST_PROPERTY_FILTERS = "list_property_filters"

    # Filtering on properties holding complex (non-scalar) types.
    COMPLEX_PROPERTY_TYPES = "complex_property_types"

    # DISTINCT inside an aggregation, e.g. COLLECT(DISTINCT n.name). Plain
    # COLLECT works on every engine and needs no capability.
    COLLECT_DISTINCT = "collect_distinct"

    # Querying relationship properties via get_related().
    RELATIONSHIP_PROPERTY_QUERIES = "relationship_property_queries"

    # Uniqueness constraints on a label/property pair, and listing and dropping
    # them. grand-cypher queries an ordinary NetworkX graph, which has no schema
    # layer to hold them.
    CONSTRAINTS = "constraints"

    # Indexes on a label/property pair, and listing and dropping them. Separate
    # from CONSTRAINTS because an engine could offer one without the other.
    INDEXES = "indexes"

    # A named path in a query matching more than one pattern - two MATCH clauses, say -
    # holding only its own nodes and relationships. grand-cypher builds a named path from
    # every node the query matched, so it joins the other patterns onto it, through
    # whatever edges lie between them or with a gap where none do.
    MULTI_PATTERN_PATHS = "multi_pattern_paths"


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

    try:
        from .ladybugengine import LadybugEngine

        engines.append(("Ladybug", LadybugEngine))

    except ImportError:  # pragma: no cover - depends on the optional ladybug extra
        pass

    lines = [
        "| Capability | " + " | ".join(name for name, _ in engines) + " |",
        "| --- | " + " | ".join("---" for _ in engines) + " |",
    ]

    for capability in Capability:
        marks = ["Yes" if engine.supports(capability) else "No" for _, engine in engines]
        lines.append(f"| `{capability.value}` | " + " | ".join(marks) + " |")

    return "\n".join(lines)
