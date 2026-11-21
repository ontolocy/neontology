"""Connecting read-only, and running one query read-only, so nothing can be written.

A read-only connection has neontology refuse its own writes with a ReadOnlyError, and the
database refuse any other query which writes. A read-only query asks the database to
refuse that one query if it writes, on a connection which can.
"""

from typing import ClassVar

import pytest

from neontology import (
    BaseNode,
    BaseRelationship,
    Capability,
    CapabilityNotSupportedError,
    GraphConnection,
    ReadOnlyError,
    get_ontology_schema,
    init_neontology,
)
from neontology.graphengines.graphengine import WRITE_METHODS, GraphEngineBase


class ReadOnlyNode(BaseNode):
    __primarylabel__: ClassVar[str] = "ReadOnlyLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str


class ReadOnlyLink(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "READ_ONLY_LINK"

    source: ReadOnlyNode
    target: ReadOnlyNode


SCHEMA = get_ontology_schema(models=[ReadOnlyNode, ReadOnlyLink])


def _count() -> int:
    # on NetworkX a count over no matches returns no rows, where the others return 0
    return GraphConnection().evaluate_query_single("MATCH (n:ReadOnlyLabel) RETURN COUNT(n)") or 0


@pytest.fixture
def read_only(get_graph_config, tmp_path):
    """A read-only connection of the test's own, to the engine under test."""
    session_instance = GraphConnection._instance

    GraphConnection._instance = None

    config = get_graph_config

    try:
        # Ladybug can only open a database read-only on disk, and only with its tables
        # declared - a schema first database refuses a query naming a table it lacks - so
        # its database is made there first
        if "db_path" in type(config).model_fields:
            config = config.model_copy(update={"db_path": str(tmp_path / "graph.lbdb")})

            init_neontology(config)
            GraphConnection().initialise_graph(SCHEMA)
            GraphConnection().close()

        init_neontology(config.model_copy(update={"read_only": True}))

        yield GraphConnection()

    finally:
        established = GraphConnection._instance

        GraphConnection._instance = session_instance

        if established is not None:
            established.engine.close_connection()


def test_a_read_only_connection_says_so(read_only):
    assert "read_only=True" in repr(read_only)


def test_a_read_only_connection_reads(read_only):
    assert ReadOnlyNode.get_count() == 0
    assert ReadOnlyNode.match("anything") is None


WRITES = [
    pytest.param(lambda: ReadOnlyNode(name="a").create(), id="create"),
    pytest.param(lambda: ReadOnlyNode(name="a").merge(), id="merge"),
    pytest.param(lambda: ReadOnlyNode.merge_records([{"name": "a"}]), id="merge_records"),
    pytest.param(lambda: ReadOnlyNode.delete("a"), id="delete"),
    pytest.param(lambda: ReadOnlyLink(source=ReadOnlyNode(name="a"), target=ReadOnlyNode(name="b")).merge(), id="relationship"),
    pytest.param(lambda: GraphConnection().initialise_graph(SCHEMA), id="initialise_graph"),
    pytest.param(lambda: GraphConnection().apply_constraints([ReadOnlyNode]), id="apply_constraints"),
    pytest.param(lambda: GraphConnection().apply_indexes([ReadOnlyNode]), id="apply_indexes"),
]


@pytest.mark.parametrize("write", WRITES)
def test_neontology_refuses_its_own_writes(read_only, write):
    """Refused before reaching the database, with one error whatever the engine."""
    with pytest.raises(ReadOnlyError, match="read-only"):
        write()

    assert _count() == 0


def test_a_query_which_writes_is_refused(read_only):
    """By the database itself, since neontology cannot tell what a query does."""
    with pytest.raises(Exception):  # noqa: B017 - each database raises its own error
        read_only.evaluate_query("CREATE (n:ReadOnlyLabel {name: 'raw'}) RETURN n")

    assert _count() == 0


@pytest.mark.requires_capability(Capability.READ_ONLY_QUERIES)
def test_a_read_only_query_reads(use_graph):
    ReadOnlyNode(name="a").merge()

    assert use_graph.evaluate_query_single("MATCH (n:ReadOnlyLabel) RETURN COUNT(n)", read_only=True) == 1
    assert len(use_graph.evaluate_query("MATCH (n:ReadOnlyLabel) RETURN n", read_only=True).nodes) == 1


@pytest.mark.requires_capability(Capability.READ_ONLY_QUERIES, Capability.GRAPH_MUTATIONS)
def test_a_read_only_query_which_writes_is_refused_and_the_connection_still_writes(use_graph):
    ReadOnlyNode(name="a").merge()

    with pytest.raises(Exception) as raised:  # noqa: B017 - each database raises its own error
        use_graph.evaluate_query("CREATE (n:ReadOnlyLabel {name: 'raw'}) RETURN n", read_only=True)

    # refused by the database, rather than because the engine cannot run it read-only
    assert not isinstance(raised.value, CapabilityNotSupportedError)
    assert _count() == 1

    use_graph.evaluate_query("CREATE (n:ReadOnlyLabel {name: 'raw'}) RETURN n")

    assert _count() == 2


def test_a_read_only_config_is_a_different_connection(get_graph_config):
    """So reuse_connection never hands back a connection which can write."""
    assert get_graph_config.model_copy(update={"read_only": True}) != get_graph_config


# every public engine method which does not write. Anything else an engine defines has to
# be in one list or the other, so a new method is decided on rather than left unguarded
READ_METHODS = {
    "check_connection",
    "close_connection",
    "evaluate_query",
    "evaluate_query_single",
    "export_dict_converter",
    "get_constraints",
    "get_count",
    "get_indexes",
    "identifier",
    "label_pattern",
    "match_nodes",
    "match_relationships",
    "matching_property_values",
    "supports",
    "verify_connection",
}


def _engine_classes():
    """Neontology's own engine classes - not the stand-ins tests define - whichever extras are installed."""
    found, pending = [], [GraphEngineBase]

    while pending:
        cls = pending.pop()

        if cls.__module__.startswith("neontology."):
            found.append(cls)

        pending += cls.__subclasses__()

    return sorted(found, key=lambda cls: cls.__name__)


@pytest.mark.parametrize("engine_class", _engine_classes(), ids=lambda cls: cls.__name__)
def test_every_engine_method_is_known_to_write_or_not(engine_class):
    """A method added to an engine must be added to WRITE_METHODS, which a read-only engine refuses, or READ_METHODS here."""
    methods = {name for name, value in vars(engine_class).items() if not name.startswith("_") and callable(value)}

    assert methods - set(WRITE_METHODS) - READ_METHODS == set()

    for name in methods & set(WRITE_METHODS):
        assert getattr(getattr(engine_class, name), "_refused_when_read_only", False), f"{name} is not guarded"
