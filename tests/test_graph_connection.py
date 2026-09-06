from typing import ClassVar, Optional

import pytest
from pydantic import Field

from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship
from neontology.graphconnection import GraphConnection
from neontology.graphengines.capabilities import Capability


class PracticeNodeGC(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "PracticeNodeGC"
    pp: str


class PracticeRelationshipGC(BaseRelationship):
    source: PracticeNodeGC
    target: PracticeNodeGC
    __relationshiptype__: ClassVar[Optional[str]] = "PRACTICE_RELATIONSHIP_GC"


create_test_node_table_cypher = "CREATE NODE TABLE TestNode(name STRING, PRIMARY KEY (name))"


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_evaluate_query_single(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn:TestNode {name: "Foo Bar"})
    RETURN tn.name
    """

    result = gc.evaluate_query_single(create_cypher)

    assert result == "Foo Bar"


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_evaluate_query_single_node(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn:TestNode {name: "Foo Bar"})
    RETURN tn
    """

    result = gc.evaluate_query_single(create_cypher)

    assert dict(result)["name"] == "Foo Bar"


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_evaluate_query_single_multiple(use_graph):
    gc = GraphConnection()

    gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Foo'})")
    gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Bar'})")

    match_cypher = """
    MATCH (n:TestNode)
    RETURN n
    """

    with pytest.warns(UserWarning):
        gc.evaluate_query_single(match_cypher)


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_evaluate_query_single_collected(use_graph):
    gc = GraphConnection()

    gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Foo'})")
    gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Bar'})")

    match_cypher = """
    MATCH (n:TestNode)
    WITH n as nodes ORDER BY n.name LIMIT 5
    RETURN COLLECT(nodes.name)
    """

    result = gc.evaluate_query_single(match_cypher)

    assert result == ["Bar", "Foo"]


def test_evaluate_query_empty(use_graph):
    gc = GraphConnection()

    cypher = "MATCH (n) RETURN n"

    result = gc.evaluate_query(cypher)

    assert result.records == []
    assert result.nodes == []
    assert result.relationships == []
    assert result.node_link_data == {"edges": [], "nodes": [], "directed": True}


def test_evaluate_query_records(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")
    rel = PracticeRelationshipGC(source=foo, target=bar)

    foo.merge()
    bar.merge()
    rel.merge()

    cypher = "MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert len(result.records) == 1


def test_evaluate_query_neontology_records(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")
    rel = PracticeRelationshipGC(source=foo, target=bar)

    foo.merge()
    bar.merge()
    rel.merge()

    cypher = "MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.records[0]["nodes"]["n"].pp == "foo"
    assert result.records[0]["nodes"]["o"].pp == "bar"
    assert result.records[0]["relationships"]["r"].source.pp == "foo"
    assert result.records[0]["relationships"]["r"].target.pp == "bar"


def test_evaluate_query_nodes(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")

    foo.merge()
    bar.merge()

    cypher = "MATCH (n) RETURN n ORDER BY n.pp DESC"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.nodes[0].pp == "foo"
    assert result.nodes[1].pp == "bar"


def test_evaluate_query_relationships(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")
    rel = PracticeRelationshipGC(source=foo, target=bar)

    foo.merge()
    bar.merge()
    rel.merge()

    cypher = "MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.relationships[0].__relationshiptype__ == "PRACTICE_RELATIONSHIP_GC"
    assert result.relationships[0].source.pp == "foo"
    assert result.relationships[0].target.pp == "bar"


def test_evaluate_query_paths(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")
    baz = PracticeNodeGC(pp="baz")
    rel1 = PracticeRelationshipGC(source=foo, target=bar)
    rel2 = PracticeRelationshipGC(source=bar, target=baz)

    foo.merge()
    bar.merge()
    baz.merge()
    rel1.merge()
    rel2.merge()

    cypher = "MATCH p = (n)-[r]->(o)-[r1]->(o2) RETURN n,r,o,r1,o2,p"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.paths[0][0].source.get_pp() == "foo"
    assert result.paths[0][1].target.get_pp() == "baz"


def test_evaluate_query_nodes_records_simple(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")

    foo.merge()
    bar.merge()

    cypher = "MATCH (n) RETURN n ORDER BY n.pp DESC"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.records[0]["nodes"]["n"].pp == "foo"
    assert result.records[1]["nodes"]["n"].pp == "bar"


def test_evaluate_query_params(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")

    foo.merge()
    bar.merge()

    cypher = "MATCH (n) WHERE n.pp = 'bar' RETURN n ORDER BY n.pp DESC"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert len(result.records) == 1
    assert result.nodes[0].pp == "bar"


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_undefined_label(use_graph):
    gc = GraphConnection()

    result = gc.evaluate_query_single("CREATE (tn1:WeirdTestNode {name: 'Foo'})")
    result = gc.evaluate_query_single("CREATE (tn1:WeirdTestNode {name: 'Bar'})")

    match_cypher = """
    MATCH (n:WeirdTestNode)
    RETURN n
    """

    with pytest.warns(UserWarning, match="Unexpected primary labels returned:"):
        result = gc.evaluate_query(match_cypher)

    assert len(result.records) == 2
    assert len(result.nodes) == 0


class SpecialTestNodeGC(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "SpecialTestNodeGC"
    pp: str


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_multiple_primary_labels(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn1:SpecialTestNodeGC:PracticeNodeGC {pp: "Foo"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:SpecialTestNodeGC)
    RETURN n
    """

    with pytest.warns(
        UserWarning,
        match=(
            r"Unexpected primary labels returned: "
            r"{('SpecialTestNodeGC'|'PracticeNodeGC'), ('SpecialTestNodeGC'|'PracticeNodeGC')}"
        ),
    ):
        result = gc.evaluate_query(match_cypher)

    assert len(result.records) == 1
    assert len(result.nodes) == 0


@pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
def test_warn_on_unexpected_secondary_labels(use_graph):
    gc = GraphConnection()

    # create a node which looks like a practice node but has additional labels

    create_cypher = """
    CREATE (tn1:PracticeNodeGC:WeirdTestNode {pp: "Foo"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:PracticeNodeGC)
    RETURN n
    """

    # check we raise a warning

    with pytest.warns(UserWarning, match="Unexpected secondary labels returned: {'WeirdTestNode'}"):
        result = gc.evaluate_query(match_cypher)

    # we should still capture as records and nodes
    assert len(result.records) == 1
    assert len(result.nodes) == 1


def test_evaluate_rel_only_query(use_graph):
    """When we return just a relationship,
    this can only be turned into a 'neontology relationship'
    if the result also includes the source and target nodes.
    """
    source_node = PracticeNodeGC(pp="Source Node")
    source_node.create()

    target_node = PracticeNodeGC(pp="Target Node")
    target_node.create()

    br = PracticeRelationshipGC(source=source_node, target=target_node)

    br.merge()

    gc = GraphConnection()

    cypher = """
    MATCH (a)-[r]->(b)
    RETURN r
    """

    with pytest.warns(
        UserWarning,
        match=r"PRACTICE_RELATIONSHIP_GC relationship type query did not include nodes.",
    ):
        result = gc.evaluate_query(cypher)

    assert len(result.records) == 1
    assert len(result.nodes) == 0
    assert len(result.relationships) == 0

    cypher2 = """
    MATCH (a)-[r]->(b)
    RETURN r, a
    """

    with pytest.warns(
        UserWarning,
        match=r"PRACTICE_RELATIONSHIP_GC relationship type query did not include nodes.",
    ):
        result2 = gc.evaluate_query(cypher2)

    assert len(result2.records) == 1
    assert len(result2.nodes) == 1
    assert len(result2.relationships) == 0

    cypher3 = """
    MATCH (a)-[r]->(b)
    RETURN r, b
    """

    with pytest.warns(
        UserWarning,
        match=r"PRACTICE_RELATIONSHIP_GC relationship type query did not include nodes.",
    ):
        result3 = gc.evaluate_query(cypher3)

    assert len(result3.records) == 1
    assert len(result3.nodes) == 1
    assert len(result3.relationships) == 0

    cypher4 = """
    MATCH (a)-[r]->(b)
    RETURN r, b, a
    """

    result4 = gc.evaluate_query(cypher4)

    assert len(result4.records) == 1
    assert len(result4.nodes) == 2
    assert len(result4.relationships) == 1


class ComplexPracticeNodeGC(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "ComplexPracticeNodeGC"
    pp: str

    a_list: Optional[list] = None


class ComplexPracticeRelationshipGC(BaseRelationship):
    source: ComplexPracticeNodeGC
    target: ComplexPracticeNodeGC
    __relationshiptype__: ClassVar[Optional[str]] = "COMPLEX_PRACTICE_RELATIONSHIP_GC"

    b_list: Optional[list] = None
    number: int = Field(default=42, json_schema_extra={"merge_on": True})


@pytest.mark.requires_capability(Capability.RETURN_STAR)
def test_evaluate_query_node_links(use_graph):
    foo = ComplexPracticeNodeGC(pp="foo", a_list=[1, 2, 3])
    bar = ComplexPracticeNodeGC(pp="bar")
    baz = ComplexPracticeNodeGC(pp="baz", a_list=[])
    rel1 = ComplexPracticeRelationshipGC(source=foo, target=bar, b_list=[4, 5, 6])
    rel2 = ComplexPracticeRelationshipGC(source=bar, target=baz)
    rel3 = ComplexPracticeRelationshipGC(source=baz, target=foo, b_list=["hello", "world"], number=9)
    rel4 = ComplexPracticeRelationshipGC(source=baz, target=foo)

    foo.merge()
    bar.merge()
    baz.merge()
    rel1.merge()
    rel2.merge()
    rel3.merge()
    rel4.merge()

    gc = GraphConnection()

    cypher = "MATCH (n)-[r]->(o) MATCH (n1)-[r1]-(o1) RETURN *"

    results = gc.evaluate_query(cypher)

    node_link_data = results.node_link_data

    assert len(node_link_data["nodes"]) == 3
    assert len(node_link_data["edges"]) == 4


def test_evaluate_query_node_links_simple(use_graph):
    foo = PracticeNodeGC(pp="foo")
    bar = PracticeNodeGC(pp="bar")
    rel1 = PracticeRelationshipGC(source=foo, target=bar)

    foo.merge()
    bar.merge()

    rel1.merge()

    gc = GraphConnection()

    cypher = "MATCH (n)-[r]->(o) RETURN n,r,o"

    results = gc.evaluate_query(cypher)

    node_link_data = results.node_link_data

    assert node_link_data["nodes"][0]["__pp__"] == "foo"
    assert node_link_data["nodes"][0]["__str__"] == "foo"
    assert node_link_data["nodes"][0]["LABEL"] == "PracticeNodeGC"

    assert len(node_link_data["nodes"]) == 2
    assert len(node_link_data["edges"]) == 1


class TestConnectionVerification:
    """The connection is verified where it is established, not on every access.

    GraphConnection() is how the library reaches the singleton - including once per
    model dump - so verifying in __init__ cost a database round trip per call, which
    dominated bulk operations.
    """

    def test_accessing_the_connection_does_not_verify(self, use_graph, monkeypatch):
        calls = []

        monkeypatch.setattr(
            type(use_graph.engine),
            "verify_connection",
            lambda self: calls.append(1) or True,
        )

        for _ in range(5):
            GraphConnection()

        assert calls == []

    def test_using_a_model_does_not_verify_per_record(self, use_graph, monkeypatch):
        """Bulk work must not scale database round trips with the number of records."""

        class VerifyCountNode(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "VerifyCountNode"

            pp: str

        calls = []

        monkeypatch.setattr(
            type(use_graph.engine),
            "verify_connection",
            lambda self: calls.append(1) or True,
        )

        VerifyCountNode.merge_records([{"pp": f"n{i}"} for i in range(20)])

        assert calls == []

    def test_uninitialised_connection_still_explains_itself(self):
        """The helpful error comes from __new__, so it survives moving the check."""
        original = GraphConnection._instance

        GraphConnection._instance = None

        try:
            with pytest.raises(RuntimeError, match="init_neontology"):
                GraphConnection()

        finally:
            GraphConnection._instance = original

    def test_change_engine_verifies_the_new_connection(self, use_graph, get_graph_config, monkeypatch):
        """Swapping the engine establishes a connection, so that is checked."""
        engine_class = type(use_graph.engine)

        monkeypatch.setattr(engine_class, "verify_connection", lambda self: False)

        with pytest.raises(RuntimeError, match="could not connect"):
            GraphConnection.change_engine(get_graph_config)

        # restore a working engine for the rest of the session
        monkeypatch.undo()

        GraphConnection.change_engine(get_graph_config)

        assert GraphConnection().engine.verify_connection() is True
