from typing import ClassVar, Optional

import pytest


from neontology.graphconnection import GraphConnection

from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship


class PracticeNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "PracticeNode"
    pp: str


class PracticeRelationship(BaseRelationship):
    source: PracticeNode
    target: PracticeNode
    __relationshiptype__: ClassVar[Optional[str]] = "PRACTICE_RELATIONSHIP"


def test_evaluate_query_single(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn:TestNode {name: "Foo Bar"})
    RETURN tn.name
    """

    result = gc.evaluate_query_single(create_cypher)

    assert result == "Foo Bar"


def test_evaluate_query_single_node(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn:TestNode {name: "Foo Bar"})
    RETURN tn
    """

    result = gc.evaluate_query_single(create_cypher)

    assert dict(result)["name"] == "Foo Bar"


def test_evaluate_query_single_multiple(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn1:TestNode {name: "Foo"})
    CREATE (tn2:TestNode {name: "Bar"})
    """

    gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:TestNode)
    RETURN n
    """

    with pytest.warns(UserWarning):
        gc.evaluate_query_single(match_cypher)


def test_evaluate_query_single_collected(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn1:TestNode {name: "Foo"})
    CREATE (tn2:TestNode {name: "Bar"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:TestNode)
    WITH n as nodes ORDER BY n.name
    RETURN COLLECT(nodes.name)
    """

    result = gc.evaluate_query_single(match_cypher)

    assert result == ["Bar", "Foo"]


def test_evaluate_query_empty(use_graph):
    gc = GraphConnection()

    cypher = "MATCH (n) RETURN n"

    result = gc.evaluate_query(cypher)

    assert result.records == []
    assert result.neontology_records == []
    assert result.nodes == []
    assert result.relationships == []
    assert result.node_link_data == {"links": [], "nodes": []}


def test_evaluate_query_records(use_graph):
    foo = PracticeNode(pp="foo")
    bar = PracticeNode(pp="bar")
    rel = PracticeRelationship(source=foo, target=bar)

    foo.merge()
    bar.merge()
    rel.merge()

    cypher = "MATCH (n) OPTIONAL MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert len(result.records) == 2


def test_evaluate_query_neontology_records(use_graph):
    foo = PracticeNode(pp="foo")
    bar = PracticeNode(pp="bar")
    rel = PracticeRelationship(source=foo, target=bar)

    foo.merge()
    bar.merge()
    rel.merge()

    cypher = "MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.neontology_records[0]["nodes"]["n"].pp == "foo"
    assert result.neontology_records[0]["nodes"]["o"].pp == "bar"
    assert result.neontology_records[0]["relationships"]["r"].source.pp == "foo"
    assert result.neontology_records[0]["relationships"]["r"].target.pp == "bar"


def test_evaluate_query_nodes(use_graph):
    foo = PracticeNode(pp="foo")
    bar = PracticeNode(pp="bar")

    foo.merge()
    bar.merge()

    cypher = "MATCH (n) RETURN n ORDER BY n.pp DESC"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.nodes[0].pp == "foo"
    assert result.nodes[1].pp == "bar"


def test_evaluate_query_relationships(use_graph):
    foo = PracticeNode(pp="foo")
    bar = PracticeNode(pp="bar")
    rel = PracticeRelationship(source=foo, target=bar)

    foo.merge()
    bar.merge()
    rel.merge()

    cypher = "MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert result.relationships[0].__relationshiptype__ == "PRACTICE_RELATIONSHIP"
    assert result.relationships[0].source.pp == "foo"
    assert result.relationships[0].target.pp == "bar"


def test_evaluate_query_params(use_graph):
    foo = PracticeNode(pp="foo")
    bar = PracticeNode(pp="bar")

    foo.merge()
    bar.merge()

    cypher = "MATCH (n) WHERE n.pp = 'bar' RETURN n ORDER BY n.pp DESC"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert len(result.records) == 1
    assert result.nodes[0].pp == "bar"


def test_undefined_label(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn1:TestNode {name: "Foo"})
    CREATE (tn2:TestNode {name: "Bar"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:TestNode)
    RETURN n
    """

    with pytest.warns(UserWarning, match="Unexpected primary labels returned: set()"):
        result = gc.evaluate_query(match_cypher)

    assert len(result.records) == 2
    assert len(result.nodes) == 0


def test_multiple_primary_labels(use_graph):
    gc = GraphConnection()

    class SpecialTestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "SpecialTestNode"
        pp: str

    create_cypher = """
    CREATE (tn1:SpecialTestNode:PracticeNode {pp: "Foo"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:SpecialTestNode)
    RETURN n
    """

    with pytest.warns(
        UserWarning,
        match=r"Unexpected primary labels returned: {('SpecialTestNode'|'PracticeNode'), ('SpecialTestNode'|'PracticeNode')}",
    ):
        result = gc.evaluate_query(match_cypher)

    assert len(result.records) == 1
    assert len(result.nodes) == 0


def test_warn_on_unexpected_secondary_labels(use_graph):

    gc = GraphConnection()

    # create a node which looks like a practice node but has additional labels

    create_cypher = """
    CREATE (tn1:PracticeNode:TestNode {pp: "Foo"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:PracticeNode)
    RETURN n
    """

    # check we raise a warning

    with pytest.warns(
        UserWarning, match="Unexpected secondary labels returned: {'TestNode'}"
    ):
        result = gc.evaluate_query(match_cypher)

    # we should still capture as records and nodes
    assert len(result.records) == 1
    assert len(result.nodes) == 1


def test_evaluate_rel_only_query(use_graph):
    """When we return just a relationship,
    this can only be turned into a 'neontology relationship'
    if the result also includes the source and target nodes.
    """

    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    br = PracticeRelationship(source=source_node, target=target_node)

    br.merge()

    gc = GraphConnection()

    cypher = """
    MATCH (a)-[r]-(b)
    RETURN DISTINCT(r)
    """

    with pytest.warns(
        UserWarning,
        match=r"PRACTICE_RELATIONSHIP relationship type query did not include nodes.",
    ):
        result = gc.evaluate_query(cypher)

    assert len(result.records) == 1
    assert len(result.nodes) == 0
    assert len(result.relationships) == 0

    cypher2 = """
    MATCH (a)-[r]->(b)
    RETURN DISTINCT(r), a
    """

    with pytest.warns(
        UserWarning,
        match=r"PRACTICE_RELATIONSHIP relationship type query did not include nodes.",
    ):
        result2 = gc.evaluate_query(cypher2)

    assert len(result2.records) == 1
    assert len(result2.nodes) == 1
    assert len(result2.relationships) == 0

    cypher3 = """
    MATCH (a)-[r]->(b)
    RETURN DISTINCT(r), b
    """

    with pytest.warns(
        UserWarning,
        match=r"PRACTICE_RELATIONSHIP relationship type query did not include nodes.",
    ):
        result3 = gc.evaluate_query(cypher3)

    assert len(result3.records) == 1
    assert len(result3.nodes) == 1
    assert len(result3.relationships) == 0

    cypher4 = """
    MATCH (a)-[r]->(b)
    RETURN DISTINCT(r), b, a
    """

    result4 = gc.evaluate_query(cypher4)

    assert len(result4.records) == 1
    assert len(result4.nodes) == 2
    assert len(result4.relationships) == 1
