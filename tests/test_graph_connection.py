from typing import ClassVar, Optional

import pytest


from neontology.graphconnection import GraphConnection
from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship


class PracticeNodeGC(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "PracticeNodeGC"
    pp: str


class PracticeRelationshipGC(BaseRelationship):
    source: PracticeNodeGC
    target: PracticeNodeGC
    __relationshiptype__: ClassVar[Optional[str]] = "PRACTICE_RELATIONSHIP_GC"


create_test_node_table_cypher = (
    "CREATE NODE TABLE TestNode(name STRING, PRIMARY KEY (name))"
)


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

    gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Foo'})")
    gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Bar'})")

    match_cypher = """
    MATCH (n:TestNode)
    RETURN n
    """

    with pytest.warns(UserWarning):
        gc.evaluate_query_single(match_cypher)


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

    cypher = "MATCH (n) OPTIONAL MATCH (n)-[r]->(o) RETURN n,r,o"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    assert len(result.records) == 2


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

    cypher = "MATCH p = (n)-[r]->(o)-[r1]->(o2) RETURN *"

    gc = GraphConnection()
    result = gc.evaluate_query(cypher)

    for entry in result.records_raw:
        print(entry)

    # print(result)

    print(result.paths)

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


def test_undefined_label(use_graph):
    gc = GraphConnection()

    result = gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Foo'})")
    result = gc.evaluate_query_single("CREATE (tn1:TestNode {name: 'Bar'})")

    match_cypher = """
    MATCH (n:TestNode)
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
        match=r"Unexpected primary labels returned: {('SpecialTestNodeGC'|'PracticeNodeGC'), ('SpecialTestNodeGC'|'PracticeNodeGC')}",
    ):
        result = gc.evaluate_query(match_cypher)

    assert len(result.records) == 1
    assert len(result.nodes) == 0


def test_warn_on_unexpected_secondary_labels(use_graph):
    gc = GraphConnection()

    # create a node which looks like a practice node but has additional labels

    create_cypher = """
    CREATE (tn1:PracticeNodeGC:TestNode {pp: "Foo"})
    """

    result = gc.evaluate_query_single(create_cypher)

    match_cypher = """
    MATCH (n:PracticeNodeGC)
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
