import pytest

from neontology.graphconnection import GraphConnection


def test_evaluate_query_single(use_graph):
    gc = GraphConnection()

    create_cypher = """
    CREATE (tn:TestNode {name: "Foo Bar"})
    RETURN tn.name
    """

    result = gc.evaluate_query_single(create_cypher)

    assert result == "Foo Bar"


def test_evaluate_query_node(use_graph):
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
    RETURN COLLECT(n.name)
    """

    result = gc.evaluate_query_single(match_cypher)

    assert result == ["Foo", "Bar"]
