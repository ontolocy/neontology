# type: ignore

import os

import pytest
from py2neo import Graph
from dotenv import load_dotenv

from neontology import GraphConnection, init_neontology


@pytest.fixture(scope="session")
def neo4j_db():

    load_dotenv()

    neo4j_uri = os.getenv("TEST_NEO4J_URI")
    neo4j_username = os.getenv("TEST_NEO4J_USERNAME")
    neo4j_password = os.getenv("TEST_NEO4J_PASSWORD")

    assert neo4j_uri is not None
    assert neo4j_username is not None
    assert neo4j_password is not None

    init_neontology(neo4j_uri, neo4j_username, neo4j_password)

    print("initialised neontology")

    graph = Graph(neo4j_uri, auth=(neo4j_username, neo4j_password))

    # confirm we're starting with an empty database
    cypher = """
    MATCH (n)
    RETURN COUNT(n)
    """

    node_count = graph.evaluate(cypher)
    assert (
        node_count == 0
    ), f"Looks like there are {node_count} nodes in the database, it should be empty."

    all_constraints_cypher = """
    SHOW CONSTRAINTS yield name
    RETURN COLLECT(DISTINCT name)
    """

    constraints = graph.evaluate(all_constraints_cypher)

    for constraint_name in constraints:

        drop_cypher = f"""
        DROP CONSTRAINT {constraint_name}
        """
        graph.evaluate(drop_cypher)

    yield graph

    graph_connection = GraphConnection()

    # tidy up by explicitly closing the graph connection here
    # otherwise something weird happens which closes the connection before
    # our GraphConnection gets the chance to
    graph_connection.driver.close()


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests which use the graph with the 'uses_graph' markers"""
    for item in items:
        if "use_graph" in item.fixturenames:
            item.add_marker("uses_graph")


@pytest.fixture(scope="function")
def use_graph(neo4j_db):

    yield neo4j_db

    # at the end of every individual test function, we want to empty the database

    cypher = """
    MATCH (n)
    DETACH DELETE n
    """

    neo4j_db.evaluate(cypher)
