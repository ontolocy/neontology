# type: ignore

import logging
import os

import pytest
from dotenv import load_dotenv

from neontology import GraphConnection, init_neontology
from neontology.graphengines import MemgraphConfig, Neo4jConfig, NetworkxConfig

logger = logging.getLogger(__name__)


def reset_constraints():
    gc = GraphConnection()

    try:
        constraints = gc.engine.get_constraints()

    except NotImplementedError:
        return

    for constraint_name in constraints:
        gc.engine.drop_constraint(constraint_name)


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(
            {
                "graph_config_vars": {
                    "uri": "TEST_NEO4J_URI",
                    "username": "TEST_NEO4J_USERNAME",
                    "password": "TEST_NEO4J_PASSWORD",
                },
                "graph_engine": "NEO4J",
            },
            id="neo4j-engine",
        ),
        pytest.param(
            {
                "graph_config_vars": {
                    "uri": "TEST_MEMGRAPH_URI",
                    "username": "TEST_MEMGRAPH_USER",
                    "password": "TEST_MEMGRAPH_PASSWORD",
                },
                "graph_engine": "MEMGRAPH",
            },
            id="memgraph-engine",
        ),
        pytest.param(
            {
                "graph_config_vars": {},
                "graph_engine": "NETWORKX",
            },
            id="networkx-engine",
        ),
    ],
)
def get_graph_config(request, tmp_path_factory) -> tuple:
    load_dotenv()

    graph_engines = {
        "NEO4J": Neo4jConfig,
        "MEMGRAPH": MemgraphConfig,
        "NETWORKX": NetworkxConfig,
    }

    graph_config_vars = request.param["graph_config_vars"]

    graph_config = {}

    # build config using environment variables
    for key, value in graph_config_vars.items():

        graph_config[key] = os.getenv(value)
        assert (
            graph_config[key] is not None
        ), f"Environment variable {value} is not set."

    graph_engine = request.param["graph_engine"]

    config = graph_engines[graph_engine](**graph_config)

    return config


@pytest.fixture(
    scope="session",
)
def graph_db(request, tmp_path_factory, get_graph_config):
    load_dotenv()

    init_neontology(get_graph_config)

    gc = GraphConnection()

    gc.change_engine(get_graph_config)

    # confirm we're starting with an empty database
    cypher = """
    MATCH (n)
    RETURN COUNT(n)
    """

    node_count = gc.evaluate_query_single(cypher)

    # most backends will return 0
    # Grand will return an empty list
    assert (
        not node_count
    ), f"Looks like there are {node_count} nodes in the database, it should be empty."

    yield gc


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests which use the graph with the 'uses_graph' markers"""
    for item in items:
        if "use_graph" in item.fixturenames:
            item.add_marker("uses_graph")


@pytest.fixture(scope="function")
def use_graph(request, graph_db):
    """Fixture to use the graph database in tests."""
    yield graph_db

    # at the end of every individual test function, we want to empty the database

    cypher = """
    MATCH (n) DETACH DELETE n;
    """

    if "networkx-engine" not in request.node.callspec.id:
        try:
            graph_db.evaluate_query_single(cypher)
        except RuntimeError:
            pass

    if request.node.callspec.id in ["networkx-engine"]:
        # grand cypher doesn't support DETACH DELETE
        graph_db.engine.driver.clear()

    # not all engines will implement constraints, so we don't always have to reset them

    reset_constraints()
