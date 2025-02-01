# type: ignore

import logging
import os

import pytest
from dotenv import load_dotenv

from neontology import GraphConnection, init_neontology
from neontology.graphengines import MemgraphConfig, Neo4jConfig

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
    ],
)
def get_graph_config(request, tmp_path_factory) -> tuple:
    load_dotenv()

    graph_engines = {
        "NEO4J": Neo4jConfig,
        "MEMGRAPH": MemgraphConfig,
    }

    graph_config_vars = request.param["graph_config_vars"]

    graph_config = {}

    # build config using environment variables
    for key, value in graph_config_vars.items():
        if value == "TMP FILE":
            file_path = tmp_path_factory.mktemp("graph_db") / f"{key}.pytest"
            graph_config[key] = file_path

            logger.info(f"Graph DB at {file_path}")

        else:
            graph_config[key] = os.getenv(value)
            assert graph_config[key] is not None

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

    assert (
        node_count == 0
    ), f"Looks like there are {node_count} nodes in the database, it should be empty."

    yield gc


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests which use the graph with the 'uses_graph' markers"""
    for item in items:
        if "use_graph" in item.fixturenames:
            item.add_marker("uses_graph")


@pytest.fixture(scope="function")
def use_graph(graph_db):
    yield graph_db

    # at the end of every individual test function, we want to empty the database

    cypher = """
    MATCH (n) DETACH DELETE n;
    """

    try:
        graph_db.evaluate_query_single(cypher)
    except RuntimeError:
        pass

    # not all engines will implement constraints, so we don't always have to reset them

    reset_constraints()
