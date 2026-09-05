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
        assert graph_config[key] is not None, f"Environment variable {value} is not set."

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
    assert not node_count, f"Looks like there are {node_count} nodes in the database, it should be empty."

    yield gc


def _engine_for_item(item):
    """Return the engine class a parametrised test item will run against."""
    callspec = getattr(item, "callspec", None)

    if callspec is None:
        return None

    param = callspec.params.get("get_graph_config")

    if not param:
        return None

    graph_engines = {
        "NEO4J": Neo4jConfig,
        "MEMGRAPH": MemgraphConfig,
        "NETWORKX": NetworkxConfig,
    }

    config_class = graph_engines.get(param["graph_engine"])

    return config_class.engine if config_class else None


def pytest_collection_modifyitems(config, items):
    """Mark tests which use the graph, and xfail those needing an unsupported capability.

    Capabilities an engine lacks are declared on the engine itself, so tests say what
    they need rather than naming engines. `xfail(strict=True)` rather than skip means a
    capability that starts working fails the build instead of passing unnoticed.
    """
    for item in items:
        if "use_graph" in item.fixturenames:
            item.add_marker("uses_graph")

        marker = item.get_closest_marker("requires_capability")

        if marker is None:
            continue

        engine = _engine_for_item(item)

        if engine is None:
            continue

        missing = [c for c in marker.args if not engine.supports(c)]

        if missing:
            names = ", ".join(c.value for c in missing)
            item.add_marker(
                pytest.mark.xfail(
                    strict=True,
                    reason=f"{engine.__name__} does not support: {names}",
                )
            )


@pytest.fixture(scope="function")
def engine(get_graph_config):
    """The engine class under test, for asking what it supports.

    Use `engine.supports(Capability.X)` in a test body rather than comparing engine
    names, so a divergence is stated by capability and declared in one place.
    """
    return get_graph_config.engine


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

    if "networkx-engine" in request.node.callspec.id:
        # grand cypher doesn't support DETACH DELETE
        graph_db.engine.driver.clear()

    # not all engines will implement constraints, so we don't always have to reset them

    reset_constraints()
