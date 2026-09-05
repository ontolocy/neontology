# type: ignore

import logging
import os

import pytest
from dotenv import load_dotenv

from neontology import GraphConnection, init_neontology
from neontology.graphengines import MemgraphConfig, Neo4jConfig
from neontology.graphengines.capabilities import Capability

try:
    from neontology.graphengines import NetworkxConfig

    HAS_GRAND = True

except ImportError:
    # the networkx engine needs the optional [grand] extra - without it the
    # suite must still collect and run against the other engines
    NetworkxConfig = None
    HAS_GRAND = False


# The single source of truth for the engines the suite runs against. To add a
# backend, add one entry here: the parametrisation, the config construction and
# the capability lookup are all derived from it.
#
#   id:         the pytest param id, used to select an engine with -k
#   config:     the GraphEngineConfig subclass
#   env_vars:   config field -> environment variable holding its value
#   available:  False when an optional dependency is missing, so the param skips
ENGINES = [
    {
        "id": "neo4j-engine",
        "config": Neo4jConfig,
        "env_vars": {
            "uri": "TEST_NEO4J_URI",
            "username": "TEST_NEO4J_USERNAME",
            "password": "TEST_NEO4J_PASSWORD",
        },
        "available": True,
        "skip_reason": "",
    },
    {
        "id": "memgraph-engine",
        "config": MemgraphConfig,
        "env_vars": {
            "uri": "TEST_MEMGRAPH_URI",
            "username": "TEST_MEMGRAPH_USER",
            "password": "TEST_MEMGRAPH_PASSWORD",
        },
        "available": True,
        "skip_reason": "",
    },
    {
        "id": "networkx-engine",
        "config": NetworkxConfig,
        "env_vars": {},
        "available": HAS_GRAND,
        "skip_reason": "needs the [grand] extra",
    },
]

ENGINES_BY_ID = {entry["id"]: entry for entry in ENGINES}

ENGINE_PARAMS = [
    pytest.param(
        entry["id"],
        id=entry["id"],
        marks=([] if entry["available"] else [pytest.mark.skipif(True, reason=entry["skip_reason"])]),
    )
    for entry in ENGINES
]


logger = logging.getLogger(__name__)


def reset_constraints():
    gc = GraphConnection()

    try:
        constraints = gc.engine.get_constraints()

    except NotImplementedError:
        return

    for constraint_name in constraints:
        gc.engine.drop_constraint(constraint_name)


@pytest.fixture(scope="session", params=ENGINE_PARAMS)
def engine_id(request) -> str:
    """The id of the engine under test.

    This is the parametrisation point. Fixtures needing only the engine class derive
    from it directly, so they do not require database credentials to be configured.
    """
    return request.param


@pytest.fixture(scope="session")
def get_graph_config(engine_id) -> object:
    """Build the config for the engine under test, from the ENGINES table."""
    load_dotenv()

    entry = ENGINES_BY_ID[engine_id]

    graph_config = {}

    for field, env_var in entry["env_vars"].items():
        value = os.getenv(env_var)

        assert value is not None, f"Environment variable {env_var} is not set."

        graph_config[field] = value

    return entry["config"](**graph_config)


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

    engine_id = callspec.params.get("engine_id")

    entry = ENGINES_BY_ID.get(engine_id)

    if entry is None or entry["config"] is None:
        return None

    return entry["config"].engine


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
def engine(engine_id):
    """The engine class under test, for asking what it supports.

    Use `engine.supports(Capability.X)` in a test body rather than comparing engine
    names, so a divergence is stated by capability and declared in one place.

    Derived from engine_id rather than the config, so tests that only ask what an
    engine supports do not require database credentials.
    """
    return ENGINES_BY_ID[engine_id]["config"].engine


@pytest.fixture(scope="function")
def use_graph(request, graph_db):
    """Fixture to use the graph database in tests."""
    yield graph_db

    # at the end of every individual test function, we want to empty the database

    cypher = """
    MATCH (n) DETACH DELETE n;
    """

    if graph_db.engine.supports(Capability.GRAPH_MUTATIONS):
        try:
            graph_db.evaluate_query_single(cypher)
        except RuntimeError:
            pass

    else:
        # without GRAPH_MUTATIONS there is no DETACH DELETE, so empty the
        # underlying graph directly
        graph_db.engine.driver.clear()

    # not all engines will implement constraints, so we don't always have to reset them

    reset_constraints()
