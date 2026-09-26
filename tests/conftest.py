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

try:
    from neontology.graphengines import LadybugConfig

    HAS_LADYBUG = True

except ImportError:
    LadybugConfig = None
    HAS_LADYBUG = False


# The single source of truth for the engines the suite runs against. To add a
# backend, add one entry here: the parametrisation, the config construction and
# the capability lookup are all derived from it.
#
#   id:         the pytest param id, used to select an engine with -k
#   config:     the GraphEngineConfig subclass
#   env_vars:   config field -> environment variable holding its value
#   kwargs:     config arguments that are the same on every run
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
    {
        "id": "ladybug-engine",
        "config": LadybugConfig,
        "env_vars": {},
        # Ladybug is schema first, and most of the suite defines its models inside the
        # test function - after the fixture has run, so nothing could have declared their
        # tables. auto_create declares each table as it is first written, which is what
        # lets the shared suite run here. The schema first default is covered directly,
        # in tests/test_ladybug.py.
        "kwargs": {"auto_create": True},
        "available": HAS_LADYBUG,
        "skip_reason": "needs the [ladybug] extra",
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


def reset_schema():
    """Drop every constraint and index, so each test starts from a bare schema.

    Capability-guarded rather than wrapped in try/except: an engine that starts
    supporting these should be reset, not silently skipped.
    """
    gc = GraphConnection()

    if gc.supports(Capability.CONSTRAINTS):
        for constraint in gc.get_constraints():
            gc.drop_constraint(constraint)

    if gc.supports(Capability.INDEXES):
        # get_indexes() excludes constraint-backed and database-owned indexes, so this
        # cannot drop neo4j's own token LOOKUP indexes
        for index in gc.get_indexes():
            gc.drop_index(index)


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

    graph_config = dict(entry.get("kwargs", {}))

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

    # init_neontology connects, replacing whatever the previous engine parameter left
    # behind - so this is all that is needed to move the suite onto the next engine.
    # It used to need a change_engine call as well, because init alone could not
    # re-initialise.
    init_neontology(get_graph_config)

    gc = GraphConnection()

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
        # keyed on the fixture that needs database credentials rather than on use_graph,
        # so a test connecting by itself is deselected by -m "not uses_graph" too.
        # fixturenames includes the fixtures a fixture requests, so use_graph tests count
        if "get_graph_config" in item.fixturenames:
            item.add_marker("uses_graph")

        # every marker, not the closest: a test can be marked at the class, the function
        # and the parameter, and one of those quietly shadowing the others would leave a
        # test failing on an engine it was marked for
        needed = [capability for marker in item.iter_markers("requires_capability") for capability in marker.args]

        if not needed:
            continue

        engine = _engine_for_item(item)

        if engine is None:
            continue

        missing = [c for c in dict.fromkeys(needed) if not engine.supports(c)]

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

    reset_schema()
