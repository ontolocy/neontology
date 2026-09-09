"""Tests for constraint and index management on the graph engines.

Constraints and indexes are backend features: what they are called, how they are
identified and whether they exist at all is decided by the database, not by
neontology. So the interface is on the engine, gated by a capability, and these
tests say what they need rather than naming engines.
"""

from typing import ClassVar, Optional

import pytest

from neontology import GraphConnection
from neontology.basenode import BaseNode
from neontology.graphengines.capabilities import Capability, CapabilityNotSupportedError
from neontology.graphengines.dbschema import Constraint, ConstraintType, Index


class DBSchemaNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "DBSchemaNode"
    pp: str


class DBSchemaOtherNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "identifier"
    __primarylabel__: ClassVar[Optional[str]] = "DBSchemaOtherNode"
    identifier: str


# --------------------------------------------------------------------------
# Unsupported engines
# --------------------------------------------------------------------------


def test_unsupported_constraints_raise_a_clear_error(use_graph, engine):
    """An engine without constraints must say so, naming itself and the capability."""
    if engine.supports(Capability.CONSTRAINTS):
        pytest.skip("engine supports constraints")

    with pytest.raises(CapabilityNotSupportedError) as excinfo:
        use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    message = str(excinfo.value)

    assert engine.__name__ in message
    assert Capability.CONSTRAINTS.value in message


def test_unsupported_indexes_raise_a_clear_error(use_graph, engine):
    """An engine without indexes must say so, naming itself and the capability."""
    if engine.supports(Capability.INDEXES):
        pytest.skip("engine supports indexes")

    with pytest.raises(CapabilityNotSupportedError) as excinfo:
        use_graph.engine.apply_index("DBSchemaNode", "pp")

    assert engine.__name__ in str(excinfo.value)


def test_capability_error_is_a_not_implemented_error():
    """Existing `except NotImplementedError` callers must keep working."""
    assert issubclass(CapabilityNotSupportedError, NotImplementedError)


def test_networkx_constraint_error_explains_the_structural_guarantee(use_graph, engine):
    """NetworkX keys nodes by (pp, label), so the invariant already holds.

    The error should say so rather than leaving the caller thinking their uniqueness
    assumption is unenforced.
    """
    if engine.supports(Capability.CONSTRAINTS):
        pytest.skip("engine supports constraints")

    with pytest.raises(CapabilityNotSupportedError) as excinfo:
        use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    assert "structural" in str(excinfo.value).lower()


# --------------------------------------------------------------------------
# Constraints
# --------------------------------------------------------------------------


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_apply_uniqueness_constraint_is_listed(use_graph):
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    constraints = use_graph.engine.get_constraints()

    assert len(constraints) == 1

    constraint = constraints[0]

    assert isinstance(constraint, Constraint)
    assert constraint.constraint_type == ConstraintType.UNIQUENESS
    assert constraint.label == "DBSchemaNode"
    assert constraint.properties == ("pp",)


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_apply_uniqueness_constraint_is_idempotent(use_graph):
    """Applying twice must not raise and must not double up."""
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    assert len(use_graph.engine.get_constraints()) == 1


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_composite_uniqueness_constraint(use_graph):
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", ["pp", "other"])

    constraints = use_graph.engine.get_constraints()

    assert len(constraints) == 1
    assert constraints[0].properties == ("pp", "other")


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_drop_constraint_round_trips(use_graph):
    """What get_constraints() returns must be droppable.

    Neo4j identifies a constraint by name, Memgraph by pattern - so the value read
    back has to carry enough to drop it on either.
    """
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    for constraint in use_graph.engine.get_constraints():
        use_graph.engine.drop_constraint(constraint)

    assert use_graph.engine.get_constraints() == []


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_apply_constraints_for_node_types(use_graph):
    """Node classes get a uniqueness constraint on primary label + primary property."""
    use_graph.engine.apply_constraints([DBSchemaNode, DBSchemaOtherNode])

    by_label = {c.label: c for c in use_graph.engine.get_constraints()}

    assert by_label["DBSchemaNode"].properties == ("pp",)
    assert by_label["DBSchemaOtherNode"].properties == ("identifier",)
    assert by_label["DBSchemaNode"].constraint_type == ConstraintType.UNIQUENESS


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_apply_constraints_rejects_abstract_node_types(use_graph):
    """A node type with no primary label cannot be constrained."""

    class DBSchemaAbstractNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    with pytest.raises(ValueError):
        use_graph.engine.apply_constraints([DBSchemaAbstractNode])


def test_auto_constrain_raises_on_unsupported_engines_even_with_no_nodes(use_graph, engine):
    """The guard fires on the request, not on whether there was anything to do."""
    if engine.supports(Capability.CONSTRAINTS):
        pytest.skip("engine supports constraints")

    with pytest.raises(CapabilityNotSupportedError):
        use_graph.engine.apply_constraints([])


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_auto_constrain_covers_defined_nodes(use_graph):
    """auto_constrain discovers node types rather than being handed them."""
    use_graph.auto_constrain()

    labels = {c.label for c in use_graph.engine.get_constraints()}

    assert "DBSchemaNode" in labels
    assert "DBSchemaOtherNode" in labels


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_uniqueness_constraint_is_enforced(use_graph):
    """The constraint must actually do something."""
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    DBSchemaNode(pp="only-one").create()

    with pytest.raises(Exception):
        DBSchemaNode(pp="only-one").create()


# --------------------------------------------------------------------------
# Indexes
# --------------------------------------------------------------------------


@pytest.mark.requires_capability(Capability.INDEXES)
def test_apply_index_is_listed(use_graph):
    use_graph.engine.apply_index("DBSchemaNode", "pp")

    indexes = use_graph.engine.get_indexes()

    assert len(indexes) == 1

    index = indexes[0]

    assert isinstance(index, Index)
    assert index.label == "DBSchemaNode"
    assert index.properties == ("pp",)


@pytest.mark.requires_capability(Capability.INDEXES)
def test_apply_index_is_idempotent(use_graph):
    use_graph.engine.apply_index("DBSchemaNode", "pp")
    use_graph.engine.apply_index("DBSchemaNode", "pp")

    assert len(use_graph.engine.get_indexes()) == 1


@pytest.mark.requires_capability(Capability.INDEXES)
def test_drop_index_round_trips(use_graph):
    use_graph.engine.apply_index("DBSchemaNode", "pp")

    for index in use_graph.engine.get_indexes():
        use_graph.engine.drop_index(index)

    assert use_graph.engine.get_indexes() == []


@pytest.mark.requires_capability(Capability.INDEXES)
def test_dropping_a_non_existent_index_is_a_no_op(use_graph):
    """Drops are idempotent on both engines, so the interface should be too."""
    use_graph.engine.drop_index(Index(label="DBSchemaNeverIndexed", properties=("pp",)))


@pytest.mark.requires_capability(Capability.INDEXES, Capability.CONSTRAINTS)
def test_get_indexes_excludes_constraint_owned_indexes(use_graph):
    """A uniqueness constraint creates a backing index on Neo4j.

    That index belongs to the constraint and cannot be dropped on its own, so it must
    not be reported as an index a caller could manage.
    """
    use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    assert use_graph.engine.get_indexes() == []


@pytest.mark.requires_capability(Capability.INDEXES)
def test_get_indexes_excludes_database_owned_indexes(use_graph):
    """Neo4j ships token LOOKUP indexes of its own.

    They are not something neontology created or should offer to drop - a teardown
    loop over get_indexes() would otherwise destroy them.
    """
    assert use_graph.engine.get_indexes() == []


@pytest.mark.requires_capability(Capability.INDEXES, Capability.DUPLICATE_CREATE)
def test_index_does_not_enforce_uniqueness(use_graph):
    """The whole point of an index over a constraint."""
    use_graph.engine.apply_index("DBSchemaNode", "pp")

    DBSchemaNode(pp="duplicated").create()
    DBSchemaNode(pp="duplicated").create()

    # counted in the database rather than through match_nodes(), which dedupes
    # equal models and would report one either way
    node_count = use_graph.evaluate_query_single("MATCH (n:DBSchemaNode) RETURN COUNT(n)")

    assert node_count == 2


# --------------------------------------------------------------------------
# GraphConnection surface
# --------------------------------------------------------------------------


def test_graph_connection_reports_capabilities(use_graph, engine):
    """Callers need to be able to ask before they call."""
    assert use_graph.supports(Capability.CONSTRAINTS) == engine.supports(Capability.CONSTRAINTS)
    assert use_graph.supports(Capability.INDEXES) == engine.supports(Capability.INDEXES)


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_graph_connection_forwards_constraint_methods(use_graph):
    gc = GraphConnection()

    gc.apply_constraints([DBSchemaNode])

    assert [c.label for c in gc.get_constraints()] == ["DBSchemaNode"]

    for constraint in gc.get_constraints():
        gc.drop_constraint(constraint)

    assert gc.get_constraints() == []


@pytest.mark.requires_capability(Capability.INDEXES)
def test_graph_connection_forwards_index_methods(use_graph):
    gc = GraphConnection()

    gc.apply_index("DBSchemaNode", "pp")

    assert [i.label for i in gc.get_indexes()] == ["DBSchemaNode"]

    for index in gc.get_indexes():
        gc.drop_index(index)

    assert gc.get_indexes() == []
