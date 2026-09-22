"""Tests for constraint and index management on the graph engines.

Constraints and indexes are backend features: what they are called, how they are
identified and whether they exist at all is decided by the database, not by
neontology. So the interface is on the engine, gated by a capability, and these
tests say what they need rather than naming engines.
"""

from typing import ClassVar, Optional

import pytest
from pydantic import Field

from neontology import BaseRelationship, GraphConnection, get_ontology_schema
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


class DBSchemaTaggedBase(BaseNode):
    """Abstract, so its tagged property applies to the classes inheriting it."""

    __primaryproperty__: ClassVar[str] = "pp"
    pp: str
    email: str = Field(json_schema_extra={"unique": True})


class DBSchemaTaggedNode(DBSchemaTaggedBase):
    __primarylabel__: ClassVar[Optional[str]] = "DBSchemaTaggedNode"
    name: str = Field(json_schema_extra={"index": True})
    code: Optional[str] = Field(default=None, alias="ref_code", json_schema_extra={"unique": True, "index": True})


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


def test_constraint_error_explains_why_the_engine_cannot(use_graph, engine):
    """An engine without constraints must say why, not only that it refuses.

    NetworkX keys nodes by (pp, label) and Ladybug by a table's primary key, so in both
    cases uniqueness of the primary property already holds - the caller should not be
    left thinking their assumption is unenforced. Each engine declares its own reason,
    so the assertion is that the reason travels rather than what it says.
    """
    if engine.supports(Capability.CONSTRAINTS):
        pytest.skip("engine supports constraints")

    with pytest.raises(CapabilityNotSupportedError) as excinfo:
        use_graph.engine.apply_uniqueness_constraint("DBSchemaNode", "pp")

    assert engine.capability_hints[Capability.CONSTRAINTS] in str(excinfo.value)


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


def test_apply_constraints_raises_on_unsupported_engines_even_with_no_nodes(use_graph, engine):
    """The guard fires on the request, not on whether there was anything to do."""
    if engine.supports(Capability.CONSTRAINTS):
        pytest.skip("engine supports constraints")

    with pytest.raises(CapabilityNotSupportedError):
        use_graph.engine.apply_constraints([])


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


# --------------------------------------------------------------------------
# Properties tagged unique or index on a model
# --------------------------------------------------------------------------


def schema_objects(objects):
    return {(o.label, o.properties) for o in objects}


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_apply_constraints_includes_properties_tagged_unique(use_graph):
    """Inherited tags count, and a property is constrained under the key the graph stores it by."""
    applied = use_graph.engine.apply_constraints([DBSchemaTaggedNode])

    expected = {
        ("DBSchemaTaggedNode", ("pp",)),
        ("DBSchemaTaggedNode", ("email",)),
        ("DBSchemaTaggedNode", ("ref_code",)),
    }

    assert schema_objects(applied) == expected
    assert schema_objects(use_graph.engine.get_constraints()) == expected


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_a_property_tagged_unique_is_enforced(use_graph):
    use_graph.initialise_graph()

    DBSchemaTaggedNode(pp="first", email="same@example.com", name="First").create()

    with pytest.raises(Exception):
        DBSchemaTaggedNode(pp="second", email="same@example.com", name="Second").create()


@pytest.mark.requires_capability(Capability.INDEXES)
def test_apply_indexes_includes_properties_tagged_index_and_unique_ones(use_graph, engine):
    """A unique property is indexed too, by its constraint where the database's constraints carry one."""
    applied = use_graph.engine.apply_indexes([DBSchemaTaggedNode])

    if engine.uniqueness_constraints_are_indexed:
        expected = {("DBSchemaTaggedNode", ("name",))}

    else:
        expected = {("DBSchemaTaggedNode", (prop,)) for prop in ("pp", "email", "ref_code", "name")}

    assert schema_objects(applied) == expected
    assert schema_objects(use_graph.engine.get_indexes()) == expected


@pytest.mark.requires_capability(Capability.INDEXES)
def test_apply_indexes_is_idempotent(use_graph):
    use_graph.engine.apply_indexes([DBSchemaTaggedNode])
    applied = use_graph.engine.apply_indexes([DBSchemaTaggedNode])

    assert len(use_graph.engine.get_indexes()) == len(applied)


@pytest.mark.requires_capability(Capability.CONSTRAINTS, Capability.INDEXES)
@pytest.mark.parametrize("indexes_first", [False, True], ids=["constraints-first", "indexes-first"])
def test_tagged_constraints_and_indexes_apply_in_either_order(use_graph, indexes_first):
    """Neo4j refuses a uniqueness constraint where a plain index already covers the property."""
    steps = [use_graph.apply_constraints, use_graph.apply_indexes]

    for step in reversed(steps) if indexes_first else steps:
        step([DBSchemaTaggedNode])

    assert ("DBSchemaTaggedNode", ("ref_code",)) in schema_objects(use_graph.get_constraints())


@pytest.mark.requires_capability(Capability.INDEXES)
def test_apply_indexes_rejects_abstract_node_types(use_graph):
    with pytest.raises(ValueError):
        use_graph.engine.apply_indexes([DBSchemaTaggedBase])


def test_apply_indexes_raises_on_unsupported_engines_even_with_no_nodes(use_graph, engine):
    if engine.supports(Capability.INDEXES):
        pytest.skip("engine supports indexes")

    with pytest.raises(CapabilityNotSupportedError):
        use_graph.apply_indexes([])


def test_a_tagged_model_works_on_every_engine(use_graph):
    """Tags only take effect when applied, so a tagged model can be used on any backend."""
    DBSchemaTaggedNode(pp="tagged", email="tagged@example.com", name="Tagged").merge()

    assert DBSchemaTaggedNode.match("tagged").email == "tagged@example.com"


def test_tagging_a_relationship_property_raises_when_the_class_is_defined():
    with pytest.raises(TypeError, match="DBSchemaTaggedRel.since"):

        class DBSchemaTaggedRel(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "DBSCHEMA_TAGGED_REL"

            source: DBSchemaNode
            target: DBSchemaNode

            since: str = Field(json_schema_extra={"index": True})


# --------------------------------------------------------------------------
# Initialising a graph
# --------------------------------------------------------------------------


def split(applied):
    """The constraints and the indexes among what was applied."""
    constraints = schema_objects(o for o in applied if isinstance(o, Constraint))
    indexes = schema_objects(o for o in applied if isinstance(o, Index))

    return constraints, indexes


@pytest.mark.requires_capability(Capability.CONSTRAINTS, Capability.INDEXES)
def test_initialise_graph_applies_what_the_schema_declares(use_graph, engine):
    """The abstract base is in the schema too, but has no label to apply anything under."""
    constraints, indexes = split(use_graph.initialise_graph(get_ontology_schema(DBSchemaTaggedBase)))

    assert constraints == {("DBSchemaTaggedNode", (prop,)) for prop in ("pp", "email", "ref_code")}

    if engine.uniqueness_constraints_are_indexed:
        assert indexes == {("DBSchemaTaggedNode", ("name",))}

    else:
        assert indexes == {("DBSchemaTaggedNode", (prop,)) for prop in ("pp", "email", "ref_code", "name")}

    assert schema_objects(use_graph.get_constraints()) == constraints
    assert schema_objects(use_graph.get_indexes()) == indexes


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_initialise_graph_defaults_to_every_model_defined(use_graph):
    use_graph.initialise_graph()

    labels = {c.label for c in use_graph.get_constraints()}

    assert {"DBSchemaNode", "DBSchemaOtherNode", "DBSchemaTaggedNode"} <= labels


@pytest.mark.requires_capability(Capability.CONSTRAINTS, Capability.INDEXES)
def test_initialise_graph_is_safe_to_run_again(use_graph):
    schema = get_ontology_schema(models=[DBSchemaTaggedNode])

    use_graph.initialise_graph(schema)
    constraints, indexes = split(use_graph.initialise_graph(schema))

    assert schema_objects(use_graph.get_constraints()) == constraints
    assert schema_objects(use_graph.get_indexes()) == indexes


def test_initialise_graph_applies_only_what_the_backend_supports(use_graph, engine):
    """Where apply_constraints and apply_indexes raise, initialising skips what the backend cannot do."""
    constraints, indexes = split(use_graph.initialise_graph(get_ontology_schema(models=[DBSchemaTaggedNode])))

    assert bool(constraints) == engine.supports(Capability.CONSTRAINTS)
    assert bool(indexes) == engine.supports(Capability.INDEXES)
