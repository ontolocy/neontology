"""The LadybugDB engine's own behaviour.

The shared suite runs against Ladybug with `auto_create=True`, because most of it
defines its models inside the test function - after any fixture could have declared
their tables. What that leaves uncovered is everything specific to an embedded, schema
first backend, which is what this file is for: the default that refuses an undeclared
write, what `initialise_graph()` declares and reports, how a database on disk behaves,
and how a class is matched when a node can only carry one label.
"""

from contextlib import contextmanager
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship, GraphConnection, Table, get_ontology_schema, init_neontology
from neontology.graphengines.dbschema import SchemaEntity

ladybug = pytest.importorskip("ladybug", reason="needs the [ladybug] extra")

from neontology.graphengines.ladybugengine import LadybugConfig, ladybug_type  # noqa: E402


@contextmanager
def ladybug_connection(**kwargs):
    """Connect to a Ladybug database of this test's own, then put the suite's back.

    The connection is a process-wide singleton, and `init_neontology` closes the engine
    it replaces. Clearing the instance first means a new one is built beside the
    suite's rather than over it, so the engine the other tests are using stays open.

    Args:
        **kwargs: passed to LadybugConfig.

    Yields:
        GraphConnection: the connection to the new database.
    """
    previous = GraphConnection._instance

    GraphConnection._instance = None

    try:
        init_neontology(LadybugConfig(**kwargs))

        yield GraphConnection()

    finally:
        established = GraphConnection._instance

        GraphConnection._instance = previous

        if established is not None:
            established.engine.close_connection()


def _node(label, **fields):
    """Define a node class with the given primary label and properties.

    Built rather than declared so that a label can be reused with different properties,
    which is how a model changing under a database that already holds it is tested.

    Args:
        label (str): the primary label.
        **fields: annotations for the properties beyond the primary one. Each defaults
            to None, so a model can gain a property without every earlier node needing
            a value for it.

    Returns:
        type: the node class.
    """
    return type(
        f"Ladybug{label}",
        (BaseNode,),
        {
            "__annotations__": {"name": str, **fields},
            "__primaryproperty__": "name",
            "__primarylabel__": label,
            **dict.fromkeys(fields, None),
        },
    )


class TestSchemaFirstByDefault:
    """Ladybug declares its schema before anything is written, and says so if it has not."""

    def test_a_write_to_an_undeclared_label_explains_itself(self):
        Undeclared = _node("LbugUndeclared")

        with ladybug_connection():
            with pytest.raises(RuntimeError) as excinfo:
                Undeclared(name="x").merge()

        message = str(excinfo.value)

        assert "LbugUndeclared" in message
        assert "initialise_graph" in message
        assert "auto_create" in message

    def test_initialise_graph_declares_the_tables_a_write_needs(self):
        Declared = _node("LbugDeclared", size=Optional[int])

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[Declared]))

            Declared(name="x", size=3).merge()

            assert Declared.match("x").size == 3

    def test_a_property_added_to_a_model_becomes_a_column(self):
        """Re-running initialise_graph must bring a table up to date, not skip it."""
        before = _node("LbugGrowing")

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[before]))

            before(name="x").merge()

            after = _node("LbugGrowing", size=Optional[int])

            gc.initialise_graph(get_ontology_schema(models=[after]))

            after(name="y", size=7).merge()

            assert after.match("y").size == 7
            assert after.match("x").size is None

    def test_a_write_naming_a_column_the_table_lacks_points_at_initialise_graph(self):
        """The model changed but the database was not re-initialised."""
        before = _node("LbugStale")

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[before]))

            after = _node("LbugStale", size=Optional[int])

            with pytest.raises(RuntimeError, match="initialise_graph"):
                after(name="x", size=1).merge()


class TestWhatInitialiseGraphReports:
    """It returns what it declared, the way the other engines return what they applied."""

    def test_it_returns_a_table_per_node_class_and_relationship_type(self):
        Person = _node("LbugPerson", age=Optional[int])

        class Likes(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "LBUG_LIKES"

            source: Person
            target: Person

        with ladybug_connection() as gc:
            applied = gc.initialise_graph(get_ontology_schema(models=[Person, Likes]))

        assert all(isinstance(table, Table) for table in applied)

        by_label = {table.label: table for table in applied}

        assert by_label["LbugPerson"].entity is SchemaEntity.NODE
        assert by_label["LbugPerson"].properties == ("name",)
        assert set(by_label["LbugPerson"].columns) == {"name", "age"}

        assert by_label["LBUG_LIKES"].entity is SchemaEntity.RELATIONSHIP

    def test_a_relationship_can_be_written_once_its_table_is_declared(self):
        Host = _node("LbugHost")

        class Connects(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "LBUG_CONNECTS"

            source: Host
            target: Host

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[Host, Connects]))

            Host(name="a").merge()
            Host(name="b").merge()

            Connects(source=Host(name="a"), target=Host(name="b")).merge()

            result = gc.evaluate_query("MATCH (n:LbugHost)-[r:LBUG_CONNECTS]->(o:LbugHost) RETURN n, r, o")

        assert len(result.relationships) == 1

    def test_running_it_twice_changes_nothing_and_does_not_raise(self):
        Repeat = _node("LbugRepeat")

        with ladybug_connection() as gc:
            schema = get_ontology_schema(models=[Repeat])

            first = gc.initialise_graph(schema)
            second = gc.initialise_graph(schema)

        assert first == second


class TestAutoCreate:
    """Opting out of declaring the schema up front."""

    def test_tables_are_declared_as_they_are_first_written(self):
        Auto = _node("LbugAuto", size=Optional[int])

        class Uses(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "LBUG_USES"

            source: Auto
            target: Auto

        with ladybug_connection(auto_create=True) as gc:
            Auto(name="a", size=1).merge()
            Auto(name="b").merge()

            Uses(source=Auto(name="a"), target=Auto(name="b")).merge()

            result = gc.evaluate_query("MATCH (n:LbugAuto)-[r:LBUG_USES]->(o:LbugAuto) RETURN n, r, o")

            assert len(result.relationships) == 1
            assert Auto.match("a").size == 1
            assert Auto.match("b").size is None


class TestWhereTheDatabaseLives:
    """In memory by default, on disk when asked."""

    def test_the_default_is_in_memory(self):
        assert LadybugConfig().db_path == ":memory:"

    def test_the_path_can_come_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("LADYBUG_PATH", "/tmp/from-the-environment.lbdb")

        assert LadybugConfig().db_path == "/tmp/from-the-environment.lbdb"

    def test_an_explicit_path_wins_over_the_environment(self, monkeypatch):
        monkeypatch.setenv("LADYBUG_PATH", "/tmp/from-the-environment.lbdb")

        assert LadybugConfig(db_path=":memory:").db_path == ":memory:"

    def test_a_database_on_disk_still_holds_its_data_when_reopened(self, tmp_path):
        Stored = _node("LbugStored", size=Optional[int])

        path = str(tmp_path / "graph.lbdb")

        with ladybug_connection(db_path=path) as gc:
            gc.initialise_graph(get_ontology_schema(models=[Stored]))

            Stored(name="x", size=5).merge()

        with ladybug_connection(db_path=path):
            # no initialise_graph this time: the table is already in the database
            assert Stored.match("x").size == 5

    def test_two_in_memory_databases_do_not_share_anything(self):
        Isolated = _node("LbugIsolated")

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[Isolated]))

            Isolated(name="x").merge()

        with ladybug_connection():
            with pytest.raises(RuntimeError, match="initialise_graph"):
                Isolated(name="y").merge()


class TestMatchingAClass:
    """A node carries one label, so a class is matched as itself and its subclasses."""

    def test_a_parent_class_finds_its_subclasses(self):
        Animal = _node("LbugAnimal")

        class Dog(Animal):
            __primarylabel__: ClassVar[Optional[str]] = "LbugDog"

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[Animal, Dog]))

            Animal(name="generic").merge()
            Dog(name="rex").merge()

            matched = Animal.match_nodes()

            assert {node.name for node in matched} == {"generic", "rex"}
            assert {type(node) for node in matched} == {Animal, Dog}

            assert Animal.get_count() == 2
            assert Dog.get_count() == 1

    def test_the_pattern_names_the_class_and_its_subclasses(self):
        Vehicle = _node("LbugVehicle")

        class Car(Vehicle):
            __primarylabel__: ClassVar[Optional[str]] = "LbugCar"

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[Vehicle, Car]))

            pattern = gc.engine.label_pattern("LbugVehicle")

        assert pattern.startswith(":LbugVehicle")
        assert "LbugCar" in pattern

    def test_a_relationship_declared_to_a_parent_class_reaches_a_subclass_node(self):
        """The end is a parent class, the node is a subclass - and lives in its own table.

        Ladybug will not create a relationship whose ends are bound by several labels,
        so the union pattern a read uses is not available for the write.
        """
        Machine = _node("LbugMachine")

        class Server(Machine):
            __primarylabel__: ClassVar[Optional[str]] = "LbugServer"

        class Talks(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "LBUG_TALKS_TO"

            source: Machine
            target: Machine

        with ladybug_connection() as gc:
            gc.initialise_graph(get_ontology_schema(models=[Machine, Server, Talks]))

            Machine(name="a").merge()
            Server(name="b").merge()

            # declared as Machine at both ends, though "b" is a Server
            Talks(source=Machine(name="a"), target=Machine(name="b")).merge()

            # merging again must not make a second relationship
            Talks(source=Machine(name="a"), target=Machine(name="b")).merge()

            result = gc.evaluate_query(
                "MATCH (n:LbugMachine:LbugServer)-[r:LBUG_TALKS_TO]->(o:LbugMachine:LbugServer) RETURN n, r, o"
            )

            assert len(result.relationships) == 1

            found = result.relationships[0]

            assert (found.source.name, type(found.source)) == ("a", Machine)
            assert (found.target.name, type(found.target)) == ("b", Server)

    def test_a_class_with_no_table_says_so_rather_than_matching_nothing(self):
        Missing = _node("LbugMissing")

        with ladybug_connection() as gc:
            with pytest.raises(RuntimeError, match="initialise_graph"):
                gc.engine.label_pattern(Missing.__primarylabel__)


@pytest.mark.parametrize(
    ("json_schema", "expected"),
    [
        ({"type": "string"}, "STRING"),
        ({"type": "integer"}, "INT64"),
        ({"type": "number"}, "DOUBLE"),
        ({"type": "boolean"}, "BOOLEAN"),
        ({"type": "string", "format": "date-time"}, "TIMESTAMP"),
        ({"type": "string", "format": "date"}, "DATE"),
        ({"type": "string", "format": "duration"}, "INTERVAL"),
        ({"type": "string", "format": "uuid"}, "UUID"),
        ({"type": "string", "format": "binary"}, "BLOB"),
        # Ladybug has no TIME type, so a time is stored as its ISO string
        ({"type": "string", "format": "time"}, "STRING"),
        ({"type": "array", "items": {"type": "string"}}, "STRING[]"),
        ({"type": "array", "items": {"type": "integer"}}, "INT64[]"),
        # Optional[int]
        ({"anyOf": [{"type": "integer"}, {"type": "null"}]}, "INT64"),
        # an Enum, which pydantic describes by its values alone
        ({"enum": ["a", "b"]}, "STRING"),
        ({"enum": [1, 2]}, "INT64"),
        # nothing recognisable: the value is written as its string, so the column is one
        ({}, "STRING"),
        ({"type": "object"}, "STRING"),
    ],
)
def test_ladybug_type_maps_a_property_to_a_column_type(json_schema, expected):
    assert ladybug_type(json_schema) == expected
