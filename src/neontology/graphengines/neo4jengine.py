from typing import Any, ClassVar, Optional, Sequence, Union

from typing_extensions import LiteralString, cast

from ..gql import gql_identifier_adapter
from .bolt import BoltEngine
from .capabilities import Capability
from .dbschema import Constraint, ConstraintType, Index, SchemaEntity, normalise_properties
from .graphengine import GraphEngineBase, GraphEngineConfig

# neo4j's own names for what SHOW CONSTRAINTS reports, mapped onto neontology's
# vocabulary. Anything absent is reported as ConstraintType.OTHER rather than being
# hidden, so a database can still be described and cleaned up faithfully.
NEO4J_CONSTRAINT_TYPES = {
    "NODE_PROPERTY_UNIQUENESS": ConstraintType.UNIQUENESS,
    "RELATIONSHIP_PROPERTY_UNIQUENESS": ConstraintType.UNIQUENESS,
    "NODE_PROPERTY_EXISTENCE": ConstraintType.EXISTENCE,
    "RELATIONSHIP_PROPERTY_EXISTENCE": ConstraintType.EXISTENCE,
    "NODE_KEY": ConstraintType.NODE_KEY,
    "RELATIONSHIP_KEY": ConstraintType.NODE_KEY,
}


class Neo4jEngine(BoltEngine):
    """Graph engine for a Neo4j database.

    Connecting, querying and reading results back are shared with Memgraph through
    BoltEngine. Schema management differs, and lives here.
    """

    @staticmethod
    def _match_name(existing: Sequence[Any], wanted: Any) -> Optional[str]:
        """Find the database's name for a schema object described by label and properties.

        Neo4j identifies constraints and indexes by name, but a caller may build one by
        hand to drop, which carries no name. Matching on the pattern lets that work.

        Args:
            existing (Sequence[Any]): what the database reports.
            wanted (Any): the constraint or index to find.

        Returns:
            Optional[str]: the name, or None if nothing matches.
        """
        for candidate in existing:
            if candidate.label == wanted.label and candidate.properties == wanted.properties:
                return candidate.name

        return None

    def _property_pattern(self, properties: tuple[str, ...]) -> str:
        """Render properties as the `(n.a, n.b)` pattern Neo4j expects.

        Args:
            properties (tuple[str, ...]): the property names.

        Returns:
            str: the rendered pattern.
        """
        rendered = ", ".join(f"n.{gql_identifier_adapter.validate_strings(x)}" for x in properties)

        return f"({rendered})"

    def apply_uniqueness_constraint(self, label: str, properties: Union[str, Sequence[str]]) -> None:
        """Require a label/property combination to be unique.

        Args:
            label (str): the node label to constrain.
            properties (Union[str, Sequence[str]]): one property name, or several for a
                composite constraint.
        """
        self._require(Capability.CONSTRAINTS)

        cypher = f"""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (n:{gql_identifier_adapter.validate_strings(label)})
        REQUIRE {self._property_pattern(normalise_properties(properties))} IS UNIQUE
        """

        self.evaluate_query_single(cast(LiteralString, cypher))

    def apply_existence_constraint(self, label: str, properties: Union[str, Sequence[str]]) -> None:
        """Require a property to be present on every node with a label.

        Neo4j only offers this in Enterprise Edition, so on Community this raises the
        database's own error saying so.

        Args:
            label (str): the node label to constrain.
            properties (Union[str, Sequence[str]]): the property that must be present.
        """
        self._require(Capability.CONSTRAINTS)

        for property_name in normalise_properties(properties):
            cypher = f"""
            CREATE CONSTRAINT IF NOT EXISTS
            FOR (n:{gql_identifier_adapter.validate_strings(label)})
            REQUIRE n.{gql_identifier_adapter.validate_strings(property_name)} IS NOT NULL
            """

            self.evaluate_query_single(cast(LiteralString, cypher))

    def get_constraints(self) -> list[Constraint]:
        """Get the constraints defined in the graph.

        Returns:
            list[Constraint]: every constraint the database reports, including any
                neontology did not create.
        """
        self._require(Capability.CONSTRAINTS)

        cypher = """
        SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties
        RETURN name, type, entityType, labelsOrTypes, properties
        """

        result = self.driver.execute_query(cast(LiteralString, cypher))

        constraints = []

        for record in result.records:
            # labelsOrTypes is a list, but a constraint only ever covers one label
            labels = record["labelsOrTypes"] or []

            constraints.append(
                Constraint(
                    name=record["name"],
                    label=labels[0] if labels else "",
                    properties=tuple(record["properties"] or ()),
                    entity=(SchemaEntity.RELATIONSHIP if record["entityType"] == "RELATIONSHIP" else SchemaEntity.NODE),
                    constraint_type=NEO4J_CONSTRAINT_TYPES.get(record["type"], ConstraintType.OTHER),
                )
            )

        return constraints

    def drop_constraint(self, constraint: Constraint) -> None:
        """Drop a constraint.

        Neo4j names constraints, so the name carried on the value read back from
        `get_constraints()` is what identifies it here.

        Args:
            constraint (Constraint): the constraint to drop.

        A constraint built by hand carries no name, so it is resolved against what the
        database reports. Dropping one that does not exist is a no-op, matching Memgraph.
        """
        self._require(Capability.CONSTRAINTS)

        name = constraint.name or self._match_name(self.get_constraints(), constraint)

        if name is None:
            return

        cypher = f"""
        DROP CONSTRAINT {gql_identifier_adapter.validate_strings(name)} IF EXISTS
        """

        self.evaluate_query_single(cast(LiteralString, cypher))

    def apply_index(self, label: str, properties: Union[str, Sequence[str], None] = None) -> None:
        """Index a label/property combination, without requiring uniqueness.

        Args:
            label (str): the node label to index.
            properties (Union[str, Sequence[str], None]): one property name, or several
                for a composite index.

        Raises:
            ValueError: if no properties are given. Neo4j has no per-label index that
                covers no properties.
        """
        self._require(Capability.INDEXES)

        property_names = normalise_properties(properties)

        if not property_names:
            raise ValueError("Neo4j needs at least one property to index.")

        cypher = f"""
        CREATE INDEX IF NOT EXISTS
        FOR (n:{gql_identifier_adapter.validate_strings(label)})
        ON {self._property_pattern(property_names)}
        """

        self.evaluate_query_single(cast(LiteralString, cypher))

    def get_indexes(self) -> list[Index]:
        """Get the indexes defined in the graph.

        Two kinds are filtered out, because neither is a caller's to manage and a
        teardown loop over this list would otherwise try to drop them:

        - indexes owned by a constraint, which go when the constraint does
        - LOOKUP indexes, which neo4j creates and maintains for itself

        Returns:
            list[Index]: the manageable indexes.
        """
        self._require(Capability.INDEXES)

        cypher = """
        SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, owningConstraint
        WHERE owningConstraint IS NULL AND type <> 'LOOKUP'
        RETURN name, entityType, labelsOrTypes, properties
        """

        result = self.driver.execute_query(cast(LiteralString, cypher))

        indexes = []

        for record in result.records:
            labels = record["labelsOrTypes"] or []

            indexes.append(
                Index(
                    name=record["name"],
                    label=labels[0] if labels else "",
                    properties=tuple(record["properties"] or ()),
                    entity=(SchemaEntity.RELATIONSHIP if record["entityType"] == "RELATIONSHIP" else SchemaEntity.NODE),
                )
            )

        return indexes

    def drop_index(self, index: Index) -> None:
        """Drop an index.

        An index built by hand carries no name, so it is resolved against what the
        database reports. Dropping one that does not exist is a no-op, matching Memgraph.

        Args:
            index (Index): the index to drop, as returned by `get_indexes()`.
        """
        self._require(Capability.INDEXES)

        name = index.name or self._match_name(self.get_indexes(), index)

        if name is None:
            return

        cypher = f"""
        DROP INDEX {gql_identifier_adapter.validate_strings(name)} IF EXISTS
        """

        self.evaluate_query_single(cast(LiteralString, cypher))


class Neo4jConfig(GraphEngineConfig):
    """Configuration for a Neo4j graph engine."""

    engine: ClassVar[type[GraphEngineBase]] = Neo4jEngine
    uri: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    env_fields: ClassVar[dict[str, str]] = {
        "uri": "NEO4J_URI",
        "username": "NEO4J_USERNAME",
        "password": "NEO4J_PASSWORD",
    }

    # Properties that guarantee non-None values for type checking
    @property
    def connection_uri(self) -> str:
        """Get the URI, guaranteed to be non-None after validation."""
        if self.uri is None:
            raise ValueError("URI should be set by validator")

        return self.uri

    @property
    def connection_username(self) -> str:
        """Get the username, guaranteed to be non-None after validation."""
        if self.username is None:
            raise ValueError("Username should be set by validator")

        return self.username

    @property
    def connection_password(self) -> str:
        """Get the password, guaranteed to be non-None after validation."""
        if self.password is None:
            raise ValueError("Password should be set by validator")

        return self.password
