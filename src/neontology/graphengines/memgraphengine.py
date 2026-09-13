from typing import ClassVar, Optional, Sequence, Union

from typing_extensions import LiteralString, cast

from ..gql import gql_identifier_adapter
from .bolt import BoltEngine
from .capabilities import Capability
from .dbschema import Constraint, ConstraintType, Index, normalise_properties
from .graphengine import GraphEngineConfig

# memgraph's own names for what SHOW CONSTRAINT INFO reports, mapped onto
# neontology's vocabulary. Memgraph also has a "data_type" constraint, which has no
# neontology equivalent and so reports as OTHER.
MEMGRAPH_CONSTRAINT_TYPES = {
    "unique": ConstraintType.UNIQUENESS,
    "exists": ConstraintType.EXISTENCE,
}


class MemgraphEngine(BoltEngine):
    """Graph engine for a Memgraph database.

    Memgraph is reached through the neo4j driver, as Neo4j is, so connecting, querying and
    reading results back are shared with it through BoltEngine. Schema management differs,
    and lives here.
    """

    def _run_autocommit(self, cypher: str) -> list[dict]:
        """Run a query in an implicit transaction and return its records.

        Memgraph refuses constraint manipulation, index manipulation and storage
        information queries inside the explicit transaction that `driver.execute_query`
        opens - it raises "not allowed in multicommand transactions". `session.run`
        without an explicit transaction is the autocommit path those queries need, so
        every schema operation below goes through here rather than through
        `evaluate_query_single`.

        Args:
            cypher (str): the query to run.

        Returns:
            list[dict]: the records returned, as plain dictionaries.
        """
        with self.driver.session() as session:
            return [dict(record) for record in session.run(cast(LiteralString, cypher))]

    def _label_pattern(self, label: str, properties: tuple[str, ...]) -> str:
        """Render the `(n:Label) ASSERT n.a, n.b` fragment Memgraph constraints use.

        Args:
            label (str): the node label.
            properties (tuple[str, ...]): the property names.

        Returns:
            str: the rendered fragment.
        """
        rendered = ", ".join(f"n.{gql_identifier_adapter.validate_strings(x)}" for x in properties)

        return f"(n:{gql_identifier_adapter.validate_strings(label)}) ASSERT {rendered}"

    def apply_uniqueness_constraint(self, label: str, properties: Union[str, Sequence[str]]) -> None:
        """Require a label/property combination to be unique.

        Memgraph creates constraints idempotently, so no guard is needed.

        Args:
            label (str): the node label to constrain.
            properties (Union[str, Sequence[str]]): one property name, or several for a
                composite constraint.
        """
        self._require(Capability.CONSTRAINTS)

        pattern = self._label_pattern(label, normalise_properties(properties))

        self._run_autocommit(f"CREATE CONSTRAINT ON {pattern} IS UNIQUE")

    def apply_existence_constraint(self, label: str, properties: Union[str, Sequence[str]]) -> None:
        """Require a property to be present on every node with a label.

        Args:
            label (str): the node label to constrain.
            properties (Union[str, Sequence[str]]): the properties that must be present.
        """
        self._require(Capability.CONSTRAINTS)

        for property_name in normalise_properties(properties):
            label_id = gql_identifier_adapter.validate_strings(label)
            property_id = gql_identifier_adapter.validate_strings(property_name)

            self._run_autocommit(f"CREATE CONSTRAINT ON (n:{label_id}) ASSERT EXISTS (n.{property_id})")

    def get_constraints(self) -> list[Constraint]:
        """Get the constraints defined in the graph.

        Returns:
            list[Constraint]: every constraint the database reports.
        """
        self._require(Capability.CONSTRAINTS)

        constraints = []

        for record in self._run_autocommit("SHOW CONSTRAINT INFO"):
            constraint_type = MEMGRAPH_CONSTRAINT_TYPES.get(record.get("constraint type"), ConstraintType.OTHER)

            constraints.append(
                Constraint(
                    label=record["label"],
                    # memgraph reports a bare string for `exists` constraints but a list
                    # for `unique` ones, so this cannot just be tuple()d
                    properties=normalise_properties(record.get("properties")),
                    constraint_type=constraint_type,
                )
            )

        return constraints

    def drop_constraint(self, constraint: Constraint) -> None:
        """Drop a constraint.

        Memgraph does not name constraints, so they are identified by their pattern.
        Dropping one that does not exist is a no-op.

        Args:
            constraint (Constraint): the constraint to drop.

        Raises:
            ValueError: if asked to drop a constraint type neontology cannot express.
        """
        self._require(Capability.CONSTRAINTS)

        pattern = self._label_pattern(constraint.label, constraint.properties)

        if constraint.constraint_type == ConstraintType.UNIQUENESS:
            self._run_autocommit(f"DROP CONSTRAINT ON {pattern} IS UNIQUE")

        elif constraint.constraint_type == ConstraintType.EXISTENCE:
            label_id = gql_identifier_adapter.validate_strings(constraint.label)

            for property_name in constraint.properties:
                property_id = gql_identifier_adapter.validate_strings(property_name)

                self._run_autocommit(f"DROP CONSTRAINT ON (n:{label_id}) ASSERT EXISTS (n.{property_id})")

        else:
            raise ValueError(f"Cannot drop a {constraint.constraint_type.value} constraint on Memgraph.")

    def apply_index(self, label: str, properties: Union[str, Sequence[str], None] = None) -> None:
        """Index a label, optionally narrowed to a property.

        Memgraph indexes either a label on its own or a single label/property pair, so
        a composite index is not expressible.

        Args:
            label (str): the node label to index.
            properties (Union[str, Sequence[str], None]): a single property name, or
                none for a label-only index.

        Raises:
            ValueError: if more than one property is given.
        """
        self._require(Capability.INDEXES)

        self._run_autocommit(f"CREATE INDEX ON {self._index_pattern(label, normalise_properties(properties))}")

    def get_indexes(self) -> list[Index]:
        """Get the indexes defined in the graph.

        Memgraph keeps constraint enforcement separate from indexes, so unlike neo4j
        nothing has to be filtered out here.

        Returns:
            list[Index]: the indexes defined.
        """
        self._require(Capability.INDEXES)

        return [
            Index(
                label=record["label"],
                properties=normalise_properties(record.get("property")),
            )
            for record in self._run_autocommit("SHOW INDEX INFO")
        ]

    def drop_index(self, index: Index) -> None:
        """Drop an index.

        Memgraph does not name indexes, so they are identified by their pattern.
        Dropping one that does not exist is a no-op.

        Args:
            index (Index): the index to drop.
        """
        self._require(Capability.INDEXES)

        self._run_autocommit(f"DROP INDEX ON {self._index_pattern(index.label, index.properties)}")

    @staticmethod
    def _index_pattern(label: str, properties: tuple[str, ...]) -> str:
        """Render the `:Label(prop)` fragment Memgraph indexes use.

        Args:
            label (str): the node label.
            properties (tuple[str, ...]): zero or one property name.

        Returns:
            str: the rendered fragment.

        Raises:
            ValueError: if more than one property is given - Memgraph has no composite
                index.
        """
        label_id = gql_identifier_adapter.validate_strings(label)

        if not properties:
            return f":{label_id}"

        if len(properties) > 1:
            raise ValueError("Memgraph indexes a single property - it has no composite index.")

        return f":{label_id}({gql_identifier_adapter.validate_strings(properties[0])})"


class MemgraphConfig(GraphEngineConfig):
    """Configuration for a Neo4j graph engine."""

    engine: ClassVar[type[MemgraphEngine]] = MemgraphEngine
    uri: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    env_fields: ClassVar[dict[str, str]] = {
        "uri": "MEMGRAPH_URI",
        "username": "MEMGRAPH_USERNAME",
        "password": "MEMGRAPH_PASSWORD",
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
