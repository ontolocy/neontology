import itertools
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Sequence, TypeVar, Union

from neo4j import GraphDatabase
from neo4j import Record as Neo4jRecord
from neo4j import Result as Neo4jResult
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Path as Neo4jPath
from neo4j.graph import Relationship as Neo4jRelationship
from neo4j.time import Date as Neo4jDate
from neo4j.time import DateTime as Neo4jDateTime
from neo4j.time import Time as Neo4jTime
from typing_extensions import LiteralString, cast

from ..gql import gql_identifier_adapter
from ..result import NeontologyResult
from .capabilities import Capability
from .dbschema import Constraint, ConstraintType, Index, SchemaEntity, normalise_properties
from .graphengine import GraphEngineBase, GraphEngineConfig

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship, RelationshipTypeData

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

BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")


def convert_neo4j_types(input_dict: dict) -> dict:
    """Convert Neo4j types in a dictionary to their native Python equivalents.

    Specifically, this function converts Neo4j DateTime, Date, and Time
    objects to their native Python types.

    Args:
        input_dict (dict): Dictionary containing Neo4j types.

    Returns:
        dict: Dictionary with Neo4j types converted to native Python types.
    """
    output_dict = dict(input_dict)

    for key in output_dict:
        try:
            new_date = output_dict[key].to_native()

            if isinstance(output_dict[key], (Neo4jDateTime, Neo4jDate, Neo4jTime)):
                output_dict[key] = new_date

        except AttributeError:
            pass

    return output_dict


def neo4j_node_to_neontology_node(neo4j_node: Neo4jNode, node_classes: dict[str, type[BaseNodeT]]) -> Optional[BaseNodeT]:
    """Convert a native Neo4j node to a Neontology node.

    Args:
        neo4j_node (Neo4jNode): The Neo4j node to convert.
        node_classes (dict): Mapping of labels to node classes used for populating with results.

    Returns:
        Optional[BaseNode]: The converted Neontology node, or None if conversion fails.
    """
    node_labels = list(neo4j_node.labels)

    primary_labels = set(node_labels).intersection(set(node_classes.keys()))

    secondary_labels = set(node_labels).difference(set(node_classes.keys()))

    if len(primary_labels) == 1:
        primary_label = primary_labels.pop()

        node_dict = convert_neo4j_types(dict(neo4j_node))

        node = node_classes[primary_label](**node_dict)

        # warn if the secondary labels aren't what's expected

        if set(node.__secondarylabels__) != secondary_labels:
            warnings.warn(f"Unexpected secondary labels returned: {secondary_labels}")

        return node

    # gracefully handle cases where we don't have a class defined
    # for the identified label or where we get more than one valid primary label
    else:
        warnings.warn(f"Unexpected primary labels returned: {primary_labels}")

        return None


def neo4j_relationship_to_neontology_rel(
    neo4j_rel: Neo4jRelationship, node_classes: dict, rel_classes: dict
) -> Optional["BaseRelationship"]:
    """Convert a native Neo4j relationship to a Neontology relationship.

    Args:
        neo4j_rel (Neo4jRelationship): The Neo4j relationship to convert.
        node_classes (dict): Mapping of labels to node classes used for populating with results.
        rel_classes (dict[str, RelationshipTypeData]): Mapping of relationship types to classes for populating with results.

    Returns:
        Optional[BaseRelationship]: The converted Neontology relationship, or None if conversion fails.
    """
    rel_type = neo4j_rel.type
    rel_type_data = rel_classes[rel_type]

    if not rel_type_data:
        warnings.warn(
            (
                f"Could not find a class for {rel_type} relationship type."
                " Did you define the class before initializing Neontology?"
                " Are source and target node classes valid and resolved?"
            )
        )
        return None

    if not neo4j_rel.start_node or not neo4j_rel.start_node.labels or not neo4j_rel.end_node or not neo4j_rel.end_node.labels:
        warnings.warn(
            (
                f"{rel_type} relationship type query did not include nodes."
                " To get neontology relationships, return source and target "
                "nodes as part of result."
            )
        )
        return None

    src_node = neo4j_node_to_neontology_node(neo4j_rel.start_node, node_classes)
    tgt_node = neo4j_node_to_neontology_node(neo4j_rel.end_node, node_classes)

    rel_props = convert_neo4j_types(dict(neo4j_rel))
    rel_props["source"] = src_node
    rel_props["target"] = tgt_node

    return rel_type_data.relationship_class(**rel_props)


def neo4j_records_to_neontology_records(
    records: list[Neo4jRecord],
    node_classes: dict,
    rel_classes: dict[str, "RelationshipTypeData"],
) -> tuple:
    """Convert native Neo4j records to Neontology records.

    Args:
        records (list[Neo4jRecord]): List of Neo4j records to convert.
        node_classes (dict): Mapping of labels to node classes used for populating with results.
        rel_classes (dict[str, RelationshipTypeData]): Mapping of relationship types to classes for populating with results.

    Returns:
        tuple: A tuple containing:
            - new_records (list): List of converted records in Neontology format.
            - unique_nodes (list): List of unique nodes.
            - rels (list): List of relationships.
            - paths (list): List of paths.
    """
    new_records = []

    for record in records:
        new_record: dict[str, dict] = {"nodes": {}, "relationships": {}, "paths": {}}

        for key, entry in record.items():
            if isinstance(entry, Neo4jNode):
                neontology_node = neo4j_node_to_neontology_node(entry, node_classes)

                if neontology_node:
                    new_record["nodes"][key] = neontology_node

            elif isinstance(entry, Neo4jRelationship):
                neontology_rel = neo4j_relationship_to_neontology_rel(entry, node_classes, rel_classes)

                if neontology_rel:
                    new_record["relationships"][key] = neontology_rel

            elif isinstance(entry, Neo4jPath):
                entry_path = []
                for step in entry:
                    step_rel = neo4j_relationship_to_neontology_rel(step, node_classes, rel_classes)
                    entry_path.append(step_rel)
                if entry_path:
                    new_record["paths"][key] = entry_path

        new_records.append(new_record)

    nodes_list_of_lists = [x["nodes"].values() for x in new_records]

    nodes = list(itertools.chain.from_iterable(nodes_list_of_lists))

    nodes_map = {f"{x.__primarylabel__}:{x.get_pp()}": x for x in nodes}

    unique_nodes = list(nodes_map.values())

    rels_list_of_lists = [x["relationships"].values() for x in new_records]
    rels = list(itertools.chain.from_iterable(rels_list_of_lists))

    paths_list_of_lists = [x["paths"].values() for x in new_records]
    paths = list(itertools.chain.from_iterable(paths_list_of_lists))

    return new_records, unique_nodes, rels, paths


class Neo4jEngine(GraphEngineBase):
    def __init__(self, config: "Neo4jConfig") -> None:
        """Initialise connection to the engine.

        Args:
            config (Neo4jConfig): Takes a Neo4jConfig object.
        """
        self.driver = GraphDatabase.driver(
            config.connection_uri,
            auth=(config.connection_username, config.connection_password),
        )

    def verify_connection(self) -> bool:
        """Verify the connection to the Neo4j database.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            self.driver.verify_connectivity()
            return True
        except:  # noqa: E722
            return False

    def close_connection(self) -> None:
        """Close the connection to the Neo4j database."""
        try:
            self.driver.close()
        except AttributeError:
            pass

    def evaluate_query(
        self,
        cypher: LiteralString,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        """Evaluate a Cypher query and return the results as Neontology records.

        Args:
            cypher (str): query to evaluate.
            params (dict, optional): parameters to pass through. Defaults to {}.
            node_classes (dict, optional): mapping of labels to node classes used for populating with results. Defaults to {}.
            relationship_classes (dict, optional): mapping of relationship types to classes used for populating with results.
                Defaults to {}.

        Returns:
            NeontologyResult: Result object containing the records, nodes, relationships, and paths.
        """
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records, nodes, rels, paths = neo4j_records_to_neontology_records(
            neo4j_records, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=neo4j_records,
            records=neontology_records,
            nodes=nodes,
            relationships=rels,
            paths=paths,
        )

    def evaluate_query_single(self, cypher: LiteralString, params: dict = {}) -> Optional[Any]:
        """Evaluate a Cypher query which returns a single result.

        Args:
            cypher (str): query to evaluate.
            params (dict, optional): parameters to pass through. Defaults to {}.

        Returns:
            Optional[Any]: Query result, or None if no result is found.
        """
        result = self.driver.execute_query(cypher, parameters_=params, result_transformer_=Neo4jResult.single)

        if result:
            return result.value()

        else:
            return None

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
