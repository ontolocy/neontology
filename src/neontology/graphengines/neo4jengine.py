import itertools
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar

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
from .graphengine import GraphEngineBase, GraphEngineConfig

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship, RelationshipTypeData

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

    def apply_constraint(self, label: str, property: str) -> None:
        """Apply a constraint to ensure that a property is unique for a label.

        Args:
            label (str): The label to which the constraint applies.
            property (str): The property that must be unique.
        """
        cypher = f"""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (n:{gql_identifier_adapter.validate_strings(label)})
        REQUIRE n.{gql_identifier_adapter.validate_strings(property)} IS UNIQUE
        """

        self.evaluate_query_single(cast(LiteralString, cypher))

    def drop_constraint(self, constraint_name: str) -> None:
        """Drop a constraint by its name.

        Args:
            constraint_name (str): The name of the constraint to drop.
        """
        drop_cypher = f"""
        DROP CONSTRAINT {gql_identifier_adapter.validate_strings(constraint_name)}
        """
        self.evaluate_query_single(cast(LiteralString, drop_cypher))

    def get_constraints(self) -> list:
        """Get the constraints defined in the graph.

        Returns:
            list: A list of constraint names.
        """
        get_constraints_query = """
        SHOW CONSTRAINTS yield name
        RETURN COLLECT(DISTINCT name)
        """

        constraints = self.evaluate_query_single(get_constraints_query)

        if constraints:
            return constraints

        else:
            return []


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
