import itertools
import os
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j import Record as Neo4jRecord
from neo4j import Result as Neo4jResult
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Path as Neo4jPath
from neo4j.graph import Relationship as Neo4jRelationship
from neo4j.time import Date as Neo4jDate
from neo4j.time import DateTime as Neo4jDateTime
from neo4j.time import Time as Neo4jTime
from pydantic import model_validator

from ..gql import gql_identifier_adapter
from ..result import NeontologyResult
from .graphengine import GraphEngineBase, GraphEngineConfig

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship, RelationshipTypeData


def convert_neo4j_types(input_dict: dict) -> dict:
    output_dict = dict(input_dict)

    for key in output_dict:
        try:
            new_date = output_dict[key].to_native()

            if isinstance(output_dict[key], (Neo4jDateTime, Neo4jDate, Neo4jTime)):
                output_dict[key] = new_date

        except AttributeError:
            pass

    return output_dict


def neo4j_node_to_neontology_node(
    neo4j_node: Neo4jNode, node_classes: dict
) -> Optional["BaseNode"]:
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
    rel_type = neo4j_rel.type
    rel_type_data = rel_classes[rel_type]

    if not rel_type_data:
        warnings.warn(
            (
                f"Could not find a class for {rel_type} relationship type."
                " Did you define the class before initializing Neontology?"
            )
        )
        return None

    if (
        not neo4j_rel.start_node
        or not neo4j_rel.start_node.labels
        or not neo4j_rel.end_node
        or not neo4j_rel.end_node.labels
    ):
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
    records: List[Neo4jRecord],
    node_classes: dict,
    rel_classes: Dict[str, "RelationshipTypeData"],
) -> tuple:
    new_records = []

    for record in records:
        new_record: Dict[str, dict] = {"nodes": {}, "relationships": {}, "paths": {}}

        for key, entry in record.items():
            if isinstance(entry, Neo4jNode):
                neontology_node = neo4j_node_to_neontology_node(entry, node_classes)

                if neontology_node:
                    new_record["nodes"][key] = neontology_node

            elif isinstance(entry, Neo4jRelationship):
                neontology_rel = neo4j_relationship_to_neontology_rel(
                    entry, node_classes, rel_classes
                )

                if neontology_rel:
                    new_record["relationships"][key] = neontology_rel

            elif isinstance(entry, Neo4jPath):
                entry_path = []
                for step in entry:
                    step_rel = neo4j_relationship_to_neontology_rel(
                        step, node_classes, rel_classes
                    )
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
        """Initialise connection to the engine

        Args:
            config - Takes a Neo4jConfig object.
        """

        self.driver = GraphDatabase.driver(  # type: ignore
            config.uri,
            auth=(config.username, config.password),
        )

    def verify_connection(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except:  # noqa: E722
            return False

    def close_connection(self) -> None:
        try:
            self.driver.close()
        except AttributeError:
            pass

    def evaluate_query(
        self,
        cypher: str,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
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

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        result = self.driver.execute_query(
            cypher, parameters_=params, result_transformer_=Neo4jResult.single
        )

        if result:
            return result.value()

        else:
            return None

    def apply_constraint(self, label: str, property: str) -> None:
        cypher = f"""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (n:{gql_identifier_adapter.validate_strings(label)})
        REQUIRE n.{gql_identifier_adapter.validate_strings(property)} IS UNIQUE
        """

        self.evaluate_query_single(cypher)

    def drop_constraint(self, constraint_name: str) -> None:
        drop_cypher = f"""
        DROP CONSTRAINT {gql_identifier_adapter.validate_strings(constraint_name)}
        """
        self.evaluate_query_single(drop_cypher)

    def get_constraints(self) -> list:
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
    engine: ClassVar[type[Neo4jEngine]] = Neo4jEngine
    uri: str
    username: str
    password: str

    @model_validator(mode="before")
    @classmethod
    def populate_defaults(cls, data: Any) -> Any:
        load_dotenv()

        if data.get("uri") is None:
            data["uri"] = os.getenv("NEO4J_URI")

        if data.get("username") is None:
            data["username"] = os.getenv("NEO4J_USERNAME")

        if data.get("password") is None:
            data["password"] = os.getenv("NEO4J_PASSWORD")

        return data
