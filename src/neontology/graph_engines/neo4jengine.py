import os
import itertools
import warnings
from typing import List

from neo4j import Record as Neo4jRecord
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j import Result as Neo4jResult

from ..result import NeontologyResult
from ..graphengine import GraphEngineBase


def neo4j_node_to_neontology_node(neo4j_node, node_classes):
    node_labels = list(neo4j_node.labels)

    primary_labels = set(node_labels).intersection(set(node_classes.keys()))

    secondary_labels = set(node_labels).difference(set(node_classes.keys()))

    if len(primary_labels) == 1:
        primary_label = primary_labels.pop()

        node = node_classes[primary_label](**dict(neo4j_node))

        # warn if the secondary labels aren't what's expected

        if set(node.__secondarylabels__) != secondary_labels:
            warnings.warn(f"Unexpected secondary labels returned: {secondary_labels}")

        return node

    # gracefully handle cases where we don't have a class defined
    # for the identified label or where we get more than one valid primary label
    else:
        warnings.warn(f"Unexpected primary labels returned: {primary_labels}")

        return None


def neo4j_records_to_neontology_records(
    records: List[Neo4jRecord], node_classes: dict, rel_classes: dict
) -> tuple:
    new_records = []

    for record in records:
        new_record = {"nodes": {}, "relationships": {}}
        for key, entry in record.items():
            if isinstance(entry, Neo4jNode):
                neontology_node = neo4j_node_to_neontology_node(entry, node_classes)

                if neontology_node:
                    new_record["nodes"][key] = neontology_node

            elif isinstance(entry, Neo4jRelationship):
                rel_type = entry.type

                rel_dict = rel_classes[rel_type]

                if not rel_dict:
                    warnings.warn(
                        (
                            f"Could not find a class for {rel_type} relationship type."
                            " Did you define the class before initializing Neontology?"
                        )
                    )
                    continue

                if not entry.nodes[0].labels or not entry.nodes[1].labels:
                    warnings.warn(
                        (
                            f"{rel_type} relationship type query did not include nodes."
                            " To get neontology relationships, return source and target "
                            "nodes as part of result."
                        )
                    )
                    continue

                src_label = list(entry.nodes[0].labels)[0]
                tgt_label = list(entry.nodes[1].labels)[0]

                src_node = node_classes[src_label](**dict(entry.nodes[0]))
                tgt_node = node_classes[tgt_label](**dict(entry.nodes[1]))

                rel_props = dict(entry)
                rel_props["source"] = src_node
                rel_props["target"] = tgt_node

                rel = rel_dict["rel_class"](**rel_props)

                new_record["relationships"][key] = rel

        new_records.append(new_record)

    nodes_list_of_lists = [x["nodes"].values() for x in new_records]

    nodes = list(itertools.chain.from_iterable(nodes_list_of_lists))

    nodes_list_of_lists = [x["relationships"].values() for x in new_records]
    rels = list(itertools.chain.from_iterable(nodes_list_of_lists))

    return new_records, nodes, rels


class Neo4jEngine(GraphEngineBase):
    def __init__(self, config: dict) -> None:
        """Initialise connection to the engine

        Args:
            config (Optional[dict]): _description_
        """

        # try to load environment variables from .env file
        load_dotenv()

        neo4j_uri = config.get("neo4j_uri", os.getenv("NEO4J_URI"))

        neo4j_username = config.get("neo4j_username", os.getenv("NEO4J_USERNAME"))

        neo4j_password = config.get("neo4j_password", os.getenv("NEO4J_PASSWORD"))

        self.driver = GraphDatabase.driver(  # type: ignore
            neo4j_uri,
            auth=(neo4j_username, neo4j_password),
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
        self, cypher, params={}, node_classes={}, relationship_classes={}
    ):
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records, nodes, rels = neo4j_records_to_neontology_records(
            neo4j_records, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=neo4j_records,
            records=neontology_records,
            nodes=nodes,
            relationships=rels,
        )

    def evaluate_query_single(self, cypher, params={}):
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
        FOR (n:{label})
        REQUIRE n.{property} IS UNIQUE
        """

        self.evaluate_query_single(cypher)

    def drop_constraint(self, constraint_name: str) -> None:
        drop_cypher = f"""
        DROP CONSTRAINT {constraint_name}
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
