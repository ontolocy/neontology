from __future__ import annotations

import logging
import os
import warnings
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional

import kuzu
import pandas as pd
from dotenv import load_dotenv
from kuzu import QueryResult
from pydantic import model_validator

from ..gql import gql_identifier_adapter, int_adapter
from ..result import NeontologyResult
from ..schema_utils import extract_type_mapping
from .graphengine import GraphEngineBase, GraphEngineConfig

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import RelationshipTypeData

logger = logging.getLogger(__name__)


def kuzu_node_to_neontology_node(
    kuzu_node: dict, node_classes: dict
) -> Optional["BaseNode"]:
    node_label = kuzu_node["_label"]

    try:
        node_class = node_classes[node_label]

    except KeyError:
        warnings.warn(f"Unexpected primary labels returned: {node_label}")
        return None

    node_dict = {
        k: v for k, v in kuzu_node.items() if k in node_class.model_fields.keys()
    }

    new_node = node_class(**node_dict)

    return new_node


def kuzu_results_to_neontology_records(
    results: QueryResult,
    node_classes: dict,
    rel_classes: Dict[str, "RelationshipTypeData"],
) -> tuple:
    result_df = results.get_as_df()

    result_keys = list(result_df.columns)

    result_records = []

    all_nodes = {}
    all_rels = []

    # first we need to process the nodes to populate the node table mappings
    for result_row in result_df.iterrows():
        record_nodes = {}
        record_rels = {}
        node_table_mappings: dict = defaultdict(dict)

        # first do the nodes and populate the node table mappings
        for result_key in result_keys:
            record_entry_raw = result_row[1][result_key]

            # skip any relationships
            if (
                pd.isna(record_entry_raw)
                or "_src" in record_entry_raw
                or "_dst" in record_entry_raw
            ):
                continue

            record_node = kuzu_node_to_neontology_node(record_entry_raw, node_classes)

            if not record_node:
                continue

            table_id = record_entry_raw["_id"]["table"]
            table_offset = record_entry_raw["_id"]["offset"]

            node_table_mappings[table_id][table_offset] = record_node
            record_nodes[result_key] = record_node
            all_nodes[record_node.get_pp()] = record_node

        # now repeat but just do any relationships
        for result_key in result_keys:
            record_entry_raw = result_row[1][result_key]

            # skip any nodes
            if pd.isna(record_entry_raw) or (
                "_src" not in record_entry_raw and "_dst" not in record_entry_raw
            ):
                continue

            rel_type = record_entry_raw["_label"]

            src_table = record_entry_raw["_src"]["table"]
            src_offset = record_entry_raw["_src"]["offset"]
            src_node = node_table_mappings.get(src_table, {}).get(src_offset)

            tgt_table = record_entry_raw["_dst"]["table"]
            tgt_offset = record_entry_raw["_dst"]["offset"]
            tgt_node = node_table_mappings.get(tgt_table, {}).get(tgt_offset)

            # if we don't have information about the nodes, we can't create the relationship
            if not src_node or not tgt_node:
                warnings.warn(
                    (
                        f"{rel_type} relationship type query did not include nodes."
                        " To get neontology relationships, return source and target "
                        "nodes as part of result."
                    )
                )
                continue

            # steal rel population code from below!
            rel_props = {
                k: v
                for k, v in record_entry_raw.items()
                if k not in ["_src", "_dst", "_label"] and v is not None
            }

            rel_props["source"] = src_node
            rel_props["target"] = tgt_node

            rel_type_dict = rel_classes[rel_type]

            rel = rel_type_dict.relationship_class(**rel_props)

            record_rels[result_key] = rel
            all_rels.append(rel)

        record = {"nodes": record_nodes, "relationships": record_rels}
        result_records.append(record)

    # kuzu engine doesn't yet support paths
    paths = []

    return result_records, list(all_nodes.values()), all_rels, paths


class KuzuEngine(GraphEngineBase):
    _kuzu_type_mappings = {
        "list": "STRING[]",
        "bool": "BOOL",
        "int": "INT64",
        "float": "FLOAT",
        "str": "STRING",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timedelta": "INTERVAL",
        "set": "STRING[]",
        "tuple": "STRING[]",
        "UUID": "STRING",
        "Enum": "STRING",
        "List[bool]": "BOOL[]",
        "List[int]": "INT64[]",
        "List[float]": "FLOAT[]",
        "List[str]": "STRING[]",
        "List[date]": "DATE[]",
        "List[datetime]": "TIMESTAMP[]",
        "List[timedelta]": "INTERVAL[]",
        "List[Enum]": "STRING[]",
    }

    def __init__(self, config: "KuzuConfig") -> None:
        """Initialise connection to the engine

        Graph Config:
        * kuzu_db
        * nodes (dict of node types indexed by label)
        * relationships
            (dict of rel types, indexed by rel_type and including source/target node classes)

        Args:
            config (Optional[dict]): _description_
        """

        self._kuzu_db = kuzu.Database(config.path)

        self.driver = kuzu.Connection(self._kuzu_db)

        nodes = config.nodes
        relationships = config.relationships

        self.initialise_tables(nodes=nodes, relationships=relationships)

    def initialise_tables(
        self,
        nodes: Optional[dict],
        relationships: Optional[Dict[str, RelationshipTypeData]],
    ) -> None:
        if nodes is None:
            from ..utils import get_node_types

            nodes = get_node_types()

        if relationships is None:
            from ..utils import get_rels_by_type

            relationships = get_rels_by_type()

        # Create node tables in the database
        for label, node_type in nodes.items():
            db_creation_field_info = []

            primary_key = node_type.__primaryproperty__

            for field_name, model_field in node_type.model_fields.items():
                type_mapping = extract_type_mapping(
                    model_field.annotation, show_optional=False
                )

                field_representation = type_mapping.representation

                try:
                    kuzu_field_type = self._kuzu_type_mappings[field_representation]

                except KeyError:
                    logger.warn(
                        f"No defined Kuzu type mapping for {field_representation}, using STRING"
                    )

                    if "List" in field_representation:
                        kuzu_field_type = "STRING[]"

                    else:
                        kuzu_field_type = "STRING"

                db_creation_field_info.append(f"{field_name} {kuzu_field_type}")

            creation_string = f"CREATE NODE TABLE {label}({', '.join(db_creation_field_info)}, PRIMARY KEY ({primary_key}))"

            try:
                self.driver.execute(creation_string)
                logger.info("Created Kuzu node table: " + creation_string)

            except RuntimeError:
                logger.debug(
                    "Creation of Kuzu node table failed, table likely already exists: "
                    + creation_string
                )

        for rel_type, rel_type_data in relationships.items():
            db_rel_creation_field_info = []

            rel_class = rel_type_data.relationship_class

            # skip relationships which don't have concrete source / target nodes
            try:
                source_label = rel_type_data.source_class.__primarylabel__
                target_label = rel_type_data.target_class.__primarylabel__

            except AttributeError:
                warnings.warn(
                    f"Kuzu schema will not support {rel_type}. Relationship has a source or target with no primary label."
                )
                continue

            for field_name, model_field in rel_class.model_fields.items():
                if field_name in ["source", "target"]:
                    continue

                field_representation = type_mapping.representation

                try:
                    kuzu_field_type = self._kuzu_type_mappings[field_representation]

                except KeyError:
                    logger.warn(
                        f"No defined Kuzu type mapping for {field_representation}, using STRING"
                    )

                    if "List" in field_representation:
                        kuzu_field_type = "STRING[]"

                    else:
                        kuzu_field_type = "STRING"

                db_rel_creation_field_info.append(f"{field_name} {kuzu_field_type}")

            rel_creation_string = (
                f"CREATE REL TABLE {rel_type}(FROM {source_label}"
                f" TO {target_label}, {', '.join(db_rel_creation_field_info)})"
            )

            try:
                self.driver.execute(rel_creation_string)
                logger.info("Created Kuzu relationship table: " + rel_creation_string)

            except RuntimeError as e:
                logger.debug(
                    "Creation of Kuzu relationship table failed, table likely already exists: "
                    + rel_creation_string
                )
                logger.debug(e)

    def verify_connection(self) -> bool:
        if self.driver.is_closed is True:
            return False
        else:
            return True

    def close_connection(self) -> None:
        self.driver.close()

    def evaluate_query(
        self,
        cypher: str,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        result = self.driver.execute(cypher, params)

        result_df = result.get_as_df()

        neontology_records, nodes, rels, paths = kuzu_results_to_neontology_records(
            result, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=result_df,
            records=neontology_records,
            nodes=nodes,
            relationships=rels,
            paths=paths,
        )

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        result = self.driver.execute(cypher, params)

        if result.has_next():
            first_result = result.get_next()

            if result.has_next() or len(first_result) > 1:
                warnings.warn("Multiple results for evaluate_query_single.")

            return first_result[0]

        else:
            return None

    def create_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> list:
        """
        Args:
            labels (list): a list of labels to give created nodes
            pp_key (str): the primary property for the nodes
            properties (list): A list of dictionaries representing each node to be created.
                two keys with associated values pp (the value to assign the primary property)
                and props (dict with key value pairs for all other properties).

        Returns:
            list: list of created Nodes
        """

        node_classes = {node_class.__primarylabel__: node_class}

        results = []

        for entry in properties:
            all_props = entry["props"]
            all_props[pp_key] = entry["pp"]

            prop_set_string = ", ".join(
                [
                    f"{gql_identifier_adapter.validate_strings(x)}: ${x}"
                    for x in all_props
                ]
            )

            label_identifiers = [
                gql_identifier_adapter.validate_strings(x) for x in labels
            ]

            cypher = f"""
            CREATE (n:{":".join(label_identifiers)} {{{prop_set_string}}})
            RETURN n
            """

            params = all_props

            results += self.evaluate_query(cypher, params, node_classes).nodes

        return results

    def merge_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> list:
        """
        Args:
            labels (list): a list of labels to give created nodes
            pp_key (str): the primary property for the nodes
            properties (list): A list of dictionaries representing each node to be created.
                two keys with associated values pp (the value to assign the primary property)
                and props (dict with key value pairs for all other properties).

        Returns:
            list: list of created Nodes
        """

        node_classes = {node_class.__primarylabel__: node_class}

        results = []

        for entry in properties:
            pp_value = entry["pp"]

            if entry["set_on_match"]:
                set_on_match = "ON MATCH SET " + ", ".join(
                    [
                        f"n.{gql_identifier_adapter.validate_strings(x)}=${x}"
                        for x in entry["set_on_match"]
                    ]
                )
            else:
                set_on_match = ""

            if entry["set_on_create"]:
                set_on_create = "ON CREATE SET " + ", ".join(
                    [
                        f"n.{gql_identifier_adapter.validate_strings(x)}=${x}"
                        for x in entry["set_on_create"]
                    ]
                )
            else:
                set_on_create = ""

            if entry["always_set"]:
                always_set = "SET " + ", ".join(
                    [
                        f"n.{gql_identifier_adapter.validate_strings(x)}=${x}"
                        for x in entry["always_set"]
                    ]
                )
            else:
                always_set = ""

            label_identifiers = [
                gql_identifier_adapter.validate_strings(x) for x in labels
            ]

            cypher = f"""
            MERGE (n:{":".join(label_identifiers)} {{{gql_identifier_adapter.validate_strings(pp_key)}: $pp}})
            {set_on_match}
            {set_on_create}
            {always_set}
            RETURN n
            """

            params = {
                "pp": pp_value,
                **entry["set_on_match"],
                **entry["set_on_create"],
                **entry["always_set"],
            }

            results += self.evaluate_query(cypher, params, node_classes).nodes

        return results

    def merge_relationships(
        self,
        source_label: str,
        target_label: str,
        source_prop: str,
        target_prop: str,
        rel_type: str,
        merge_on_props: List[str],
        rel_props: List[dict],
    ) -> None:
        merge_props = ", ".join(
            [
                f"{gql_identifier_adapter.validate_strings(x)}: ${x}"
                for x in merge_on_props
            ]
        )

        for entry in rel_props:
            set_on_match = ""
            set_on_create = ""
            always_set = ""

            merge_params = {k: v for k, v in entry.items() if k in merge_on_props}

            if entry["set_on_match"]:
                set_on_match = "ON MATCH SET " + ", ".join(
                    [
                        f"r.{gql_identifier_adapter.validate_strings(x)}=${x}"
                        for x in entry["set_on_match"]
                    ]
                )

            if entry["set_on_create"]:
                set_on_create = "ON CREATE SET " + ", ".join(
                    [
                        f"r.{gql_identifier_adapter.validate_strings(x)}=${x}"
                        for x in entry["set_on_create"]
                    ]
                )

            if entry["always_set"]:
                always_set = "SET " + ", ".join(
                    [
                        f"r.{gql_identifier_adapter.validate_strings(x)}=${x}"
                        for x in entry["always_set"]
                    ]
                )

            cypher = f"""
            MATCH (source:{gql_identifier_adapter.validate_strings(source_label)})
            WHERE source.{gql_identifier_adapter.validate_strings(source_prop)} = $source_prop
            MATCH (target:{gql_identifier_adapter.validate_strings(target_label)})
            WHERE target.{gql_identifier_adapter.validate_strings(target_prop)} = $target_prop
            MERGE (source)-[r:{gql_identifier_adapter.validate_strings(rel_type)} {{ {merge_props} }}]->(target)
            {set_on_match}
            {set_on_create}
            {always_set}
            """

            params = {
                "source_prop": entry["source_prop"],
                "target_prop": entry["target_prop"],
                **merge_params,
                **entry["set_on_match"],
                **entry["set_on_create"],
                **entry["always_set"],
            }

            self.evaluate_query_single(cypher, params)

    def match_nodes(
        self,
        node_class: type["BaseNode"],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[List[B]]: A list of node instances.
        """

        cypher = f"""
        MATCH(n:{node_class.__primarylabel__})
        RETURN n
        """

        params: dict = {}

        if skip:
            # kuzu doesn't seem to like taking SKIP as a parameter
            cypher += f" SKIP {int_adapter.validate_python(skip)} "

        if limit:
            # kuzu doesn't seem to like taking LIMIT as a parameter
            cypher += f" LIMIT {int_adapter.validate_python(limit)} "

        result = self.evaluate_query(
            cypher, params, node_classes={node_class.__primarylabel__: node_class}
        )

        return result.nodes


class KuzuConfig(GraphEngineConfig):
    engine: ClassVar[type[KuzuEngine]] = KuzuEngine
    path: Path
    nodes: Optional[dict] = None
    relationships: Optional[dict] = None

    @model_validator(mode="before")
    @classmethod
    def populate_defaults(cls, data: Any):
        load_dotenv()

        if data.get("path") is None:
            data["path"] = os.getenv("KUZU_DB")

        return data
