import os
from typing import Optional
import warnings
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv
import kuzu


from ..graphengine import GraphEngineBase
from ..result import (
    NeontologyResult,
)


def kuzu_node_to_neontology_node(kuzu_node, node_classes):
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


def kuzu_results_to_neontology_records(results, node_classes: dict, rel_classes: dict):
    result_df = results.get_as_df()

    result_keys = list(result_df.columns)

    result_records = []

    all_nodes = {}
    all_rels = []

    # first we need to process the nodes to populate the node table mappings
    for result_row in result_df.iterrows():
        record_nodes = {}
        record_rels = {}
        node_table_mappings = defaultdict(dict)

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
            all_nodes[record_node.get_primary_property_value()] = record_node

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

            rel = rel_type_dict["rel_class"](**rel_props)

            record_rels[result_key] = rel
            all_rels.append(rel)

        record = {"nodes": record_nodes, "relationships": record_rels}
        result_records.append(record)

    return result_records, list(all_nodes.values()), all_rels


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
        "List[bool]": "BOOL[]",
        "List[int]": "INT64[]",
        "List[float]": "FLOAT[]",
        "List[str]": "STRING[]",
        "List[date]": "DATE[]",
        "List[datetime]": "TIMESTAMP[]",
        "List[timedelta]": "INTERVAL[]",
    }

    def __init__(self, config: dict) -> None:
        """Initialise connection to the engine

        Graph Config:
        * kuzu_db
        * Nodes (dict of node types indexed by label)
        * Relationships
            (dict of rel types, indexed by rel_type and including source/target node classes)

        Args:
            config (Optional[dict]): _description_
        """

        # try to load environment variables from .env file
        load_dotenv()

        kuzu_db_path = config.get("kuzu_db", os.getenv("KUZU_DB"))

        self._kuzu_db = kuzu.Database(kuzu_db_path)

        self.connection = kuzu.Connection(self._kuzu_db)

        nodes = config.get("nodes")
        relationships = config.get("relationships")

        self.initialise_tables(nodes=nodes, relationships=relationships)

    def initialise_tables(self, nodes, relationships):
        from ..utils import extract_type_mapping

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
                field_types = set()

                type_mapping = extract_type_mapping(
                    model_field.annotation, show_optional=False
                )
                field_types.add(type_mapping)

                kuzu_field_type = self._kuzu_type_mappings[field_types.pop()]

                db_creation_field_info.append(f"{field_name} {kuzu_field_type}")

            creation_string = f"CREATE NODE TABLE {label}({', '.join(db_creation_field_info)}, PRIMARY KEY ({primary_key}))"

            try:
                self.connection.execute(creation_string)
                print("SUCCEEDED: " + creation_string)

            except RuntimeError:
                print("FAILED: " + creation_string)

        for rel_type, rel in relationships.items():
            db_rel_creation_field_info = []

            rel_class = rel["rel_class"]
            try:
                source_label = rel["source_class"].__primarylabel__
                target_label = rel["target_class"].__primarylabel__
            except AttributeError:
                continue

            for field_name, model_field in rel_class.model_fields.items():
                if field_name in ["source", "target"]:
                    continue

                field_types = set()

                type_mapping = extract_type_mapping(
                    model_field.annotation, show_optional=False
                )
                field_types.add(type_mapping)

                kuzu_field_type = self._kuzu_type_mappings[field_types.pop()]

                db_rel_creation_field_info.append(f"{field_name} {kuzu_field_type}")

            rel_creation_string = f"CREATE REL TABLE {rel_type}(FROM {source_label} TO {target_label}, {', '.join(db_rel_creation_field_info)})"

            try:
                self.connection.execute(rel_creation_string)
                print("SUCCEEDED: " + rel_creation_string)

            except RuntimeError:
                print("FAILED: " + rel_creation_string)

    def verify_connection(self) -> bool:
        if self.connection.is_closed is True:
            return False
        else:
            return True

    def close_connection(self) -> None:
        self.connection.close()

    def evaluate_query(
        self, cypher, params={}, node_classes={}, relationship_classes={}
    ):
        print(cypher)
        result = self.connection.execute(cypher, params)

        result_df = result.get_as_df()

        neontology_records, nodes, rels = kuzu_results_to_neontology_records(
            result, node_classes, relationship_classes
        )

        return NeontologyResult(
            records=result_df,
            neontology_records=neontology_records,
            nodes=nodes,
            relationships=rels,
        )

    def evaluate_query_single(self, cypher, params={}):
        result = self.connection.execute(cypher, params)

        if result.has_next():
            first_result = result.get_next()

            if result.has_next() or len(first_result) > 1:
                warnings.warn("Multiple results for evaluate_query_single.")

            return first_result[0]

        else:
            return None

    def create_nodes(
        self, labels: list, pp_key: str, properties: list, node_class
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

            prop_set_string = ", ".join([f"{x}: ${x}" for x in all_props])

            cypher = f"""
            CREATE (n:{":".join(labels)} {{{prop_set_string}}})
            RETURN n
            """

            params = all_props

            results += self.evaluate_query(cypher, params, node_classes).nodes

        return results

    def merge_nodes(
        self, labels: list, pp_key: str, properties: list, node_class
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
                    [f"n.{x}=${x}" for x in entry["set_on_match"]]
                )
            else:
                set_on_match = ""

            if entry["set_on_create"]:
                set_on_create = "ON CREATE SET " + ", ".join(
                    [f"n.{x}=${x}" for x in entry["set_on_create"]]
                )
            else:
                set_on_create = ""

            if entry["always_set"]:
                always_set = "SET " + ", ".join(
                    [f"n.{x}=${x}" for x in entry["always_set"]]
                )
            else:
                always_set = ""

            cypher = f"""
            MERGE (n:{":".join(labels)} {{{pp_key}: $pp}})
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
        source_label,
        target_label,
        source_prop,
        target_prop,
        rel_type,
        merge_on_props,
        rel_props,
    ):
        merge_props = ", ".join([f"{x}: ${x}" for x in merge_on_props])

        for entry in rel_props:
            set_on_match = ""
            set_on_create = ""
            always_set = ""

            merge_params = {k: v for k, v in entry.items() if k in merge_on_props}

            if entry["set_on_match"]:
                set_on_match = "ON MATCH SET " + ", ".join(
                    [f"r.{x}=${x}" for x in entry["set_on_match"]]
                )

            if entry["set_on_create"]:
                set_on_create = "ON CREATE SET " + ", ".join(
                    [f"r.{x}=${x}" for x in entry["set_on_create"]]
                )

            if entry["always_set"]:
                always_set = "SET " + ", ".join(
                    [f"r.{x}=${x}" for x in entry["always_set"]]
                )

            cypher = f"""
            MATCH (source:{source_label})
            WHERE source.{source_prop} = $source_prop
            MATCH (target:{target_label})
            WHERE target.{target_prop} = $target_prop
            MERGE (source)-[r:{rel_type} {{ {merge_props} }}]->(target)
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
        node_class,
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
        ORDER BY n.created DESC
        """

        params = {}

        if skip:
            # kuzu doesn't seem to like taking SKIP as a parameter
            if isinstance(skip, int):
                cypher += f" SKIP {skip} "
            else:
                raise TypeError("Skip value must be an integer")

        if limit:
            # kuzu doesn't seem to like taking LIMIT as a parameter
            if isinstance(limit, int):
                cypher += f" LIMIT {limit} "
            else:
                raise TypeError("Limit value must be an integer")

        result = self.evaluate_query(
            cypher, params, node_classes={node_class.__primarylabel__: node_class}
        )

        return result.nodes
