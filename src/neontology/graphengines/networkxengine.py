import re
import warnings
from string import Template
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar

import networkx as nx
from grandcypher import GrandCypher
from typing_extensions import LiteralString

from ..gql import gql_identifier_adapter
from ..result import NeontologyResult
from .graphengine import GraphEngineBase, GraphEngineConfig

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship, RelationshipTypeData


BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")


def generate_node_id(pp, label):
    """Take primary property and label to generate a unique node ID.

    NetworkX does not natively differentiate nodes by label, so we use a combination of
    primary property and label to create a unique identifier.
    """
    return hash((pp, label))  # Simple hash for unique ID generation


def escape_cypher_string(value):
    """Escape string values for Cypher queries."""
    if isinstance(value, str):
        # Escape single quotes and wrap in quotes
        return '"' + value.replace('"', '\\"') + '"'
    if isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return "null"
    elif isinstance(value, list):
        return f"[{', '.join([escape_cypher_string(x) for x in value])}]"
    else:
        raise ValueError(f"Unsupported type: {type(value)}")


def escape_cypher_identifier(identifier):
    """Escape identifiers (labels, property names, etc.)."""
    # Check if identifier needs backticks
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
        return f"`{identifier.replace('`', '``')}`"
    return identifier


def substitute_cypher(query, params):
    """Convert $param to ${param} format and substitute."""
    # Convert Cypher $param format to Python ${param} format
    template_query = re.sub(r"\$(\w+)", r"${\1}", query)

    # Escape all parameter values
    escaped_params = {escape_cypher_identifier(k): escape_cypher_string(v) for k, v in params.items()}

    return Template(template_query).substitute(escaped_params).replace("'", '"')


def grand_node_to_neontology_node(grand_node: dict, node_classes: dict[str, type[BaseNodeT]]) -> Optional[BaseNodeT]:
    """Convert a GrandCypher node to a Neontology node.

    Args:
        grand_node (dict): A dictionary representing a GrandCypher node.
        node_classes (dict[str, type[BaseNodeT]]): Mapping of labels to node classes.

    Returns:
        Optional[BaseNodeT]: An instance of the corresponding Neontology node class, or None if not found.
    """
    node_labels = grand_node["__labels__"]

    primary_labels = node_labels.intersection(set(node_classes.keys()))

    secondary_labels = node_labels.difference(set(node_classes.keys()))

    if len(primary_labels) == 1:
        primary_label = primary_labels.pop()

        node_dict = {k: v for k, v in grand_node.items() if k not in ["__labels__"]}

        node = node_classes[primary_label](**node_dict)

        # warn if the secondary labels aren't what's expected

        if set(node.__secondarylabels__) != secondary_labels:
            warnings.warn(f"Unexpected secondary labels returned: {secondary_labels}")

        return node

    # gracefully handle cases where we don't have a class defined
    # for the identified label or where we get more than one valid primary label
    elif len(primary_labels) == 0:
        warnings.warn(f"Labels {node_labels} do not match any known node classes.")

        return None

    else:
        warnings.warn(
            f"Multiple primary labels found: {primary_labels}. Please ensure that the node has a unique primary label."
        )
        return None


def grand_relationship_to_neontology_relationship(
    grand_relationship: dict,
    source_node,
    target_node,
    relationship_classes: dict[str, "RelationshipTypeData"],
) -> Optional["BaseRelationship"]:
    """Convert a GrandCypher relationship to a Neontology relationship.

    Args:
        grand_relationship (dict): A dictionary representing a GrandCypher relationship.
        source_node (BaseNodeT): The source node of the relationship.
        target_node (BaseNodeT): The target node of the relationship.
        relationship_classes (dict[str, type[BaseNodeT]]): Mapping of relationship types to classes.

    Returns:
        Optional["BaseRelationship"]: An instance of the corresponding Neontology relationship class, or None if not found.
    """
    rel_type = next(iter(grand_relationship["__labels__"]))

    if rel_type in relationship_classes:
        rel_dict = {
            k: v
            for k, v in grand_relationship.items()
            if k not in ["__neograndrel__", "__sourcepp__", "__targetpp__", "__labels__"]
        }

        return relationship_classes[rel_type].relationship_class(source=source_node, target=target_node, **rel_dict)

    warnings.warn(
        f"Relationship type {rel_type} does not match any known classes."
        " Did you define the class before initializing Neontology?"
        " Are source and target node classes valid and resolved?"
    )
    return None


def grand_cypher_to_neontology_records(records: dict, node_classes: dict, relationship_classes: dict):
    """Convert GrandCypher records to Neontology records.

    Args:
        records (dict): Records from GrandCypher.
        node_classes (dict): Mapping of labels to node classes.
        relationship_classes (dict): Mapping of relationship types to classes.

    Returns:
        tuple: A tuple containing Neontology records, nodes, relationships, and paths.
    """
    new_records = []
    all_nodes = {}
    all_rels = []
    all_paths = []

    # grand dict represents each key returned, with a list of records for that key
    for key, entries in records.items():
        for idx, entry in enumerate(entries):
            # skip relationships and handle nodes first so that relationships can reference them
            if "__labels__" in entry:
                # handle nodes
                node = grand_node_to_neontology_node(entry, node_classes)

                if node:
                    all_nodes[generate_node_id(node.get_pp(), node.__primarylabel__)] = node

                if len(new_records) == idx:
                    new_records.append({"nodes": {}, "relationships": {}, "paths": {}})

                new_records[idx]["nodes"][key.value] = node

    for key, entries in records.items():
        for idx, entry in enumerate(entries):
            # process relationships
            if isinstance(entry, dict):
                for subentry in entry.values():
                    if isinstance(subentry, dict):
                        source_node = all_nodes.get(subentry["__sourcepp__"])
                        target_node = all_nodes.get(subentry["__targetpp__"])

                        rel_type = list(subentry["__labels__"])[0]

                        if not source_node or not target_node:
                            warnings.warn(
                                (
                                    f"{rel_type} relationship type query did not include nodes."
                                    " To get neontology relationships, return source and target "
                                    "nodes as part of result."
                                )
                            )
                            rel = None

                        else:
                            rel = grand_relationship_to_neontology_relationship(
                                subentry, source_node, target_node, relationship_classes
                            )

                        if rel:
                            all_rels.append(rel)

                        try:
                            new_records[idx]["relationships"][key.value] = rel

                        except IndexError:
                            new_records.append(
                                {
                                    "nodes": {},
                                    "relationships": {key.value: rel},
                                    "paths": {},
                                }
                            )

            # handle paths
            elif isinstance(entry, list):
                this_path = []
                for entity in entry:
                    if isinstance(entity, dict):
                        # this is a relationship, process it
                        path_rel_record = entity[0]

                        source_node = all_nodes.get(path_rel_record["__sourcepp__"])
                        target_node = all_nodes.get(path_rel_record["__targetpp__"])

                        if not source_node or not target_node:
                            warnings.warn(f"Source or target node not found for '{key.value}'")
                            path_rel = None

                        else:
                            path_rel = grand_relationship_to_neontology_relationship(
                                path_rel_record,
                                all_nodes[path_rel_record["__sourcepp__"]],
                                all_nodes[path_rel_record["__targetpp__"]],
                                relationship_classes,
                            )
                        if path_rel:
                            this_path.append(path_rel)

                if this_path:
                    all_paths.append(this_path)

                new_records[idx]["paths"][key.value] = this_path

    unique_nodes = list(all_nodes.values())

    return new_records, unique_nodes, all_rels, all_paths


class NetworkxEngine(GraphEngineBase):
    def __init__(self, config: "NetworkxConfig") -> None:
        """Initialise connection to the engine.

        Args:
            config (NetworkxConfig): Takes a NetworkxConfig object.
        """
        self.driver = nx.MultiDiGraph()

    def _filters_to_where_clause(self, filters: Optional[dict] = None) -> tuple[Optional[str], dict]:
        """Convert a dictionary of filters into a WHERE clause and parameter dictionary for a query.

        Args:
            filters (dict | None): A dictionary of filters. Each key is a field name possibly followed
                                by '__' and a lookup type (e.g., 'exact', 'contains'). The value is
                                the filter value. If None, returns an empty WHERE clause.

        Returns:
            tuple: A tuple containing the WHERE clause string and a dictionary of parameters.
        """
        supported_filters = [
            "exact",
            "contains",
            "startswith",
            "gt",
            "lt",
            "gte",
            "lte",
            "in",
            "isnull",
        ]

        if filters:
            for key in filters.keys():
                if "__" in key:
                    _, lookup_type = key.split("__")
                else:
                    _, lookup_type = key, "exact"

                if lookup_type not in supported_filters:
                    raise NotImplementedError(f"{lookup_type} filter is not implemented for NetworkX engine.")

        return super()._filters_to_where_clause(filters)

    def _swap_prop(self, all_props: list[dict], props_key: str, prop_to_update: str, new_prop: str):
        """Swap a property in a list of dictionaries.

        Args:
            all_props (list): List of dictionaries containing properties.
            props_key (str): The key in the dictionaries to update (e.g source_prop / target_prop).
            prop_to_update (str): The property to be replaced (the non-primary prop to match on).
            new_prop (str): The new property to set. (e.g. __primaryproperty__)

        Returns:
            list: Updated list of dictionaries with the swapped property.
        """
        for entry in all_props:
            # find the source node by the property
            this_node = None
            for node, data in self.driver.nodes(data=True):
                if data.get(prop_to_update) == entry[props_key]:
                    this_node = data

            if not this_node:
                warnings.warn(f"Source node with property {prop_to_update}={entry[props_key]} not found.")
                continue

            # get the actual node id

            actual_node_id = this_node[new_prop]

            # update the source_prop to the actual node's pp
            entry[props_key] = actual_node_id

        return all_props

    def verify_connection(self) -> bool:
        """Verify the connection to the backend.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        # There is no remote connection to verify
        return True

    def close_connection(self) -> None:
        """Close the connection to the backend."""
        pass

    def create_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Create nodes with specified labels and properties.

        Args:
            labels (list): a list of labels to give created nodes
            pp_key (str): the primary property for the nodes
            properties (list): A list of dictionaries representing each node to be created.
                two keys with associated values pp (the value to assign the primary property)
                and props (dict with key value pairs for all other properties).
            node_class (type[BaseNodeT]): the type of nodes to create

        Returns:
            list: list of created Nodes
        """
        label_identifiers = [gql_identifier_adapter.validate_strings(x) for x in labels]

        node_records = [
            (
                generate_node_id(x["pp"], node_class.__primarylabel__),
                {
                    pp_key: x["pp"],
                    **x.get("props", {}),
                    "__labels__": set(label_identifiers),
                },
            )
            for x in properties
        ]

        self.driver.add_nodes_from(node_records)

        results = [
            node_class(
                **{
                    pp_key: x["pp"],
                    **x.get("props", {}),
                },
            )
            for x in properties
        ]

        return results

    def merge_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Merge nodes with specified labels and property.

        Args:
            labels (list): _description_
            pp_key (str): _description_
            properties (list): A list of dictionaries representing each node to be created.
                four keys with associated values: pp (the value to assign the primary property)
                set_on_match, set_on_create and always_set (dicts with key value pairs for all other properties).
            node_class (type["BaseNode"]): The class of the nodes to be merged.

        Returns:
            list: list of merged Nodes
        """
        label_identifiers = [gql_identifier_adapter.validate_strings(x) for x in labels]

        # NetworkX doesn't natively merge, so we handle new and existing nodes separately
        create_records = {
            x["pp"]: x for x in properties if not self.driver.has_node(generate_node_id(x["pp"], node_class.__primarylabel__))
        }

        create_results = self.create_nodes(
            labels=labels,
            pp_key=pp_key,
            properties=[{"pp": pp, "props": {**v["always_set"], **v["set_on_create"]}} for pp, v in create_records.items()],
            node_class=node_class,
        )

        merge_pps = list(set([x["pp"] for x in properties]) - set(create_records.keys()))

        existing_node_records = {
            x: self.driver.nodes[generate_node_id(x, node_class.__primarylabel__)]
            for x in merge_pps
            if generate_node_id(x, node_class.__primarylabel__) in self.driver.nodes
        }

        merge_props = []

        full_merge_props = [x for x in properties if x["pp"] in existing_node_records]

        for entry in full_merge_props:
            if not entry["set_on_create"]:
                merge_props.append(entry)
            else:
                # we want to take the 'set_on_create' properties from the existing_node_records
                # and update them in for the new merge_props
                existing_node = existing_node_records[entry["pp"]]
                create_props = entry.get("set_on_create", {}).keys()
                new_set_on_create = {k: existing_node[k] for k in create_props if k in existing_node}

                entry["previously_set_on_create"] = new_set_on_create
                merge_props.append(entry)

        merge_records = [
            (
                generate_node_id(x["pp"], node_class.__primarylabel__),
                {
                    **x.get("always_set", {}),
                    **x.get("set_on_match", {}),
                    **x.get("previously_set_on_create", {}),
                    "__labels__": set(label_identifiers),
                },
            )
            for x in merge_props
        ]

        self.driver.add_nodes_from(merge_records)

        merge_results = [
            node_class(
                **{
                    **x.get("always_set", {}),
                    **x.get("set_on_match", {}),
                    **x.get("previously_set_on_create", {}),
                },
            )
            for x in merge_props
        ]

        return create_results + merge_results

    def delete_nodes(
        self,
        label: Optional[str] = None,
        pp_key: Optional[str] = None,
        pp_values: list[Any] = [],
    ) -> None:
        """Delete nodes with a specific label and primary property value.

        Args:
            label (str): The primary label of the nodes to delete.
            pp_key (str): The primary property key to match on.
            pp_values (list[Any]): A list of primary property values to match on for deletion.
        """
        self.driver.remove_nodes_from([generate_node_id(x, label) for x in pp_values])

    def _existing_edges(self, node1, node2, **attributes):
        """Find matching edges in the graph."""
        if self.driver.has_edge(node1, node2):
            edge_dict = self.driver.get_edge_data(node1, node2)

            keys_to_delete = []

            for key, edge_data in edge_dict.items():
                if all(edge_data.get(attr) == value for attr, value in attributes.items()):
                    keys_to_delete.append(key)

            return keys_to_delete
        else:
            return None

    def merge_relationships(
        self,
        source_label: str,
        target_label: str,
        source_prop: str,
        target_prop: str,
        rel_type: str,
        merge_on_props: list[str],
        rel_props: list[dict],
    ) -> None:
        """Merge relationships between nodes in the database.

        Args:
            source_label (str): The label of the source node.
            target_label (str): The label of the target node.
            source_prop (str): The property of the source node to match on.
            target_prop (str): The property of the target node to match on.
            rel_type (str): The type of relationship to create or merge.
            merge_on_props (list[str]): A list of properties to merge on.
            rel_props (list[dict]): A list of dictionaries representing each relationship to be merged.
                Each dictionary should contain keys for `source_prop`, `target_prop`, and any additional properties
                to set on the relationship.
        """
        from ..utils import get_node_types

        all_types = get_node_types()

        source_type = all_types[source_label]
        target_type = all_types[target_label]

        if source_prop != source_type.__primaryproperty__:
            rel_props = self._swap_prop(
                rel_props,
                "source_prop",
                source_prop,
                source_type.__primaryproperty__,
            )

        if target_prop != target_type.__primaryproperty__:
            rel_props = self._swap_prop(
                rel_props,
                "target_prop",
                target_prop,
                target_type.__primaryproperty__,
            )

        edge_records = []

        for x in rel_props:
            all_props = {**x["always_set"], **x["set_on_match"], **x["set_on_create"]}

            source_id = generate_node_id(x["source_prop"], source_label)
            target_id = generate_node_id(x["target_prop"], target_label)

            merge_attributes = {"__labels__": {gql_identifier_adapter.validate_strings(rel_type)}}

            # if there are props to merge on, pull them out too

            for merge_on_prop in merge_on_props:
                if all_props.get(merge_on_prop):
                    merge_attributes[merge_on_prop] = all_props[merge_on_prop]

            # now check if there is already a relationship which meets that criteria
            existing_edges = self._existing_edges(source_id, target_id, **merge_attributes)

            if existing_edges is not None:
                # if the edge already exists, we need to delete it
                for key in existing_edges:
                    self.driver.remove_edge(source_id, target_id, key=key)

            new_record = (
                source_id,
                target_id,
                {
                    **all_props,
                    "__labels__": {gql_identifier_adapter.validate_strings(rel_type)},
                    "__neograndrel__": True,
                    "__sourcepp__": generate_node_id(x["source_prop"], source_label),
                    "__targetpp__": generate_node_id(x["target_prop"], target_label),
                },
            )

            edge_records.append(new_record)

        self.driver.add_edges_from(edge_records)

    def evaluate_query(
        self,
        cypher: LiteralString,
        params: Optional[dict] = None,
        node_classes: Optional[dict] = None,
        relationship_classes: Optional[dict] = None,
    ) -> NeontologyResult:
        """Evaluate a Cypher query and return the results as Neontology records.

        Args:
            cypher (str): query to evaluate.
            params (dict, optional): parameters to pass through. Defaults to None.
            node_classes (dict, optional): mapping of labels to node classes used for populating with results. Defaults to None.
            relationship_classes (dict, optional): mapping of relationship types to classes used for populating with results.
                Defaults to None.

        Returns:
            NeontologyResult: Result object containing the records, nodes, relationships, and paths.
        """
        params = params or {}
        node_classes = node_classes or {}
        relationship_classes = relationship_classes or {}

        subbed_cypher = substitute_cypher(cypher, params)

        raw_result = GrandCypher(self.driver).run(subbed_cypher)

        neontology_records, nodes, rels, paths = grand_cypher_to_neontology_records(
            raw_result, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=raw_result,
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
        subbed_cypher = substitute_cypher(cypher, params)

        raw_result = GrandCypher(self.driver).run(subbed_cypher)

        if raw_result:
            first_record = next(iter(raw_result.values()))
            return first_record

        else:
            return None

    def get_count(
        self,
        node_class: type,
        filters: Optional[dict] = None,
    ) -> int:
        """Get the count of nodes based on the given node class and filters.

        Args:
            node_class (type): The class of the nodes to count.
            filters (dict | None): A dictionary of filters to apply. If None, no filters are applied.

        Returns:
            int: The count of nodes that match the given criteria.
        """
        cypher = f"MATCH (n:{node_class.__primarylabel__})"
        where_clause, params = self._filters_to_where_clause(filters)
        if where_clause:
            cypher += where_clause
        cypher += " RETURN COUNT(n)"

        result = self.evaluate_query_single(cypher, params)

        if result:
            return result[0]["_"]

        else:
            return 0


class NetworkxConfig(GraphEngineConfig):
    """Configuration for a Grand graph engine."""

    engine: ClassVar[type[GraphEngineBase]] = NetworkxEngine
