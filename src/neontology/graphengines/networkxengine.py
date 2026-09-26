import re
import warnings
from string import Template
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar

import networkx as nx
from grandcypher import GrandCypher
from typing_extensions import LiteralString

from ..gql import gql_identifier_adapter
from ..result import NeontologyResult
from .capabilities import Capability
from .graphengine import GraphEngineBase, GraphEngineConfig
from .hydration import RawNode, RawPath, RawRelationship, build_result

if TYPE_CHECKING:
    from ..basenode import BaseNode


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


# attributes the engine keeps on nodes and edges for itself, which are not model properties
_INTERNAL_NODE_KEYS = frozenset({"__labels__"})
_INTERNAL_EDGE_KEYS = frozenset({"__labels__", "__neograndrel__", "__sourcepp__", "__targetpp__"})


def _is_node(value: Any) -> bool:
    """Whether a grand-cypher value is a node: attributes carrying labels, not marked as an edge."""
    return isinstance(value, dict) and "__labels__" in value and "__neograndrel__" not in value


def _is_edge(value: Any) -> bool:
    """Whether a grand-cypher value is an edge: attributes the engine marked as a relationship."""
    return isinstance(value, dict) and "__neograndrel__" in value


def _is_path(value: Any) -> bool:
    """Whether a grand-cypher value is a named path.

    A path is a list alternating node ids with hops, each hop mapping edge keys to edges. A
    variable length relationship is returned as a list too, but of edges on their own, and
    is not a path.
    """
    return (
        isinstance(value, list)
        and len(value) % 2 == 1
        and not isinstance(value[0], dict)
        and all(isinstance(hop, dict) and hop and all(_is_edge(edge) for edge in hop.values()) for hop in value[1::2])
    )


def _hashable(value: Any) -> Any:
    """Represent a property value so it can be part of a dictionary key.

    Relationships are grouped by the properties they merge on, and a property may hold
    a list, which cannot be hashed. Two values which are equal have equal
    representations, which is all the grouping needs.

    Args:
        value (Any): the property value.

    Returns:
        Any: the value, or its representation where it cannot be hashed.
    """
    try:
        hash(value)

    except TypeError:
        return repr(value)

    return value


def networkx_rows(raw_result: dict, graph: nx.MultiDiGraph) -> list[dict[str, Any]]:
    """Read a grand-cypher result as rows of engine-neutral values, for `build_result`.

    grand-cypher returns columns rather than rows, and returns each node and edge as the
    graph's own attribute dictionary. That dictionary is what identifies it: values which
    are the same dictionary are the same node or edge, while parallel edges with equal
    attributes are still different dictionaries.

    As on the other engines, a relationship's source and target are only known when the
    result also returns those nodes, anywhere in it. A path carries its own nodes.

    Args:
        raw_result (dict): grand-cypher's result, from column to a list of values.
        graph (nx.MultiDiGraph): the graph the query ran against.

    Returns:
        list[dict[str, Any]]: one dictionary per row, from column name to the nodes,
            relationships and paths in it.
    """
    # the nodes the result includes - returned as nodes, or along a path - by attributes
    included: set[int] = set()

    for values in raw_result.values():
        for value in values:
            if _is_node(value):
                included.add(id(value))

            elif _is_path(value):
                included.update(id(graph.nodes[node_id]) for node_id in value[::2] if node_id in graph)

    raw_nodes: dict[int, RawNode] = {}
    raw_relationships: dict[int, RawRelationship] = {}

    def node(data: dict) -> RawNode:
        raw = raw_nodes.get(id(data))

        if raw is None:
            raw = raw_nodes[id(data)] = RawNode(
                key=id(data),
                labels=data["__labels__"],
                properties={k: v for k, v in data.items() if k not in _INTERNAL_NODE_KEYS},
            )

        return raw

    def endpoint(node_id: Any) -> Optional[RawNode]:
        if node_id not in graph:
            return None

        data = graph.nodes[node_id]

        return node(data) if id(data) in included else None

    def relationship(data: dict) -> RawRelationship:
        raw = raw_relationships.get(id(data))

        if raw is None:
            raw = raw_relationships[id(data)] = RawRelationship(
                key=id(data),
                type=next(iter(data["__labels__"])),
                properties={k: v for k, v in data.items() if k not in _INTERNAL_EDGE_KEYS},
                source=endpoint(data["__sourcepp__"]),
                target=endpoint(data["__targetpp__"]),
            )

        return raw

    rows: list[dict[str, Any]] = [{} for _ in range(max((len(values) for values in raw_result.values()), default=0))]

    for column, values in raw_result.items():
        name = getattr(column, "value", str(column))

        for index, value in enumerate(values):
            if _is_node(value):
                rows[index][name] = node(value)

            elif _is_edge(value):
                rows[index][name] = relationship(value)

            elif _is_path(value):
                # each hop holds the edge taken between two nodes along the path
                rows[index][name] = RawPath(relationships=[relationship(next(iter(hop.values()))) for hop in value[1::2]])

    return rows


class NetworkxEngine(GraphEngineBase):
    # grand-cypher is a query language over an in-memory NetworkX graph, with no
    # mutation clauses and a reduced expression language, so it supports few of
    # the named capabilities. Everything not named in Capability works normally.
    # The exceptions are the three that name another backend's constraints rather than
    # grand-cypher's: nodes carry every label they are given in a `__labels__` attribute,
    # which is what grand-cypher matches a label against; there is no schema to declare,
    # so a query naming an unknown label or property simply matches nothing; and property
    # values are the Python objects themselves, so a datetime keeps its timezone.
    supported_capabilities: ClassVar[frozenset[Capability]] = frozenset(
        {
            Capability.SECONDARY_LABELS,
            Capability.UNDECLARED_SCHEMA,
            Capability.TIMEZONE_AWARE_DATETIMES,
        }
    )

    capability_hints: ClassVar[dict[Capability, str]] = {
        Capability.CONSTRAINTS: (
            "NetworkX nodes are keyed by a hash of (primary property, primary label), so"
            " uniqueness on the primary property already holds structurally and there is"
            " nothing to apply. This is the same property that makes DUPLICATE_CREATE"
            " unsupported here. Other properties tagged unique are not enforced."
        ),
        Capability.INDEXES: ("NetworkX graphs are held in memory and queried by traversal, so there is no index to build."),
    }

    def __init__(self, config: "NetworkxConfig") -> None:
        """Initialise connection to the engine.

        Args:
            config (NetworkxConfig): Takes a NetworkxConfig object.
        """
        self.driver = nx.MultiDiGraph()

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
        # index the graph once rather than scanning every node for every entry -
        # merging n relationships over a graph of m nodes was O(n * m)
        by_prop = {}

        for _, data in self.driver.nodes(data=True):
            if prop_to_update in data:
                by_prop[data[prop_to_update]] = data

        for entry in all_props:
            this_node = by_prop.get(entry[props_key])

            if not this_node:
                warnings.warn(f"Source node with property {prop_to_update}={entry[props_key]} not found.")
                continue

            # update the source_prop to the actual node's primary property value
            entry[props_key] = this_node[new_prop]

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
                    # labels are added as cypher's SET does, never removing one the node has
                    "__labels__": set(label_identifiers) | existing_node_records[x["pp"]].get("__labels__", set()),
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

        Nodes are matched on the label as a query would match them, rather than looked up
        by the id derived from their primary label. A subclass node carrying the label is
        deleted too, as it is on the other engines.

        Args:
            label (str): The label of the nodes to delete.
            pp_key (str): The primary property key to match on.
            pp_values (list[Any]): A list of primary property values to match on for deletion.
        """
        wanted = set(pp_values)

        matched = [
            node_id
            for node_id, data in self.driver.nodes(data=True)
            if label in data.get("__labels__", ()) and data.get(pp_key) in wanted
        ]

        self.driver.remove_nodes_from(matched)

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

        rel_type_identifier = gql_identifier_adapter.validate_strings(rel_type)

        # Collected by what identifies a relationship rather than appended to, so two
        # records which merge onto the same relationship produce one edge - as MERGE
        # does for two rows of an UNWIND. The edges are only added to the graph once
        # the batch has been read, so the existence check below sees the graph as it
        # was before the batch started: without this, the second record would find
        # nothing and a parallel edge would be added beside the first.
        edge_records: dict[tuple, tuple] = {}

        for x in rel_props:
            all_props = {**x["always_set"], **x["set_on_match"], **x["set_on_create"]}

            source_id = generate_node_id(x["source_prop"], source_label)
            target_id = generate_node_id(x["target_prop"], target_label)

            # every merge_on property is part of what identifies the relationship,
            # including one whose value is falsy - a relationship tagged 0 is not the
            # one tagged 1, and must not match and overwrite it
            merge_on_values = {prop: all_props.get(prop) for prop in merge_on_props}

            merge_attributes = {"__labels__": {rel_type_identifier}, **merge_on_values}

            merge_key = (
                source_id,
                target_id,
                rel_type_identifier,
                tuple(sorted((k, _hashable(v)) for k, v in merge_on_values.items())),
            )

            pending = edge_records.get(merge_key)

            if pending is not None:
                # already merged earlier in this batch, so this record matches it
                existing_props: Optional[dict] = pending[2]

            else:
                existing_edges = self._existing_edges(source_id, target_id, **merge_attributes) or []

                # read before removing: the attribute dictionary belongs to the edge
                existing_props = (
                    dict(self.driver.get_edge_data(source_id, target_id, key=existing_edges[0])) if existing_edges else None
                )

                for key in existing_edges:
                    self.driver.remove_edge(source_id, target_id, key=key)

            if existing_props is None:
                props = {**x["always_set"], **x["set_on_create"]}

            else:
                # as in merge_nodes: what was set when the relationship was created
                # stays as it was, and set_on_match applies in its place
                carried = {k: existing_props[k] for k in x["set_on_create"] if k in existing_props}

                props = {**x["always_set"], **x["set_on_match"], **carried}

            edge_records[merge_key] = (
                source_id,
                target_id,
                {
                    **props,
                    "__labels__": {rel_type_identifier},
                    "__neograndrel__": True,
                    "__sourcepp__": source_id,
                    "__targetpp__": target_id,
                },
            )

        self.driver.add_edges_from(edge_records.values())

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

        return build_result(raw_result, networkx_rows(raw_result, self.driver), node_classes, relationship_classes)

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

        if not raw_result:
            return None

        # take the first column, then the first row, to match the neo4j driver's
        # Result.single().value() behaviour
        first_column = next(iter(raw_result.values()))

        if not first_column:
            return None

        return first_column[0]

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
        cypher = f"MATCH (n{self.label_pattern(node_class.__primarylabel__)})"
        where_clause, params = self._filters_to_where_clause(filters)
        if where_clause:
            cypher += where_clause
        cypher += " RETURN COUNT(n)"

        result = self.evaluate_query_single(cypher, params)

        # a count over zero matches returns no rows at all, where neo4j returns 0
        if result is None:
            return 0

        return result


class NetworkxConfig(GraphEngineConfig):
    """Configuration for a Grand graph engine."""

    engine: ClassVar[type[GraphEngineBase]] = NetworkxEngine
