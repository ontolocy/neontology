import json
from collections.abc import Callable
from typing import Any, Union

from pydantic import BaseModel, computed_field

# the keys a relationship declared under its source node does not need: which node it
# leaves is the node declaring it, and its target is named as a list instead
_DECLARED_BY_SOURCE = frozenset({"SOURCE", "SOURCE_LABEL", "TARGET"})


def _as_link(relationship: Any) -> dict:
    """Dump a relationship as an edge of node link data.

    Node link data is a format of its own: NetworkX, D3 and Cytoscape all expect an
    edge to name its ends `source` and `target`. Neontology's own content format names
    them `SOURCE` and `TARGET`, along with every other key which says how to build the
    graph, so the two are spelled differently on purpose.

    Args:
        relationship (Any): the relationship to dump.

    Returns:
        dict: the relationship as an edge.
    """
    dumped = relationship.neontology_dump()

    dumped["source"] = dumped.pop("SOURCE", None)
    dumped["target"] = dumped.pop("TARGET", None)

    return dumped


def _nested_records(nodes: list, relationships: list, dump: Callable[[Any], dict]) -> list[dict]:
    """Describe nodes and relationships as a record per node.

    Each relationship is declared under the node it leaves, in that node's
    `RELATIONSHIPS_OUT`, which is how content is usually written by hand. A
    relationship is only built when the result held the nodes at both of its ends, so
    a query has to return them for it to be declared anywhere.

    Args:
        nodes (list): the nodes in the result.
        relationships (list): the relationships in the result.
        dump (Callable[[Any], dict]): dumps one node or relationship as a record.

    Returns:
        list[dict]: a record per node, in the order the nodes were returned.
    """
    records: dict[tuple, dict] = {}

    for node in nodes:
        records[(node.__primarylabel__, node.get_pp())] = dump(node)

    for relationship in relationships:
        source = relationship.source

        record = records.get((source.__primarylabel__, source.get_pp()))

        if record is None:
            continue

        dumped = dump(relationship)

        # the node declaring it is the source of everything in the block, so the record
        # names only the far end - as a list, which is how the block reads
        declared = {k: v for k, v in dumped.items() if k not in _DECLARED_BY_SOURCE}

        declared["TARGETS"] = [dumped["TARGET"]]

        record.setdefault("RELATIONSHIPS_OUT", []).append(declared)

    return list(records.values())


class NeontologyResult(BaseModel):
    records_raw: Any
    records: list
    nodes: list
    relationships: list
    paths: list

    # built on demand by dumping every node and relationship, so it is left out of the
    # repr - printing or logging a result should not pay for it
    @computed_field(repr=False)  # type: ignore[misc]
    @property
    def node_link_data(self) -> dict:
        """Get the result as a dictionary with 'nodes' and 'edges' keys.

        Returns:
            dict: Dictionary with 'nodes' and 'edges' keys, suitable for use with
            networkx or network visualisation libraries like D3.js or Cytoscape.js.
        """
        # the format identifies a node by its label and primary property
        nodes = {
            f"{x.__primarylabel__}:{str(x.get_pp())}": {
                **x.neontology_dump(),
                **{
                    "__pp__": x.get_pp(),
                    "__str__": str(x),
                },
            }
            for x in self.nodes
        }

        # relationships are already distinct by database identity, so none are merged here:
        # parallel relationships with equal properties are separate edges
        links = [_as_link(x) for x in self.relationships]

        unique_nodes = list(nodes.values())

        data = {"nodes": unique_nodes, "edges": links, "directed": True}

        return data

    def neontology_dump(self, nested: bool = False) -> Union[dict, list]:
        """Dump the results, in 'Neontology' format.

        This includes LABEL and RELATIONSHIP_TYPE keys for easy import with Neontology.

        Args:
            nested (bool): dump a record per node, each carrying the relationships that
                leave it, rather than nodes and edges side by side. Defaults to False.

        Returns:
            Union[dict, list]: the nodes and edges, or a list of node records where
            `nested` is set.
        """
        if nested is True:
            return _nested_records(self.nodes, self.relationships, lambda x: x.neontology_dump())

        nodes = [x.neontology_dump() for x in self.nodes]
        relationships = [x.neontology_dump() for x in self.relationships]

        data = {"nodes": nodes, "edges": relationships}

        return data

    def neontology_dump_json(self, nested: bool = False) -> str:
        """Dump the results as a JSON string, in 'Neontology' format.

        This includes LABEL and RELATIONSHIP_TYPE keys for easy import with Neontology.

        Args:
            nested (bool): dump a record per node, each carrying the relationships that
                leave it, rather than nodes and edges side by side. Defaults to False.

        Returns:
            str: the results as JSON.
        """

        def dump(entry: Any) -> dict:
            return json.loads(entry.neontology_dump_json())

        if nested is True:
            return json.dumps(_nested_records(self.nodes, self.relationships, dump))

        nodes = [dump(x) for x in self.nodes]
        relationships = [dump(x) for x in self.relationships]

        data = {"nodes": nodes, "edges": relationships}

        return json.dumps(data)
