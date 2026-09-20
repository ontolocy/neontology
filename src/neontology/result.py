import json
from typing import Any

from pydantic import BaseModel, computed_field


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

    def neontology_dump(self) -> dict:
        """Dump the results as a dictionary, in 'Neontology' format.

        This includes LABEL and RELATIONSHIP_TYPE keys for easy import with Neontology.
        """
        nodes = [x.neontology_dump() for x in self.nodes]
        relationships = [x.neontology_dump() for x in self.relationships]

        data = {"nodes": nodes, "edges": relationships}

        return data

    def neontology_dump_json(self) -> str:
        """Dump the results as a JSON string, in 'Neontology' format.

        This includes LABEL and RELATIONSHIP_TYPE keys for easy import with Neontology.
        """
        nodes = [json.loads(x.neontology_dump_json()) for x in self.nodes]
        relationships = [json.loads(x.neontology_dump_json()) for x in self.relationships]

        data = {"nodes": nodes, "edges": relationships}

        return json.dumps(data)
