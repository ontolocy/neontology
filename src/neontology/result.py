import json
from typing import Any

from pydantic import BaseModel, computed_field


class NeontologyResult(BaseModel):
    records_raw: Any
    records: list
    nodes: list
    relationships: list

    @computed_field  # type: ignore[misc]
    @property
    def node_link_data(self) -> dict:
        nodes = [
            {
                "id": x.get_pp(),
                "label": x.__primarylabel__,
                "name": str(x),
            }
            for x in self.nodes
        ]

        links = [
            {
                "source": x.source.get_pp(),
                "target": x.target.get_pp(),
                "link_label": x.__relationshiptype__,
            }
            for x in self.relationships
        ]

        unique_nodes = list({frozenset(item.items()): item for item in nodes}.values())
        unique_links = list({frozenset(item.items()): item for item in links}.values())
        data = {
            "nodes": unique_nodes,
            "links": unique_links,
        }

        return data

    def neontology_dump(self):
        nodes = [x.neontology_dump() for x in self.nodes]
        relationships = [x.neontology_dump() for x in self.relationships]

        data = {"nodes": nodes, "links": relationships}

        return data

    def neontology_dump_json(self):
        nodes = [json.loads(x.neontology_dump_json()) for x in self.nodes]
        relationships = [
            json.loads(x.neontology_dump_json()) for x in self.relationships
        ]

        data = {"nodes": nodes, "links": relationships}

        return json.dumps(data)
