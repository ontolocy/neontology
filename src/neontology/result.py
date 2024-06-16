from typing import Any


from pydantic import BaseModel, computed_field


class NeontologyResult(BaseModel):
    records_raw: Any
    records: list
    nodes: list
    relationships: list

    @computed_field
    @property
    def node_link_data(self) -> dict:
        nodes = [
            {
                "id": x.get_primary_property_value(),
                "label": x.__primarylabel__,
                "name": str(x),
            }
            for x in self.nodes
        ]

        links = [
            {
                "source": x.source.get_primary_property_value(),
                "target": x.target.get_primary_property_value(),
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
