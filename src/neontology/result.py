import json
from typing import Any

from pydantic import BaseModel, computed_field


class NeontologyResult(BaseModel):
    records_raw: Any
    records: list
    nodes: list
    relationships: list
    paths: list

    @computed_field  # type: ignore[misc]
    @property
    def node_link_data(self) -> dict:
        nodes = [
            {
                **x.neontology_dump(),
                **{
                    "__pp__": x.get_pp(),
                    "__str__": str(x),
                },
            }
            for x in self.nodes
        ]

        links = [x.neontology_dump() for x in self.relationships]

        unique_nodes = list(
            {frozenset(tuple(item.items())): item for item in nodes}.values()
        )
        unique_links = list(
            {frozenset(tuple(item.items())): item for item in links}.values()
        )
        data = {"nodes": unique_nodes, "edges": unique_links, "directed": True}

        return data

    def neontology_dump(self) -> dict:
        nodes = [x.neontology_dump() for x in self.nodes]
        relationships = [x.neontology_dump() for x in self.relationships]

        data = {"nodes": nodes, "edges": relationships}

        return data

    def neontology_dump_json(self) -> str:
        nodes = [json.loads(x.neontology_dump_json()) for x in self.nodes]
        relationships = [
            json.loads(x.neontology_dump_json()) for x in self.relationships
        ]

        data = {"nodes": nodes, "edges": relationships}

        return json.dumps(data)
