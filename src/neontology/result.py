import json
from typing import Any
from hashlib import sha1

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

        links = {
            sha1(
                x.neontology_dump_json().encode("utf-8")
            ).hexdigest(): x.neontology_dump()
            for x in self.relationships
        }

        # deduplicate nodes and links
        unique_nodes = list(nodes.values())
        unique_links = list(links.values())

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
