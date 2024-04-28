import itertools
import warnings
from typing import List

from neo4j import Record as Neo4jRecord
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from pydantic import BaseModel, computed_field


def neo4j_node_to_neontology_node(neo4j_node, node_classes):
    node_labels = list(neo4j_node.labels)

    primary_labels = set(node_labels).intersection(set(node_classes.keys()))

    secondary_labels = set(node_labels).difference(set(node_classes.keys()))

    if len(primary_labels) == 1:
        primary_label = primary_labels.pop()

        node = node_classes[primary_label](**dict(neo4j_node))

        # warn if the secondary labels aren't what's expected

        if set(node.__secondarylabels__) != secondary_labels:
            warnings.warn(f"Unexpected secondary labels returned: {secondary_labels}")

        return node

    # gracefully handle cases where we don't have a class defined
    # for the identified label or where we get more than one valid primary label
    else:
        warnings.warn(f"Unexpected primary labels returned: {primary_labels}")

        return None


def neo4j_records_to_neontology_records(
    records: List[Neo4jRecord], node_classes: dict, rel_classes: dict
) -> list:
    new_records = []

    for record in records:
        new_record = {"nodes": {}, "relationships": {}}
        for key, entry in record.items():
            if isinstance(entry, Neo4jNode):
                neontology_node = neo4j_node_to_neontology_node(entry, node_classes)

                if neontology_node:
                    new_record["nodes"][key] = neontology_node

            elif isinstance(entry, Neo4jRelationship):
                rel_type = entry.type

                rel_dict = rel_classes[rel_type]

                if not rel_dict:
                    warnings.warn(
                        (
                            f"Could not find a class for {rel_type} relationship type."
                            " Did you define the class before initializing Neontology?"
                        )
                    )
                    continue

                if not entry.nodes[0].labels or not entry.nodes[1].labels:
                    warnings.warn(
                        (
                            f"{rel_type} relationship type query did not include nodes."
                            " To get neontology relationships, return source and target "
                            "nodes as part of result."
                        )
                    )
                    continue

                src_label = list(entry.nodes[0].labels)[0]
                tgt_label = list(entry.nodes[1].labels)[0]

                src_node = node_classes[src_label](**dict(entry.nodes[0]))
                tgt_node = node_classes[tgt_label](**dict(entry.nodes[1]))

                rel_props = dict(entry)
                rel_props["source"] = src_node
                rel_props["target"] = tgt_node

                rel = rel_dict["rel_class"](**rel_props)

                new_record["relationships"][key] = rel

        new_records.append(new_record)

    return new_records


class NeontologyResult(BaseModel):
    records: list
    neontology_records: list

    @computed_field
    @property
    def nodes(self) -> list:
        nodes_list_of_lists = [x["nodes"].values() for x in self.neontology_records]
        return list(itertools.chain.from_iterable(nodes_list_of_lists))

    @computed_field
    @property
    def relationships(self) -> list:
        nodes_list_of_lists = [
            x["relationships"].values() for x in self.neontology_records
        ]
        return list(itertools.chain.from_iterable(nodes_list_of_lists))

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
