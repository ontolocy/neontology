from collections import defaultdict
from typing import Dict, Set, Type

from .basenode import BaseNode
from .baserelationship import BaseRelationship
from .graphconnection import GraphConnection


def get_node_types(base_type: Type[BaseNode] = BaseNode) -> Dict[str, Type[BaseNode]]:
    node_types = {}

    for subclass in base_type.__subclasses__():
        # we can define 'abstract' nodes which don't have a label
        # these are to provide common properties to be used by subclassed nodes
        # but shouldn't be put in the graph
        if (
            hasattr(subclass, "__primarylabel__")
            and subclass.__primarylabel__ is not None
        ):
            node_types[subclass.__primarylabel__] = subclass

        if subclass.__subclasses__():
            subclass_node_types = get_node_types(subclass)

            node_types.update(subclass_node_types)

    return node_types


def get_rels_by_type(
    base_type: Type[BaseRelationship] = BaseRelationship,
) -> Dict[str, dict]:
    rel_types: dict = defaultdict(dict)

    for rel_subclass in base_type.__subclasses__():
        # we can define 'abstract' relationships which don't have a label
        # these are to provide common properties to be used by subclassed relationships
        # but shouldn't be put in the graph
        if (
            hasattr(rel_subclass, "__relationshiptype__")
            and rel_subclass.__relationshiptype__ is not None
        ):
            rel_types[rel_subclass.__relationshiptype__] = {
                "rel_class": rel_subclass,
                "source_class": rel_subclass.model_fields["source"].annotation,
                "target_class": rel_subclass.model_fields["target"].annotation,
            }

        if rel_subclass.__subclasses__():
            subclass_rel_types = get_rels_by_type(rel_subclass)

            rel_types.update(subclass_rel_types)

    return rel_types


def all_subclasses(cls: type) -> set:
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def get_rels_by_node(
    base_type: Type[BaseRelationship] = BaseRelationship, by_source: bool = True
) -> Dict[str, Set[str]]:
    if by_source is True:
        node_dir = "source_class"

    else:
        node_dir = "target_class"

    all_rels = get_rels_by_type(base_type)

    by_node: Dict[str, Set[str]] = defaultdict(set)

    for rel_type, entry in all_rels.items():
        try:
            node_label = entry[node_dir].__primarylabel__
        except AttributeError:
            node_label = None

        if node_label is not None:
            by_node[node_label].add(rel_type)

        for node_subclass in all_subclasses(entry[node_dir]):
            subclass_label = node_subclass.__primarylabel__
            if subclass_label is not None:
                by_node[subclass_label].add(rel_type)

    return by_node


def get_rels_by_source(
    base_type: Type[BaseRelationship] = BaseRelationship,
) -> Dict[str, Set[str]]:
    return get_rels_by_node(by_source=True)


def get_rels_by_target(
    base_type: Type[BaseRelationship] = BaseRelationship,
) -> Dict[str, Set[str]]:
    return get_rels_by_node(by_source=False)


def auto_constrain() -> None:
    """Automatically apply constraints

    Get information about all the defined nodes in the current environment.

    Apply constraints based on the primary label and primary property for each node.
    """

    graph = GraphConnection()

    for node_label, node_type in get_node_types().items():
        graph.apply_constraint(node_label, node_type.__primaryproperty__)
