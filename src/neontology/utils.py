from __future__ import annotations

from collections import defaultdict

from .basenode import BaseNode
from .baserelationship import BaseRelationship, RelationshipTypeData
from .graphconnection import GraphConnection


def get_node_types(base_type: type[BaseNode] = BaseNode) -> dict[str, type[BaseNode]]:
    node_types = {}

    # if we're starting with a node type that has a primary label, include this in results
    # if it has a primary label, it is a concrete class and don't searcch subclasses
    if (
        hasattr(base_type, "__primarylabel__")
        and base_type.__primarylabel__ is not None
    ):
        node_types[base_type.__primarylabel__] = base_type
    else:
        for subclass in base_type.__subclasses__():
            # we can define 'abstract' nodes which don't have a label
            # these are to provide common properties to be used by subclassed nodes
            # but shouldn't be put in the graph
            subclass_node_types = get_node_types(subclass)

            # TODO: the update on existing causes weird class leakage failure in pytest
            #   When test_basenode and test_baserelationship are run sequentially
            # update_non_existing_inplace(node_types, subclass_node_types)
            node_types.update(subclass_node_types)

    return node_types


def generate_relationship_type_data(rel_class: type[BaseRelationship]):
    defined_source_class = rel_class.model_fields["source"].annotation
    defined_target_class = rel_class.model_fields["target"].annotation

    all_source_classes = list(get_node_types(defined_source_class).values())
    all_target_classes = list(get_node_types(defined_target_class).values())

    return RelationshipTypeData(
        relationship_class=rel_class,
        source_class=defined_source_class,
        target_class=defined_target_class,
        all_source_classes=all_source_classes,
        all_target_classes=all_target_classes,
    )


def get_rels_by_type(
    base_type: type[BaseRelationship] = BaseRelationship,
) -> dict[str, RelationshipTypeData]:
    rel_types: dict = defaultdict(dict)

    # if we're starting with a relationship type that has a relationshiptype, include this in results
    # if it has a relationshiptype, it is a concrete class and don't searcch subclasses
    if (
        hasattr(base_type, "__relationshiptype__")
        and base_type.__relationshiptype__ is not None
    ):
        rel_types[base_type.__relationshiptype__] = generate_relationship_type_data(
            base_type
        )
    else:
        for rel_subclass in base_type.__subclasses__():
            # we can define 'abstract' relationships which don't have a label
            # these are to provide common properties to be used by subclassed relationships
            # but shouldn't be put in the graph

            subclass_rel_types = get_rels_by_type(rel_subclass)

            update_non_existing_inplace(rel_types, subclass_rel_types)

    return rel_types


def update_non_existing_inplace(original_dict: dict, to_add: dict) -> None:
    for key in to_add.keys():
        if key not in original_dict:
            original_dict[key] = to_add[key]
        else:
            import warnings

            warnings.warn(
                UserWarning(
                    f"Duplicate dictionary key {key} ignored."
                    f" Requested {to_add[key]}, but already has {original_dict[key]}"
                )
            )


def all_subclasses(cls: type) -> set:
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def get_rels_by_node(
    base_type: type[BaseRelationship] = BaseRelationship, by_source: bool = True
) -> dict[str, set[str]]:
    if by_source is True:
        node_dir = "source_class"

    else:
        node_dir = "target_class"

    all_rels = get_rels_by_type(base_type)

    by_node: dict[str, set[str]] = defaultdict(set)

    for rel_type, entry in all_rels.items():
        try:
            node_label = entry.model_dump()[node_dir].__primarylabel__
        except AttributeError:
            node_label = None

        if node_label is not None:
            by_node[node_label].add(rel_type)

        for node_subclass in all_subclasses(entry.model_dump()[node_dir]):
            subclass_label = node_subclass.__primarylabel__
            if subclass_label is not None:
                by_node[subclass_label].add(rel_type)

    return by_node


def get_rels_by_source(
    base_type: type[BaseRelationship] = BaseRelationship,
) -> dict[str, set[str]]:
    return get_rels_by_node(base_type, by_source=True)


def get_rels_by_target(
    base_type: type[BaseRelationship] = BaseRelationship,
) -> dict[str, set[str]]:
    return get_rels_by_node(base_type, by_source=False)


def apply_neo4j_constraints(node_types: list[type[BaseNode]]) -> None:
    """Apply constraints based on primary properties for arbitrary set of node types"""

    graph = GraphConnection()

    for node_type in node_types:
        label = node_type.__primarylabel__
        if not label:
            raise ValueError(
                "Node must have an explicit primary label to apply a constraint."
            )
        graph.engine.apply_constraint(label, node_type.__primaryproperty__)


def auto_constrain_neo4j() -> None:
    """Automatically apply constraints

    Get information about all the defined nodes in the current environment.

    Apply constraints based on the primary label and primary property for each node.
    """

    node_types = list(get_node_types().values())

    apply_neo4j_constraints(node_types)
