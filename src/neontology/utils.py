from __future__ import annotations

from collections import defaultdict

from .basenode import BaseNode
from .baserelationship import BaseRelationship, RelationshipTypeData
from .graphconnection import GraphConnection


def get_node_types(
    base_type: type[BaseNode] = BaseNode,
) -> dict[str, type[BaseNode]]:
    """Get a dictionary of node types keyed by primary label.

    This is based on the class hierarchy of BaseNode and its subclasses.

    Optionally pass in a node class to only retrieve classes and subclasses of that type.

    Args:
        base_type (type[BaseNode], optional): Node type to return subclass information for. Defaults to BaseNode.

    Returns:
        dict[str, type[BaseNode]]: Dictionary of node types keyed by primary label.
    """
    node_types = {}

    # if we're starting with a node type that has a primary label, include this in results
    if getattr(base_type, "__primarylabel__", None):
        node_types[base_type.__primarylabel__] = base_type

    for subclass in base_type.__subclasses__():
        # we can define 'abstract' nodes which don't have a label
        # these are to provide common properties to be used by subclassed nodes
        # but shouldn't be put in the graph
        if getattr(subclass, "__primarylabel__", None):
            node_types[subclass.__primarylabel__] = subclass

        node_types.update(get_node_types(subclass))

    return node_types


def generate_relationship_type_data(
    rel_class: type[BaseRelationship],
) -> RelationshipTypeData:
    """Generate relationship type data for a given relationship class."""
    defined_source_class = rel_class.model_fields["source"].annotation
    defined_target_class = rel_class.model_fields["target"].annotation

    if not defined_source_class or not defined_target_class:
        raise ValueError(f"Relationship {rel_class.__name__} must have source and target classes defined.")

    all_source_classes = list(get_node_types(defined_source_class).values())
    all_target_classes = list(get_node_types(defined_target_class).values())

    return RelationshipTypeData(
        relationship_class=rel_class,
        source_class=defined_source_class,
        target_class=defined_target_class,
        all_source_classes=all_source_classes,
        all_target_classes=all_target_classes,
    )


def _validate_relationship_nodes(rel_class: type[BaseRelationship]) -> bool:
    """Validate that a relationship class has valid source and target node classes defined.

    This handles cases where the relationship class is defined but the source or target are ForwardRefs
        which may be resolved later.
    """
    defined_source_class = rel_class.model_fields["source"].annotation
    defined_target_class = rel_class.model_fields["target"].annotation

    if not isinstance(defined_source_class, type) or not isinstance(defined_target_class, type):
        return False

    return True


def get_rels_by_type(
    base_type: type[BaseRelationship] = BaseRelationship,
) -> dict[str, RelationshipTypeData]:
    """Get a dictionary of relationship type data keyed by relationship type.

    Optionally pass in a relationship class to only retrieve classes and subclasses of that type.
    """
    rel_types: dict = defaultdict(dict)

    if getattr(base_type, "__relationshiptype__", None) and _validate_relationship_nodes(base_type):
        rel_types[base_type.__relationshiptype__] = generate_relationship_type_data(base_type)

    for rel_subclass in base_type.__subclasses__():
        # we can define 'abstract' relationships which don't have a label
        # these are to provide common properties to be used by subclassed relationships
        # but shouldn't be put in the graph

        if getattr(rel_subclass, "__relationshiptype__", None) and _validate_relationship_nodes(rel_subclass):
            rel_types[rel_subclass.__relationshiptype__] = generate_relationship_type_data(rel_subclass)

        rel_types.update(get_rels_by_type(rel_subclass))

    return rel_types


def all_subclasses(cls: type) -> set:
    """Recursively get all subclasses of a given class."""
    subclasses = set(cls.__subclasses__())
    for subclass in cls.__subclasses__():
        subclasses.update(all_subclasses(subclass))
    return subclasses


def get_rels_by_node(base_type: type[BaseRelationship] = BaseRelationship, by_source: bool = True) -> dict[str, set[str]]:
    """Get a dictionary of relationship types by source or target node type."""
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
    """Get a dictionary of relationship types by source node type."""
    return get_rels_by_node(base_type, by_source=True)


def get_rels_by_target(
    base_type: type[BaseRelationship] = BaseRelationship,
) -> dict[str, set[str]]:
    """Get a dictionary of relationship types by target node type."""
    return get_rels_by_node(base_type, by_source=False)


def apply_neo4j_constraints(node_types: list[type[BaseNode]]) -> None:
    """Apply constraints based on primary properties for arbitrary set of node types."""
    graph = GraphConnection()

    for node_type in node_types:
        label = node_type.__primarylabel__
        if not label:
            raise ValueError("Node must have an explicit primary label to apply a constraint.")
        graph.engine.apply_constraint(label, node_type.__primaryproperty__)


def auto_constrain_neo4j() -> None:
    """Automatically apply constraints.

    Get information about all the defined nodes in the current environment.

    Apply constraints based on the primary label and primary property for each node.
    """
    node_types = list(get_node_types().values())

    apply_neo4j_constraints(node_types)
