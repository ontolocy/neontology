from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Set, Type, Union, get_args, get_origin

from jinja2 import Template
from pydantic import BaseModel

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


def apply_neo4j_constraints(node_types: List[type[BaseNode]]) -> None:
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


def extract_type_mapping(annotation: Any, show_optional: bool = True) -> str:
    if isinstance(annotation, type):
        # we have a plain type, just return the name
        return annotation.__name__
    elif get_origin(annotation) == Union:
        # We can only support union's of a single type plus none (i.e. Optional)
        if len(get_args(annotation)) == 2 and type(None) in get_args(annotation):
            # we need to extract the optional type
            for entry in get_args(annotation):
                if isinstance(entry, type(None)):
                    pass
                else:
                    # we do this recursively in case the next layer down is something like List[int]
                    if show_optional is True:
                        return f"Optional[{extract_type_mapping(entry)}]"
                    else:
                        return extract_type_mapping(entry)
                raise TypeError(f"Unsupported type annotation: {annotation}")
        else:
            raise TypeError(f"Unsupported union type: {annotation}")
    elif get_origin(annotation) == list:
        if len(get_args(annotation)) == 1:
            # field type will be something like typing.List[str]
            # just return the List[str] bit
            return str(annotation).split(".")[1]
        else:
            raise TypeError(f"Cannot have lists of multiple types: {annotation}")

    raise TypeError(f"Unsupported type annotation: {annotation}")


class SchemaProperty(BaseModel):
    name: str
    type_annotation: str
    required: bool


class RelationshipSchema(BaseModel):
    name: str
    relationship_type: str
    source_labels: List[str]
    target_labels: List[str]

    properties: List[SchemaProperty]


class NodeSchema(BaseModel):
    label: str
    title: str
    secondary_labels: List[str]
    properties: List[SchemaProperty]
    outgoing_relationships: List[RelationshipSchema] = []


def generate_node_schema(
    node_type: type[BaseNode], include_outgoing_rels: bool = True
) -> NodeSchema:
    if not node_type.__primarylabel__:
        raise ValueError(
            "Node does not have a primary label defined for generating schema."
        )

    schema_dict: dict = {}
    schema_dict["label"] = node_type.__primarylabel__
    schema_dict["title"] = node_type.__name__
    schema_dict["secondary_labels"] = node_type.__secondarylabels__

    model_properties: list = []

    for field_name, field_props in node_type.model_fields.items():
        field_type = extract_type_mapping(field_props.annotation, show_optional=True)

        node_property = SchemaProperty(
            type_annotation=field_type,
            name=field_name,
            required=field_props.is_required(),
        )

        if field_props.is_required() is True:
            model_properties.insert(0, node_property)

        # put optional fields at the end
        else:
            model_properties.append(node_property)

    schema_dict["properties"] = model_properties
    schema_dict["outgoing_relationships"] = []

    if include_outgoing_rels is False:
        return NodeSchema(**schema_dict)

    outgoing_rels = get_rels_by_source().get(node_type.__primarylabel__, set())
    all_rel_types = get_rels_by_type()

    for rel in outgoing_rels:
        rel_props: list = []

        # first pull out any additional properties on the relationship
        for field_name, field_props in all_rel_types[rel][
            "rel_class"
        ].model_fields.items():
            if field_name not in ["source", "target"]:
                field_type = extract_type_mapping(
                    field_props.annotation, show_optional=True
                )

                prop_entry = SchemaProperty(
                    type_annotation=field_type,
                    name=field_name,
                    required=field_props.is_required(),
                )

                if field_props.is_required() is True:
                    rel_props.insert(0, prop_entry)

                # put optional fields at the end
                else:
                    rel_props.append(prop_entry)

        # handle situations where we have an abstract target label defined
        if (
            hasattr(all_rel_types[rel]["target_class"], "__primarylabel__")
            and all_rel_types[rel]["target_class"].__primarylabel__
        ):
            target_labels = [all_rel_types[rel]["target_class"].__primarylabel__]

        else:
            # return concrete subclasses of the abstract node class given
            retrieved_node_types = get_node_types(all_rel_types[rel]["target_class"])
            target_labels = list(retrieved_node_types.keys())

        rel_schema = RelationshipSchema(
            relationship_type=rel,
            name=rel,
            target_labels=target_labels,
            source_labels=[schema_dict["label"]],
            properties=rel_props,
        )

        schema_dict["outgoing_relationships"].append(rel_schema)

    return NodeSchema(**schema_dict)


def schema_to_markdown(node_schema: NodeSchema) -> str:
    """Take a node schema and produce markdown ontology documentation"""

    schema_template_raw = """
# {{model_schema.label}}

Primary Label: {{model_schema.label}}

Python Class Name: {{model_schema.title}}

{%if model_schema.secondary_labels %}{{model_schema.secondary_labels}}{% endif %}

## Node Properties

| Property Name | Type | Required |
| ------------- | ---- | -------- |
{% for field in model_schema.properties -%}
| {{field.name}} | {{field.type_annotation}} | {{field.required}} |
{% endfor %}

{% if model_schema.outgoing_relationships %}
## Outgoing Relationships

{% for outgoing_rel in model_schema.outgoing_relationships -%}
### {{ outgoing_rel.relationship_type }}

Target Label(s): {{ outgoing_rel.target_labels |join(', ') }}
{% if outgoing_rel.properties %}
| Property Name | Type | Required |
| ------------- | ---- | -------- |
{% for rel_prop in outgoing_rel.properties -%}
| {{rel_prop.name}} | {{rel_prop.type_annotation}} | {{rel_prop.required}} |
{% endfor %}
{% endif %}

{% endfor %}
{% endif %}
"""

    schema_template = Template(schema_template_raw)

    return schema_template.render(model_schema=node_schema).strip()
