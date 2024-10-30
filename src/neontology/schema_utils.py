from __future__ import annotations

import enum
from logging import getLogger
from typing import Any, List, Optional, Union, get_args, get_origin

from jinja2 import Template
from pydantic import BaseModel

logger = getLogger(__name__)


class NeontologyAnnotationData(BaseModel):
    representation: str
    core_type: Any
    optional: bool = False
    union: bool = False
    enum_values: Optional[list] = None


class SchemaProperty(BaseModel):
    name: str
    type_annotation: NeontologyAnnotationData
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

    def md_node_table(self) -> str:
        """Take a node schema and produce markdown ontology documentation"""

        schema_template_raw = """
| Property Name | Type | Required |
| ------------- | ---- | -------- |
{% for field in model_schema.properties -%}
| {{field.name}} | {{field.type_annotation.representation}} | {{field.required}} |
{% endfor %}
"""

        schema_template = Template(schema_template_raw)

        return schema_template.render(model_schema=self).strip()

    def md_rel_tables(self, heading_level: int = 3) -> str:
        """Take a node schema and produce markdown ontology documentation"""

        schema_template_raw = """
{% if model_schema.outgoing_relationships %}
{% for outgoing_rel in model_schema.outgoing_relationships -%}
{{"#"*heading_level}} {{ outgoing_rel.relationship_type }}

Target Label(s): {{ outgoing_rel.target_labels |join(', ') }}
{% if outgoing_rel.properties %}
| Property Name | Type | Required |
| ------------- | ---- | -------- |
{% for rel_prop in outgoing_rel.properties -%}
| {{rel_prop.name}} | {{rel_prop.type_annotation.representation}} | {{rel_prop.required}} |
{% endfor %}
{% endif %}

{% endfor %}
{% endif %}
"""

        schema_template = Template(schema_template_raw)

        return schema_template.render(
            model_schema=self, heading_level=heading_level
        ).strip()


def extract_type_mapping(
    annotation: Any, show_optional: bool = True
) -> NeontologyAnnotationData:
    if isinstance(annotation, type):
        # we have a plain type, just return the name

        if issubclass(annotation, enum.Enum):
            enum_values = [e.value for e in annotation]
            return NeontologyAnnotationData(
                representation="Enum",
                core_type=annotation,
                enum_values=enum_values,
            )

        return NeontologyAnnotationData(
            representation=str(annotation.__name__), core_type=annotation
        )

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
                        representation = (
                            f"Optional[{extract_type_mapping(entry).representation}]"
                        )
                        core_type = extract_type_mapping(entry).core_type
                        return NeontologyAnnotationData(
                            optional=True,
                            representation=representation,
                            core_type=core_type,
                        )
                    else:
                        return extract_type_mapping(entry)
                raise TypeError(f"Unsupported type annotation: {annotation}")
        else:
            raise TypeError(f"Unsupported union type: {annotation}")

    elif get_origin(annotation) == list:
        if len(get_args(annotation)) == 1:
            try:
                # field type is something like typing.List[str]
                representation = str(annotation).split(".")[1]

            except IndexError:
                # some Python versions support just list[x]
                representation = str(annotation).title()
            return NeontologyAnnotationData(
                representation=representation,
                core_type=annotation,
            )
        else:
            raise TypeError(f"Cannot have lists of multiple types: {annotation}")

    logger.warn(f"Complex type annotation: {annotation}")
    return NeontologyAnnotationData(
        representation=str(annotation.__name__), core_type=annotation
    )
