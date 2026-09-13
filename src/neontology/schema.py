"""Describing an ontology: the nodes, relationships and properties your models define.

`get_ontology_schema()` describes the models Neontology knows about as plain data, which
serialises to JSON, so documentation and tooling can be generated from the models
themselves. `node_markdown()`, `index_markdown()` and `mermaid_diagram()` render that data;
anything else can be built from it directly.

Property types, descriptions, defaults, allowed values and constraints are read from each
model's pydantic JSON Schema, so a property is described exactly as pydantic validates it.
"""

from __future__ import annotations

import inspect
import types
import warnings
from typing import Any, Callable, Literal, Optional, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema

from .basenode import BaseNode
from .baserelationship import BaseRelationship, RelationshipTypeData
from .utils import generate_relationship_type_data, get_node_types, get_rels_by_type


class PropertySchema(BaseModel):
    """A property of a node or relationship.

    Attributes:
        name (str): the key the property is stored under in the graph - its alias, where
            it has one.
        type (str): the type as it would be written in Python, such as `list[str] | None`.
        required (bool): whether a value must be given, because there is no default.
        nullable (bool): whether None is accepted.
        default (Any): the default value, as JSON. None where there is no default, or it is
            produced by a factory.
        description (Optional[str]): the field's description.
        examples (Optional[list[Any]]): the field's examples.
        allowed_values (Optional[list[Any]]): the values it, or each of its items, may
            take, where the type is an Enum or Literal.
        primary_property (bool): whether this is the node's primary property.
        set_on_create (bool): whether it is only set when the node or relationship is created.
        set_on_match (bool): whether it is only set when an existing one is matched.
        merge_on (bool): whether a relationship is merged on it.
        json_schema (dict[str, Any]): the property's JSON Schema from pydantic, including any
            constraints and any custom keys given with `json_schema_extra`.
    """

    name: str
    type: str
    required: bool
    nullable: bool
    default: Any = None
    description: Optional[str] = None
    examples: Optional[list[Any]] = None
    allowed_values: Optional[list[Any]] = None
    primary_property: bool = False
    set_on_create: bool = False
    set_on_match: bool = False
    merge_on: bool = False
    json_schema: dict[str, Any] = {}


class NodeSchema(BaseModel):
    """A node class.

    Classes are identified by their qualified name, which stays unique where two classes
    share a name. Abstract classes are described too, since they define shared properties.

    Attributes:
        class_name (str): the class' name.
        qualified_name (str): the class' module and qualified name.
        label (Optional[str]): the primary label, or None for an abstract class.
        secondary_labels (list[str]): every other label the node carries, inheritable
            labels included.
        abstract (bool): whether the class is abstract, and so never written to the graph.
        description (Optional[str]): the class' docstring.
        primary_property (Optional[str]): the primary property's name.
        parents (list[str]): the qualified names of the node classes it directly inherits from.
        properties (list[PropertySchema]): its properties, inherited ones included, in the
            order they are defined.
        outgoing_relationships (list[str]): the types of relationship it can be the source
            of, including those declared on a class it inherits from.
        incoming_relationships (list[str]): the types of relationship it can be the target of.
    """

    class_name: str
    qualified_name: str
    label: Optional[str]
    secondary_labels: list[str]
    abstract: bool
    description: Optional[str] = None
    primary_property: Optional[str] = None
    parents: list[str] = []
    properties: list[PropertySchema] = []
    outgoing_relationships: list[str] = []
    incoming_relationships: list[str] = []

    def md_node_table(self) -> str:
        """Render this node's properties as a Markdown table.

        Deprecated since v3.0: use `node_markdown()`, which renders a whole page.

        Returns:
            str: the table.
        """
        warnings.warn(
            "NodeSchema.md_node_table is deprecated and will be removed in v4. Use neontology.schema.node_markdown instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        return _properties_table(self.properties)

    def md_rel_tables(self, heading_level: int = 3) -> str:
        """Render this node's outgoing relationships as Markdown.

        Deprecated since v3.0: use `node_markdown()`, which renders a whole page.

        Args:
            heading_level (int): the heading level for each relationship. Defaults to 3.

        Returns:
            str: a heading and property table for each relationship.
        """
        warnings.warn(
            "NodeSchema.md_rel_tables is deprecated and will be removed in v4. Use neontology.schema.node_markdown instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        relationships = get_rels_by_type()

        lines: list[str] = []

        for rel_type in self.outgoing_relationships:
            data = relationships.get(rel_type)

            if data:
                rel = _relationship_schema(data.relationship_class, data)

                lines += _relationship_section(rel, False, heading_level, {}, None)

        return "\n".join(lines).strip()


class RelationshipSchema(BaseModel):
    """A relationship class.

    Attributes:
        relationship_type (str): the relationship type.
        class_name (str): the class' name.
        qualified_name (str): the class' module and qualified name.
        description (Optional[str]): the class' docstring.
        source (str): the qualified name of the node class declared as its source.
        target (str): the qualified name of the node class declared as its target.
        source_labels (list[str]): the label of every concrete node class which can be its
            source - the declared class and any class inheriting from it.
        target_labels (list[str]): the label of every concrete node class which can be its target.
        properties (list[PropertySchema]): its properties, in the order they are defined.
    """

    relationship_type: str
    class_name: str
    qualified_name: str
    description: Optional[str] = None
    source: str
    target: str
    source_labels: list[str]
    target_labels: list[str]
    properties: list[PropertySchema] = []


class OntologySchema(BaseModel):
    """The node and relationship classes of an ontology.

    Attributes:
        nodes (list[NodeSchema]): the node classes, in the order they were defined, with
            abstract classes before the classes inheriting from them.
        relationships (list[RelationshipSchema]): the relationship classes, in the order
            they were defined.
    """

    nodes: list[NodeSchema] = []
    relationships: list[RelationshipSchema] = []


def get_ontology_schema(base_type: Optional[type[BaseNode]] = None) -> OntologySchema:
    """Describe the ontology your models define.

    Models are known once they are defined, so import them first.

    Args:
        base_type (Optional[type[BaseNode]]): if given, describe only the node classes
            inheriting from it - pass an abstract node class to describe one branch of your
            models. A relationship is included when a node class described can be either end
            of it.

    Returns:
        OntologySchema: the node and relationship classes.
    """
    concrete = list(get_node_types(base_type or BaseNode).values())

    classes: list[type[BaseNode]] = []

    for cls in concrete:
        # abstract classes are described too, ahead of the classes inheriting from them
        for ancestor in reversed(cls.__mro__):
            if (
                issubclass(ancestor, BaseNode)
                and ancestor is not BaseNode
                and (base_type is None or issubclass(ancestor, base_type))
                and (ancestor._is_abstract() or ancestor in concrete)
                and ancestor not in classes
            ):
                classes.append(ancestor)

    relationships = [
        (rel_type, data)
        for rel_type, data in get_rels_by_type().items()
        if any(issubclass(cls, data.source_class) or issubclass(cls, data.target_class) for cls in classes)
    ]

    return OntologySchema(
        nodes=[_node_schema(cls, relationships) for cls in classes],
        relationships=[_relationship_schema(data.relationship_class, data) for _, data in relationships],
    )


# --- building the schema -----------------------------------------------------------------


class _LenientJsonSchema(GenerateJsonSchema):
    """Describe what pydantic cannot put in JSON Schema as {}, rather than raising.

    Neontology models allow arbitrary types, which pydantic validates but cannot describe.
    """

    def handle_invalid_for_json_schema(self, schema: Any, error_info: str) -> dict:
        """Describe a type JSON Schema cannot express as an empty schema.

        Args:
            schema (Any): the pydantic core schema.
            error_info (str): why it cannot be expressed.

        Returns:
            dict: an empty schema.
        """
        return {}


def _qualified_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _description(cls: type) -> Optional[str]:
    return inspect.cleandoc(cls.__doc__) if cls.__doc__ else None


def _type_name(annotation: Any) -> str:
    """Write an annotation as it would appear in Python, such as `list[str] | None`."""
    if annotation is type(None):
        return "None"

    if annotation is Any:
        return "Any"

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin in (Union, types.UnionType):
        return " | ".join(_type_name(arg) for arg in args)

    if origin is Literal:
        return f"Literal[{', '.join(repr(arg) for arg in args)}]"

    if origin is not None:
        name = getattr(origin, "__name__", str(origin))
        inner = ", ".join("..." if arg is Ellipsis else _type_name(arg) for arg in args)

        return f"{name}[{inner}]" if args else name

    return getattr(annotation, "__name__", str(annotation))


def _is_nullable(annotation: Any) -> bool:
    if annotation is Any or annotation is type(None):
        return True

    return get_origin(annotation) in (Union, types.UnionType) and type(None) in get_args(annotation)


def _resolve_refs(fragment: Any, defs: dict, resolving: frozenset = frozenset()) -> Any:
    """Inline the `$ref` links in a JSON Schema fragment, so it stands on its own.

    A reference already being resolved further up is left as it is, so a recursive type
    cannot recurse forever.
    """
    if isinstance(fragment, list):
        return [_resolve_refs(item, defs, resolving) for item in fragment]

    if not isinstance(fragment, dict):
        return fragment

    reference = fragment.get("$ref", "").rsplit("/", 1)[-1]

    if reference in defs and reference not in resolving:
        fragment = {**defs[reference], **{k: v for k, v in fragment.items() if k != "$ref"}}
        resolving = resolving | {reference}

    return {key: _resolve_refs(value, defs, resolving) for key, value in fragment.items()}


def _allowed_values(fragment: dict) -> Optional[list]:
    """Find the values an Enum or Literal allows, in a property's JSON Schema."""
    if "enum" in fragment:
        return list(fragment["enum"])

    if "const" in fragment:
        return [fragment["const"]]

    for branch in fragment.get("anyOf", []):
        values = _allowed_values(branch)

        if values is not None:
            return values

    items = fragment.get("items")

    return _allowed_values(items) if isinstance(items, dict) else None


def _properties(model: type[BaseModel], primary_property: Optional[str] = None, skip: tuple = ()) -> list[PropertySchema]:
    json_schema = model.model_json_schema(schema_generator=_LenientJsonSchema)

    defs = json_schema.get("$defs", {})

    properties = []

    for field_name, field in model.model_fields.items():
        if field_name in skip:
            continue

        # stored under its alias where it has one, as the JSON Schema also names it
        name = field.alias or field_name

        fragment = _resolve_refs(json_schema.get("properties", {}).get(name, {}), defs)

        properties.append(
            PropertySchema(
                name=name,
                type=_type_name(field.annotation),
                required=field.is_required(),
                nullable=_is_nullable(field.annotation),
                default=fragment.get("default"),
                description=field.description,
                examples=field.examples,
                allowed_values=_allowed_values(fragment),
                primary_property=name == primary_property,
                set_on_create=fragment.get("set_on_create") is True,
                set_on_match=fragment.get("set_on_match") is True,
                merge_on=fragment.get("merge_on") is True,
                json_schema=fragment,
            )
        )

    return properties


def _node_schema(cls: type[BaseNode], relationships: Optional[list[tuple[str, RelationshipTypeData]]] = None) -> NodeSchema:
    """Describe a node class.

    Args:
        cls (type[BaseNode]): the node class.
        relationships (Optional[list[tuple[str, RelationshipTypeData]]]): the relationships
            it may take part in, by type. Defaults to every relationship defined.

    Returns:
        NodeSchema: the description.
    """
    if relationships is None:
        relationships = list(get_rels_by_type().items())

    primary_property = getattr(cls, "__primaryproperty__", None)

    return NodeSchema(
        class_name=cls.__name__,
        qualified_name=_qualified_name(cls),
        label=None if cls._is_abstract() else cls.__primarylabel__,
        secondary_labels=cls._all_labels()[1:],
        abstract=cls._is_abstract(),
        description=_description(cls),
        primary_property=primary_property,
        parents=[_qualified_name(base) for base in cls.__bases__ if issubclass(base, BaseNode) and base is not BaseNode],
        properties=_properties(cls, primary_property),
        outgoing_relationships=[rel_type for rel_type, data in relationships if issubclass(cls, data.source_class)],
        incoming_relationships=[rel_type for rel_type, data in relationships if issubclass(cls, data.target_class)],
    )


def _relationship_schema(cls: type[BaseRelationship], data: Optional[RelationshipTypeData] = None) -> RelationshipSchema:
    """Describe a relationship class.

    Args:
        cls (type[BaseRelationship]): the relationship class.
        data (Optional[RelationshipTypeData]): its source and target, if already known.

    Returns:
        RelationshipSchema: the description.

    Raises:
        ValueError: if the class is abstract, with no relationship type.
    """
    if cls._is_abstract():
        raise ValueError(f"{cls.__name__} has no __relationshiptype__, so it is abstract and has no relationship to describe.")

    if data is None:
        data = generate_relationship_type_data(cls)

    return RelationshipSchema(
        relationship_type=cls.__relationshiptype__,
        class_name=cls.__name__,
        qualified_name=_qualified_name(cls),
        description=_description(cls),
        source=_qualified_name(data.source_class),
        target=_qualified_name(data.target_class),
        source_labels=list(get_node_types(data.source_class)),
        target_labels=list(get_node_types(data.target_class)),
        properties=_properties(cls, skip=("source", "target")),
    )


# --- rendering ---------------------------------------------------------------------------

#: turns a node into the URL of its page, or None to name it without linking
Link = Callable[[NodeSchema], Optional[str]]


def _cell(text: Any) -> str:
    """Make text safe for a Markdown table cell: on one line, with pipes escaped."""
    return " ".join(str(text).split()).replace("|", "\\|")


def _summary(description: Optional[str]) -> str:
    return description.split("\n\n")[0] if description else ""


def _node_ref(node: Optional[NodeSchema], fallback: str, link: Optional[Link]) -> str:
    if node is None:
        return f"`{fallback}`"

    url = link(node) if link else None

    return f"[{node.class_name}]({url})" if url else node.class_name


def _properties_table(properties: list[PropertySchema]) -> str:
    rows = ["| Property | Type | Required | Description |", "| --- | --- | --- | --- |"]

    for prop in properties:
        notes = [prop.description] if prop.description else []

        if prop.allowed_values:
            notes.append("One of: " + ", ".join(f"`{value}`" for value in prop.allowed_values) + ".")

        required = "Yes" if prop.required else "No"

        rows.append(f"| {_cell(prop.name)} | {_cell(prop.type)} | {required} | {_cell(' '.join(notes))} |")

    return "\n".join(rows)


def _relationship_section(
    rel: RelationshipSchema, incoming: bool, heading_level: int, by_label: dict[str, NodeSchema], link: Optional[Link]
) -> list[str]:
    labels = rel.source_labels if incoming else rel.target_labels
    ends = ", ".join(_node_ref(by_label.get(label), label, link) for label in labels)

    lines = [f"{'#' * heading_level} {rel.relationship_type}", ""]

    if rel.description:
        lines += [rel.description, ""]

    lines += [f"{'From' if incoming else 'To'}: {ends}", ""]

    if rel.properties:
        lines += [_properties_table(rel.properties), ""]

    return lines


def node_markdown(
    node: NodeSchema,
    schema: OntologySchema,
    *,
    incoming: bool = False,
    link: Optional[Link] = None,
    heading_level: int = 1,
) -> str:
    """Render a Markdown page for a node class.

    The page describes the class, its properties, and the relationships it can be the
    source of - and, if asked, the target of - with each relationship's properties.

    Args:
        node (NodeSchema): the node class to describe.
        schema (OntologySchema): the ontology it belongs to, which describes its relationships.
        incoming (bool): whether to include incoming relationships. Defaults to False.
        link (Optional[Link]): turns a node into the URL of its page, to link the nodes
            mentioned. Defaults to naming them without links.
        heading_level (int): the level of the page's title. Defaults to 1.

    Returns:
        str: the page.
    """
    heading = "#" * heading_level
    by_name = {n.qualified_name: n for n in schema.nodes}
    by_label = {n.label: n for n in schema.nodes if n.label}
    by_type = {r.relationship_type: r for r in schema.relationships}

    lines = [f"{heading} {node.class_name}", ""]

    if node.description:
        lines += [node.description, ""]

    if node.abstract:
        facts = ["Abstract: never written to the graph, but shares its properties with the classes inheriting from it."]

    else:
        facts = [f"Label: `{node.label}`"]

    if node.secondary_labels:
        facts.append("Secondary labels: " + ", ".join(f"`{label}`" for label in node.secondary_labels))

    if node.primary_property:
        facts.append(f"Primary property: `{node.primary_property}`")

    if node.parents:
        facts.append(
            "Inherits from: "
            + ", ".join(_node_ref(by_name.get(parent), parent.rsplit(".", 1)[-1], link) for parent in node.parents)
        )

    children = [n for n in schema.nodes if node.qualified_name in n.parents]

    if children:
        facts.append("Inherited by: " + ", ".join(_node_ref(child, child.class_name, link) for child in children))

    lines += [f"- {fact}" for fact in facts] + [""]

    if node.properties:
        lines += [f"{heading}# Properties", "", _properties_table(node.properties), ""]

    sections = [(False, "Outgoing", node.outgoing_relationships)]

    if incoming:
        sections.append((True, "Incoming", node.incoming_relationships))

    for is_incoming, title, rel_types in sections:
        described = [by_type[rel_type] for rel_type in rel_types if rel_type in by_type]

        if described:
            lines += [f"{heading}# {title} relationships", ""]

            for rel in described:
                lines += _relationship_section(rel, is_incoming, heading_level + 2, by_label, link)

    return "\n".join(lines).rstrip() + "\n"


def index_markdown(
    schema: OntologySchema,
    *,
    link: Optional[Link] = None,
    heading_level: int = 1,
    title: str = "Ontology",
) -> str:
    """Render a Markdown index of an ontology's node and relationship classes.

    Args:
        schema (OntologySchema): the ontology.
        link (Optional[Link]): turns a node into the URL of its page. Defaults to naming
            nodes without links.
        heading_level (int): the level of the page's title. Defaults to 1.
        title (str): the page's title. Defaults to "Ontology".

    Returns:
        str: the page.
    """
    heading = "#" * heading_level
    by_label = {n.label: n for n in schema.nodes if n.label}

    lines = [f"{heading} {title}", "", f"{heading}# Nodes", "", "| Node | Label | Description |", "| --- | --- | --- |"]

    for node in schema.nodes:
        label = f"`{node.label}`" if node.label else "abstract"

        lines.append(f"| {_node_ref(node, node.class_name, link)} | {label} | {_cell(_summary(node.description))} |")

    if schema.relationships:
        lines += [
            "",
            f"{heading}# Relationships",
            "",
            "| Relationship | From | To | Description |",
            "| --- | --- | --- | --- |",
        ]

        for rel in schema.relationships:
            sources = ", ".join(_node_ref(by_label.get(label), label, link) for label in rel.source_labels)
            targets = ", ".join(_node_ref(by_label.get(label), label, link) for label in rel.target_labels)

            lines.append(f"| {rel.relationship_type} | {sources} | {targets} | {_cell(_summary(rel.description))} |")

    return "\n".join(lines) + "\n"


def mermaid_diagram(schema: OntologySchema, focus: Optional[NodeSchema] = None) -> str:
    """Render a Mermaid class diagram of an ontology.

    Node classes are drawn as classes, inheritance as inheritance, and each relationship
    as an arrow between the classes it is declared on, labelled with its type.

    Args:
        schema (OntologySchema): the ontology.
        focus (Optional[NodeSchema]): if given, draw only this node class, the classes it
            inherits from or is inherited by, and its relationships.

    Returns:
        str: the diagram, to put in a `mermaid` code block.
    """
    if focus is None:
        nodes = list(schema.nodes)
        relationships = list(schema.relationships)

    else:
        rel_types = set(focus.outgoing_relationships) | set(focus.incoming_relationships)
        relationships = [r for r in schema.relationships if r.relationship_type in rel_types]

        wanted = {focus.qualified_name, *focus.parents}
        wanted |= {n.qualified_name for n in schema.nodes if focus.qualified_name in n.parents}
        wanted |= {end for r in relationships for end in (r.source, r.target)}

        nodes = [n for n in schema.nodes if n.qualified_name in wanted]

    drawn = {n.qualified_name for n in nodes}

    def name(qualified_name: str) -> str:
        return qualified_name.rsplit(".", 1)[-1]

    lines = ["classDiagram"]

    for node in nodes:
        lines.append(f"    class {node.class_name}")

        if node.abstract:
            lines.append(f"    <<abstract>> {node.class_name}")

    for node in nodes:
        lines += [f"    {name(parent)} <|-- {node.class_name}" for parent in node.parents if parent in drawn]

    # an end outside a scoped schema is still drawn, as Mermaid adds any class an arrow names
    for rel in relationships:
        lines.append(f"    {name(rel.source)} --> {name(rel.target)} : {rel.relationship_type}")

    return "\n".join(lines) + "\n"
