"""Describing an ontology: its nodes, relationships and properties.

`get_ontology_schema()` describes the models Neontology knows about as plain, JSON
serialisable data, so documentation can be generated from the models themselves. The
renderers turn that data into Markdown pages and Mermaid diagrams.

Each ontology here is scoped to its own abstract base class, so its schema is independent
of the other models the suite defines.
"""

import enum
import json
from datetime import date, datetime
from typing import Annotated, Any, ClassVar, List, Literal, Optional, Union
from uuid import UUID

import pytest
from pydantic import Field

from neontology import BaseNode, BaseRelationship, get_ontology_schema
from neontology.schema import index_markdown, mermaid_diagram, node_markdown


class Colour(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Unconvertible:
    """A type pydantic cannot describe in JSON Schema."""


# --- property types ----------------------------------------------------------------------


class SchemaTypesBase(BaseNode):
    """Abstract base for the property type cases."""

    __primaryproperty__: ClassVar[str] = "pp"

    pp: str


# annotation, type name, nullable, allowed values
TYPE_CASES = [
    (str, "str", False, None),
    (int, "int", False, None),
    (datetime, "datetime", False, None),
    (UUID, "UUID", False, None),
    (Optional[str], "str | None", True, None),
    (str | None, "str | None", True, None),
    (list[str], "list[str]", False, None),
    (List[str], "list[str]", False, None),
    (list[datetime], "list[datetime]", False, None),
    (list[UUID], "list[UUID]", False, None),
    (Optional[list[int]], "list[int] | None", True, None),
    (list[str] | None, "list[str] | None", True, None),
    (list[Optional[str]], "list[str | None]", False, None),
    (set[str], "set[str]", False, None),
    (tuple[str, ...], "tuple[str, ...]", False, None),
    (Colour, "Colour", False, ["red", "blue"]),
    (Optional[Colour], "Colour | None", True, ["red", "blue"]),
    (list[Colour], "list[Colour]", False, ["red", "blue"]),
    (Literal["a", "b"], "Literal['a', 'b']", False, ["a", "b"]),
    (Literal["only"], "Literal['only']", False, ["only"]),
    (Union[str, int], "str | int", False, None),
    (Any, "Any", True, None),
    (Unconvertible, "Unconvertible", False, None),
]


@pytest.mark.parametrize(
    "index, annotation, type_name, nullable, allowed_values",
    [(index, *case) for index, case in enumerate(TYPE_CASES)],
    ids=[case[1] for case in TYPE_CASES],
)
def test_property_types_are_described(index, annotation, type_name, nullable, allowed_values):
    node = type(
        f"SchemaTypeCase{index}",
        (SchemaTypesBase,),
        {"__annotations__": {"value": annotation}, "__primarylabel__": f"SchemaTypeCase{index}"},
    )

    value = next(prop for prop in node.neontology_schema().properties if prop.name == "value")

    assert (value.type, value.nullable, value.allowed_values) == (type_name, nullable, allowed_values)


def test_a_schema_with_every_type_serialises_to_json():
    node = type(
        "SchemaEveryType",
        (SchemaTypesBase,),
        {
            "__annotations__": {f"value_{index}": annotation for index, (annotation, *_) in enumerate(TYPE_CASES)},
            "__primarylabel__": "SchemaEveryType",
        },
    )

    dumped = json.loads(node.neontology_schema().model_dump_json())

    assert len(dumped["properties"]) == len(TYPE_CASES) + 1


# --- an ontology to describe -------------------------------------------------------------


class SchemaThing(BaseNode):
    """Abstract: everything in this ontology."""

    __primaryproperty__: ClassVar[str] = "ident"
    __inheritablelabels__: ClassVar[list[str]] = ["SchemaThing"]

    ident: str = Field(description="Unique identifier.")


class SchemaOrganisation(SchemaThing):
    """An organisation people can work for."""

    __primarylabel__: ClassVar[Optional[str]] = "SchemaOrganisation"

    founded: date | None = None


class SchemaPerson(SchemaThing):
    """Anyone we know about."""

    __primarylabel__: ClassVar[Optional[str]] = "SchemaPerson"
    __inheritablelabels__: ClassVar[list[str]] = ["SchemaPerson"]

    name: str | None = Field(
        default=None,
        description="Full name.\n\nAs they prefer it written.",
        examples=["Ada Lovelace"],
        json_schema_extra={"index": True},
    )
    nickname: Optional[str] = Field(default=None, alias="known_as")
    favourite: Colour = Colour.RED
    created: datetime = Field(default_factory=datetime.now, json_schema_extra={"set_on_create": True})
    updated: Optional[datetime] = Field(default=None, json_schema_extra={"set_on_match": True, "unit": "timestamp"})


class SchemaEmployee(SchemaPerson):
    """A person employed by an organisation."""

    __primarylabel__: ClassVar[Optional[str]] = "SchemaEmployee"

    staff_number: Annotated[int, Field(gt=0, json_schema_extra={"unique": True})]


class SchemaWorksFor(BaseRelationship):
    """Employment."""

    __relationshiptype__: ClassVar[Optional[str]] = "SCHEMA_WORKS_FOR"

    source: SchemaPerson
    target: SchemaOrganisation

    role: str = Field(json_schema_extra={"merge_on": True}, description="Job title.")
    since: date | None = None


class SchemaKnows(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "SCHEMA_KNOWS"

    source: SchemaThing
    target: SchemaPerson


def qualified(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


@pytest.fixture
def schema():
    return get_ontology_schema(SchemaThing)


def node(schema, cls):
    return next(n for n in schema.nodes if n.qualified_name == qualified(cls))


def relationship(schema, rel_type):
    return next(r for r in schema.relationships if r.relationship_type == rel_type)


def prop(item, name):
    return next(p for p in item.properties if p.name == name)


class TestOntology:
    def test_nodes_are_listed_in_definition_order_with_abstract_classes(self, schema):
        assert [(n.class_name, n.label, n.abstract) for n in schema.nodes] == [
            ("SchemaThing", None, True),
            ("SchemaOrganisation", "SchemaOrganisation", False),
            ("SchemaPerson", "SchemaPerson", False),
            ("SchemaEmployee", "SchemaEmployee", False),
        ]

    def test_relationships_touching_the_ontology_are_listed(self, schema):
        assert [r.relationship_type for r in schema.relationships] == ["SCHEMA_WORKS_FOR", "SCHEMA_KNOWS"]

    def test_the_schema_serialises_to_json(self, schema):
        dumped = json.loads(schema.model_dump_json())

        assert [n["class_name"] for n in dumped["nodes"]][2] == "SchemaPerson"

    def test_a_single_classes_schema_matches_its_entry_in_the_ontology(self, schema):
        assert SchemaPerson.neontology_schema() == node(schema, SchemaPerson)
        assert SchemaWorksFor.neontology_schema() == relationship(schema, "SCHEMA_WORKS_FOR")


class TestSchemaOfModels:
    """A schema of exactly the classes given, as a library exporting its models would be described."""

    def test_the_models_given_in_order_without_repeats(self):
        schema = get_ontology_schema(models=[SchemaPerson, SchemaWorksFor, SchemaOrganisation, SchemaPerson])

        assert [n.class_name for n in schema.nodes] == ["SchemaPerson", "SchemaOrganisation"]
        assert [r.relationship_type for r in schema.relationships] == ["SCHEMA_WORKS_FOR"]

    def test_node_relationships_are_limited_to_those_in_the_schema(self):
        schema = get_ontology_schema(models=[SchemaPerson, SchemaOrganisation, SchemaWorksFor])

        person = node(schema, SchemaPerson)

        assert (person.outgoing_relationships, person.incoming_relationships) == (["SCHEMA_WORKS_FOR"], [])

    def test_entries_otherwise_match_the_full_schema(self, schema):
        subset = get_ontology_schema(models=[SchemaOrganisation, SchemaWorksFor])

        assert subset.nodes[0].properties == node(schema, SchemaOrganisation).properties
        assert subset.relationships[0] == relationship(schema, "SCHEMA_WORKS_FOR")

    def test_base_type_and_models_cannot_both_be_given(self):
        with pytest.raises(ValueError):
            get_ontology_schema(SchemaThing, models=[SchemaPerson])

    def test_models_must_be_node_or_relationship_classes(self):
        with pytest.raises(TypeError, match="str"):
            get_ontology_schema(models=[str])


class TestNodes:
    def test_descriptions_come_from_docstrings(self, schema):
        assert node(schema, SchemaPerson).description == "Anyone we know about."
        assert node(schema, SchemaThing).description == "Abstract: everything in this ontology."

    def test_labels_primary_property_and_parents(self, schema):
        employee = node(schema, SchemaEmployee)

        assert employee.label == "SchemaEmployee"
        assert set(employee.secondary_labels) == {"SchemaPerson", "SchemaThing"}
        assert employee.primary_property == "ident"
        assert employee.parents == [qualified(SchemaPerson)]

    def test_properties_include_inherited_ones_in_definition_order(self, schema):
        assert [p.name for p in node(schema, SchemaEmployee).properties] == [
            "ident",
            "name",
            "known_as",
            "favourite",
            "created",
            "updated",
            "staff_number",
        ]

    def test_relationships_include_those_inherited_from_parent_classes(self, schema):
        employee = node(schema, SchemaEmployee)
        organisation = node(schema, SchemaOrganisation)

        assert employee.outgoing_relationships == ["SCHEMA_WORKS_FOR", "SCHEMA_KNOWS"]
        assert employee.incoming_relationships == ["SCHEMA_KNOWS"]
        assert organisation.outgoing_relationships == ["SCHEMA_KNOWS"]
        assert organisation.incoming_relationships == ["SCHEMA_WORKS_FOR"]

    def test_an_abstract_class_can_be_described(self):
        thing = SchemaThing.neontology_schema()

        assert (thing.abstract, thing.label, [p.name for p in thing.properties]) == (True, None, ["ident"])


class TestProperties:
    def test_description_examples_and_default(self, schema):
        person = node(schema, SchemaPerson)

        name = prop(person, "name")

        assert name.description == "Full name.\n\nAs they prefer it written."
        assert name.examples == ["Ada Lovelace"]
        assert prop(person, "favourite").default == "red"

    def test_a_default_factory_is_not_required_and_has_no_default_value(self, schema):
        created = prop(node(schema, SchemaPerson), "created")

        assert (created.required, created.default) == (False, None)

    def test_the_primary_property_is_flagged(self, schema):
        assert [p.name for p in node(schema, SchemaEmployee).properties if p.primary_property] == ["ident"]

    def test_set_on_create_and_set_on_match_are_flagged(self, schema):
        person = node(schema, SchemaPerson)

        assert (prop(person, "created").set_on_create, prop(person, "created").set_on_match) == (True, False)
        assert (prop(person, "updated").set_on_create, prop(person, "updated").set_on_match) == (False, True)

    def test_index_and_unique_are_flagged(self, schema):
        name = prop(node(schema, SchemaPerson), "name")
        staff_number = prop(node(schema, SchemaEmployee), "staff_number")

        assert (name.index, name.unique) == (True, False)
        assert (staff_number.index, staff_number.unique) == (False, True)

    def test_a_property_is_named_as_the_graph_stores_it(self, schema):
        assert "known_as" in [p.name for p in node(schema, SchemaPerson).properties]

    def test_json_schema_carries_constraints_and_custom_keys(self, schema):
        assert prop(node(schema, SchemaEmployee), "staff_number").json_schema["exclusiveMinimum"] == 0
        assert prop(node(schema, SchemaPerson), "updated").json_schema["unit"] == "timestamp"

    def test_required_properties(self, schema):
        employee = node(schema, SchemaEmployee)

        assert [p.name for p in employee.properties if p.required] == ["ident", "staff_number"]


class TestRelationships:
    def test_a_relationship_is_described(self, schema):
        works_for = relationship(schema, "SCHEMA_WORKS_FOR")

        assert works_for.class_name == "SchemaWorksFor"
        assert works_for.description == "Employment."
        assert (works_for.source, works_for.target) == (qualified(SchemaPerson), qualified(SchemaOrganisation))
        assert [p.name for p in works_for.properties] == ["role", "since"]
        assert prop(works_for, "role").merge_on is True

    def test_endpoint_labels_include_every_concrete_class_which_can_be_there(self, schema):
        works_for = relationship(schema, "SCHEMA_WORKS_FOR")
        knows = relationship(schema, "SCHEMA_KNOWS")

        assert works_for.source_labels == ["SchemaPerson", "SchemaEmployee"]
        assert works_for.target_labels == ["SchemaOrganisation"]
        assert knows.source_labels == ["SchemaOrganisation", "SchemaPerson", "SchemaEmployee"]


def link(node):
    return f"{node.class_name}.md"


class TestMarkdown:
    def test_a_node_page(self, schema):
        page = node_markdown(node(schema, SchemaPerson), schema, link=link)

        assert page.startswith("# SchemaPerson\n")
        assert "Anyone we know about." in page
        assert "| name | str \\| None | No | Full name. As they prefer it written. |" in page
        assert "One of: `red`, `blue`." in page
        assert "## Outgoing relationships" in page
        assert "### SCHEMA_WORKS_FOR" in page
        assert "[SchemaOrganisation](SchemaOrganisation.md)" in page
        assert "| role | str | Yes | Job title. |" in page

    def test_incoming_relationships_are_included_when_asked_for(self, schema):
        person = node(schema, SchemaPerson)

        assert "Incoming relationships" not in node_markdown(person, schema, link=link)

        page = node_markdown(person, schema, incoming=True, link=link)

        assert "## Incoming relationships" in page
        assert "### SCHEMA_KNOWS" in page
        assert "[SchemaOrganisation](SchemaOrganisation.md)" in page.split("## Incoming relationships")[1]

    def test_without_a_link_function_nodes_are_named_not_linked(self, schema):
        page = node_markdown(node(schema, SchemaPerson), schema, incoming=True)

        assert "](" not in page
        assert "SchemaOrganisation" in page

    def test_heading_level(self, schema):
        page = node_markdown(node(schema, SchemaPerson), schema, heading_level=2)

        assert page.startswith("## SchemaPerson\n")
        assert "### Outgoing relationships" in page

    def test_an_abstract_node_page_says_so(self, schema):
        assert "abstract" in node_markdown(node(schema, SchemaThing), schema).lower()

    def test_a_node_page_links_to_the_classes_inheriting_from_it(self, schema):
        page = node_markdown(node(schema, SchemaThing), schema, link=link)

        assert "- Inherited by: [SchemaOrganisation](SchemaOrganisation.md), [SchemaPerson](SchemaPerson.md)\n" in page

    def test_an_index_page(self, schema):
        page = index_markdown(schema, link=link)

        assert "[SchemaPerson](SchemaPerson.md)" in page
        assert "Anyone we know about." in page
        assert "SCHEMA_WORKS_FOR" in page


class TestMermaid:
    def test_the_whole_ontology(self, schema):
        diagram = mermaid_diagram(schema)

        assert diagram.startswith("classDiagram\n")
        assert "SchemaThing <|-- SchemaPerson" in diagram
        assert "SchemaPerson <|-- SchemaEmployee" in diagram
        assert "SchemaPerson --> SchemaOrganisation : SCHEMA_WORKS_FOR" in diagram
        assert "<<abstract>>" in diagram

    def test_a_diagram_around_one_node(self, schema):
        diagram = mermaid_diagram(schema, focus=node(schema, SchemaOrganisation))

        assert "SchemaThing <|-- SchemaOrganisation" in diagram
        assert "SchemaPerson --> SchemaOrganisation : SCHEMA_WORKS_FOR" in diagram
        assert "SchemaEmployee" not in diagram

    def test_relationships_leaving_a_scoped_schema_are_still_drawn(self):
        # the organisation is outside the schema, as the Markdown pages still name it
        diagram = mermaid_diagram(get_ontology_schema(SchemaPerson))

        assert "SchemaPerson --> SchemaOrganisation : SCHEMA_WORKS_FOR" in diagram
        assert "SchemaThing --> SchemaPerson : SCHEMA_KNOWS" in diagram


class TestDeprecatedMarkdownMethods:
    def test_md_node_table(self):
        with pytest.warns(DeprecationWarning, match="node_markdown"):
            table = SchemaPerson.neontology_schema().md_node_table()

        assert "| ident | str | Yes | Unique identifier. |" in table

    def test_md_rel_tables(self):
        with pytest.warns(DeprecationWarning, match="node_markdown"):
            tables = SchemaPerson.neontology_schema().md_rel_tables(heading_level=4)

        assert "#### SCHEMA_WORKS_FOR" in tables
        assert "| role | str | Yes | Job title. |" in tables
