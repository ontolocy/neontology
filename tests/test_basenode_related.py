"""Related nodes, related properties and schema generation."""

import json
from typing import ClassVar, Optional

import pytest
from models_basenode import SampleEnum
from pydantic import (
    Field,
    field_serializer,
)

from neontology import (
    BaseNode,
    BaseRelationship,
    GQLIdentifier,
    related_nodes,
    related_property,
)
from neontology.graphengines.capabilities import Capability


class AugmentedPerson(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "name"
    __primarylabel__: ClassVar[GQLIdentifier] = "AugmentedPerson"

    name: str
    optional_enum: Optional[SampleEnum] = Field(default_factory=lambda: SampleEnum.VALUE1)

    @field_serializer("optional_enum")
    def serialize_enum(self, value: Optional[SampleEnum]) -> Optional[str]:
        return value.value if value is not None else None

    @related_nodes
    def followers(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN o"

    @related_property
    def follower_count(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN COUNT(o)"

    @property
    @related_property
    def follower_names(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN COLLECT(DISTINCT o.name)"


class AugmentedPersonRelationship(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "AUGMENTED_PERSON_FOLLOWS"

    source: AugmentedPerson
    target: AugmentedPerson

    follow_tag: Optional[str] = None


def test_get_related_node_methods():
    assert set(AugmentedPerson.get_related_node_methods().keys()) == {
        "followers",
    }


def test_get_related_prop_methods():
    # note that only decorated methods, not properties are returned
    assert set(AugmentedPerson.get_related_property_methods().keys()) == {
        "follower_count",
    }


def test_node_schema():
    schema = AugmentedPerson.neontology_schema()

    assert schema.properties[0].name == "name"
    assert schema.properties[0].required is True
    assert schema.outgoing_relationships[0].name == "AUGMENTED_PERSON_FOLLOWS"


def test_node_schema_json():
    schema_json = AugmentedPerson.neontology_schema().model_dump_json()

    schema_dict = json.loads(schema_json)

    assert schema_dict["properties"][1]["type_annotation"]["core_type"] == "SampleEnum"

    assert schema_dict["properties"][1]["type_annotation"]["enum_values"] == [
        "value1",
        "value2",
        "value3",
    ]


def test_node_schema_md():
    schema = AugmentedPerson.neontology_schema()

    schema_md = schema.md_node_table()

    assert "| Property Name | Type | Required |" in schema_md
    assert "| name | str | True |" in schema_md


def test_rels_schema_md():
    schema = AugmentedPerson.neontology_schema()

    schema_md = schema.md_rel_tables(heading_level=4)

    assert "#### AUGMENTED_PERSON_FOLLOWS" in schema_md
    assert "| follow_tag | Optional[str] | False |" in schema_md


def test_related_nodes(engine, use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(source=alice, target=bob, follow_tag="test-tag")
    follows.merge()

    follows2 = AugmentedPersonRelationship(source=bob, target=alice, follow_tag="second-tag")
    follows2.merge()

    alice_rels = alice.get_related()

    related_nodes = [x for x in alice_rels.nodes if x.get_pp() != alice.get_pp()]

    assert len(related_nodes) == 1
    assert related_nodes[0].name == "Bob"

    # grand cypher has limited support for relationship property queries
    if engine.supports(Capability.RELATIONSHIP_PROPERTY_QUERIES):
        bobs_followers = bob.get_related(
            relationship_types=["AUGMENTED_PERSON_FOLLOWS"],
            incoming=True,
            outgoing=False,
            relationship_properties={"follow_tag": "test-tag"},
        )

        assert len(bobs_followers.nodes) == 2  # this will include bob himself
        assert bobs_followers.nodes[0].name == "Alice"

        bobs_rels = bob.get_related(incoming=True, distinct=True)

        assert len(bobs_rels.nodes) == 2
        assert bobs_rels.nodes[0].name == "Alice"

        assert len(bobs_rels.relationships) == 2


def test_related_nodes_unmerged(use_graph):
    alice = AugmentedPerson(name="Alice")

    alice_rels = alice.get_related()

    assert len(alice_rels.nodes) == 0


def test_related_nodes_no_rels(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    alice_rels = alice.get_related()

    assert len(alice_rels.nodes) == 0


def test_retrieve_property(engine, use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(source=alice, target=bob, follow_tag="test-tag")
    follows.merge()

    assert bob.follower_count() == 1

    # grand cypher behaves differently for returning collected values
    if engine.supports(Capability.COLLECT_DISTINCT):
        assert bob.follower_names == ["Alice"]


def test_retrieve_property_none(engine, use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    assert not bob.follower_count()

    if engine.supports(Capability.COLLECT_DISTINCT):
        assert not bob.follower_names


def test_retrieve_nodes_none(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    followers = bob.followers()

    assert len(followers) == 0


class TestDecoratedQueryFunctions:
    """A decorated function may return a query, or a (query, params) pair.

    Which shape it is used to be decided by unpacking optimistically and catching the
    failure, which called the function a second time and swallowed any ValueError the
    function itself raised.
    """

    def test_bare_query_string_calls_the_function_once(self, use_graph):
        calls = []

        class OnceBare(BaseNode):
            __primaryproperty__: ClassVar[str] = "name"
            __primarylabel__: ClassVar[Optional[str]] = "OnceBareNode"

            name: str

            @related_property
            def label(self):
                calls.append(1)
                return "MATCH (#ThisNode) RETURN ThisNode.name"

        node = OnceBare(name="x")
        node.merge()

        assert node.label() == "x"
        assert len(calls) == 1

    def test_query_and_params_calls_the_function_once(self, use_graph):
        calls = []

        class OncePair(BaseNode):
            __primaryproperty__: ClassVar[str] = "name"
            __primarylabel__: ClassVar[Optional[str]] = "OncePairNode"

            name: str

            @related_property
            def label(self):
                calls.append(1)
                return "MATCH (#ThisNode) RETURN ThisNode.name", {}

        node = OncePair(name="x")
        node.merge()

        assert node.label() == "x"
        assert len(calls) == 1

    def test_a_value_error_from_the_function_is_not_swallowed(self, use_graph):
        """A ValueError raised by the function is the author's bug, not a shape signal."""

        class Exploding(BaseNode):
            __primaryproperty__: ClassVar[str] = "name"
            __primarylabel__: ClassVar[Optional[str]] = "ExplodingNode"

            name: str

            @related_property
            def label(self):
                raise ValueError("something went wrong building the query")

        node = Exploding(name="x")
        node.merge()

        with pytest.raises(ValueError, match="something went wrong"):
            node.label()
