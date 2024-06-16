# type: ignore
from typing import ClassVar, Optional, List
from datetime import datetime
from uuid import UUID

import pandas as pd
from pydantic import Field, field_validator
import pytest

from neontology import BaseNode, BaseRelationship, retrieve_nodes, retrieve_property


class PracticeNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "PracticeNode"

    pp: str


def test_set_pp_field_valid():
    tn = PracticeNode(pp="Some Value")

    assert tn.pp == "Some Value"


def test_create(use_graph):
    tn = PracticeNode(pp="Test Node")

    tn.create()

    cypher = """
    MATCH (n:PracticeNode)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert result.nodes[0].__primarylabel__ == "PracticeNode"

    assert result.nodes[0].pp == "Test Node"


def test_no_primary_label():
    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    with pytest.raises(AttributeError):
        SpecialPracticeNode(pp="Test Node")


def test_none_primary_label():
    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = None
        pp: str

    with pytest.raises(NotImplementedError):
        SpecialPracticeNode(pp="Test Node")


def test_create_multilabel(use_graph):
    # Not all engines support multiple labels
    if use_graph.engine.__class__.__name__ in ["KuzuEngine"]:
        return

    class MultipleLabelNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "PrimaryLabel"
        __secondarylabels__: ClassVar[Optional[list]] = ["ExtraLabel1", "ExtraLabel2"]
        pp: str

    tn = MultipleLabelNode(pp="Test Node")

    tn.create()

    cypher = """
    MATCH (n:ExtraLabel1)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert result.nodes[0].__primarylabel__ == "PrimaryLabel"

    # confirm the secondary labels were written to the database
    assert "ExtraLabel1" in result.records_raw[0].values()[0].labels
    assert "ExtraLabel2" in result.records_raw[0].values()[0].labels

    assert result.nodes[0].pp == "Test Node"


def test_create_multilabel_inheritance(use_graph):
    # Not all engines support multiple labels
    if use_graph.engine.__class__.__name__ in ["KuzuEngine"]:
        return

    class Mammal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Mammal"]
        pp: str

    class Human(Mammal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Human"
        pp: str

    tn = Human(pp="Bob")

    tn.create()

    cypher = """
    MATCH (n:Human)
    WHERE n.pp = 'Bob'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert "Human" in result.records_raw[0].values()[0].labels
    assert "Mammal" in result.records_raw[0].values()[0].labels

    assert result.nodes[0].pp == "Bob"


def test_create_multilabel_inheritance_multiple(use_graph):
    # Not all engines support multiple labels
    if use_graph.engine.__class__.__name__ in ["KuzuEngine"]:
        return

    class Animal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
        pp: str

    class Human(Animal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Human"
        pp: str

    class Elephant(Animal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Elephant"
        pp: str

    human = Human(pp="Bob")

    human.create()

    elephant = Elephant(pp="Bob")

    # this should create a new node because the primary label is different
    elephant.create()

    elephant2 = Elephant(pp="Bob")

    # this shouldn't create a new node because the primary label is the same
    elephant2.merge()

    cypher = """
    MATCH (n:Animal)
    WHERE n.pp = 'Bob'
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2


def test_merge_defined_label_inherited(use_graph):
    # Not all engines support multiple labels
    if use_graph.engine.__class__.__name__ in ["KuzuEngine"]:
        return

    class Mammal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Mammal"]
        pp: str

    class Human(Mammal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Human"
        pp: str

    tn = Human(pp="Bob")

    tn.merge()

    cypher = """
    MATCH (n:Human)
    WHERE n.pp = 'Bob'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert "Human" in result.records_raw[0].values()[0].labels
    assert "Mammal" in result.records_raw[0].values()[0].labels

    assert result.nodes[0].pp == "Bob"


class SpecialPracticeNode(BaseNode):
    __primarylabel__: ClassVar[Optional[str]] = "SpecialTestLabel1"
    __primaryproperty__: ClassVar[str] = "pp"

    pp: str


def test_merge_multiple_defined_label(use_graph):
    tn = SpecialPracticeNode(pp="Special Test Node")

    tn2 = SpecialPracticeNode(pp="Special Test Node2")

    SpecialPracticeNode.merge_nodes([tn, tn2])

    cypher = """
    MATCH (n:SpecialTestLabel1)
    RETURN COLLECT(DISTINCT n.pp) as node_names
    """

    results = use_graph.evaluate_query_single(cypher)

    assert "Special Test Node" in results
    assert "Special Test Node2" in results


def test_create_multiple_defined_label(use_graph):
    tn = SpecialPracticeNode(pp="Special Test Node")

    tn2 = SpecialPracticeNode(pp="Special Test Node2")

    SpecialPracticeNode.create_nodes([tn, tn2])

    cypher = """
    MATCH (n:SpecialTestLabel1)
    RETURN COLLECT(DISTINCT n.pp) as node_names
    """

    results = use_graph.evaluate_query_single(cypher)

    assert "Special Test Node" in results
    assert "Special Test Node2" in results


def test_creation_datetime(use_graph):
    """
    Check we can manually define the created datetime, and then check we can
    query for it using neo4j DateTime type.
    """

    # Not all engines support the same datetime/timestamp operations
    if use_graph.engine.__class__.__name__ in ["KuzuEngine"]:
        return

    my_datetime = datetime(year=2022, month=5, day=4, hour=3, minute=21)

    bn = PracticeNode(pp="Test Node", created=my_datetime)

    bn.create()

    cypher = """
    MATCH (n:PracticeNode)
    WHERE n.pp = 'Test Node'
    RETURN n.created.year
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2022


def test_match_nodes(use_graph):
    tn = PracticeNode(pp="Special Test Node")

    tn2 = PracticeNode(pp="Special Test Node2")

    PracticeNode.merge_nodes([tn, tn2])

    results = PracticeNode.match_nodes()

    for result in results:
        assert isinstance(result, PracticeNode)

    pps = [x.pp for x in results]

    assert "Special Test Node" in pps

    assert "Special Test Node2" in pps


def test_match_nodes_limit(use_graph):
    tn = PracticeNode(pp="Special Test Node")
    tn.merge()

    tn2 = PracticeNode(pp="Special Test Node2")

    tn2.merge()

    results = PracticeNode.match_nodes(limit=1)

    assert len(results) == 1

    # match nodes returns the most recently created node first
    assert results[0].pp == "Special Test Node2"


def test_match_nodes_skip(use_graph):
    tn = PracticeNode(pp="Special Test Node")
    tn.merge()

    tn2 = PracticeNode(pp="Special Test Node2")

    tn2.merge()

    results = PracticeNode.match_nodes(limit=1, skip=1)

    assert len(results) == 1

    # match nodes returns the most recently created node first
    assert results[0].pp == "Special Test Node"


def test_match_node(use_graph):
    tn = PracticeNode(pp="Special Test Node")

    tn.create()

    result = PracticeNode.match("Special Test Node")

    assert isinstance(result, PracticeNode)

    assert "Special Test Node" == result.pp


def test_match_none(use_graph):
    bn = PracticeNode(pp="Test Node")
    bn.create()

    result = PracticeNode.match("Does Not Exist")

    assert result is None


def test_match_many_none(use_graph):
    class PracticeNode2(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[str] = "PracticeNode2"

        pp: str

    bn = PracticeNode2(pp="Test Node")
    bn.create()

    results = PracticeNode.match_nodes()

    assert results == []


def test_delete_node(use_graph):
    tn = PracticeNode(pp="Special Test Node")

    tn.create()

    result = PracticeNode.match("Special Test Node")

    assert isinstance(result, PracticeNode)

    PracticeNode.delete("Special Test Node")

    result = PracticeNode.match("Special Test Node")

    assert result is None


class ModelTestString(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelString"
    pp: str
    test_prop_string: str


class ModelTestInt(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelInt"
    pp: str
    test_prop_int: int


class ModelTestTuple(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelTuple"
    pp: str
    test_prop_tuple: tuple


class ModelTestSet(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelSet"
    pp: str
    test_prop_set: set


class ModelTestUUID(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelUUID"
    pp: str
    test_prop_uuid: UUID


class ModelTestDateTime(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelDateTime"
    pp: str
    test_prop_datetime: datetime


class ModelTestStringList(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelStringList"
    pp: str
    test_prop_list: list


class ModelTestIntListExplicit(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelIntListExplicit"
    pp: str
    test_prop_int_list_exp: List[int]


@pytest.mark.parametrize(
    "test_model,test_prop,input_value,expected_value",
    [
        (ModelTestString, "test_prop_string", "hello world", ["hello world"]),
        (ModelTestInt, "test_prop_int", 5071, [5071]),
        (ModelTestTuple, "test_prop_tuple", ("hello", "world"), [["hello", "world"]]),
        (
            ModelTestSet,
            "test_prop_set",
            {"foo", "bar"},
            [["bar", "foo"], ["foo", "bar"]],
        ),
        (
            ModelTestUUID,
            "test_prop_uuid",
            UUID("32d4a4cb-29c3-4aa8-9b55-7790431819e3"),
            ["32d4a4cb-29c3-4aa8-9b55-7790431819e3"],
        ),
        (
            ModelTestDateTime,
            "test_prop_datetime",
            datetime(year=1984, month=1, day=2),
            [datetime(year=1984, month=1, day=2)],
        ),
        (
            ModelTestStringList,
            "test_prop_list",
            ["foo", "bar"],
            [["foo", "bar"]],
        ),
        (
            ModelTestIntListExplicit,
            "test_prop_int_list_exp",
            [1, 2, 3],
            [[1, 2, 3]],
        ),
    ],
)
def test_property_types(use_graph, test_model, test_prop, input_value, expected_value):
    pp = "test_node"

    input_data = {"pp": pp}
    input_data[test_prop] = input_value

    testmodel = test_model(**input_data)

    testmodel.create()

    result = test_model.match(pp)

    assert result.model_dump()[test_prop] == input_value

    cypher = f"""
    MATCH (n:{test_model.__primarylabel__})
    WHERE n.pp = 'test_node'
    RETURN n.{test_prop}
    """

    cypher_result = use_graph.evaluate_query_single(cypher)

    # in the case of sets, we may get the result back ordered one of two ways
    # therefore, we check that the result is one of the expected values rather
    assert cypher_result in expected_value


def test_empty_list_property(use_graph):
    class TestModelListProp(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel1"
        pp: str
        list_prop: list

    pp = "test_node"

    testmodel = TestModelListProp(list_prop=[], pp=pp)

    testmodel.create()

    result = TestModelListProp.match(pp)

    assert result.list_prop == []


def test_set_on_match(use_graph):
    """Check that we successfully identify field to set on match"""

    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel2"
        pp: str = "test_node"
        only_set_on_match: Optional[str] = Field(
            json_schema_extra={"set_on_match": True}, default=None
        )
        normal_field: str

    test_node = TestModel(only_set_on_match="Foo", normal_field="Bar", pp="test_node")
    test_node.merge()

    cypher = """
    MATCH (n:TestModel2)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    assert cypher_result.nodes[0].only_set_on_match is None
    assert cypher_result.nodes[0].normal_field == "Bar"

    test_node2 = TestModel(only_set_on_match="Foo", normal_field="Bar", pp="test_node")
    test_node2.merge()

    cypher_result2 = use_graph.evaluate_query(cypher)

    assert cypher_result2.nodes[0].only_set_on_match == "Foo"


def test_set_on_create(use_graph):
    """Check that we successfully identify field to set on match"""

    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel3"
        pp: str = "test_node"
        only_set_on_create: str = Field(json_schema_extra={"set_on_create": True})
        normal_field: str

    test_node = TestModel(only_set_on_create="Foo", normal_field="Bar", pp="test_node")
    test_node.merge()

    cypher = """
    MATCH (n:TestModel3)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    assert cypher_result.nodes[0].only_set_on_create == "Foo"
    assert cypher_result.nodes[0].normal_field == "Bar"

    test_node2 = TestModel(only_set_on_create="Fee", normal_field="Fi", pp="test_node")
    test_node2.merge()

    cypher_result2 = use_graph.evaluate_query(cypher)

    assert cypher_result2.nodes[0].only_set_on_create == "Foo"
    assert cypher_result2.nodes[0].normal_field == "Fi"


class Person(BaseNode):
    __primaryproperty__: ClassVar[str] = "identifier"
    __primarylabel__: ClassVar[str] = (
        "PersonLabel1"  # optionally specify the label to use
    )

    name: str
    age: int
    identifier: Optional[str] = Field(default=None, validate_default=True)

    @field_validator("identifier")
    def set_identifier(cls, v, values):
        if v is None:
            v = f"{values.data['name']}_{values.data['age']}"

        return v


def test_merge_df_with_duplicates(use_graph):
    people_records = [
        {"name": "arthur", "age": 70},
        {"name": "betty", "age": 65},
        {"name": "betty", "age": 65},
        {"name": "ted", "age": 50},
        {"name": "betty", "age": 75},
        {"name": "arthur", "age": 70},
    ]

    people_df = pd.DataFrame.from_records(people_records)

    results = Person.merge_df(people_df)

    names = [x.name for x in results]

    assert names == ["arthur", "betty", "betty", "ted", "betty", "arthur"]

    assert results[0].identifier == results[5].identifier

    assert results[1].identifier == results[2].identifier

    assert results[1].identifier != results[4].identifier

    assert results[3].name == "ted"


class Person2(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[str] = (
        "PersonLabel2"  # optionally specify the label to use
    )

    name: str
    age: int
    favorite_colors: Optional[list] = None


def test_merge_df_with_lists(use_graph):
    people_records = [
        {"name": "arthur", "age": 70, "favorite_colors": ["red"]},
        {"name": "betty", "age": 65, "favorite_colors": ["red", "blue"]},
        {"name": "ted", "age": 50, "favorite_colors": []},
        {"name": "ben", "age": 75},
    ]

    people_df = pd.DataFrame.from_records(people_records)

    Person2.merge_df(people_df, deduplicate=False)

    arthur = Person2.match("arthur")
    assert arthur.favorite_colors == ["red"]

    betty = Person2.match("betty")
    assert betty.favorite_colors == ["red", "blue"]

    ted = Person2.match("ted")
    assert ted.favorite_colors == []

    ben = Person2.match("ben")
    assert ben.favorite_colors is None


def test_merge_empty_df():
    df = pd.DataFrame()

    result = PracticeNode.merge_df(df)

    assert len(result) == 0
    assert isinstance(result, pd.Series)


class AugmentedPerson(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[str] = "AugmentedPerson"

    name: str

    @retrieve_nodes
    def followers(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN o"

    @retrieve_property
    def follower_count(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN COUNT(DISTINCT o)"


class AugmentedPersonRelationship(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "AUGMENTED_PERSON_FOLLOWS"

    source: AugmentedPerson
    target: AugmentedPerson

    follow_tag: Optional[str] = None


def test_related_nodes(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(
        source=alice, target=bob, follow_tag="test-tag"
    )
    follows.merge()

    follows2 = AugmentedPersonRelationship(
        source=bob, target=alice, follow_tag="second-tag"
    )
    follows2.merge()

    alice_rels = alice.related_nodes()

    assert len(alice_rels) == 1
    assert alice_rels[0].name == "Bob"

    bobs_followers = bob.related_nodes(
        relationship_types=["AUGMENTED_PERSON_FOLLOWS"],
        incoming=True,
        outgoing=False,
        relationship_properties={"follow_tag": "test-tag"},
    )

    assert len(bobs_followers) == 1
    assert bobs_followers[0].name == "Alice"

    bobs_rels = bob.related_nodes(incoming=True, distinct=True)

    assert len(bobs_rels) == 1
    assert bobs_rels[0].name == "Alice"


def test_retrieve_property(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(
        source=alice, target=bob, follow_tag="test-tag"
    )
    follows.merge()

    assert bob.follower_count() == 1


def test_retrieve_nodes(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(
        source=alice, target=bob, follow_tag="test-tag"
    )
    follows.merge()

    followers = bob.followers()

    assert len(followers) == 1
    assert followers[0].name == "Alice"
