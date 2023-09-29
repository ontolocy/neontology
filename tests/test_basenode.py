# type: ignore
from typing import ClassVar, Optional
from datetime import datetime
from uuid import UUID

import pandas as pd
from pydantic import Field, field_validator
import pytest

from neontology import BaseNode


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

    result = use_graph.evaluate(cypher)

    assert result.has_label("PracticeNode")

    assert result.get("pp") == "Test Node"


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

    result = use_graph.evaluate(cypher)

    assert result.has_label("PrimaryLabel")
    assert result.has_label("ExtraLabel1")
    assert result.has_label("ExtraLabel2")

    assert result.get("pp") == "Test Node"


def test_create_multilabel_inheritance(use_graph):
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

    result = use_graph.evaluate(cypher)

    assert result.has_label("Human")
    assert result.has_label("Mammal")

    assert result.get("pp") == "Bob"


def test_create_multilabel_inheritance_multiple(use_graph):
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

    result = use_graph.evaluate(cypher)

    assert result == 2


def test_merge_defined_label_inherited(use_graph):
    class SpecialPracticeNode(BaseNode):
        __primarylabel__: ClassVar[Optional[str]] = "SpecialTestLabel"
        __primaryproperty__: ClassVar[str] = "pp"

        pp: str

    tn = SpecialPracticeNode(pp="Special Test Node")

    tn.merge()

    cypher = """
    MATCH (n:SpecialTestLabel)
    WHERE n.pp = 'Special Test Node'
    RETURN n
    """

    result = use_graph.evaluate(cypher)

    assert result.has_label("SpecialTestLabel")

    assert result.get("pp") == "Special Test Node"


def test_merge_multiple_defined_label_inherited(use_graph):
    class SpecialPracticeNode(BaseNode):
        __primarylabel__: ClassVar[Optional[str]] = "SpecialTestLabel"
        __primaryproperty__: ClassVar[str] = "pp"

        pp: str

    tn = SpecialPracticeNode(pp="Special Test Node")

    tn2 = SpecialPracticeNode(pp="Special Test Node2")

    SpecialPracticeNode.merge_nodes([tn, tn2])

    cypher = """
    MATCH (n:SpecialTestLabel)
    RETURN COLLECT(DISTINCT n.pp) as node_names
    """

    results = use_graph.evaluate(cypher)

    assert "Special Test Node" in results
    assert "Special Test Node2" in results


def test_create_multiple_defined_label_inherited(use_graph):
    class SpecialPracticeNode(BaseNode):
        __primarylabel__: ClassVar[Optional[str]] = "SpecialTestLabel"
        __primaryproperty__: ClassVar[str] = "pp"

        pp: str

    tn = SpecialPracticeNode(pp="Special Test Node")

    tn2 = SpecialPracticeNode(pp="Special Test Node2")

    SpecialPracticeNode.create_nodes([tn, tn2])

    cypher = """
    MATCH (n:SpecialTestLabel)
    RETURN COLLECT(DISTINCT n.pp) as node_names
    """

    results = use_graph.evaluate(cypher)

    assert "Special Test Node" in results
    assert "Special Test Node2" in results


def test_creation_datetime(use_graph):
    """
    Check we can manually define the created datetime, and then check we can
    query for it using neo4j DateTime type.
    """

    my_datetime = datetime(year=2022, month=5, day=4, hour=3, minute=21)

    bn = PracticeNode(pp="Test Node", created=my_datetime)

    bn.create()

    cypher = """
    MATCH (n:PracticeNode)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate(cypher)

    # if the created date has been stored as a Neo4j datetime,
    # it should come back as an interchange DateTime format from py2neo
    # we call to_native to convert it back to a native Python datetime
    assert result.get("created").to_native() == my_datetime


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


@pytest.mark.parametrize(
    "field_type,python_value,neo4j_values",
    [
        (str, "hello world", ["hello world"]),
        (tuple, ("hello", "world"), [["hello", "world"]]),
        (set, {"foo", "bar"}, [["bar", "foo"], ["foo", "bar"]]),
        (
            UUID,
            UUID("32d4a4cb-29c3-4aa8-9b55-7790431819e3"),
            ["32d4a4cb-29c3-4aa8-9b55-7790431819e3"],
        ),
        (
            datetime,
            datetime(year=1984, month=1, day=2),
            [datetime(year=1984, month=1, day=2)],
        ),
        (
            list,
            ["foo", "bar"],
            [["foo", "bar"]],
        ),
        (
            list,
            [1, 2, 3],
            [[1, 2, 3]],
        ),
    ],
)
def test_neo4j_dict_create(use_graph, field_type, python_value, neo4j_values):
    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel"
        pp: str
        test_prop: field_type

    pp = "test_node"

    testmodel = TestModel(test_prop=python_value, pp=pp)

    testmodel.create()

    result = TestModel.match(pp)

    assert result.test_prop == python_value

    cypher = """
    MATCH (n:TestModel)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate(cypher)

    assert cypher_result.get("test_prop") in neo4j_values


def test_set_on_match(use_graph):
    """Check that we successfully identify field to set on match"""

    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel"
        pp: str = "test_node"
        only_set_on_match: Optional[str] = Field(
            json_schema_extra={"set_on_match": True}, default=None
        )
        normal_field: str

    test_node = TestModel(only_set_on_match="Foo", normal_field="Bar", pp="test_node")
    test_node.merge()

    cypher = """
    MATCH (n:TestModel)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate(cypher)

    assert cypher_result.get("only_set_on_match") is None
    assert cypher_result.get("normal_field") == "Bar"

    test_node2 = TestModel(only_set_on_match="Foo", normal_field="Bar", pp="test_node")
    test_node2.merge()

    cypher_result2 = use_graph.evaluate(cypher)

    assert cypher_result2.get("only_set_on_match") == "Foo"


def test_set_on_create(use_graph):
    """Check that we successfully identify field to set on match"""

    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel"
        pp: str = "test_node"
        only_set_on_create: str = Field(json_schema_extra={"set_on_create": True})
        normal_field: str

    test_node = TestModel(only_set_on_create="Foo", normal_field="Bar", pp="test_node")
    test_node.merge()

    cypher = """
    MATCH (n:TestModel)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate(cypher)

    assert cypher_result.get("only_set_on_create") == "Foo"
    assert cypher_result.get("normal_field") == "Bar"

    test_node2 = TestModel(only_set_on_create="Fee", normal_field="Fi", pp="test_node")
    test_node2.merge()

    cypher_result2 = use_graph.evaluate(cypher)

    assert cypher_result2.get("only_set_on_create") == "Foo"
    assert cypher_result2.get("normal_field") == "Fi"


def test_merge_df_with_duplicates(use_graph):
    class Person(BaseNode):
        __primaryproperty__: ClassVar[str] = "identifier"
        __primarylabel__: ClassVar[
            str
        ] = "PersonLabel"  # optionally specify the label to use

        name: str
        age: int
        identifier: Optional[str] = Field(default=None, validate_default=True)

        @field_validator("identifier")
        def set_identifier(cls, v, values):
            if v is None:
                v = f"{values.data['name']}_{values.data['age']}"

            return v

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


def test_merge_empty_df():
    df = pd.DataFrame()

    result = PracticeNode.merge_df(df)

    assert len(result) == 0
    assert isinstance(result, pd.Series)
