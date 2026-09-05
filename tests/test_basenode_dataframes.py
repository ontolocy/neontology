"""Bulk operations: dataframes, counts, mass creation and aliased properties."""

from typing import ClassVar, Optional
from uuid import UUID, uuid4

import pandas as pd
from models_basenode import PracticeNode
from pydantic import (
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)
from rawresult import raw_property

from neontology import (
    BaseNode,
)
from neontology.result import NeontologyResult


class Person(BaseNode):
    __primaryproperty__: ClassVar[str] = "identifier"
    __primarylabel__: ClassVar[str] = "PersonLabel1"  # optionally specify the label to use

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
    __primarylabel__: ClassVar[str] = "PersonLabel2"  # optionally specify the label to use

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


def test_get_count(use_graph):
    people_records = [
        {"name": "arthur", "age": 70, "favorite_colors": ["red"]},
        {"name": "betty", "age": 65, "favorite_colors": ["red", "blue"]},
        {"name": "ted", "age": 50, "favorite_colors": []},
        {"name": "ben", "age": 75},
    ]

    people_df = pd.DataFrame.from_records(people_records)

    Person2.merge_df(people_df, deduplicate=False)

    assert Person2.get_count() == 4
    assert Person2.get_count(filters={"age__gt": 60}) == 3
    assert Person2.get_count(filters={"name__contains": "et"}) == 1


def test_get_count_none(use_graph):
    assert not Person2.get_count()


def test_merge_empty_df():
    df = pd.DataFrame()

    result = PracticeNode.merge_df(df)

    assert len(result) == 0
    assert isinstance(result, pd.Series)


class ComplexPerson(BaseNode):
    __primaryproperty__: ClassVar[str] = "identifier"
    __primarylabel__: ClassVar[str] = "PersonLabel1RetrieveNone"  # optionally specify the label to use

    name: str = Field(default_factory=uuid4)
    age: int
    favorite_colors: list = ["red", "green", "blue"]
    favorite_numbers: list = [1, 2, 3]
    extra_str1: UUID = Field(default_factory=uuid4)
    extra_str2: UUID = Field(default_factory=uuid4)

    identifier: Optional[str] = Field(default=None, validate_default=True)

    @field_validator("identifier")
    def set_identifier(cls, v, values):
        if v is None:
            v = f"{values.data['name']}_{values.data['age']}"

        return v

    @field_serializer("extra_str1", "extra_str2")
    def serialize_to_str(self, v: UUID):
        return str(v)


def test_create_mass_nodes(use_graph, benchmark):
    people_records = [{"age": x, "name": uuid4().hex} for x in range(1000)]

    people_df = pd.DataFrame.from_records(people_records)

    benchmark(ComplexPerson.merge_df, people_df)

    assert Person.get_count() == 1000


class UserWithAliases(BaseNode):
    __primaryproperty__: ClassVar[str] = "userName"
    __primarylabel__: ClassVar[str] = "AliasedUser"
    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        populate_by_name=True,  # allow population by name and alias
        extra="ignore",  # allow data to be passed in to aliased fields
    )
    user_name: str = Field(alias="userName")
    some_other_property: Optional[str] = Field(None, alias="otherProperty")


def test_aliased_properties(use_graph):
    user1: UserWithAliases = UserWithAliases(userName="User1")
    user2: UserWithAliases = UserWithAliases(user_name="User2", some_other_property="alpha")
    user3: UserWithAliases = UserWithAliases(userName="User3", otherProperty="beta")
    assert user1.user_name == "User1"
    assert user3.some_other_property == "beta"

    user1.merge()
    user2.merge()
    user3.merge()

    cypher = """
    MATCH (n:AliasedUser)
    RETURN n
    ORDER BY n.userName ASC
    """

    result: NeontologyResult = use_graph.evaluate_query(cypher)
    assert result.nodes[0].user_name == "User1"
    assert not hasattr(result.nodes[0], "userName")

    assert result.nodes[0].some_other_property is None
    assert result.nodes[1].some_other_property == "alpha"
    assert result.nodes[2].user_name == "User3"
    assert result.nodes[1].user_name == "User2"

    assert raw_property(result, "userName") == "User1"
    assert raw_property(result, "otherProperty") is None

    assert raw_property(result, "otherProperty", index=1) == "alpha"
    assert raw_property(result, "otherProperty", index=2) == "beta"
