"""Property types, and set_on_match / set_on_create behaviour."""

from datetime import datetime
from typing import ClassVar, Optional
from uuid import UUID

import pytest
from pydantic import (
    Field,
)

from neontology import (
    BaseNode,
)


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
    test_prop_int_list_exp: list[int]


@pytest.mark.parametrize(
    "test_model,test_prop,input_value,expected_value",
    [
        (ModelTestString, "test_prop_string", "hello world", ["hello world"]),
        (ModelTestInt, "test_prop_int", 5071, [5071]),
        (
            ModelTestTuple,
            "test_prop_tuple",
            ("hello", "world"),
            [
                ("hello", "world"),
                ["hello", "world"],
            ],  # some engines support tuples, some don't
        ),
        (
            ModelTestSet,
            "test_prop_set",
            {"foo", "bar"},
            [
                ["bar", "foo"],
                ["foo", "bar"],
                {"foo", "bar"},
            ],  # some engines support sets, some don't
        ),
        (
            ModelTestUUID,
            "test_prop_uuid",
            UUID("32d4a4cb-29c3-4aa8-9b55-7790431819e3"),
            [
                UUID("32d4a4cb-29c3-4aa8-9b55-7790431819e3"),
                "32d4a4cb-29c3-4aa8-9b55-7790431819e3",
            ],
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
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    # in the case of sets, we may get the result back ordered one of two ways
    # therefore, we check that the result is one of the expected values rather
    assert cypher_result.nodes[0].model_dump()[test_prop] in expected_value


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
        only_set_on_match: Optional[str] = Field(json_schema_extra={"set_on_match": True}, default=None)
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


class TestGetPrimaryPropertyValue:
    """get_pp() returns the primary property converted the way the engine expects.

    It dumps only that one property rather than the whole model, so these pin down the
    cases where the converted value differs from the raw attribute, or where the
    property is addressed by an alias rather than its field name.
    """

    def test_engine_conversion_is_applied(self, use_graph):
        """A UUID reaches the graph as a string, so get_pp() must convert it.

        Reading the attribute directly would return a UUID object and break the node
        identity keys built from get_pp() when results are hydrated.
        """
        from uuid import uuid4

        class UuidPrimary(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "GetPpUuidNode"
            pp: UUID

        value = uuid4()
        node = UuidPrimary(pp=value)

        assert node.pp == value
        assert node.get_pp() == str(value)
        assert isinstance(node.get_pp(), str)

    def test_primary_property_may_be_an_alias(self, use_graph):
        """__primaryproperty__ is the alias where a field has one.

        Dumps are taken by_alias, and pydantic's include= works on field names only, so
        an alias has to be mapped back to its field before a single-property dump.
        """
        from pydantic import ConfigDict

        class AliasPrimary(BaseNode):
            __primaryproperty__: ClassVar[str] = "userName"
            __primarylabel__: ClassVar[Optional[str]] = "GetPpAliasNode"
            model_config = ConfigDict(
                validate_by_name=True,
                validate_by_alias=True,
                populate_by_name=True,
                extra="ignore",
            )
            user_name: str = Field(alias="userName")
            other: Optional[str] = Field(None, alias="otherProp")

        node = AliasPrimary(userName="User1")

        assert node.get_pp() == "User1"

    def test_matches_a_full_dump_of_the_same_model(self, use_graph):
        """get_pp() must agree with the value in a whole-model engine dump."""

        class MixedPrimary(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "GetPpMixedNode"
            pp: str
            when: datetime
            on_match: str = Field("m", json_schema_extra={"set_on_match": True})
            on_create: str = Field("c", json_schema_extra={"set_on_create": True})

        node = MixedPrimary(pp="the-value", when=datetime(2026, 1, 2, 3, 4, 5))

        assert node.get_pp() == node._engine_dict()[node.__primaryproperty__]

        # the other buckets are untouched by the single-property dump
        assert node._get_merge_parameters()["set_on_match"] == {"on_match": "m"}
        assert node._get_merge_parameters()["set_on_create"] == {"on_create": "c"}


def test_create_nodes_builds_each_props_dict_once(use_graph, monkeypatch):
    """Bulk create dumped every node twice, once for props and once for the pp."""

    class CountedNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "CreateNodesCountedNode"
        pp: str
        other: str = "x"

    # only whole-model dumps are counted. get_pp() also reaches _engine_dict, but asks
    # for a single property with include=, and results are hydrated through it.
    calls = {"full": 0}
    original = CountedNode._engine_dict

    def counting_engine_dict(self, *args, **kwargs):
        if "include" not in kwargs:
            calls["full"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(CountedNode, "_engine_dict", counting_engine_dict)

    nodes = [CountedNode(pp=f"node-{i}") for i in range(5)]

    CountedNode.create_nodes(nodes)

    assert calls["full"] == 5, f"expected one full dump per node, got {calls['full']}"
