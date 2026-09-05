from datetime import datetime
from typing import ClassVar, Optional
from uuid import UUID

import pytest
from pydantic import Field

from neontology import BaseNode, BaseRelationship
from neontology.commonmodel import CommonModel


class PracticeModel(CommonModel):
    def _get_merge_parameters(self):
        return {}

    optional_string: Optional[str] = None


def test_common_model_creation():
    common = PracticeModel(optional_string="testing123")

    assert isinstance(common.optional_string, str)


def test_set_on_match():
    """Check that we successfully identify field to set on match"""

    class TestModel(PracticeModel):
        only_set_on_match: str = Field(json_schema_extra={"set_on_match": True})
        normal_field: str

    test_model = TestModel(only_set_on_match="Foo", normal_field="Bar")

    assert test_model._set_on_create == []
    assert test_model._set_on_match == ["only_set_on_match"]

    assert test_model._always_set == [
        "optional_string",
        "normal_field",
    ]


def test_set_on_create():
    """Check that we successfully identify field to set on match"""

    class TestModel(PracticeModel):
        only_set_on_create: str = Field(json_schema_extra={"set_on_create": True})
        normal_field: str

    test_model = TestModel(only_set_on_create="Foo", normal_field="Bar")

    assert test_model._set_on_create == ["only_set_on_create"]
    assert test_model._set_on_match == []
    assert test_model._always_set == [
        "optional_string",
        "normal_field",
    ]


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
    ],
)
def test_engine_dict(field_type, python_value, neo4j_values, use_graph):
    class TestModel(PracticeModel):
        test_prop: field_type

    testmodel = TestModel(test_prop=python_value)

    test_prop_result = testmodel._engine_dict()["test_prop"]

    assert test_prop_result in neo4j_values


@pytest.mark.parametrize("field_type,python_value", [(dict, {"foo": "bar"}), (list, [123, "foo"])])
def test_engine_dict_bad_types(field_type, python_value, use_graph):
    class TestModel(PracticeModel):
        test_prop: field_type

    testmodel = TestModel(test_prop=python_value)

    with pytest.raises(TypeError):
        testmodel._engine_dict()


class TestPropertyUsageCaching:
    """Property usage is computed once per class, so subclasses must not inherit it.

    The buckets are derived from the model definition and identical for every instance,
    so they are cached on the class - computing them per instance generated the pydantic
    JSON schema on every instantiation. The risk of caching is a subclass silently
    reusing its parent's answer.
    """

    def test_subclass_computes_its_own_buckets(self):
        class CacheParent(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "PropCacheParent"

            pp: str
            only_on_match: Optional[str] = Field(default=None, json_schema_extra={"set_on_match": True})

        class CacheChild(CacheParent):
            __primarylabel__: ClassVar[Optional[str]] = "PropCacheChild"

            extra_on_create: Optional[str] = Field(default=None, json_schema_extra={"set_on_create": True})

        # instantiate the parent first, so its cache is populated before the child is built
        CacheParent(pp="parent")

        assert CacheParent._set_on_match == ["only_on_match"]
        assert CacheParent._set_on_create == []

        CacheChild(pp="child")

        assert CacheChild._set_on_create == ["extra_on_create"]
        assert CacheChild._set_on_match == ["only_on_match"]

        # the child must not have written its buckets onto the parent
        assert CacheParent._set_on_create == []

    def test_relationship_subclass_computes_its_own_merge_on(self):
        class CacheRelNode(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "PropCacheRelNode"

            pp: str

        class CacheRelParent(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "PROP_CACHE_PARENT"

            source: CacheRelNode
            target: CacheRelNode

        class CacheRelChild(CacheRelParent):
            __relationshiptype__: ClassVar[Optional[str]] = "PROP_CACHE_CHILD"

            merge_key: Optional[str] = Field(default=None, json_schema_extra={"merge_on": True})

        source = CacheRelNode(pp="a")
        target = CacheRelNode(pp="b")

        CacheRelParent(source=source, target=target)

        assert CacheRelParent._merge_on == []

        CacheRelChild(source=source, target=target)

        assert CacheRelChild._merge_on == ["merge_key"]
        assert CacheRelParent._merge_on == []
