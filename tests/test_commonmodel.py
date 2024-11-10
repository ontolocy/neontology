from datetime import datetime
from uuid import UUID
from typing import Optional

from pydantic import Field, ValidationError
import pytest

from neontology.commonmodel import CommonModel


class PracticeModel(CommonModel):
    def _get_merge_parameters(self):
        return {}

    optional_string: Optional[str] = None


def test_common_model_creation():
    common = PracticeModel(optional_string="testing123")

    assert isinstance(common.optional_string, str)


def test_merged_created_deprecation(capsys):
    now_time = datetime.now()

    with pytest.raises(ValidationError):
        PracticeModel(merged=now_time, created=now_time)
        captured = capsys.readouterr()

        assert (
            "Native neontology support for 'merged' and 'created' properties has been removed."
            in captured.err
        )

        assert 0


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


@pytest.mark.parametrize(
    "field_type,python_value", [(dict, {"foo": "bar"}), (list, [123, "foo"])]
)
def test_engine_dict_bad_types(field_type, python_value, use_graph):
    class TestModel(PracticeModel):
        test_prop: field_type

    testmodel = TestModel(test_prop=python_value)

    with pytest.raises(TypeError):
        testmodel._engine_dict()
