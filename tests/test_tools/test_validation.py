"""Locating problems in content, and reporting all of them rather than the first."""

import warnings
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship
from neontology.tools import (
    ImportContentError,
    ImportReport,
    ImportValidationError,
    import_records,
    import_yaml,
)


class ValidatedNode(BaseNode):
    __primarylabel__: ClassVar[str] = "ValidatedLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    age: Optional[int] = None


class ValidatedRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "VALIDATED_KNOWS"

    source: ValidatedNode
    target: ValidatedNode

    since: Optional[int] = None


def _node(name, **kwargs):
    return {"LABEL": "ValidatedLabel", "name": name, **kwargs}


def _rel(**kwargs):
    return {
        "RELATIONSHIP_TYPE": "VALIDATED_KNOWS",
        "SOURCE_LABEL": "ValidatedLabel",
        "TARGET_LABEL": "ValidatedLabel",
        **kwargs,
    }


def _write_yaml(dir_path, name, records):
    import yaml

    (dir_path / name).write_text(yaml.safe_dump(records, sort_keys=False))

    return dir_path / name


# --- locating a problem -------------------------------------------------------------


def test_error_names_the_file_and_the_entry(use_graph, tmp_path_factory):
    # the entry which does not validate is buried among many, in one of several files
    dir_path = tmp_path_factory.mktemp("content")

    _write_yaml(dir_path, "aaa.yaml", [_node(f"aaa-{i}", age=i) for i in range(30)])

    bad = [_node(f"bbb-{i}", age=i) for i in range(30)]
    bad.append(_node("broken", age="twenty-two"))
    _write_yaml(dir_path, "bbb.yaml", bad)

    _write_yaml(dir_path, "ccc.yaml", [_node(f"ccc-{i}", age=i) for i in range(30)])

    with pytest.raises(ImportContentError) as raised:
        import_yaml(dir_path)

    message = str(raised.value)

    assert "bbb.yaml" in message
    assert "record 31" in message
    # and it still says what was actually wrong
    assert "age" in message


def test_error_names_the_position_of_an_inline_node(use_graph):
    records = [
        [
            _node("alpha"),
            _rel(SOURCE="alpha", TARGET_NODES=[_node("beta"), _node("gamma", age="not-a-number")]),
        ]
    ]

    with pytest.raises(ImportContentError, match=r"TARGET_NODES\[1\]"):
        import_records(records)


def test_error_names_the_record_in_a_list(use_graph):
    records = [[_node("alpha"), _node("beta"), _node("gamma", age="not-a-number")]]

    with pytest.raises(ImportContentError, match="record 3"):
        import_records(records)


def test_error_names_the_relationship_declared_under_a_node(use_graph):
    records = [
        [
            _node("alpha"),
            _node("beta"),
            {
                **_node("gamma"),
                "RELATIONSHIPS_OUT": [
                    _rel(TARGETS=["beta"]),
                    _rel(TARGETS=["beta"], since="not-a-number"),
                ],
            },
        ]
    ]

    with pytest.raises(ImportContentError, match=r"RELATIONSHIPS_OUT\[1\]"):
        import_records(records)


# --- reporting every problem --------------------------------------------------------


def test_validate_only_reports_every_problem(use_graph):
    records = [
        [
            _node("ok-one"),
            _node("bad-one", age="x"),
            _node("bad-two", age="y"),
            {"LABEL": "NoSuchLabel", "name": "bad-three"},
        ]
    ]

    with pytest.raises(ImportValidationError) as raised:
        import_records(records, validate_only=True)

    assert len(raised.value.issues) == 3

    message = str(raised.value)

    assert "bad-one" in message or "record 2" in message
    assert "3 problems" in message


def test_validate_only_writes_nothing(use_graph):
    records = [[_node("ok-one"), _node("bad-one", age="x")]]

    with pytest.raises(ImportValidationError):
        import_records(records, validate_only=True)

    assert ValidatedNode.get_count() == 0


def test_validate_only_returns_a_report_when_the_content_is_good(use_graph):
    records = [[_node("alpha"), _node("beta"), _rel(SOURCE="alpha", TARGET="beta")]]

    report = import_records(records, validate_only=True)

    assert isinstance(report, ImportReport)
    assert report.validated_only is True
    assert report.nodes == {"ValidatedLabel": 2}
    assert report.relationships == {"VALIDATED_KNOWS": 1}

    # nothing was written
    assert ValidatedNode.get_count() == 0


def test_validation_report_groups_problems_by_file(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write_yaml(dir_path, "one.yaml", [_node("a", age="x")])
    _write_yaml(dir_path, "two.yaml", [_node("b", age="y")])

    with pytest.raises(ImportValidationError) as raised:
        import_yaml(dir_path, validate_only=True)

    message = str(raised.value)

    assert "one.yaml" in message
    assert "two.yaml" in message
    assert len(raised.value.issues) == 2

    # each file is named once, with its problems under it
    assert message.count("one.yaml") == 1


# --- endpoints resolve against the content as well as the graph ---------------------


def test_validate_accepts_an_endpoint_the_content_defines(use_graph):
    # nothing is written when validating, so a target defined in the content has to
    # count as resolvable or every relationship in a fresh repository would be reported
    records = [[_node("alpha"), _node("beta"), _rel(SOURCE="alpha", TARGET="beta")]]

    report = import_records(records, validate_only=True, error_on_unmatched=True)

    assert report.unresolved == []


def test_validate_accepts_an_inline_defined_endpoint(use_graph):
    records = [[_node("alpha"), _rel(SOURCE="alpha", TARGET_NODES=[_node("beta")])]]

    report = import_records(records, validate_only=True, error_on_unmatched=True)

    assert report.unresolved == []


def test_validate_reports_an_endpoint_which_resolves_nowhere(use_graph):
    records = [[_node("alpha"), _rel(SOURCE="alpha", TARGETS=["nowhere"])]]

    with pytest.raises(ImportValidationError, match="nowhere"):
        import_records(records, validate_only=True, error_on_unmatched=True)


def test_unresolved_endpoints_are_listed_in_the_report(use_graph):
    records = [[_node("alpha"), _rel(SOURCE="alpha", TARGETS=["nowhere"])]]

    report = import_records(records)

    assert len(report.unresolved) == 1
    assert "nowhere" in report.unresolved[0]


# --- mistyped control keys ----------------------------------------------------------


def test_a_mistyped_control_key_suggests_the_right_one(use_graph):
    records = [
        [
            _node("alpha"),
            _node("beta", age=1),
            _rel(SOURCE="alpha", TARGETS=["beta"], TARGET_PROPERTIES="name"),
        ]
    ]

    with pytest.raises(ImportContentError, match="TARGET_PROPERTY"):
        import_records(records)


def test_a_mistyped_node_control_key_is_reported(use_graph):
    records = [[{"LABELS": "ValidatedLabel", "name": "alpha"}]]

    with pytest.raises(ImportContentError, match="Did you mean 'LABEL'"):
        import_records(records)


class ShoutyNode(BaseNode):
    __primarylabel__: ClassVar[str] = "ShoutyLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    STATUS: Optional[str] = None


def test_an_uppercase_model_property_is_not_mistaken_for_a_control_key(use_graph):
    import_records([[{"LABEL": "ShoutyLabel", "name": "alpha", "STATUS": "live"}]])

    assert ShoutyNode.match("alpha").STATUS == "live"


# --- what the import did ------------------------------------------------------------


def test_import_returns_counts(use_graph):
    records = [
        [
            _node("alpha"),
            _node("beta"),
            _rel(SOURCE="alpha", TARGET="beta"),
        ]
    ]

    report = import_records(records)

    assert report.nodes == {"ValidatedLabel": 2}
    assert report.relationships == {"VALIDATED_KNOWS": 1}
    assert report.node_count == 2
    assert report.relationship_count == 1
    assert report.validated_only is False


def test_file_import_report_lists_the_files_read(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write_yaml(dir_path, "one.yaml", [_node("a")])
    _write_yaml(dir_path, "two.yaml", [_node("b")])

    report = import_yaml(dir_path)

    assert len(report.files) == 2
    assert report.node_count == 2
    assert "one.yaml" in report.files[0]


def test_report_describes_itself(use_graph):
    report = import_records([[_node("alpha")]])

    assert "1 nodes" in str(report)


# --- the deprecated lowercase endpoint keys ------------------------------------------


def test_lowercase_endpoint_keys_warn_once_for_the_run(use_graph):
    records = [
        [
            _node("alpha"),
            _node("beta"),
            _rel(source="alpha", target="beta"),
            _rel(source="beta", target="alpha"),
        ]
    ]

    with pytest.warns(DeprecationWarning) as warned:
        import_records(records)

    assert len(warned) == 1

    # counted by record rather than by key, so two records reads as two
    assert "2 relationship records" in str(warned[0].message)


def test_uppercase_endpoint_keys_do_not_warn(use_graph):
    records = [[_node("alpha"), _node("beta"), _rel(SOURCE="alpha", TARGET="beta")]]

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)

        import_records(records)


def test_a_write_failure_no_record_explains_points_at_validate(use_graph, monkeypatch):
    # a batch can fail for a reason no single record accounts for - the database
    # refusing the write, most often - which cannot be narrowed down to one record
    def refuse(*args, **kwargs):
        raise RuntimeError("the database said no")

    monkeypatch.setattr(ValidatedNode, "merge_records", classmethod(refuse))

    with pytest.raises(ImportContentError) as raised:
        import_records([[_node("alpha"), _node("beta")]])

    message = str(raised.value)

    assert "validate_only=True" in message
    assert "the database said no" in message
    assert raised.value.__cause__ is not None
