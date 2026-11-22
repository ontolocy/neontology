"""Validating content with no database connected, as CI or an extraction pipeline does.

Endpoints resolve against the content alone, and values are converted as every engine
would convert them, so the content is held to what any of them can store.
"""

import logging
import uuid
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship, GraphConnection
from neontology.tools import ImportValidationError, import_records


class OfflineNode(BaseNode):
    __primarylabel__: ClassVar[str] = "OfflineLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    meta: Optional[dict] = None


class OfflineRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "OFFLINE_LINK"

    source: OfflineNode
    target: OfflineNode


class OfflineUUIDNode(BaseNode):
    __primarylabel__: ClassVar[str] = "OfflineUUIDLabel"
    __primaryproperty__: ClassVar[str] = "uid"

    uid: uuid.UUID


def _node(name, **kwargs):
    return {"LABEL": "OfflineLabel", "name": name, **kwargs}


def _rel(**kwargs):
    return {"RELATIONSHIP_TYPE": "OFFLINE_LINK", "SOURCE_LABEL": "OfflineLabel", "TARGET_LABEL": "OfflineLabel", **kwargs}


@pytest.fixture(autouse=True)
def not_connected():
    """Run with no connection, then put back whatever the suite had."""
    session_instance = GraphConnection._instance

    GraphConnection._instance = None

    try:
        yield

    finally:
        GraphConnection._instance = session_instance


def test_content_validates_without_a_database():
    report = import_records([[_node("a"), _node("b"), _rel(SOURCE="a", TARGET="b")]], validate_only=True)

    assert report.nodes == {"OfflineLabel": 2}
    assert report.relationships == {"OFFLINE_LINK": 1}
    assert report.unresolved == []


def test_content_with_no_relationships_validates_without_a_database():
    """It used to fail looking for a connection before checking whether there was anything to resolve."""
    report = import_records([[_node("a")]], validate_only=True)

    assert report.nodes == {"OfflineLabel": 1}


def test_a_value_no_engine_can_store_is_reported_without_a_database():
    with pytest.raises(ImportValidationError, match="'meta'.*a dict"):
        import_records([[_node("a", meta={"x": 1})]], validate_only=True)


def test_an_endpoint_the_content_does_not_define_is_warned_about(caplog):
    with caplog.at_level(logging.WARNING, logger="neontology.tools.import_records"):
        report = import_records([[_node("a"), _rel(SOURCE="a", TARGET="nowhere")]], validate_only=True)

    assert len(report.unresolved) == 1
    assert "'nowhere'" in report.unresolved[0]
    assert "only the content was checked" in report.unresolved[0]
    assert "'nowhere'" in caplog.text


def test_an_endpoint_the_content_does_not_define_is_an_error_when_asked():
    with pytest.raises(ImportValidationError, match="only the content was checked"):
        import_records([[_node("a"), _rel(SOURCE="a", TARGET="nowhere")]], validate_only=True, error_on_unmatched=True)


def test_a_primary_property_reads_the_same_offline_as_when_connected():
    """Converted as an engine would, where it used to be the raw value until connected."""
    uid = uuid.uuid4()

    assert OfflineUUIDNode(uid=uid).get_pp() == str(uid)
