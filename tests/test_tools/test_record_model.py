"""A pydantic type for import records, built from the models, for content a program produces."""

import enum
from typing import ClassVar, Optional

import pytest
from pydantic import Field, ValidationError

from neontology import BaseNode, BaseRelationship, GraphConnection
from neontology.tools import import_records, record_model


class RecMotivation(str, enum.Enum):
    ESPIONAGE = "espionage"
    FINANCIAL = "financial"


class RecAgent(BaseNode):
    """Abstract: anything which acts."""

    __primaryproperty__: ClassVar[str] = "name"

    name: str
    nickname: Optional[str] = Field(default=None, alias="known_as")


class RecActor(RecAgent):
    """A threat actor."""

    __primarylabel__: ClassVar[Optional[str]] = "RecActor"

    motivation: Optional[RecMotivation] = None


class RecGroup(RecAgent):
    __primarylabel__: ClassVar[Optional[str]] = "RecGroup"


class RecTechnique(BaseNode):
    __primarylabel__: ClassVar[Optional[str]] = "RecTechnique"
    __primaryproperty__: ClassVar[str] = "tid"

    tid: str


class RecUses(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "REC_USES"

    source: RecAgent
    target: RecTechnique

    quote: Optional[str] = None


MODELS = [RecAgent, RecActor, RecGroup, RecTechnique, RecUses]


def _uses(source_label, **kwargs):
    return {"RELATIONSHIP_TYPE": "REC_USES", "SOURCE_LABEL": source_label, "TARGET_LABEL": "RecTechnique", **kwargs}


GOOD = [
    {"LABEL": "RecActor", "name": "apt1", "known_as": "Comment Crew", "motivation": "espionage"},
    {"LABEL": "RecGroup", "name": "crew"},
    {"LABEL": "RecTechnique", "tid": "T1566"},
    {
        "RELATIONSHIP_TYPE": "REC_USES",
        "SOURCE_LABEL": "RecActor",
        "TARGET_LABEL": "RecTechnique",
        "SOURCE": "apt1",
        "TARGET": "T1566",
    },
    {
        "RELATIONSHIP_TYPE": "REC_USES",
        "SOURCE_LABEL": "RecGroup",
        "TARGET_LABEL": "RecTechnique",
        "SOURCE": "crew",
        "TARGET": "T1566",
        "quote": "sends phishing emails",
    },
]


def _dumped(records) -> list[dict]:
    return [record.model_dump(exclude_unset=True) for record in record_model(MODELS).model_validate(records).root]


def test_records_dump_with_the_keys_the_import_reads():
    """An aliased property is keyed by its alias, as content writes it, with nothing added."""
    assert _dumped(GOOD) == GOOD


def test_parsed_records_validate_without_a_database():
    session_instance = GraphConnection._instance
    GraphConnection._instance = None

    try:
        report = import_records(_dumped(GOOD), validate_only=True, error_on_unmatched=True)

    finally:
        GraphConnection._instance = session_instance

    assert report.nodes == {"RecActor": 1, "RecGroup": 1, "RecTechnique": 1}
    assert report.relationships == {"REC_USES": 2}


def test_parsed_records_import(use_graph):
    import_records(_dumped(GOOD), error_on_unmatched=True)

    actor = RecActor.match("apt1")

    assert actor is not None and actor.nickname == "Comment Crew" and actor.motivation == RecMotivation.ESPIONAGE
    assert RecUses.get_count() == 2


BAD = [
    pytest.param({"label": "RecGroup", "name": "crew"}, id="lowercase-label-key"),
    pytest.param({"LABEL": "RecGrup", "name": "crew"}, id="unknown-label"),
    pytest.param({"LABEL": "RecGroup", "name": "crew", "colour": "red"}, id="unknown-property"),
    pytest.param({"LABEL": "RecActor", "name": "apt1", "motivation": "boredom"}, id="value-not-allowed"),
    pytest.param({"LABEL": "RecTechnique"}, id="missing-primary-property"),
    pytest.param(
        {
            "RELATIONSHIP_TYPE": "REC_USES",
            "SOURCE_LABEL": "RecTechnique",
            "TARGET_LABEL": "RecTechnique",
            "SOURCE": "a",
            "TARGET": "b",
        },
        id="end-label-the-class-does-not-allow",
    ),
    pytest.param(
        {"RELATIONSHIP_TYPE": "REC_USES", "SOURCE_LABEL": "RecActor", "TARGET_LABEL": "RecTechnique", "TARGETS": ["b"]},
        id="plural-end",
    ),
]


@pytest.mark.parametrize("record", BAD)
def test_records_the_import_would_reject_are_rejected(record):
    with pytest.raises(ValidationError):
        record_model(MODELS).model_validate([record])


def test_the_schema_describes_each_concrete_class():
    schema = record_model(MODELS).model_json_schema()

    records = sorted(name for name in schema["$defs"] if name.endswith("Record"))

    assert records == ["RecActorRecord", "RecGroupRecord", "RecTechniqueRecord", "RecUsesRecord"]
    assert schema["$defs"]["RecActorRecord"]["properties"]["LABEL"]["const"] == "RecActor"
    assert schema["$defs"]["RecActorRecord"]["additionalProperties"] is False
    assert "known_as" in schema["$defs"]["RecActorRecord"]["properties"]
    assert schema["$defs"]["RecActorRecord"]["description"] == "A threat actor."

    uses = schema["$defs"]["RecUsesRecord"]["properties"]

    assert uses["SOURCE_LABEL"]["enum"] == ["RecActor", "RecGroup"]
    assert uses["TARGET_LABEL"]["const"] == "RecTechnique"


def test_with_nothing_concrete_to_describe_it_says_so():
    with pytest.raises(ValueError, match="no concrete"):
        record_model([RecAgent])
