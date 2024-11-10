from uuid import uuid4, UUID
from typing import ClassVar
import logging

import pytest
from pydantic import Field, ValidationError, field_serializer

from neontology import BaseNode, BaseRelationship
from neontology.tools.import_records import import_records

logger = logging.getLogger(__name__)


class PersonImportNode(BaseNode):
    __primarylabel__: ClassVar[str] = "PersonImportLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    age: int

    import_id: UUID = Field(default_factory=uuid4)

    @field_serializer("import_id")
    def serialize_to_str(self, v: UUID):
        return str(v)


class FollowsImportRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "IMPORT_FOLLOWS"

    source: PersonImportNode
    target: PersonImportNode

    import_follows_prop_1: str


records_raw = {
    "nodes": [
        {
            "LABEL": "PersonImportLabel",
            "name": "Bob",
            "age": 84,
            "import_id": UUID("154b0bb7-33ce-4c5b-ba6a-44b5b09b0067"),
        },
        {
            "LABEL": "PersonImportLabel",
            "name": "Alice",
            "age": 76,
            "import_id": UUID("78fcd953-1d0d-47ae-871c-670a0a7a7406"),
        },
    ],
    "edges": [
        {
            "source": "Bob",
            "target": "Alice",
            "import_follows_prop_1": "TEST IMPORT FOLLOWS PROPERTY VALUE",
            "SOURCE_LABEL": "PersonImportLabel",
            "TARGET_LABEL": "PersonImportLabel",
            "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
        }
    ],
}


def test_export_import(use_graph):
    archy = PersonImportNode(name="archy", age=55)
    betty = PersonImportNode(name="betty", age=66)
    bobalicerel = FollowsImportRel(
        source=archy, target=betty, import_follows_prop_1="testing"
    )

    import_data = {
        "nodes": [archy.neontology_dump(), betty.neontology_dump()],
        "edges": [bobalicerel.neontology_dump()],
    }

    import_records([import_data])

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_dump_and_import(use_graph):
    archy = PersonImportNode(name="archy", age=55)
    archy.merge()
    betty = PersonImportNode(name="betty", age=66)
    betty.merge()
    bobalicerel = FollowsImportRel(
        source=archy, target=betty, import_follows_prop_1="testing"
    )
    bobalicerel.merge()

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1

    results = use_graph.evaluate_query("MATCH (n)-[r]->(o) RETURN *")

    import_data = results.neontology_dump()

    use_graph.evaluate_query_single("MATCH (n) DETACH DELETE n")

    assert len(PersonImportNode.match_nodes()) == 0
    assert len(FollowsImportRel.match_relationships()) == 0

    import_records([import_data])

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_import_records_basic_link_data(use_graph):
    import_records([records_raw], error_on_unmatched=True)

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_import_records_sub_record_target_nodes(use_graph):
    with_sub_records = {
        "LABEL": "PersonImportLabel",
        "name": "Bob",
        "age": 84,
        "import_id": UUID("154b0bb7-33ce-4c5b-ba6a-44b5b09b0067"),
        "RELATIONSHIPS_OUT": [
            {
                "TARGET_NODES": [
                    {
                        "LABEL": "PersonImportLabel",
                        "name": "Alice",
                        "age": 76,
                        "import_id": UUID("78fcd953-1d0d-47ae-871c-670a0a7a7406"),
                    },
                ],
                "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
                "TARGET_LABEL": "PersonImportLabel",
                "import_follows_prop_1": "TEST IMPORT FOLLOWS PROPERTY VALUE 2",
            }
        ],
    }

    import_records([with_sub_records], error_on_unmatched=True)

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_import_records_sub_record_targets(use_graph):
    with_sub_records = [
        {
            "LABEL": "PersonImportLabel",
            "name": "Bob",
            "age": 84,
            "import_id": UUID("154b0bb7-33ce-4c5b-ba6a-44b5b09b0067"),
            "RELATIONSHIPS_OUT": [
                {
                    "TARGETS": ["Alice"],
                    "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
                    "TARGET_LABEL": "PersonImportLabel",
                    "import_follows_prop_1": "TEST IMPORT FOLLOWS PROPERTY VALUE 3",
                }
            ],
        },
        {
            "LABEL": "PersonImportLabel",
            "name": "Alice",
            "age": 76,
            "import_id": UUID("78fcd953-1d0d-47ae-871c-670a0a7a7406"),
        },
    ]

    import_records([with_sub_records], error_on_unmatched=True)

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_import_records_bad_node(use_graph):
    bad_records = [
        {
            "LABELED": "PersonImportLabel",
            "name": "Alice",
            "age": 76,
            "import_id": UUID("78fcd953-1d0d-47ae-871c-670a0a7a7406"),
        },
    ]

    with pytest.raises(ValueError):
        import_records(bad_records, error_on_unmatched=True)


def test_import_records_bad_node_validate_only(use_graph):
    # doesn't have a TARGET_LABEL
    bad_records = [
        {
            "source": "Bob",
            "target": "Alice",
            "import_follows_prop_1": "TEST IMPORT FOLLOWS PROPERTY VALUE",
            "SOURCE_LABEL": "PersonImportLabel",
            "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
        }
    ]

    with pytest.raises(ValidationError):
        import_records(bad_records, validate_only=True, error_on_unmatched=True)
