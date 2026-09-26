import logging
from typing import ClassVar, Optional

import pytest
from pydantic import ValidationError

from neontology import BaseNode, BaseRelationship, GraphConnection
from neontology.graphengines.capabilities import Capability
from neontology.tools.errors import (
    ConflictingNodeRecordError,
    DuplicateNodeDefinitionError,
    ImportContentError,
    ImportValidationError,
)
from neontology.tools.import_records import import_records

logger = logging.getLogger(__name__)


class PersonImportNode(BaseNode):
    __primarylabel__: ClassVar[str] = "PersonImportLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    age: int

    import_id: str


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
            "import_id": "bob-1",
        },
        {
            "LABEL": "PersonImportLabel",
            "name": "Alice",
            "age": 76,
            "import_id": "alice-1",
        },
    ],
    "edges": [
        {
            "SOURCE": "Bob",
            "TARGET": "Alice",
            "import_follows_prop_1": "TEST IMPORT FOLLOWS PROPERTY VALUE",
            "SOURCE_LABEL": "PersonImportLabel",
            "TARGET_LABEL": "PersonImportLabel",
            "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
        }
    ],
}


def test_export_import(use_graph):
    archy = PersonImportNode(name="archy", age=55, import_id="archy-1")
    betty = PersonImportNode(name="betty", age=66, import_id="betty-1")
    bobalicerel = FollowsImportRel(source=archy, target=betty, import_follows_prop_1="testing")

    import_data = {
        "nodes": [archy.neontology_dump(), betty.neontology_dump()],
        "edges": [bobalicerel.neontology_dump()],
    }

    import_records([import_data])

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_dump_and_import(use_graph):
    archy = PersonImportNode(name="archy", age=55, import_id="archy-1")
    archy.merge()
    betty = PersonImportNode(name="betty", age=66, import_id="betty-1")
    betty.merge()
    bobalicerel = FollowsImportRel(source=archy, target=betty, import_follows_prop_1="testing")
    bobalicerel.merge()

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1

    results = use_graph.evaluate_query("MATCH (n)-[r]->(o) RETURN n,r,o")

    import_data = results.neontology_dump()

    if use_graph.engine.supports(Capability.GRAPH_MUTATIONS):
        use_graph.evaluate_query_single("MATCH (n) DETACH DELETE n")

    else:
        # without GRAPH_MUTATIONS there is no DETACH DELETE
        use_graph.engine.driver.clear()

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
        "import_id": "bob-1",
        "RELATIONSHIPS_OUT": [
            {
                "TARGET_NODES": [
                    {
                        "LABEL": "PersonImportLabel",
                        "name": "Alice",
                        "age": 76,
                        "import_id": "alice-1",
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
            "import_id": "bob-1",
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
            "import_id": "alice-1",
        },
    ]

    import_records([with_sub_records], error_on_unmatched=True)

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_import_records_target_props(use_graph):
    with_tgt_props = [
        {
            "LABEL": "PersonImportLabel",
            "name": "Beth",
            "age": 84,
            "import_id": "beth-id-1",
            "RELATIONSHIPS_OUT": [
                {
                    "TARGETS": ["alex-id-1"],
                    "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
                    "TARGET_LABEL": "PersonImportLabel",
                    "TARGET_PROPERTY": "import_id",
                    "import_follows_prop_1": "TEST IMPORT WITH TARGET PROPS",
                }
            ],
        },
        {
            "LABEL": "PersonImportLabel",
            "name": "Alex",
            "age": 76,
            "import_id": "alex-id-1",
        },
    ]

    import_records([with_tgt_props], error_on_unmatched=True)

    assert len(PersonImportNode.match_nodes()) == 2
    assert len(FollowsImportRel.match_relationships()) == 1


def test_import_records_bad_node(use_graph):
    bad_records = [
        {
            "LABELED": "PersonImportLabel",
            "name": "Alice",
            "age": 76,
            "import_id": "alice-1",
        },
    ]

    with pytest.raises(ValueError):
        import_records(bad_records, error_on_unmatched=True)


def test_import_records_bad_node_validate_only(use_graph):
    # doesn't have a TARGET_LABEL
    bad_records = [
        {
            "SOURCE": "Bob",
            "TARGET": "Alice",
            "import_follows_prop_1": "TEST IMPORT FOLLOWS PROPERTY VALUE",
            "SOURCE_LABEL": "PersonImportLabel",
            "RELATIONSHIP_TYPE": "IMPORT_FOLLOWS",
        }
    ]

    # validating reports every problem it finds rather than raising the first, so the
    # model's own error arrives inside an ImportValidationError
    with pytest.raises(ImportValidationError, match="TARGET_LABEL"):
        import_records(bad_records, validate_only=True, error_on_unmatched=True)


class HostImportNode(BaseNode):
    __primarylabel__: ClassVar[str] = "HostImportLabel"
    __primaryproperty__: ClassVar[str] = "hostname"

    hostname: str
    ip: Optional[str] = None
    owner: Optional[str] = None


class ResolvesImportRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "IMPORT_RESOLVES_TO"

    source: HostImportNode
    target: HostImportNode


def _host(name, **kwargs):
    return {"LABEL": "HostImportLabel", "hostname": name, **kwargs}


def _resolves(**kwargs):
    return {
        "RELATIONSHIP_TYPE": "IMPORT_RESOLVES_TO",
        "SOURCE_LABEL": "HostImportLabel",
        "TARGET_LABEL": "HostImportLabel",
        **kwargs,
    }


# --- endpoint keys: a top level relationship record --------------------------------


def test_top_level_relationship_uppercase_source_target(use_graph):
    # the format is uppercase throughout, so a hand written relationship record
    # spells its endpoints SOURCE and TARGET
    import_records([[_host("a"), _host("b"), _resolves(SOURCE="a", TARGET="b")]])

    assert len(ResolvesImportRel.match_relationships()) == 1


def test_top_level_relationship_lowercase_source_target_still_works(use_graph):
    # what the importer has always accepted, and neontology_dump() wrote before v3
    with pytest.warns(DeprecationWarning, match="lowercase 'source' or 'target'"):
        import_records([[_host("a"), _host("b"), _resolves(source="a", target="b")]])

    assert len(ResolvesImportRel.match_relationships()) == 1


def test_top_level_relationship_targets_list(use_graph):
    import_records([[_host("a"), _host("b"), _host("c"), _resolves(SOURCE="a", TARGETS=["b", "c"])]])

    assert len(ResolvesImportRel.match_relationships()) == 2


def test_top_level_relationship_target_nodes_inline(use_graph):
    import_records([[_host("a"), _resolves(SOURCE="a", TARGET_NODES=[_host("b"), _host("c")])]])

    assert HostImportNode.get_count() == 3
    assert len(ResolvesImportRel.match_relationships()) == 2


def test_top_level_relationship_target_property(use_graph):
    import_records(
        [
            [
                _host("a"),
                _host("b", ip="10.0.0.1"),
                _resolves(SOURCE="a", TARGETS=["10.0.0.1"], TARGET_PROPERTY="ip"),
            ]
        ]
    )

    assert len(ResolvesImportRel.match_relationships()) == 1


# --- clear errors -------------------------------------------------------------------


def test_relationship_record_without_source_label_errors(use_graph):
    records = [
        [
            _host("a"),
            _host("b"),
            {
                "RELATIONSHIP_TYPE": "IMPORT_RESOLVES_TO",
                "TARGET_LABEL": "HostImportLabel",
                "SOURCE": "a",
                "TARGET": "b",
            },
        ]
    ]

    with pytest.raises((ValueError, ValidationError), match="SOURCE_LABEL"):
        import_records(records)


def test_unknown_label_errors_clearly(use_graph):
    with pytest.raises(ValueError, match="NotAModelLabel"):
        import_records([[{"LABEL": "NotAModelLabel", "hostname": "a"}]])


def test_unknown_relationship_type_errors_clearly(use_graph):
    records = [[_host("a"), _host("b"), _resolves(RELATIONSHIP_TYPE="NOT_A_REL_TYPE", SOURCE="a", TARGET="b")]]

    with pytest.raises(ValueError, match="NOT_A_REL_TYPE"):
        import_records(records)


# --- endpoints which do not resolve -------------------------------------------------


def test_missing_source_node_is_reported(use_graph):
    # a relationship whose source is absent merges nothing at all, so it must not
    # pass silently the way it used to
    records = [[_host("b"), _resolves(SOURCE="nosuchhost", TARGET="b")]]

    with pytest.raises(ValueError, match="nosuchhost"):
        import_records(records, error_on_unmatched=True)


def test_missing_source_node_warns_by_default(use_graph, caplog):
    records = [[_host("b"), _resolves(SOURCE="nosuchhost", TARGET="b")]]

    with caplog.at_level(logging.WARNING):
        import_records(records)

    assert "nosuchhost" in caplog.text
    assert len(ResolvesImportRel.match_relationships()) == 0


def test_missing_target_node_is_reported(use_graph):
    records = [[_host("a"), _resolves(SOURCE="a", TARGET="nosuchhost")]]

    with pytest.raises(ValueError, match="nosuchhost"):
        import_records(records, error_on_unmatched=True)


def test_endpoint_checks_are_batched(use_graph, monkeypatch):
    # the endpoint check looks up each label and property once, rather than issuing a
    # query per relationship, so the cost does not grow with the size of the import
    calls = []

    original = GraphConnection.evaluate_query_single

    def counting_evaluate_query_single(self, *args, **kwargs):
        calls.append(args[0] if args else None)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(GraphConnection, "evaluate_query_single", counting_evaluate_query_single)

    hosts = [_host(f"h{i}") for i in range(20)]
    rels = [_resolves(SOURCE="h0", TARGET=f"h{i}") for i in range(1, 20)]

    import_records([hosts + rels], error_on_unmatched=True)

    assert len(ResolvesImportRel.match_relationships()) == 19

    # one to merge the nodes, one to merge the relationships, and one per label and
    # property the endpoints are looked up by - not one per relationship
    assert len(calls) <= 6, f"expected the endpoint check to batch, got {len(calls)} queries"


# --- node records for the same node: union, duplicate and conflict ------------------


def test_inline_node_does_not_clobber_a_definition(use_graph):
    # the inline record names the node so a relationship can point at it; the
    # definition carries its properties. Neither order may lose the properties.
    records = [
        [
            _host("dns1", owner="ops"),
            _resolves(SOURCE="dns1", TARGET_NODES=[_host("web1")]),
            _host("web1", ip="10.0.0.1", owner="web"),
        ]
    ]

    import_records(records)

    web1 = HostImportNode.match("web1")

    assert web1.ip == "10.0.0.1"
    assert web1.owner == "web"


def test_inline_node_does_not_clobber_a_definition_reverse_order(use_graph):
    records = [
        [
            _host("web1", ip="10.0.0.1", owner="web"),
            _host("dns1", owner="ops"),
            _resolves(SOURCE="dns1", TARGET_NODES=[_host("web1")]),
        ]
    ]

    import_records(records)

    web1 = HostImportNode.match("web1")

    assert web1.ip == "10.0.0.1"
    assert web1.owner == "web"


def test_identical_inline_records_for_one_node_are_fine(use_graph):
    # the same IP related to two DNS records - inline nodes merge, so this is normal
    records = [
        [
            _host("dns1"),
            _resolves(SOURCE="dns1", TARGET_NODES=[_host("web1", ip="10.0.0.1")]),
            _host("dns2"),
            _resolves(SOURCE="dns2", TARGET_NODES=[_host("web1", ip="10.0.0.1")]),
        ]
    ]

    import_records(records)

    assert HostImportNode.get_count() == 3
    assert HostImportNode.match("web1").ip == "10.0.0.1"


def test_inline_record_agreeing_with_a_definition_is_fine(use_graph):
    records = [
        [
            _host("dns1"),
            _resolves(SOURCE="dns1", TARGET_NODES=[_host("web1", ip="10.0.0.1")]),
            _host("web1", ip="10.0.0.1", owner="web"),
        ]
    ]

    import_records(records)

    assert HostImportNode.match("web1").owner == "web"


def test_two_definitions_of_one_node_error(use_graph):
    records = [[_host("web1", ip="10.0.0.1"), _host("web1", ip="10.0.0.1")]]

    with pytest.raises(DuplicateNodeDefinitionError, match="web1"):
        import_records(records)


def test_contradicting_records_error(use_graph):
    records = [
        [
            _host("dns1"),
            _resolves(SOURCE="dns1", TARGET_NODES=[_host("web1", ip="10.0.0.1")]),
            _host("web1", ip="10.0.0.99"),
        ]
    ]

    with pytest.raises(ConflictingNodeRecordError, match="ip"):
        import_records(records)


def test_duplicate_and_conflict_errors_can_be_caught_together(use_graph):
    assert issubclass(DuplicateNodeDefinitionError, ImportContentError)
    assert issubclass(ConflictingNodeRecordError, ImportContentError)
    assert issubclass(ImportContentError, ValueError)


def test_nested_dump_round_trips(use_graph):
    # the node oriented shape a repository is usually written in, back into the graph
    archy = PersonImportNode(name="archy", age=55, import_id="archy-1")
    betty = PersonImportNode(name="betty", age=66, import_id="betty-1")
    archy.merge()
    betty.merge()
    FollowsImportRel(source=archy, target=betty, import_follows_prop_1="testing").merge()

    result = use_graph.evaluate_query("MATCH (n:PersonImportLabel)-[r:IMPORT_FOLLOWS]->(o:PersonImportLabel) RETURN n, r, o")

    records = result.neontology_dump(nested=True)

    if use_graph.engine.supports(Capability.GRAPH_MUTATIONS):
        use_graph.evaluate_query_single("MATCH (n) DETACH DELETE n")

    else:
        use_graph.engine.driver.clear()

    assert PersonImportNode.get_count() == 0

    report = import_records([records], error_on_unmatched=True)

    assert report.nodes == {"PersonImportLabel": 2}
    assert report.relationships == {"IMPORT_FOLLOWS": 1}
    assert PersonImportNode.match("archy").age == 55
