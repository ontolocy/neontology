"""Relationships arriving at a node, and matching either end on another property."""

from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship
from neontology.tools import ImportContentError, import_records


class InHost(BaseNode):
    __primarylabel__: ClassVar[str] = "InHost"
    __primaryproperty__: ClassVar[str] = "hostname"

    hostname: str
    asset_tag: Optional[str] = None


class InService(BaseNode):
    __primarylabel__: ClassVar[str] = "InService"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    code: Optional[str] = None


class InRunsOn(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "IN_RUNS_ON"

    source: InService
    target: InHost

    note: Optional[str] = None


def _host(hostname, **kwargs):
    return {"LABEL": "InHost", "hostname": hostname, **kwargs}


def _service(name, **kwargs):
    return {"LABEL": "InService", "name": name, **kwargs}


# --- relationships arriving at the node that declares them --------------------------


def test_relationships_in_names_the_declaring_node_as_the_target(use_graph):
    records = [
        [
            _service("web"),
            _service("api"),
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCES": ["web", "api"],
                    }
                ],
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    rels = InRunsOn.match_relationships()

    assert len(rels) == 2
    assert {x.source.name for x in rels} == {"web", "api"}
    assert {x.target.hostname for x in rels} == {"host1"}


def test_relationships_in_takes_a_single_source(use_graph):
    records = [
        [
            _service("web"),
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCE": "web",
                    }
                ],
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert len(InRunsOn.match_relationships()) == 1


def test_relationships_in_can_define_its_sources_inline(use_graph):
    records = [
        [
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCE_NODES": [_service("web"), _service("api")],
                    }
                ],
            }
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert InService.get_count() == 2
    assert len(InRunsOn.match_relationships()) == 2


def test_relationships_in_carries_its_own_properties(use_graph):
    records = [
        [
            _service("web"),
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCES": ["web"],
                        "note": "declared from the host",
                    }
                ],
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert InRunsOn.match_relationships()[0].note == "declared from the host"


def test_a_node_can_declare_relationships_both_ways(use_graph):
    records = [
        [
            _service("web"),
            _service("api"),
            _host("host2"),
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCES": ["web"],
                    }
                ],
                "RELATIONSHIPS_OUT": [
                    {
                        "RELATIONSHIP_TYPE": "IN_HOSTED_BY",
                        "TARGET_LABEL": "InHost",
                        "TARGETS": ["host2"],
                    }
                ],
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert len(InRunsOn.match_relationships()) == 1
    assert len(InHostedBy.match_relationships()) == 1


class InHostedBy(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "IN_HOSTED_BY"

    source: InHost
    target: InHost


def test_relationships_in_without_a_source_label_is_reported(use_graph):
    records = [
        [
            _service("web"),
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {"RELATIONSHIP_TYPE": "IN_RUNS_ON", "SOURCES": ["web"]},
                ],
            },
        ]
    ]

    with pytest.raises(ImportContentError, match="SOURCE_LABEL"):
        import_records(records)


def test_relationships_in_naming_no_source_is_reported(use_graph):
    records = [
        [
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {"RELATIONSHIP_TYPE": "IN_RUNS_ON", "SOURCE_LABEL": "InService"},
                ],
            }
        ]
    ]

    with pytest.raises(ImportContentError, match="SOURCE"):
        import_records(records)


def test_both_ends_naming_several_nodes_is_reported(use_graph):
    records = [
        [
            {
                "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                "SOURCE_LABEL": "InService",
                "TARGET_LABEL": "InHost",
                "SOURCES": ["web", "api"],
                "TARGETS": ["host1", "host2"],
            }
        ]
    ]

    with pytest.raises(ImportContentError, match="both ends"):
        import_records(records, check_unmatched=False)


def test_a_top_level_record_can_name_several_sources(use_graph):
    records = [
        [
            _service("web"),
            _service("api"),
            _host("host1"),
            {
                "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                "SOURCE_LABEL": "InService",
                "TARGET_LABEL": "InHost",
                "SOURCES": ["web", "api"],
                "TARGET": "host1",
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert len(InRunsOn.match_relationships()) == 2


# --- matching the source on a property which is not its primary property ------------


def test_source_property_matches_on_another_property(use_graph):
    records = [
        [
            _service("web", code="SVC-1"),
            _host("host1"),
            {
                "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                "SOURCE_LABEL": "InService",
                "TARGET_LABEL": "InHost",
                "SOURCE": "SVC-1",
                "SOURCE_PROPERTY": "code",
                "TARGET": "host1",
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    rels = InRunsOn.match_relationships()

    assert len(rels) == 1
    assert rels[0].source.name == "web"


def test_source_property_works_under_relationships_in(use_graph):
    records = [
        [
            _service("web", code="SVC-1"),
            {
                **_host("host1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCES": ["SVC-1"],
                        "SOURCE_PROPERTY": "code",
                    }
                ],
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert InRunsOn.match_relationships()[0].source.name == "web"


def test_target_property_works_under_relationships_in(use_graph):
    # the declaring node is the target, so its own property can be matched on too
    records = [
        [
            _service("web"),
            {
                **_host("host1", asset_tag="ASSET-1"),
                "RELATIONSHIPS_IN": [
                    {
                        "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                        "SOURCE_LABEL": "InService",
                        "SOURCES": ["web"],
                        "TARGET_PROPERTY": "asset_tag",
                    }
                ],
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert InRunsOn.match_relationships()[0].target.hostname == "host1"


def test_source_property_which_resolves_nowhere_is_reported(use_graph):
    records = [
        [
            _service("web", code="SVC-1"),
            _host("host1"),
            {
                "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                "SOURCE_LABEL": "InService",
                "TARGET_LABEL": "InHost",
                "SOURCE": "SVC-NOPE",
                "SOURCE_PROPERTY": "code",
                "TARGET": "host1",
            },
        ]
    ]

    with pytest.raises(ImportContentError, match="SVC-NOPE"):
        import_records(records, error_on_unmatched=True)


def test_relationships_with_different_source_properties_are_both_merged(use_graph):
    # grouped by the property each end is matched on, so these cannot share a query
    records = [
        [
            _service("web", code="SVC-1"),
            _service("api"),
            _host("host1"),
            {
                "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                "SOURCE_LABEL": "InService",
                "TARGET_LABEL": "InHost",
                "SOURCE": "SVC-1",
                "SOURCE_PROPERTY": "code",
                "TARGET": "host1",
            },
            {
                "RELATIONSHIP_TYPE": "IN_RUNS_ON",
                "SOURCE_LABEL": "InService",
                "TARGET_LABEL": "InHost",
                "SOURCE": "api",
                "TARGET": "host1",
            },
        ]
    ]

    import_records(records, error_on_unmatched=True)

    assert len(InRunsOn.match_relationships()) == 2
