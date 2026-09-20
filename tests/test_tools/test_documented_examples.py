"""The examples on the "Importing Content" documentation page, run against the models it describes.

Kept in step with docs/importing-content.md: if an example there changes, change it here.
"""

import json
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship
from neontology.tools import (
    ConflictingNodeRecordError,
    DuplicateNodeDefinitionError,
    ImportContentError,
    ImportValidationError,
    import_csv,
    import_md,
    import_records,
    import_yaml,
)


class Host(BaseNode):
    __primarylabel__: ClassVar[str] = "Host"
    __primaryproperty__: ClassVar[str] = "hostname"

    hostname: str
    owner: Optional[str] = None
    asset_tag: Optional[str] = None
    description: Optional[str] = None


class IPAddress(BaseNode):
    __primarylabel__: ClassVar[str] = "IPAddress"
    __primaryproperty__: ClassVar[str] = "ip"

    ip: str


class ResolvesTo(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "RESOLVES_TO"

    source: Host
    target: IPAddress

    first_seen: Optional[str] = None


class ManagedBy(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "MANAGED_BY"

    source: IPAddress
    target: Host


def _write(dir_path, name, text):
    (dir_path / name).write_text(text)


def test_a_node_record_and_a_relationship_record(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "host.yaml", "LABEL: Host\nhostname: web1\nowner: platform-team\n")
    _write(
        dir_path,
        "ip.yaml",
        "LABEL: IPAddress\nip: 10.0.0.1\n",
    )
    _write(
        dir_path,
        "resolves.yaml",
        "RELATIONSHIP_TYPE: RESOLVES_TO\n"
        "SOURCE_LABEL: Host\n"
        "TARGET_LABEL: IPAddress\n"
        "SOURCE: web1\n"
        "TARGET: 10.0.0.1\n"
        'first_seen: "2026-01-01"\n',
    )

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.nodes == {"Host": 1, "IPAddress": 1}
    assert report.relationships == {"RESOLVES_TO": 1}
    assert Host.match("web1").owner == "platform-team"


def test_combined_layout(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(
        dir_path,
        "web1.yaml",
        "LABEL: Host\n"
        "hostname: web1\n"
        "owner: platform-team\n"
        "RELATIONSHIPS_OUT:\n"
        "  - RELATIONSHIP_TYPE: RESOLVES_TO\n"
        "    TARGET_LABEL: IPAddress\n"
        "    TARGETS:\n"
        "      - 10.0.0.1\n"
        "      - 10.0.0.2\n",
    )
    _write(dir_path, "ips.yaml", "- LABEL: IPAddress\n  ip: 10.0.0.1\n- LABEL: IPAddress\n  ip: 10.0.0.2\n")

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.relationships == {"RESOLVES_TO": 2}


def test_split_layout(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.yaml", "- LABEL: Host\n  hostname: web1\n- LABEL: Host\n  hostname: web2\n")
    _write(dir_path, "ips.yaml", "- LABEL: IPAddress\n  ip: 10.0.0.1\n")
    _write(
        dir_path,
        "resolves.yaml",
        "- RELATIONSHIP_TYPE: RESOLVES_TO\n"
        "  SOURCE_LABEL: Host\n"
        "  TARGET_LABEL: IPAddress\n"
        "  SOURCE: web1\n"
        "  TARGET: 10.0.0.1\n",
    )

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.nodes == {"Host": 2, "IPAddress": 1}
    assert report.relationships == {"RESOLVES_TO": 1}


def test_defining_nodes_inline(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(
        dir_path,
        "dns.yaml",
        "LABEL: Host\n"
        "hostname: dns1\n"
        "RELATIONSHIPS_OUT:\n"
        "  - RELATIONSHIP_TYPE: RESOLVES_TO\n"
        "    TARGET_LABEL: IPAddress\n"
        "    TARGET_NODES:\n"
        "      - LABEL: IPAddress\n"
        "        ip: 10.0.0.1\n"
        "      - LABEL: IPAddress\n"
        "        ip: 10.0.0.2\n",
    )

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.nodes == {"Host": 1, "IPAddress": 2}
    assert IPAddress.get_count() == 2


def test_an_inline_record_and_a_definition_of_one_node(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(
        dir_path,
        "dns.yaml",
        "LABEL: Host\n"
        "hostname: dns1\n"
        "RELATIONSHIPS_OUT:\n"
        "  - RELATIONSHIP_TYPE: RESOLVES_TO\n"
        "    TARGET_LABEL: IPAddress\n"
        "    TARGET_NODES:\n"
        "      - LABEL: IPAddress\n"
        "        ip: 10.0.0.1\n",
    )
    _write(dir_path, "ips.yaml", "LABEL: IPAddress\nip: 10.0.0.1\n")

    import_yaml(dir_path, error_on_unmatched=True)

    assert IPAddress.get_count() == 1


def test_an_inline_node_cannot_declare_relationships(use_graph):
    records = [
        {
            "LABEL": "Host",
            "hostname": "dns1",
            "RELATIONSHIPS_OUT": [
                {
                    "RELATIONSHIP_TYPE": "RESOLVES_TO",
                    "TARGET_LABEL": "IPAddress",
                    "TARGET_NODES": [
                        {"LABEL": "IPAddress", "ip": "10.0.0.1", "RELATIONSHIPS_OUT": []},
                    ],
                }
            ],
        }
    ]

    with pytest.raises(ImportContentError, match="RELATIONSHIPS_OUT"):
        import_records(records)


def test_matching_on_another_property(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.yaml", "LABEL: Host\nhostname: web1\nasset_tag: asset-0091\n")
    _write(dir_path, "ips.yaml", "LABEL: IPAddress\nip: 10.0.0.1\n")
    _write(
        dir_path,
        "managed.yaml",
        "RELATIONSHIP_TYPE: MANAGED_BY\n"
        "SOURCE_LABEL: IPAddress\n"
        "TARGET_LABEL: Host\n"
        "SOURCE: 10.0.0.1\n"
        "TARGETS:\n"
        "  - asset-0091\n"
        "TARGET_PROPERTY: asset_tag\n",
    )

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.relationships == {"MANAGED_BY": 1}


def test_a_duplicate_definition_is_an_error(use_graph):
    records = [
        [
            {"LABEL": "Host", "hostname": "web1"},
            {"LABEL": "Host", "hostname": "web1"},
        ]
    ]

    with pytest.raises(DuplicateNodeDefinitionError):
        import_records(records)


def test_contradicting_records_are_an_error(use_graph):
    records = [
        [
            {"LABEL": "Host", "hostname": "web1", "owner": "one"},
            {
                "LABEL": "IPAddress",
                "ip": "10.0.0.1",
                "RELATIONSHIPS_OUT": [
                    {
                        "RELATIONSHIP_TYPE": "MANAGED_BY",
                        "TARGET_LABEL": "Host",
                        "TARGET_NODES": [{"LABEL": "Host", "hostname": "web1", "owner": "two"}],
                    }
                ],
            },
        ]
    ]

    with pytest.raises(ConflictingNodeRecordError, match="owner"):
        import_records(records)


def test_a_markdown_record(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(
        dir_path,
        "web1.md",
        "---\nLABEL: Host\nhostname: web1\nBODY_PROPERTY: description\n---\nThis host serves the public website.\n",
    )

    report = import_md(dir_path)

    assert report.nodes == {"Host": 1}
    assert Host.match("web1").description == "This host serves the public website."


def test_the_report_fields_documented(use_graph):
    records = [
        [
            {"LABEL": "Host", "hostname": "web1"},
            {"LABEL": "IPAddress", "ip": "10.0.0.1"},
            {
                "RELATIONSHIP_TYPE": "RESOLVES_TO",
                "SOURCE_LABEL": "Host",
                "TARGET_LABEL": "IPAddress",
                "SOURCE": "web1",
                "TARGET": "10.0.0.1",
            },
        ]
    ]

    report = import_records(records)

    assert report.nodes == {"Host": 1, "IPAddress": 1}
    assert report.relationships == {"RESOLVES_TO": 1}
    assert report.node_count == 2
    assert report.files == []
    assert report.unresolved == []


def test_validating_reports_issues_as_data(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.yaml", "- LABEL: Host\n  hostname: web1\n  owner: 42\n")

    try:
        import_yaml(dir_path, validate_only=True)

    except ImportValidationError as exc:
        assert len(exc.issues) == 1

        for issue in exc.issues:
            assert "hosts.yaml" in str(issue.origin)
            assert issue.error is not None

    else:
        pytest.fail("expected the content not to validate")


def test_the_dump_container_round_trips(use_graph):
    host = Host(hostname="web1", owner="platform-team")
    host.merge()
    ip = IPAddress(ip="10.0.0.1")
    ip.merge()
    ResolvesTo(source=host, target=ip).merge()

    result = use_graph.evaluate_query("MATCH (n:Host)-[r:RESOLVES_TO]->(o) RETURN n, r, o")

    data = result.neontology_dump()

    assert {"nodes", "edges"} == set(data)

    report = import_records([data], error_on_unmatched=True)

    assert report.relationships == {"RESOLVES_TO": 1}


def test_relationships_arriving_at_a_node(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.yaml", "- LABEL: Host\n  hostname: dns1\n- LABEL: Host\n  hostname: dns2\n")
    _write(
        dir_path,
        "ip.yaml",
        "LABEL: IPAddress\n"
        "ip: 10.0.0.1\n"
        "RELATIONSHIPS_IN:\n"
        "  - RELATIONSHIP_TYPE: RESOLVES_TO\n"
        "    SOURCE_LABEL: Host\n"
        "    SOURCES:\n"
        "      - dns1\n"
        "      - dns2\n",
    )

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.relationships == {"RESOLVES_TO": 2}


def test_matching_the_declaring_nodes_own_end_on_a_property(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "ips.yaml", "LABEL: IPAddress\nip: 10.0.0.1\n")
    _write(
        dir_path,
        "hosts.yaml",
        "LABEL: Host\n"
        "hostname: web1\n"
        "asset_tag: asset-0091\n"
        "RELATIONSHIPS_IN:\n"
        "  - RELATIONSHIP_TYPE: MANAGED_BY\n"
        "    SOURCE_LABEL: IPAddress\n"
        "    SOURCES: [10.0.0.1]\n"
        "    TARGET_PROPERTY: asset_tag\n",
    )

    report = import_yaml(dir_path, error_on_unmatched=True)

    assert report.relationships == {"MANAGED_BY": 1}


def test_naming_several_nodes_at_both_ends_is_an_error(use_graph):
    records = [
        {
            "RELATIONSHIP_TYPE": "RESOLVES_TO",
            "SOURCE_LABEL": "Host",
            "TARGET_LABEL": "IPAddress",
            "SOURCES": ["web1", "web2"],
            "TARGETS": ["10.0.0.1", "10.0.0.2"],
        }
    ]

    with pytest.raises(ImportContentError, match="both ends"):
        import_records(records, check_unmatched=False)


def test_a_csv_of_nodes(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.csv", "LABEL,hostname,owner\nHost,web1,platform-team\nHost,web2,data-team\n")

    report = import_csv(dir_path)

    assert report.nodes == {"Host": 2}
    assert Host.match("web1").owner == "platform-team"


def test_a_csv_with_defaults(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.csv", "hostname,owner\nweb1,platform-team\n")
    _write(dir_path, "ips.csv", "ip\n10.0.0.1\n")
    _write(dir_path, "resolves.csv", "SOURCE,TARGET\nweb1,10.0.0.1\n")

    import_csv(dir_path / "hosts.csv", defaults={"LABEL": "Host"})
    import_csv(dir_path / "ips.csv", defaults={"LABEL": "IPAddress"})

    report = import_csv(
        dir_path / "resolves.csv",
        defaults={
            "RELATIONSHIP_TYPE": "RESOLVES_TO",
            "SOURCE_LABEL": "Host",
            "TARGET_LABEL": "IPAddress",
        },
        error_on_unmatched=True,
    )

    assert report.relationships == {"RESOLVES_TO": 1}


def test_a_csv_cannot_nest(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    _write(dir_path, "hosts.csv", "LABEL,hostname,RELATIONSHIPS_OUT\nHost,web1,x\n")

    with pytest.raises(ImportContentError, match="flat record"):
        import_csv(dir_path)


def test_exporting_a_result_as_content(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("content")

    host = Host(hostname="web1", owner="platform-team")
    host.merge()
    ip = IPAddress(ip="10.0.0.1")
    ip.merge()
    ResolvesTo(source=host, target=ip).merge()

    result = use_graph.evaluate_query("MATCH (n:Host)-[r:RESOLVES_TO]->(o:IPAddress) RETURN n, r, o")

    records = result.neontology_dump(nested=True)

    web1 = [x for x in records if x.get("hostname") == "web1"][0]

    declared = web1["RELATIONSHIPS_OUT"][0]

    # exactly the keys the documented example shows
    assert set(declared) == {"RELATIONSHIP_TYPE", "TARGET_LABEL", "TARGETS"}
    assert declared["TARGETS"] == ["10.0.0.1"]

    # written as ordinary json, which is what import_json reads
    path = dir_path / "graph.json"
    path.write_text(result.neontology_dump_json(nested=True))

    assert json.loads(path.read_text()) == records
