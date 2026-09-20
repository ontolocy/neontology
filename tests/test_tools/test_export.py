"""Writing a graph back out as content, and reading it in again."""

import json
from typing import ClassVar, Optional

import yaml

from neontology import BaseNode, BaseRelationship
from neontology.tools import export_json, export_records, export_yaml, import_json, import_records, import_yaml


class ExportHost(BaseNode):
    __primarylabel__: ClassVar[str] = "ExportHost"
    __primaryproperty__: ClassVar[str] = "hostname"

    hostname: str
    owner: Optional[str] = None


class ExportSite(BaseNode):
    __primarylabel__: ClassVar[str] = "ExportSite"
    __primaryproperty__: ClassVar[str] = "code"

    code: str


class ExportLocatedIn(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "EXPORT_LOCATED_IN"

    source: ExportHost
    target: ExportSite

    since: Optional[int] = None


def _build(graph):
    web1 = ExportHost(hostname="web1", owner="platform")
    web2 = ExportHost(hostname="web2")
    site = ExportSite(code="LON")

    web1.merge()
    web2.merge()
    site.merge()

    ExportLocatedIn(source=web1, target=site, since=2019).merge()
    ExportLocatedIn(source=web2, target=site, since=2021).merge()

    return graph.evaluate_query(
        "MATCH (n:ExportHost)-[r:EXPORT_LOCATED_IN]->(o:ExportSite) RETURN n, r, o",
    )


def _clear(graph):
    from neontology.graphengines.capabilities import Capability

    if graph.engine.supports(Capability.GRAPH_MUTATIONS):
        graph.evaluate_query_single("MATCH (n) DETACH DELETE n")

    else:
        graph.engine.driver.clear()


def test_export_records_writes_the_content_format(use_graph):
    result = _build(use_graph)

    records = export_records(result)

    labels = {x["LABEL"] for x in records}

    assert labels == {"ExportHost", "ExportSite"}

    # relationships are declared under the node they leave
    web1 = [x for x in records if x.get("hostname") == "web1"][0]

    assert web1["owner"] == "platform"
    assert len(web1["RELATIONSHIPS_OUT"]) == 1

    declared = web1["RELATIONSHIPS_OUT"][0]

    assert declared["RELATIONSHIP_TYPE"] == "EXPORT_LOCATED_IN"
    assert declared["TARGET_LABEL"] == "ExportSite"
    assert declared["TARGETS"] == ["LON"]
    assert declared["since"] == 2019


def test_a_node_with_no_relationships_carries_no_block(use_graph):
    result = _build(use_graph)

    records = export_records(result)

    site = [x for x in records if x.get("code") == "LON"][0]

    assert "RELATIONSHIPS_OUT" not in site


def test_exported_records_import_again(use_graph):
    result = _build(use_graph)

    records = export_records(result)

    _clear(use_graph)

    assert ExportHost.get_count() == 0

    report = import_records([records], error_on_unmatched=True)

    assert report.nodes == {"ExportHost": 2, "ExportSite": 1}
    assert report.relationships == {"EXPORT_LOCATED_IN": 2}

    assert ExportHost.match("web1").owner == "platform"
    assert {x.since for x in ExportLocatedIn.match_relationships()} == {2019, 2021}


def test_export_yaml_round_trips(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("export")

    result = _build(use_graph)

    path = dir_path / "graph.yaml"

    export_yaml(result, path)

    # readable as ordinary yaml, not just by us
    written = yaml.safe_load(path.read_text())

    assert len(written) == 3

    _clear(use_graph)

    import_yaml(path, error_on_unmatched=True)

    assert ExportHost.get_count() == 2
    assert len(ExportLocatedIn.match_relationships()) == 2


def test_export_json_round_trips(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("export")

    result = _build(use_graph)

    path = dir_path / "graph.json"

    export_json(result, path)

    written = json.loads(path.read_text())

    assert len(written) == 3

    _clear(use_graph)

    import_json(path, error_on_unmatched=True)

    assert ExportHost.get_count() == 2
    assert len(ExportLocatedIn.match_relationships()) == 2


def test_export_leaves_out_properties_which_were_never_set(use_graph):
    result = _build(use_graph)

    records = export_records(result)

    web2 = [x for x in records if x.get("hostname") == "web2"][0]

    assert "owner" not in web2


def test_a_result_of_nodes_alone_exports_no_relationships(use_graph):
    # a relationship is only built when the result holds the nodes at both its ends, so
    # a query returning nodes alone has no relationships to declare under them
    _build(use_graph)

    result = use_graph.evaluate_query("MATCH (n:ExportHost) RETURN n")

    assert result.relationships == []

    records = export_records(result)

    assert len(records) == 2
    assert all("RELATIONSHIPS_OUT" not in x for x in records)
