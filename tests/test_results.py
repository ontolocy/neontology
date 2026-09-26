"""How query results are built into Neontology nodes, relationships and paths.

Every engine builds results the same way, so the same query gives the same result whatever
the backend:

- `records` has one entry per row returned. Anything which cannot be built as a Neontology
  object - a node with no defined class, a relationship whose node cannot be built, a path
  including such a relationship - is left out of its record, with a warning.
- `nodes`, `relationships` and `paths` hold each distinct node, relationship and path in
  the database once, in the order first seen, however many rows it appears on. Distinct
  means the database's own identity, never equal contents: two parallel relationships with
  the same properties are two relationships.
- a relationship's source and target are the same objects as the nodes in the result.
- anything else a row returns - a property, an aggregate, a literal - is in its record's
  `values`, under the name it was returned as, converted to native Python types.

`records_raw` is the driver's own result, for anyone needing it verbatim.
"""

import json
import warnings
from datetime import date
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship, NeontologyResult, NeontologyWarning, get_rels_by_type
from neontology.graphengines.capabilities import Capability


class ResultsPerson(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "ResultsPerson"

    name: str
    note: Optional[str] = None


class ResultsPlace(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "ResultsPlace"

    name: str


class ResultsEvent(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "ResultsEvent"

    name: str
    held_on: date


class ResultsVisits(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "RESULTS_VISITS"

    source: ResultsPerson
    target: ResultsPlace


class ResultsHosts(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "RESULTS_HOSTS"

    source: ResultsPlace
    target: ResultsPerson


VISITS = "MATCH (p:ResultsPerson)-[r:RESULTS_VISITS]->(o:ResultsPlace) RETURN p, r, o"

# the one visit, repeated on a row for each of the three people
VISITS_ON_EVERY_ROW = "MATCH (p:ResultsPerson)-[r:RESULTS_VISITS]->(o:ResultsPlace) MATCH (c:ResultsPerson) RETURN p, r, o, c"

TWO_HOP_PATH = "MATCH x=(p:ResultsPerson)-[r:RESULTS_VISITS]->(o:ResultsPlace)-[h:RESULTS_HOSTS]->(q:ResultsPerson) RETURN x"


@pytest.fixture
def graph(use_graph):
    """alice visits paris, which hosts bob. carol is on her own."""
    alice = ResultsPerson(name="alice")
    bob = ResultsPerson(name="bob")
    carol = ResultsPerson(name="carol")
    paris = ResultsPlace(name="paris")

    for node in (alice, bob, carol, paris):
        node.merge()

    ResultsVisits(source=alice, target=paris).merge()
    ResultsHosts(source=paris, target=bob).merge()

    return use_graph


def _names(items: list) -> list:
    return sorted(item.name for item in items)


class TestRecords:
    def test_a_query_returning_only_values_has_one_record_per_row(self, graph):
        result = graph.evaluate_query("MATCH (p:ResultsPerson) RETURN p.name AS name")

        assert len(result.records) == 3

        for record in result.records:
            assert (record["nodes"], record["relationships"], record["paths"]) == ({}, {}, {})

    def test_a_node_which_cannot_be_built_is_left_out_of_its_record(self, graph):
        # the relationship to it cannot be built either, and says so
        with (
            pytest.warns(NeontologyWarning, match="ResultsPlace"),
            pytest.warns(NeontologyWarning, match="RESULTS_VISITS relationship.*could not be built"),
        ):
            result = graph.evaluate_query(VISITS, node_classes={"ResultsPerson": ResultsPerson})

        assert set(result.records[0]["nodes"]) == {"p"}


class TestValues:
    """Values which are not nodes, relationships or paths are kept in their record."""

    def test_a_value_returned_beside_a_node_is_in_the_same_record(self, graph):
        result = graph.evaluate_query("MATCH (p:ResultsPerson) WHERE p.name = 'alice' RETURN p, p.name AS name")

        (record,) = result.records

        assert record["nodes"]["p"].name == "alice"
        assert record["values"] == {"name": "alice"}

    def test_a_projection_is_read_row_by_row(self, graph):
        result = graph.evaluate_query("MATCH (p:ResultsPerson) RETURN p.name AS name, p.note AS note ORDER BY p.name")

        assert [record["values"] for record in result.records] == [
            {"name": "alice", "note": None},
            {"name": "bob", "note": None},
            {"name": "carol", "note": None},
        ]

    def test_an_aggregate_is_a_value(self, graph):
        result = graph.evaluate_query("MATCH (p:ResultsPerson) RETURN COUNT(p) AS people")

        assert [record["values"] for record in result.records] == [{"people": 3}]

    def test_a_graph_value_is_not_repeated_in_values(self, graph):
        result = graph.evaluate_query(VISITS)

        assert result.records[0]["values"] == {}

    def test_a_temporal_value_is_native(self, graph):
        ResultsEvent(name="launch", held_on=date(2026, 9, 26)).merge()

        result = graph.evaluate_query("MATCH (e:ResultsEvent) RETURN e.held_on AS held_on")

        (record,) = result.records

        assert record["values"]["held_on"] == date(2026, 9, 26)
        assert type(record["values"]["held_on"]) is date

    def test_a_value_which_is_a_list_is_native(self, graph):
        ResultsEvent(name="launch", held_on=date(2026, 9, 26)).merge()

        result = graph.evaluate_query("MATCH (e:ResultsEvent) RETURN COLLECT(e.held_on) AS dates")

        (record,) = result.records

        assert record["values"]["dates"] == [date(2026, 9, 26)]
        assert type(record["values"]["dates"][0]) is date


class TestNodes:
    def test_a_node_on_several_rows_appears_once(self, graph):
        result = graph.evaluate_query(VISITS_ON_EVERY_ROW)

        assert len(result.records) == 3
        assert _names(result.nodes) == ["alice", "bob", "carol", "paris"]

    def test_a_node_on_several_rows_is_the_same_object_on_each(self, graph):
        result = graph.evaluate_query(VISITS_ON_EVERY_ROW)

        first, *others = [record["nodes"]["p"] for record in result.records]

        assert all(other is first for other in others)

    @pytest.mark.requires_capability(Capability.DUPLICATE_CREATE)
    def test_distinct_nodes_sharing_a_primary_property_are_all_returned(self, use_graph):
        """Nodes are distinct by database identity, so neither one hides the other.

        Two nodes can only share a label and primary property where nothing constrains
        them to be unique. Returning both shows the problem, where keeping one silently
        discarded the other's properties.
        """
        ResultsPerson(name="twin", note="first").create()
        ResultsPerson(name="twin", note="second").create()

        result = use_graph.evaluate_query("MATCH (p:ResultsPerson) WHERE p.name = 'twin' RETURN p")

        assert sorted(node.note for node in result.nodes) == ["first", "second"]

        # node link data identifies nodes by label and primary property, by design
        assert len(result.node_link_data["nodes"]) == 1


class TestRelationships:
    def test_a_relationship_on_several_rows_appears_once(self, graph):
        result = graph.evaluate_query(VISITS_ON_EVERY_ROW)

        assert len(result.relationships) == 1
        assert all(record["relationships"]["r"] is result.relationships[0] for record in result.records)

    def test_source_and_target_are_the_nodes_in_the_result(self, graph):
        result = graph.evaluate_query(VISITS)

        record = result.records[0]

        assert record["relationships"]["r"].source is record["nodes"]["p"]
        assert record["relationships"]["r"].target is record["nodes"]["o"]

    def test_a_relationship_whose_node_cannot_be_built_is_left_out_with_a_warning(self, graph):
        # as is the node, which warns for itself
        with (
            pytest.warns(NeontologyWarning, match="RESULTS_VISITS relationship.*could not be built"),
            pytest.warns(NeontologyWarning, match="ResultsPlace"),
        ):
            result = graph.evaluate_query(VISITS, node_classes={"ResultsPerson": ResultsPerson})

        assert result.relationships == []
        assert result.records[0]["relationships"] == {}

    def test_relationship_classes_can_be_a_plain_dict(self, graph):
        """A relationship type missing from the classes given is warned about, not a KeyError."""
        only_hosts = {"RESULTS_HOSTS": get_rels_by_type()["RESULTS_HOSTS"]}

        with pytest.warns(NeontologyWarning, match="RESULTS_VISITS"):
            result = graph.evaluate_query(VISITS, relationship_classes=only_hosts)

        assert result.relationships == []

    @pytest.mark.requires_capability(Capability.GRAPH_MUTATIONS)
    def test_parallel_relationships_with_the_same_properties_are_both_kept(self, graph):
        graph.evaluate_query_single(
            "MATCH (p:ResultsPerson), (o:ResultsPlace) WHERE p.name = 'alice' AND o.name = 'paris'"
            " CREATE (p)-[:RESULTS_VISITS]->(o)"
        )

        result = graph.evaluate_query(VISITS)

        assert len(result.relationships) == 2
        assert len(result.node_link_data["edges"]) == 2

    def test_a_variable_length_relationship_list_is_neither_a_relationship_nor_a_path(self, graph):
        """A variable length pattern returns a list of relationships, which is not a path.

        Pinned so that the engines agree: the list is not built into Neontology objects.
        Whether a warning is raised is not pinned - a single hop is indistinguishable from
        an ordinary relationship on some engines, and is warned about as one returned
        without its source node.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            result = graph.evaluate_query("MATCH (p:ResultsPerson {name: 'alice'})-[r*1..2]->(o) RETURN r, o")

        assert result.relationships == []
        assert result.paths == []


class TestPaths:
    def test_a_path_returned_on_its_own(self, graph):
        result = graph.evaluate_query(TWO_HOP_PATH)

        assert len(result.paths) == 1

        visit, host = result.paths[0]

        assert (type(visit), type(host)) == (ResultsVisits, ResultsHosts)
        assert (visit.source.name, visit.target.name, host.target.name) == ("alice", "paris", "bob")
        assert result.records[0]["paths"]["x"] == result.paths[0]

    def test_a_path_including_a_relationship_which_cannot_be_built_is_left_out_with_a_warning(self, graph):
        only_visits = {"RESULTS_VISITS": get_rels_by_type()["RESULTS_VISITS"]}

        # as is the relationship, which warns for itself
        with (
            pytest.warns(NeontologyWarning, match="[Pp]ath 'x'"),
            pytest.warns(NeontologyWarning, match="class for the RESULTS_HOSTS relationship type"),
        ):
            result = graph.evaluate_query(TWO_HOP_PATH, relationship_classes=only_visits)

        assert result.paths == []
        assert result.records[0]["paths"] == {}

    @pytest.mark.requires_capability(Capability.MULTI_PATTERN_PATHS)
    def test_a_path_on_several_rows_appears_once(self, graph):
        """The same path on several rows is listed once.

        Repeating a path across rows takes a second pattern to vary the rows, which is what
        MULTI_PATTERN_PATHS names.
        """
        result = graph.evaluate_query(
            "MATCH x=(p:ResultsPerson)-[r:RESULTS_VISITS]->(o:ResultsPlace) MATCH (c:ResultsPerson) RETURN x, c"
        )

        assert len(result.records) == 3
        assert len(result.paths) == 1

    def test_path_steps_are_the_relationships_in_the_result(self, graph):
        result = graph.evaluate_query("MATCH x=(p:ResultsPerson)-[r:RESULTS_VISITS]->(o:ResultsPlace) RETURN p, r, o, x")

        assert result.paths[0][0] is result.relationships[0]


class TestNodeLinkData:
    def test_repr_does_not_build_node_link_data(self):
        """Building it dumps every node and relationship, so printing a result should not."""
        result = NeontologyResult(records_raw=None, records=[], nodes=[], relationships=[], paths=[])

        assert "node_link_data" not in repr(result)

        # still part of the result's data
        assert "node_link_data" in result.model_dump()

    def test_node_link_data_uses_lowercase_endpoint_keys(self, use_graph):
        # D3, Cytoscape and networkx all expect an edge to name its ends 'source' and
        # 'target', so this format keeps them however the content format spells them
        person = ResultsPerson(name="source-node")
        place = ResultsPlace(name="target-node")
        person.merge()
        place.merge()
        ResultsVisits(source=person, target=place).merge()

        result = use_graph.evaluate_query(VISITS)

        edge = result.node_link_data["edges"][0]

        assert edge["source"] == "source-node"
        assert edge["target"] == "target-node"
        assert "SOURCE" not in edge


class TestNestedDump:
    """The dump's node oriented shape: a record per node, relationships nested in it."""

    def test_nested_dump_is_a_record_per_node(self, use_graph):
        person = ResultsPerson(name="ada", note="a note")
        place = ResultsPlace(name="london")
        person.merge()
        place.merge()
        ResultsVisits(source=person, target=place).merge()

        result = use_graph.evaluate_query(VISITS)

        records = result.neontology_dump(nested=True)

        assert isinstance(records, list)
        assert {x["LABEL"] for x in records} == {"ResultsPerson", "ResultsPlace"}

        ada = [x for x in records if x.get("name") == "ada"][0]

        assert ada["note"] == "a note"
        assert len(ada["RELATIONSHIPS_OUT"]) == 1

        declared = ada["RELATIONSHIPS_OUT"][0]

        assert declared["RELATIONSHIP_TYPE"] == "RESULTS_VISITS"
        assert declared["TARGET_LABEL"] == "ResultsPlace"
        assert declared["TARGETS"] == ["london"]

        # the node declaring it is the source, so the record does not name one
        assert "SOURCE" not in declared
        assert "SOURCE_LABEL" not in declared
        assert "TARGET" not in declared

    def test_a_node_with_no_relationships_leaving_it_carries_no_block(self, use_graph):
        person = ResultsPerson(name="ada")
        place = ResultsPlace(name="london")
        person.merge()
        place.merge()
        ResultsVisits(source=person, target=place).merge()

        result = use_graph.evaluate_query(VISITS)

        london = [x for x in result.neontology_dump(nested=True) if x.get("name") == "london"][0]

        assert "RELATIONSHIPS_OUT" not in london

    def test_the_flat_shape_is_still_the_default(self, use_graph):
        ResultsPerson(name="ada").merge()

        result = use_graph.evaluate_query("MATCH (n:ResultsPerson) RETURN n")

        assert set(result.neontology_dump()) == {"nodes", "edges"}

    def test_nested_dump_json_serialises_the_same_shape(self, use_graph):
        person = ResultsPerson(name="ada")
        place = ResultsPlace(name="london")
        person.merge()
        place.merge()
        ResultsVisits(source=person, target=place).merge()

        result = use_graph.evaluate_query(VISITS)

        assert json.loads(result.neontology_dump_json(nested=True)) == result.neontology_dump(nested=True)
