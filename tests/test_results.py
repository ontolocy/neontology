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

`records_raw` is the driver's own result, for anyone needing it verbatim.
"""

import warnings
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship, get_rels_by_type
from neontology.graphengines.capabilities import Capability
from neontology.result import NeontologyResult

EMPTY_RECORD = {"nodes": {}, "relationships": {}, "paths": {}}


class ResultsPerson(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "ResultsPerson"

    name: str
    note: Optional[str] = None


class ResultsPlace(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "ResultsPlace"

    name: str


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
        result = graph.evaluate_query("MATCH (p:ResultsPerson) RETURN p.name")

        assert result.records == [EMPTY_RECORD] * 3

    def test_a_node_which_cannot_be_built_is_left_out_of_its_record(self, graph):
        with pytest.warns(UserWarning, match="ResultsPlace"):
            result = graph.evaluate_query(VISITS, node_classes={"ResultsPerson": ResultsPerson})

        assert set(result.records[0]["nodes"]) == {"p"}


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
        with pytest.warns(UserWarning, match="RESULTS_VISITS relationship.*could not be built"):
            result = graph.evaluate_query(VISITS, node_classes={"ResultsPerson": ResultsPerson})

        assert result.relationships == []
        assert result.records[0]["relationships"] == {}

    def test_relationship_classes_can_be_a_plain_dict(self, graph):
        """A relationship type missing from the classes given is warned about, not a KeyError."""
        only_hosts = {"RESULTS_HOSTS": get_rels_by_type()["RESULTS_HOSTS"]}

        with pytest.warns(UserWarning, match="RESULTS_VISITS"):
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

        with pytest.warns(UserWarning, match="[Pp]ath 'x'"):
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
