"""The contract every graph engine must satisfy.

These tests exercise the engine interface directly rather than through the model
layer, so a new backend gets a short, targeted checklist instead of a wall of
failures from unrelated model tests. If you are adding an engine, make this file
pass first.

Behaviour an engine genuinely cannot offer belongs in `Capability`, declared on the
engine, rather than being skipped here.
"""

from typing import ClassVar, Optional

import pytest
from pydantic import Field

from neontology import BaseNode, BaseRelationship, NeontologyResult
from neontology.graphengines.capabilities import Capability


class ContractNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "EngineContractNode"

    pp: str
    number: int = 0


class ContractOtherNode(BaseNode):
    """Shares a property name with ContractNode, to show matching on it respects the label."""

    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "EngineContractOtherNode"

    pp: str
    number: int = 0


class ContractRel(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "ENGINE_CONTRACT_REL"

    source: ContractNode
    target: ContractNode


def test_engine_reports_a_live_connection(use_graph):
    """verify_connection must be True for an engine that is in use."""
    assert use_graph.engine.verify_connection() is True


def test_capabilities_are_declared_from_the_vocabulary(engine):
    """An engine may only declare capabilities that exist, and must answer for all of them."""
    assert engine.supported_capabilities <= frozenset(Capability)

    for capability in Capability:
        assert isinstance(engine.supports(capability), bool)


def test_supports_rejects_a_non_capability(engine):
    """A typo must raise rather than report a feature as unsupported."""
    with pytest.raises(TypeError):
        engine.supports("not_a_capability")


def test_evaluate_query_single_returns_a_scalar(use_graph):
    """Every engine must reduce a single-value query to a plain value."""
    ContractNode(pp="alpha", number=7).create()

    result = use_graph.evaluate_query_single("MATCH (n:EngineContractNode) RETURN n.number")

    assert result == 7
    assert not isinstance(result, (list, dict))


def test_evaluate_query_returns_typed_nodes(use_graph):
    """evaluate_query must rehydrate nodes into their model class."""
    ContractNode(pp="beta", number=1).create()

    result = use_graph.evaluate_query("MATCH (n:EngineContractNode) RETURN n")

    assert isinstance(result, NeontologyResult)
    assert len(result.nodes) == 1
    assert isinstance(result.nodes[0], ContractNode)
    assert result.nodes[0].pp == "beta"


def test_evaluate_query_returns_typed_relationships(use_graph):
    """evaluate_query must rehydrate relationships into their model class."""
    source = ContractNode(pp="source")
    target = ContractNode(pp="target")
    source.create()
    target.create()
    ContractRel(source=source, target=target).merge()

    result = use_graph.evaluate_query("MATCH (n:EngineContractNode)-[r:ENGINE_CONTRACT_REL]->(o) RETURN n, r, o")

    assert len(result.relationships) == 1
    assert isinstance(result.relationships[0], ContractRel)


def test_create_and_match_round_trip(use_graph):
    """Nodes written by an engine must come back with their properties intact."""
    ContractNode(pp="gamma", number=42).create()

    matched = ContractNode.match_nodes()

    assert len(matched) == 1
    assert matched[0].pp == "gamma"
    assert matched[0].number == 42


def test_get_count_returns_an_int(use_graph):
    """get_count must return an int, including zero when nothing matches."""
    assert use_graph.engine.get_count(ContractNode) == 0

    ContractNode(pp="delta").create()

    count = use_graph.engine.get_count(ContractNode)

    assert isinstance(count, int)
    assert count == 1


def test_merge_is_idempotent(use_graph):
    """Merging the same node twice must leave one node, on every engine."""
    ContractNode(pp="epsilon", number=1).merge()
    ContractNode(pp="epsilon", number=2).merge()

    matched = ContractNode.match_nodes()

    assert len(matched) == 1
    assert matched[0].number == 2


def test_match_nodes_supports_limit_and_skip(use_graph):
    """Pagination arguments must be honoured by every engine."""
    ContractNode.merge_nodes([ContractNode(pp=f"page-{i}") for i in range(5)])

    assert len(ContractNode.match_nodes(limit=2)) == 2
    assert len(ContractNode.match_nodes(skip=3)) == 2


def test_delete_removes_a_node(use_graph):
    """delete_nodes must remove the node it names and leave others alone."""
    ContractNode(pp="keep").create()
    ContractNode(pp="remove").create()

    ContractNode.delete("remove")

    remaining = ContractNode.match_nodes()

    assert len(remaining) == 1
    assert remaining[0].pp == "keep"


@pytest.mark.requires_capability(Capability.CONSTRAINTS)
def test_get_constraints_returns_a_list(use_graph):
    """Constraints are optional, but where declared the method must work."""
    assert isinstance(use_graph.engine.get_constraints(), list)


@pytest.mark.requires_capability(Capability.INDEXES)
def test_get_indexes_returns_a_list(use_graph):
    """Indexes are optional, but where declared the method must work."""
    assert isinstance(use_graph.engine.get_indexes(), list)


def test_collect_aggregates_into_a_list(use_graph):
    """Plain COLLECT must work on every engine - it needs no capability guard."""
    ContractNode(pp="one", number=1).merge()
    ContractNode(pp="two", number=2).merge()

    result = use_graph.evaluate_query_single("MATCH (n:EngineContractNode) RETURN COLLECT(n.pp)")

    assert isinstance(result, list)
    assert sorted(result) == ["one", "two"]


@pytest.mark.requires_capability(Capability.COLLECT_DISTINCT)
def test_collect_distinct_deduplicates(use_graph):
    """COLLECT(DISTINCT ...) where the engine supports DISTINCT inside an aggregation."""
    source = ContractNode(pp="source")
    first = ContractNode(pp="first")
    second = ContractNode(pp="second")
    source.merge()
    first.merge()
    second.merge()

    # two relationships into the same target, so a non-distinct collect would repeat it
    ContractRel(source=first, target=source).merge()
    ContractRel(source=second, target=source).merge()

    result = use_graph.evaluate_query_single(
        "MATCH (n:EngineContractNode)-[r:ENGINE_CONTRACT_REL]->(o:EngineContractNode) RETURN COLLECT(DISTINCT o.pp)"
    )

    assert result == ["source"]


def test_export_dict_converter_rejects_unsupported_types(engine):
    """Every engine must reject dicts as property values, consistently."""
    with pytest.raises(TypeError):
        engine.export_dict_converter({"a_dict": {"nested": "value"}})


class ContractMergeOnRel(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "ENGINE_CONTRACT_MERGE_ON_REL"

    source: ContractNode
    target: ContractNode

    tag: int = Field(default=0, json_schema_extra={"merge_on": True})
    note: Optional[str] = None


class ContractCreateRel(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "ENGINE_CONTRACT_CREATE_REL"

    source: ContractNode
    target: ContractNode

    created: Optional[str] = Field(default=None, json_schema_extra={"set_on_create": True})
    seen: Optional[str] = None


class ContractMatchRel(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "ENGINE_CONTRACT_MATCH_REL"

    source: ContractNode
    target: ContractNode

    only_on_match: Optional[str] = Field(default=None, json_schema_extra={"set_on_match": True})


def _contract_pair():
    """Merge the two nodes the relationship contract tests relate."""
    source = ContractNode(pp="merge-source")
    target = ContractNode(pp="merge-target")

    source.merge()
    target.merge()

    return source, target


class TestRelationshipMergeContract:
    """What merging relationships means, stated once for every engine.

    A relationship is identified by its source, target, type and `merge_on` properties.
    Merging one which is already there updates it rather than adding a second, whether
    the two arrive in separate calls or in the same batch.
    """

    def test_merging_the_same_relationship_twice_in_one_call_makes_one(self, use_graph):
        source, target = _contract_pair()

        ContractMergeOnRel.merge_records(
            [
                {"source": "merge-source", "target": "merge-target", "tag": 1},
                {"source": "merge-source", "target": "merge-target", "tag": 1},
            ]
        )

        assert ContractMergeOnRel.get_count() == 1

    def test_a_relationship_to_a_node_which_does_not_exist_is_not_created(self, use_graph):
        _contract_pair()

        ContractMergeOnRel.merge_records(
            [
                {"source": "no-such-node", "target": "merge-target", "tag": 1},
                {"source": "merge-source", "target": "no-such-node", "tag": 1},
            ]
        )

        assert ContractMergeOnRel.get_count() == 0
        assert use_graph.evaluate_query_single("MATCH (n) RETURN COUNT(n)") == 2

    def test_a_relationship_matched_on_another_property_finds_the_node_of_its_class(self, use_graph):
        ContractNode(pp="numbered", number=5).merge()
        target = ContractNode(pp="merge-target")
        target.merge()

        # a node of another class holding the same value must not be picked instead
        ContractOtherNode(pp="other", number=5).merge()

        ContractRel.merge_records([{"source": 5, "target": "merge-target"}], source_prop="number")

        (relationship,) = ContractRel.match_relationships()

        assert relationship.source.pp == "numbered"

    def test_merging_the_same_relationship_in_separate_calls_makes_one(self, use_graph):
        source, target = _contract_pair()

        ContractMergeOnRel(source=source, target=target, tag=1).merge()
        ContractMergeOnRel(source=source, target=target, tag=1).merge()

        assert ContractMergeOnRel.get_count() == 1

    def test_merging_in_one_call_leaves_the_last_value(self, use_graph):
        source, target = _contract_pair()

        ContractMergeOnRel.merge_records(
            [
                {"source": "merge-source", "target": "merge-target", "tag": 1, "note": "first"},
                {"source": "merge-source", "target": "merge-target", "tag": 1, "note": "second"},
            ]
        )

        rels = ContractMergeOnRel.match_relationships()

        assert len(rels) == 1
        assert rels[0].note == "second"

    def test_relationships_differing_on_a_merge_on_property_stay_separate(self, use_graph):
        source, target = _contract_pair()

        ContractMergeOnRel.merge_records(
            [
                {"source": "merge-source", "target": "merge-target", "tag": 1},
                {"source": "merge-source", "target": "merge-target", "tag": 2},
            ]
        )

        assert ContractMergeOnRel.get_count() == 2

    def test_a_falsy_merge_on_value_still_identifies_a_relationship(self, use_graph):
        # zero is a value like any other: a relationship tagged 0 is not the one
        # tagged 1, so merging it must not match and overwrite that one
        source, target = _contract_pair()

        ContractMergeOnRel(source=source, target=target, tag=1).merge()
        ContractMergeOnRel(source=source, target=target, tag=0).merge()

        assert ContractMergeOnRel.get_count() == 2

    def test_set_on_create_survives_a_later_merge(self, use_graph):
        source, target = _contract_pair()

        ContractCreateRel(source=source, target=target, created="FIRST", seen="one").merge()
        ContractCreateRel(source=source, target=target, created="SECOND", seen="two").merge()

        rels = ContractCreateRel.match_relationships()

        assert len(rels) == 1
        # set on create applies only when the relationship is created
        assert rels[0].created == "FIRST"
        # everything else is updated as usual
        assert rels[0].seen == "two"

    def test_set_on_match_does_not_apply_when_the_relationship_is_created(self, use_graph):
        source, target = _contract_pair()

        ContractMatchRel(source=source, target=target, only_on_match="FIRST").merge()

        assert ContractMatchRel.match_relationships()[0].only_on_match is None

        ContractMatchRel(source=source, target=target, only_on_match="SECOND").merge()

        assert ContractMatchRel.match_relationships()[0].only_on_match == "SECOND"
