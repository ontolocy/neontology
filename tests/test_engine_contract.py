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

from neontology import BaseNode, BaseRelationship
from neontology.graphengines.capabilities import Capability
from neontology.result import NeontologyResult


class ContractNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "EngineContractNode"

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


def test_constraint_methods_work_or_raise_not_implemented(use_graph):
    """Constraints are optional, but the methods must not fail in some other way."""
    engine = use_graph.engine

    try:
        constraints = engine.get_constraints()

    except NotImplementedError:
        pytest.skip("engine does not implement constraints")

    assert isinstance(constraints, list)


def test_export_dict_converter_rejects_unsupported_types(engine):
    """Every engine must reject dicts as property values, consistently."""
    with pytest.raises(TypeError):
        engine.export_dict_converter({"a_dict": {"nested": "value"}})
