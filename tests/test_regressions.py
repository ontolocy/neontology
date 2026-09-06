"""Regression tests pinning behaviour that nothing else covered.

These exist so that changing the behaviour is a deliberate act with a failing test
attached, rather than something noticed after release. Several cover code paths that
are deprecated or on the way out - pinning them is what makes removing them safe.
"""

import json
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship, init_neontology
from neontology.schema_utils import extract_type_mapping


class RegressionNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "RegressionNode"

    pp: str
    number: int = 0


class RegressionRel(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "REGRESSION_REL"

    source: RegressionNode
    target: RegressionNode


class TestRemovedDeprecations:
    """Deprecated API removed in v3.

    Kept as tests so that removal stays deliberate: passing the old arguments now fails
    loudly rather than being silently ignored, which is what would happen if
    init_neontology still accepted **kwargs.
    """

    def test_neo4j_kwargs_are_rejected(self):
        """The deprecated connection kwargs are gone; passing them is an error."""
        with pytest.raises(TypeError, match="neo4j_uri"):
            init_neontology(
                neo4j_uri="bolt://example:7687",
                neo4j_username="someone",
                neo4j_password="secret",
            )

    def test_get_primary_property_value_is_gone(self):
        """Removed in favour of get_pp()."""
        assert not hasattr(RegressionNode, "get_primary_property_value")
        assert hasattr(RegressionNode, "get_pp")


class TestResultDumping:
    """NeontologyResult's dump helpers, which nothing exercised."""

    def test_neontology_dump_json_round_trips(self, use_graph):
        source = RegressionNode(pp="source", number=1)
        target = RegressionNode(pp="target", number=2)
        source.merge()
        target.merge()
        RegressionRel(source=source, target=target).merge()

        result = use_graph.evaluate_query("MATCH (n:RegressionNode)-[r:REGRESSION_REL]->(o) RETURN n, r, o")

        dumped = json.loads(result.neontology_dump_json())

        assert {"nodes", "edges"} == set(dumped)
        assert len(dumped["nodes"]) == 2
        assert len(dumped["edges"]) == 1
        assert dumped["nodes"][0]["LABEL"] == "RegressionNode"
        assert dumped["edges"][0]["RELATIONSHIP_TYPE"] == "REGRESSION_REL"

    def test_neontology_dump_matches_the_json_form(self, use_graph):
        RegressionNode(pp="only", number=5).merge()

        result = use_graph.evaluate_query("MATCH (n:RegressionNode) RETURN n")

        assert result.neontology_dump() == json.loads(result.neontology_dump_json())


class TestSchemaTypeMapping:
    """extract_type_mapping's error paths, which had no coverage."""

    def test_plain_type(self):
        assert extract_type_mapping(str).representation == "str"

    def test_optional_type_is_marked_optional(self):
        assert "Optional" in extract_type_mapping(Optional[str]).representation

    def test_optional_can_be_hidden(self):
        assert "Optional" not in extract_type_mapping(Optional[str], show_optional=False).representation

    def test_list_of_one_type(self):
        assert extract_type_mapping(list[str]).core_type == list[str]

    def test_list_of_multiple_types_is_rejected(self):
        with pytest.raises(TypeError, match="lists of multiple types"):
            extract_type_mapping(list[str, int])

    def test_union_of_multiple_concrete_types_is_rejected(self):
        from typing import Union

        with pytest.raises(TypeError):
            extract_type_mapping(Union[str, int])
