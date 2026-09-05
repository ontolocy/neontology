"""Regression tests pinning behaviour that nothing else covered.

These exist so that changing the behaviour is a deliberate act with a failing test
attached, rather than something noticed after release. Several cover code paths that
are deprecated or on the way out - pinning them is what makes removing them safe.
"""

import json
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship, init_neontology
from neontology.graphengines import Neo4jConfig
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


class TestDeprecatedInitKwargs:
    """`init_neontology(neo4j_uri=...)` is deprecated but still supported.

    Nothing covered this path, so there was no way to tell whether removing it would
    break callers. These tests pin what it does today.
    """

    def test_neo4j_kwargs_raise_a_deprecation_warning(self, monkeypatch):
        captured = {}

        def fake_connection(config):
            captured["config"] = config

        monkeypatch.setattr("neontology.graphconnection.GraphConnection", fake_connection)

        with pytest.warns(DeprecationWarning, match="being deprecated"):
            init_neontology(
                neo4j_uri="bolt://example:7687",
                neo4j_username="someone",
                neo4j_password="secret",
            )

    def test_neo4j_kwargs_build_an_equivalent_config(self, monkeypatch):
        captured = {}

        def fake_connection(config):
            captured["config"] = config

        monkeypatch.setattr("neontology.graphconnection.GraphConnection", fake_connection)

        with pytest.warns(DeprecationWarning):
            init_neontology(
                neo4j_uri="bolt://example:7687",
                neo4j_username="someone",
                neo4j_password="secret",
            )

        config = captured["config"]

        assert isinstance(config, Neo4jConfig)
        assert config.uri == "bolt://example:7687"
        assert config.username == "someone"
        assert config.password == "secret"

    def test_partial_kwargs_fall_back_to_the_environment(self, monkeypatch):
        """Only the kwargs given are used; the rest come from env vars as usual."""
        captured = {}

        monkeypatch.setattr("neontology.graphconnection.GraphConnection", lambda config: captured.setdefault("config", config))
        monkeypatch.setenv("NEO4J_USERNAME", "from-env")
        monkeypatch.setenv("NEO4J_PASSWORD", "env-secret")

        with pytest.warns(DeprecationWarning):
            init_neontology(neo4j_uri="bolt://example:7687")

        assert captured["config"].uri == "bolt://example:7687"
        assert captured["config"].username == "from-env"


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
