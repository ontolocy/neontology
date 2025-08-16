# type: ignore

from typing import ClassVar, Optional
from uuid import uuid4

import pandas as pd
import pytest
from pydantic import Field, ValidationError

from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship


class PracticeNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "PracticeNode"
    pp: str


class PracticeRelationship(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "PRACTICE_RELATIONSHIP"

    source: PracticeNode
    target: PracticeNode

    practice_rel_prop: str = "Default Practice Relationship Property"


def test_base_relationship():
    source_node = PracticeNode(pp="Source Node")

    target_node = PracticeNode(pp="Target Node")

    br = PracticeRelationship(source=source_node, target=target_node)

    assert br.source.pp == "Source Node"
    assert br.target.pp == "Target Node"

    assert br.get_relationship_type() == "PRACTICE_RELATIONSHIP"


def test_rel_schema():
    schema = PracticeRelationship.neontology_schema()

    assert schema.relationship_type == "PRACTICE_RELATIONSHIP"
    assert schema.source_labels == ["PracticeNode"]

    practice_rel_prop = [x for x in schema.properties if x.name == "practice_rel_prop"][
        0
    ]

    assert practice_rel_prop.type_annotation.representation == "str"


def test_source_target_type():
    source_node = "Not a BaseNode"
    target_node = "Also not a BaseNode"

    with pytest.raises(ValidationError) as exception_info:
        PracticeRelationship(source=source_node, target=target_node)

    expected = "2 validation errors for PracticeRelationship"

    assert expected in str(exception_info.value)


def test_merge_relationship(request, use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    br = PracticeRelationship(
        source=source_node,
        target=target_node,
        practice_rel_prop="Default Practice Relationship Property",
    )
    br.merge()

    cypher = """
    MATCH (src:PracticeNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    RETURN r.practice_rel_prop
    """

    result = use_graph.evaluate_query_single(cypher)

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result == "Default Practice Relationship Property"

    if request.node.callspec.id in ["networkx-engine"]:
        assert (
            result[0][(0, "PRACTICE_RELATIONSHIP")]
            == "Default Practice Relationship Property"
        )


def test_match_relationship(use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    br = PracticeRelationship(
        source=source_node,
        target=target_node,
        practice_rel_prop="TESTING MATCH RELATIONSHIP",
    )
    br.merge()

    rels = PracticeRelationship.match_relationships()

    assert rels[0].practice_rel_prop == "TESTING MATCH RELATIONSHIP"


class RelMergeOnMatchTest(PracticeRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "TEST_REL_MERGE_ON_MATCH"
    prop_to_merge_on: str = Field(json_schema_extra={"merge_on": True})
    my_prop: str


def test_merge_relationship_merge_on_match(use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    br = RelMergeOnMatchTest(
        source=source_node,
        target=target_node,
        prop_to_merge_on="MergeMe",
        my_prop="Foo",
    )

    br.merge()

    cypher = """
    MATCH (src:PracticeNode {pp: 'Source Node'})-[r:TEST_REL_MERGE_ON_MATCH]->(tgt:PracticeNode {pp: 'Target Node'})
    RETURN src,r,tgt
    """

    result = use_graph.evaluate_query(cypher)

    assert len(result.relationships) == 1
    assert result.relationships[0].my_prop == "Foo"

    br2 = RelMergeOnMatchTest(
        source=source_node,
        target=target_node,
        prop_to_merge_on="MergeMe",
        my_prop="Bar",
    )
    br2.merge()

    result2 = use_graph.evaluate_query(cypher)

    assert len(result2.relationships) == 1
    assert result2.relationships[0].my_prop == "Bar"


class RelMergeOnCreateTest(PracticeRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "TEST_REL_MERGE_ON_CREATE"
    prop_to_merge_on: str = Field(json_schema_extra={"merge_on": True})
    my_prop: str


def test_merge_relationship_merge_on_create(use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    br = RelMergeOnCreateTest(
        source=source_node,
        target=target_node,
        prop_to_merge_on="MergeMe",
        my_prop="Foo",
    )

    br.merge()

    cypher = """
    MATCH (src:PracticeNode {pp: 'Source Node'})-[r:TEST_REL_MERGE_ON_CREATE]->(tgt:PracticeNode {pp: 'Target Node'})
    RETURN src,r,tgt
    """

    result = use_graph.evaluate_query(cypher)

    assert len(result.relationships) == 1
    assert result.relationships[0].my_prop == "Foo"

    br2 = RelMergeOnCreateTest(
        source=source_node,
        target=target_node,
        prop_to_merge_on="Don'tMergeMe",
        my_prop="Bar",
    )
    br2.merge()

    result2 = use_graph.evaluate_query(cypher)

    assert len(result2.relationships) == 2
    assert set([x.my_prop for x in result2.relationships]) == {"Foo", "Bar"}


def test_default_relationship_type():
    source_node = PracticeNode(pp="Source Node")

    target_node = PracticeNode(pp="Target Node")

    br = PracticeRelationship(source=source_node, target=target_node)

    assert br.get_relationship_type() == "PRACTICE_RELATIONSHIP"


def test_default_relationship_type_inherited():
    source_node = PracticeNode(pp="Source Node")

    target_node = PracticeNode(pp="Target Node")

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode
        __relationshiptype__: ClassVar[Optional[str]] = "NEWRELTYPE"

    rel = NewRelType(source=source_node, target=target_node)

    assert rel.get_relationship_type() == "NEWRELTYPE"


def test_defined_relationship_type_inherited():
    source_node = PracticeNode(pp="Source Node")

    target_node = PracticeNode(pp="Target Node")

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode
        __relationshiptype__: ClassVar[Optional[str]] = "TEST_RELATIONSHIP_TYPE"

    rel = NewRelType(source=source_node, target=target_node)

    assert rel.get_relationship_type() == "TEST_RELATIONSHIP_TYPE"


class NewRelType(BaseRelationship):
    source: PracticeNode
    target: PracticeNode
    __relationshiptype__: ClassVar[Optional[str]] = "TEST_NEW_RELATIONSHIP_TYPE"

    new_rel_prop: str


def test_merge_relationships_defined_types(use_graph):
    node1 = PracticeNode(pp="Source Node")
    node1.create()

    node2 = PracticeNode(pp="Target Node")
    node2.create()

    rel1 = NewRelType(source=node1, target=node2, new_rel_prop="Rel 1")

    rel2 = NewRelType(source=node2, target=node1, new_rel_prop="Rel 2")

    NewRelType.merge_relationships([rel1, rel2])

    cypher = """
    MATCH (src:PracticeNode)-[r:TEST_NEW_RELATIONSHIP_TYPE]->(tgt:PracticeNode)
    RETURN src,r,tgt
    """

    results = use_graph.evaluate_query(cypher)

    assert len(results.relationships) == 2

    assert set([r.new_rel_prop for r in results.relationships]) == {"Rel 1", "Rel 2"}


def test_get_count(use_graph):
    node1 = PracticeNode(pp="Source Node")
    node1.create()

    node2 = PracticeNode(pp="Target Node")
    node2.create()

    rel1 = NewRelType(source=node1, target=node2, new_rel_prop="Rel 1")
    rel2 = NewRelType(source=node2, target=node1, new_rel_prop="Rel 2")

    NewRelType.merge_relationships([rel1, rel2])

    assert len(NewRelType.match_relationships()) == 2


def test_get_count_none(use_graph):
    assert not NewRelType.get_count()


class SubclassNode(PracticeNode):
    __primarylabel__: ClassVar[Optional[str]] = "SubclassNode"
    myprop: str


class NewRelType2(BaseRelationship):
    source: SubclassNode
    target: PracticeNode
    __relationshiptype__: ClassVar[Optional[str]] = "TEST_NEW_RELATIONSHIP_TYPE2"

    new_rel_prop: str


def test_merge_df(request, use_graph):
    source_node = SubclassNode(pp="Source Node", myprop="Some Value")
    source_node.merge()

    target_node = PracticeNode(pp="Target Node")
    target_node.merge()

    rel_records = [
        {"source": "Source Node", "target": "Target Node", "new_rel_prop": "New Rel 3"}
    ]

    df = pd.DataFrame.from_records(rel_records)

    NewRelType2.merge_df(df, SubclassNode, PracticeNode)

    cypher = """
    MATCH (src:SubclassNode {pp: 'Source Node'})-[r:TEST_NEW_RELATIONSHIP_TYPE2]->(tgt:PracticeNode {pp: 'Target Node'})
    RETURN r.new_rel_prop
    """

    result = use_graph.evaluate_query_single(cypher)

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result == "New Rel 3"

    if request.node.callspec.id in ["networkx-engine"]:
        assert result[0][(0, "TEST_NEW_RELATIONSHIP_TYPE2")] == "New Rel 3"


def test_merge_df_alt_prop(request, use_graph):
    source_node = SubclassNode(pp="Source Node", myprop="My Prop Value")
    source_node.merge()

    target_node = PracticeNode(pp="Target Node")
    target_node.merge()

    rel_records = [
        {
            "source": "My Prop Value",
            "target": "Target Node",
            "new_rel_prop": "New Rel 4",
        }
    ]

    df = pd.DataFrame.from_records(rel_records)

    NewRelType2.merge_df(df, SubclassNode, PracticeNode, source_prop="myprop")

    cypher = """
    MATCH (src:SubclassNode {pp: 'Source Node'})-[r:TEST_NEW_RELATIONSHIP_TYPE2]->(tgt:PracticeNode {pp: 'Target Node'})
    RETURN r.new_rel_prop
    """

    result = use_graph.evaluate_query_single(cypher)

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result == "New Rel 4"

    if request.node.callspec.id in ["networkx-engine"]:
        assert result[0][(0, "TEST_NEW_RELATIONSHIP_TYPE2")] == "New Rel 4"


def test_merge_empty_df():
    df = pd.DataFrame()

    result = PracticeRelationship.merge_df(df)

    assert result is None


def test_merge_records(request, use_graph):
    source_node = SubclassNode(pp="Source Node", myprop="My Prop Value")
    source_node.merge()

    target_node = PracticeNode(pp="Target Node")
    target_node.merge()

    records = [
        {"source": "Source Node", "target": "Target Node", "new_rel_prop": "New Rel 5"}
    ]

    NewRelType2.merge_records(records)

    cypher = """
    MATCH (src:SubclassNode {pp: 'Source Node'})-[r:TEST_NEW_RELATIONSHIP_TYPE2]->(tgt:PracticeNode {pp: 'Target Node'})
    RETURN r.new_rel_prop
    """

    result = use_graph.evaluate_query_single(cypher)

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result == "New Rel 5"

    if request.node.callspec.id in ["networkx-engine"]:
        assert result[0][(0, "TEST_NEW_RELATIONSHIP_TYPE2")] == "New Rel 5"


def test_create_mass_rels(request, use_graph, benchmark):
    practice_records = [{"pp": uuid4().hex} for x in range(1000)]

    records_df = pd.DataFrame.from_records(practice_records)

    PracticeNode.merge_df(records_df)

    assert PracticeNode.get_count() == 1000

    people_rels = [
        {"source": practice_records[x]["pp"], "target": practice_records[x + 1]["pp"]}
        for x in range(999)
    ]

    rels_df = pd.DataFrame.from_records(people_rels)

    def do_merge(input_df):
        PracticeRelationship.merge_df(input_df)

        if request.node.callspec.id not in ["networkx-engine"]:
            result = use_graph.evaluate_query_single(
                "MATCH (n)-[r]->(o) RETURN COUNT(r)"
            )
            assert result == 999

            use_graph.evaluate_query_single("MATCH (n)-[r]->(o) DELETE r")

        if request.node.callspec.id in ["networkx-engine"]:
            assert len(use_graph.engine.driver.edges) == 999
            use_graph.engine.driver.clear_edges()

    benchmark(do_merge, rels_df)
