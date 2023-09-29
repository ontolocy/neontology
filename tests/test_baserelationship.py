# type: ignore

from typing import ClassVar, Optional

import pandas as pd
from pydantic import Field
import pytest

from pydantic import ValidationError

from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship


class PracticeNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "PracticeNode"
    pp: str


class PracticeRelationship(BaseRelationship):
    source: PracticeNode
    target: PracticeNode
    __relationshiptype__: ClassVar[Optional[str]] = "PRACTICE_RELATIONSHIP"


def test_base_relationship():
    source_node = PracticeNode(pp="Source Node")

    target_node = PracticeNode(pp="Target Node")

    br = PracticeRelationship(source=source_node, target=target_node)

    assert br.source.pp == "Source Node"
    assert br.target.pp == "Target Node"

    assert br.get_relationship_type() == "PRACTICE_RELATIONSHIP"


def test_source_target_type():
    source_node = "Not a BaseNode"
    target_node = "Also not a BaseNode"

    with pytest.raises(ValidationError) as exception_info:
        PracticeRelationship(source=source_node, target=target_node)

    expected = "2 validation errors for PracticeRelationship"

    assert expected in str(exception_info.value)


def test_merge_relationship(use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    br = PracticeRelationship(source=source_node, target=target_node)
    br.merge()

    cypher = """
    MATCH (src:PracticeNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    WITH r, TYPE(r) as type_r
    RETURN COLLECT(DISTINCT r{.*, type_r})
    """

    result = use_graph.evaluate(cypher)

    assert len(result) == 1

    assert result[0]["type_r"] == "PRACTICE_RELATIONSHIP"


def test_merge_relationship_merge_on_match(use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    class TestRel(PracticeRelationship):
        __relationshiptype__: ClassVar[Optional[str]] = "TEST_REL"
        prop_to_merge_on: str = Field(json_schema_extra={"merge_on": True})
        my_prop: str

    br = TestRel(
        source=source_node,
        target=target_node,
        prop_to_merge_on="MergeMe",
        my_prop="Foo",
    )
    br.merge()

    cypher = """
    MATCH (src:PracticeNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    WITH r, TYPE(r) as type_r
    RETURN COLLECT(DISTINCT r{.*, type_r})
    """

    result = use_graph.evaluate(cypher)

    assert len(result) == 1

    assert result[0]["type_r"] == "TEST_REL"
    assert result[0]["prop_to_merge_on"] == "MergeMe"
    assert result[0]["my_prop"] == "Foo"

    br2 = TestRel(
        source=source_node,
        target=target_node,
        prop_to_merge_on="MergeMe",
        my_prop="Bar",
    )
    br2.merge()

    result2 = use_graph.evaluate(cypher)

    assert len(result2) == 1

    assert result2[0]["type_r"] == "TEST_REL"
    assert result2[0]["prop_to_merge_on"] == "MergeMe"
    assert result2[0]["my_prop"] == "Bar"


def test_merge_relationship_merge_on_create(use_graph):
    source_node = PracticeNode(pp="Source Node")
    source_node.create()

    target_node = PracticeNode(pp="Target Node")
    target_node.create()

    class TestRel(PracticeRelationship):
        __relationshiptype__: ClassVar[Optional[str]] = "TEST_REL"
        prop_to_merge_on: str = Field(json_schema_extra={"merge_on": True})
        my_prop: str

    br = TestRel(
        source=source_node,
        target=target_node,
        prop_to_merge_on="MergeMe",
        my_prop="Foo",
    )
    br.merge()

    cypher = """
    MATCH (src:PracticeNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    WITH r, TYPE(r) as type_r
    RETURN COLLECT(DISTINCT r{.*, type_r})
    """

    result = use_graph.evaluate(cypher)

    assert len(result) == 1

    assert result[0]["type_r"] == "TEST_REL"
    assert result[0]["prop_to_merge_on"] == "MergeMe"
    assert result[0]["my_prop"] == "Foo"

    br2 = TestRel(
        source=source_node,
        target=target_node,
        prop_to_merge_on="Don'tMergeMe",
        my_prop="Bar",
    )
    br2.merge()

    result2 = use_graph.evaluate(cypher)

    assert len(result2) == 2

    my_props = []

    for res in result2:
        my_props.append(res["my_prop"])

    assert set(my_props) == {"Foo", "Bar"}


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


def test_merge_relationships_defined_types(use_graph):
    node1 = PracticeNode(pp="Source Node")
    node1.create()

    node2 = PracticeNode(pp="Target Node")
    node2.create()

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode
        __relationshiptype__: ClassVar[Optional[str]] = "TEST_RELATIONSHIP_TYPE"

    rel1 = NewRelType(source=node1, target=node2)

    rel2 = NewRelType(source=node2, target=node1)

    NewRelType.merge_relationships([rel1, rel2])

    cypher = """
    MATCH (src:PracticeNode)-[r]->(tgt:PracticeNode)
    WITH DISTINCT r, TYPE(r) as type_r
    RETURN COLLECT(r{.*, type_r})
    """

    results = use_graph.evaluate(cypher)

    assert len(results) == 2

    for result in results:
        assert result["type_r"] == "TEST_RELATIONSHIP_TYPE"


def test_merge_df(use_graph):
    class SubclassNode(PracticeNode):
        __primarylabel__: ClassVar[Optional[str]] = "SubclassNode"
        myprop: str

    source_node = SubclassNode(pp="Source Node", myprop="Some Value")
    source_node.merge()

    target_node = PracticeNode(pp="Target Node")
    target_node.merge()

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode

        __relationshiptype__: ClassVar[Optional[str]] = "NEWRELTYPE"

    rel_records = [{"source": "Source Node", "target": "Target Node"}]

    df = pd.DataFrame.from_records(rel_records)

    NewRelType.merge_df(df, SubclassNode, PracticeNode)

    cypher = """
    MATCH (src:SubclassNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    WITH r, TYPE(r) as type_r
    RETURN COLLECT(DISTINCT r{.*, type_r})
    """

    result = use_graph.evaluate(cypher)

    assert len(result) == 1

    assert result[0]["type_r"] == "NEWRELTYPE"


def test_merge_df_alt_prop(use_graph):
    class SubclassNode(PracticeNode):
        __primarylabel__: ClassVar[Optional[str]] = "SubclassNode"
        myprop: str

    source_node = SubclassNode(pp="Source Node", myprop="My Prop Value")
    source_node.merge()

    target_node = PracticeNode(pp="Target Node")
    target_node.merge()

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode

        __relationshiptype__: ClassVar[Optional[str]] = "NEWRELTYPE"

    rel_records = [{"source": "My Prop Value", "target": "Target Node"}]

    df = pd.DataFrame.from_records(rel_records)

    NewRelType.merge_df(df, SubclassNode, PracticeNode, source_prop="myprop")

    cypher = """
    MATCH (src:SubclassNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    WITH r, TYPE(r) as type_r
    RETURN COLLECT(DISTINCT r{.*, type_r})
    """

    result = use_graph.evaluate(cypher)

    assert len(result) == 1

    assert result[0]["type_r"] == "NEWRELTYPE"


def test_merge_df_bad_node_type(use_graph):
    class DifferentNode(PracticeNode):
        __primarylabel__: ClassVar[Optional[str]] = "DifferentNode"
        __primaryproperty__: ClassVar[str] = "myprop"
        myprop: str

    source_node = PracticeNode(pp="something else")

    target_node = PracticeNode(pp="Target Node")

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode

        __relationshiptype__: ClassVar[Optional[str]] = "NEWRELTYPE"

    rels = [NewRelType(source=source_node, target=target_node)]

    with pytest.raises(TypeError):
        NewRelType.merge_relationships(rels, PracticeNode, DifferentNode)


def test_merge_df_bad_node_type2(use_graph):
    class DifferentNode(PracticeNode):
        __primarylabel__: ClassVar[Optional[str]] = "DifferentNode"
        __primaryproperty__: ClassVar[str] = "myprop"
        myprop: str

    source_node = DifferentNode(myprop="something", pp="something else")

    target_node = PracticeNode(pp="Target Node")

    class NewRelType(BaseRelationship):
        source: PracticeNode
        target: PracticeNode

        __relationshiptype__: ClassVar[Optional[str]] = "NEWRELTYPE"

    rels = [NewRelType(source=source_node, target=target_node)]

    with pytest.raises(TypeError):
        NewRelType.merge_relationships(rels, PracticeNode, PracticeNode)


def test_merge_empty_df():
    df = pd.DataFrame()

    result = PracticeRelationship.merge_df(df)

    assert result is None


def test_merge_records(use_graph):
    class SubclassNode(PracticeNode):
        __primarylabel__: ClassVar[Optional[str]] = "SubclassNode"
        myprop: str

    source_node = SubclassNode(pp="Source Node", myprop="My Prop Value")
    source_node.merge()

    target_node = PracticeNode(pp="Target Node")
    target_node.merge()

    class NewRelType(BaseRelationship):
        source: SubclassNode
        target: PracticeNode

        __relationshiptype__: ClassVar[Optional[str]] = "NEWRELTYPE"

    records = [{"source": "Source Node", "target": "Target Node"}]

    NewRelType.merge_records(records)

    cypher = """
    MATCH (src:SubclassNode {pp: 'Source Node'})-[r]->(tgt:PracticeNode {pp: 'Target Node'})
    WITH r, TYPE(r) as type_r
    RETURN COLLECT(DISTINCT r{.*, type_r})
    """

    result = use_graph.evaluate(cypher)

    assert len(result) == 1

    assert result[0]["type_r"] == "NEWRELTYPE"
