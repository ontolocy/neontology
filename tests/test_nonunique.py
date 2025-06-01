# type: ignore

from typing import ClassVar, Optional

from pydantic import Field

from neontology import BaseNode, BaseRelationship, ElementIdModel, GQLIdentifier


class NonUniqueNode(BaseNode, ElementIdModel):
    __primaryproperty__: ClassVar[GQLIdentifier] = "element_id"
    __primarylabel__: ClassVar[Optional[GQLIdentifier]] = "NonUniqueNode"

    nonpp: str


class UniqueNode(BaseNode, ElementIdModel):
    __primaryproperty__: ClassVar[GQLIdentifier] = "pp"
    __primarylabel__: ClassVar[Optional[GQLIdentifier]] = "UniqueNode"

    pp: str


class ParentUniqueRelationship(BaseRelationship, ElementIdModel):
    __relationshiptype__: ClassVar[Optional[str]] = "HAS_PARENT"

    source: NonUniqueNode
    target: UniqueNode


def test_match_nonunique(use_graph):
    """Test match for an element ID primary property node class"""
    tn = NonUniqueNode(nonpp="Special Test Node")

    tn.create()

    result = NonUniqueNode.match(pp=tn.element_id)

    assert isinstance(result, NonUniqueNode)

    assert "Special Test Node" == result.nonpp


def test_merge_nonunique(use_graph):
    """Test merge for an element ID primary property node class"""
    child1_node = NonUniqueNode(nonpp="Child", element_id="150")
    child1_node.merge()
    child1_elementid = child1_node.element_id
    print(f"{child1_elementid=}")
    # check server element id is stable by merging twice to ensure child1 isn't duplicated
    child1_node.merge()
    assert child1_node.element_id == child1_elementid

    parent1_node = UniqueNode(pp="Parent1", element_id="1234")
    parent1_node.merge()
    rel1 = ParentUniqueRelationship(source=child1_node, target=parent1_node)
    rel1.merge()

    child2_node = NonUniqueNode(nonpp="Child", element_id="250")
    child2_node.merge()
    parent2_node = UniqueNode(pp="Parent2", element_id="5678")
    parent2_node.merge()
    rel2 = ParentUniqueRelationship(source=child2_node, target=parent2_node)
    rel2.merge()

    cypher_nodecount = """
    MATCH (n) RETURN count(n)
    """
    node_count = use_graph.evaluate_query_single(cypher_nodecount)
    # check 2 unique and 2 nonunique nodes were created
    assert node_count == 4

    cypher_nonuniquecount = """
    MATCH (n:NonUniqueNode {nonpp:'Child'}) RETURN count(n)
    """
    node_count2 = use_graph.evaluate_query_single(cypher_nonuniquecount)
    # check two nonunique nodes are returned
    assert node_count2 == 2

    # check element ID was overwritten on merge from database
    assert parent1_node.element_id != "1234"


def test_create_nonunique(use_graph):
    """Test node and relationship creation for an element ID primary property node class"""
    child1_node = NonUniqueNode(nonpp="Child", element_id="100")
    child1_node.create()
    child2_node = NonUniqueNode(nonpp="Child", element_id="200")
    child2_node.create()
    parent1_node = UniqueNode(pp="Parent1", element_id="1234")
    parent1_node.create()
    rel1 = ParentUniqueRelationship(
        source=child1_node, target=parent1_node, element_id="4567"
    )
    rel1.merge()
    rel2 = ParentUniqueRelationship(
        source=child2_node, target=parent1_node, element_id="8910"
    )
    rel2.merge()

    # element_id should be updated from server on create/merge
    assert parent1_node.element_id != "1234"
    assert rel1.element_id != "4567"
    assert child1_node.element_id != child2_node.element_id

    cypher_nonuniquecount = """
    MATCH (n:NonUniqueNode {nonpp:'Child'}) RETURN count(n)
    """
    node_count = use_graph.evaluate_query_single(cypher_nonuniquecount)
    # check two nonunique nodes are returned
    assert node_count == 2

    cypher_relcount = """
    MATCH (n)-[r:HAS_PARENT]->(p) RETURN count(r)
    """
    rel_count = use_graph.evaluate_query_single(cypher_relcount)
    # check two relationships are returned
    assert rel_count == 2

    # check that element_id (python property) is not being saved to server
    cypher_elid = """
    MATCH (n:UniqueNode {pp:'Parent1'}) RETURN n.element_id
    """
    result_elid = use_graph.evaluate_query_single(cypher_elid)
    assert result_elid is None

    cypher_elid = """
    MATCH (n:NonUniqueNode {nonpp:'Child'}) RETURN n.element_id LIMIT 1
    """
    result_elid = use_graph.evaluate_query_single(cypher_elid)
    assert result_elid is None


def test_set_on_create_and_merge(use_graph):
    """Check that we successfully identify field to set on match and on create
    in same Node class for an element ID primary property node class"""

    class TestModel(BaseNode, ElementIdModel):
        __primaryproperty__: ClassVar[GQLIdentifier] = "element_id"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel3"
        nonpp: str = "test_node"
        only_set_on_create: str = Field(json_schema_extra={"set_on_create": True})
        only_set_on_match: Optional[str] = Field(
            json_schema_extra={"set_on_match": True}, default=None
        )
        normal_field: str

    test_node = TestModel(
        only_set_on_create="Foo",
        only_set_on_match="Fu",
        normal_field="Bar",
        nonpp="test_node",
    )
    test_node.merge()

    cypher = """
    MATCH (n:TestModel3)
    WHERE n.nonpp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    assert cypher_result.nodes[0].only_set_on_match is None
    assert cypher_result.nodes[0].only_set_on_create == "Foo"
    assert cypher_result.nodes[0].normal_field == "Bar"
    assert test_node.only_set_on_match is None
    assert test_node.only_set_on_create == "Foo"

    test_node2 = TestModel(
        only_set_on_create="Fee",
        only_set_on_match="Fa",
        normal_field="Fi",
        nonpp="test_node",
        element_id=test_node.element_id,
    )
    test_node2.merge()

    cypher_result2 = use_graph.evaluate_query(cypher)

    assert cypher_result2.nodes[0].only_set_on_create == "Foo"
    assert cypher_result2.nodes[0].only_set_on_match == "Fa"
    assert cypher_result2.nodes[0].normal_field == "Fi"
    assert test_node2.only_set_on_create == "Foo"
    assert test_node2.only_set_on_match == "Fa"
