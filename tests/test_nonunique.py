# type: ignore

from typing import ClassVar, Optional

from neontology import BaseNode, BaseRelationship, GQLIdentifier, ElementIdModel


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


def test_merge_nonunique(use_graph):
    child1_node = NonUniqueNode(nonpp="Child")
    child1_node.merge()
    parent1_node = UniqueNode(pp="Parent1", element_id="1234")
    parent1_node.merge()
    rel1 = ParentUniqueRelationship(source=child1_node, target=parent1_node)
    rel1.merge()

    child2_node = NonUniqueNode(nonpp="Child")
    child2_node.merge()
    parent2_node = UniqueNode(pp="Parent2")
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
    child1_node = NonUniqueNode(nonpp="Child")
    child1_node.create()
    child2_node = NonUniqueNode(nonpp="Child")
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
