from typing import ClassVar, Optional

from neontology.utils import (
    get_rels_by_source,
    get_node_types,
    apply_constraints,
    auto_constrain,
)
from neontology import GraphConnection
from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship


def test_get_rels_by_source():
    class AbstractNodeType(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    class MyNodeType(AbstractNodeType):
        __primarylabel__: ClassVar[Optional[str]] = "MyNodeType"

    class MyRelType(BaseRelationship):
        source: AbstractNodeType
        target: MyNodeType
        __relationshiptype__: ClassVar[Optional[str]] = "MY_REL_TYPE"

    rels_by_source = get_rels_by_source()

    assert rels_by_source["MyNodeType"] == {"MY_REL_TYPE"}


def test_no_primary_label_get_node_types():
    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    node_types = get_node_types()

    assert isinstance(node_types, dict)

    assert "SpecialPracticeNode" not in node_types.keys()


def test_autoconstrain(use_graph):

    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[str] = "SpecialPracticeNode"
        pp: str

    gc = GraphConnection()

    # make sure we start with no constraints

    all_constraints_cypher = """
    SHOW CONSTRAINTS yield name
    RETURN COLLECT(DISTINCT name)
    """

    constraints = gc.evaluate_query_single(all_constraints_cypher)

    for constraint_name in constraints:

        drop_cypher = f"""
        DROP CONSTRAINT {constraint_name}
        """
        gc.evaluate_query_single(drop_cypher)

    cypher_constraints = """
    SHOW CONSTRAINTS 
    YIELD name 
    RETURN COLLECT(name)
    """

    result = gc.evaluate_query_single(cypher=cypher_constraints)

    assert len(result) == 0

    auto_constrain()

    result2 = gc.evaluate_query_single(cypher=cypher_constraints)

    assert len(result2) >= 2


def test_apply_constraints(use_graph):

    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[str] = "SpecialPracticeNode"
        pp: str

    gc = GraphConnection()

    # make sure we start with no constraints

    all_constraints_cypher = """
    SHOW CONSTRAINTS yield name
    RETURN COLLECT(DISTINCT name)
    """

    constraints = gc.evaluate_query_single(all_constraints_cypher)

    for constraint_name in constraints:

        drop_cypher = f"""
        DROP CONSTRAINT {constraint_name}
        """
        gc.evaluate_query_single(drop_cypher)

    cypher_constraints = """
    SHOW CONSTRAINTS 
    YIELD name 
    RETURN COLLECT(name)
    """

    result = gc.evaluate_query_single(cypher=cypher_constraints)

    assert len(result) == 0

    apply_constraints([SpecialPracticeNode])

    result2 = gc.evaluate_query_single(cypher=cypher_constraints)

    assert len(result2) == 1
