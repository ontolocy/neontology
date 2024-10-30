from typing import ClassVar, Optional

from neontology.utils import (
    get_rels_by_source,
    get_node_types,
    apply_neo4j_constraints,
    auto_constrain_neo4j,
    get_rels_by_type,
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


def test_get_rels_by_type_subclasses():
    class MyNodeType1(BaseNode):
        __primarylabel__: ClassVar[Optional[str]] = "MyNodeType1"
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    class MyAbstractRelType(BaseRelationship):
        source: MyNodeType1
        target: MyNodeType1

    class MyRelType1(MyAbstractRelType):
        __relationshiptype__: ClassVar[Optional[str]] = "MY_REL_TYPE1"

    class MyRelType2(MyAbstractRelType):
        __relationshiptype__: ClassVar[Optional[str]] = "MY_REL_TYPE2"

    class MyRelType3(BaseRelationship):
        source: MyNodeType1
        target: MyNodeType1
        __relationshiptype__: ClassVar[Optional[str]] = "MY_REL_TYPE_3"

    rels_by_type = get_rels_by_type(MyAbstractRelType)

    assert set(rels_by_type.keys()) == {"MY_REL_TYPE1", "MY_REL_TYPE2"}

    assert (
        rels_by_type["MY_REL_TYPE1"].all_source_classes[0].__primarylabel__
        == "MyNodeType1"
    )


def test_no_primary_label_get_node_types():
    class SpecialPracticeLabelNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    node_types = get_node_types()

    assert isinstance(node_types, dict)

    assert "SpecialPracticeLabelNode" not in node_types.keys()


class SpecialPracticeNodeAC(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[str] = "SpecialPracticeNodeAC"
    pp: str


def test_auto_constrain_neo4j(use_graph):
    gc = GraphConnection()

    # make sure we start with no constraints

    try:
        constraints = gc.engine.get_constraints()

        for constraint_name in constraints:
            gc.engine.drop_constraint(constraint_name)

        result = gc.engine.get_constraints()

        assert len(result) == 0

        auto_constrain_neo4j()

        result2 = gc.engine.get_constraints()

        assert len(result2) >= 2

    # not all graph engines do constraints
    except NotImplementedError:
        pass


def test_apply_neo4j_constraints(use_graph):
    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[str] = "SpecialPracticeNode"
        pp: str

    gc = GraphConnection()

    try:
        # make sure we start with no constraints

        constraints = gc.engine.get_constraints()

        for constraint_name in constraints:
            gc.engine.drop_constraint(constraint_name)

        result = gc.engine.get_constraints()

        assert len(result) == 0

        apply_neo4j_constraints([SpecialPracticeNode])

        result2 = gc.engine.get_constraints()

        assert len(result2) == 1

    # not all graph engines do constraints
    except NotImplementedError:
        pass
