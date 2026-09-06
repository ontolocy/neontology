from typing import ClassVar, Optional

from neontology import GraphConnection
from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship
from neontology.utils import (
    apply_neo4j_constraints,
    auto_constrain_neo4j,
    get_node_types,
    get_rels_by_source,
    get_rels_by_type,
)


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

    assert rels_by_type["MY_REL_TYPE1"].all_source_classes[0].__primarylabel__ == "MyNodeType1"


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


class TestForwardRefRelationships:
    """A relationship may be defined before the node classes it points at.

    Discovery has to tolerate unresolved ForwardRefs rather than raising, and pick the
    relationship up once `model_rebuild()` resolves them. This was a bug fixed in v2.2.1
    and had no test.
    """

    def test_unresolved_forward_refs_are_skipped_then_picked_up(self):
        class ForwardRel(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "FORWARD_REF_REL"

            source: "ForwardNode"
            target: "ForwardNode"

        # the node class does not exist yet, so the relationship is not discoverable
        # and discovery must not raise
        assert "FORWARD_REF_REL" not in get_rels_by_type()

        class ForwardNode(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "ForwardRefNode"

            pp: str

        ForwardRel.model_rebuild()

        discovered = get_rels_by_type()

        assert "FORWARD_REF_REL" in discovered
        assert discovered["FORWARD_REF_REL"].source_class is ForwardNode
        assert discovered["FORWARD_REF_REL"].target_class is ForwardNode
