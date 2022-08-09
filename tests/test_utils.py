from typing import ClassVar, Optional

from neontology.utils import get_rels_by_source, get_node_types

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
