from typing import ClassVar, Optional, List

from neontology.utils import (
    get_rels_by_source,
    get_node_types,
    apply_constraints,
    auto_constrain,
    generate_node_schema,
    schema_to_markdown,
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


def test_autoconstrain(use_graph):
    gc = GraphConnection()

    # make sure we start with no constraints

    try:
        constraints = gc.engine.get_constraints()

        for constraint_name in constraints:
            gc.engine.drop_constraint(constraint_name)

        result = gc.engine.get_constraints()

        assert len(result) == 0

        auto_constrain()

        result2 = gc.engine.get_constraints()

        assert len(result2) >= 2

    # not all graph engines do constraints
    except NotImplementedError:
        pass


def test_apply_constraints(use_graph):
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

        apply_constraints([SpecialPracticeNode])

        result2 = gc.engine.get_constraints()

        assert len(result2) == 1

    # not all graph engines do constraints
    except NotImplementedError:
        pass


class DocumentaryNode(BaseNode):
    pass


class PersonToDocument(DocumentaryNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[str] = "PersonLabelToDocument"

    name: str
    age: int
    eyes: Optional[int] = None
    eye_colours: Optional[List[str]] = None
    favourite_things: List[int]

    def __str__(self) -> str:
        return self.name


class TigerToDocument(DocumentaryNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[str] = "TigerLabelToDocument"

    name: str


class RelToDocument(BaseRelationship):
    source: PersonToDocument
    target: DocumentaryNode

    how_much: Optional[str] = None

    __relationshiptype__: ClassVar[str] = "RELATIONSHIP_TO_DOCUMENT"


def test_generate_node_schema():
    person_schema = generate_node_schema(PersonToDocument)

    assert person_schema.label == "PersonLabelToDocument"

    assert person_schema.properties[0].name == "favourite_things"
    assert person_schema.properties[0].type_annotation == "List[int]"
    assert person_schema.properties[0].required is True

    assert person_schema.outgoing_relationships[0].name == "RELATIONSHIP_TO_DOCUMENT"


def test_schema_to_markdown():
    person_schema = generate_node_schema(PersonToDocument)

    md = schema_to_markdown(person_schema)

    assert (
        """# PersonLabelToDocument

Primary Label: PersonLabelToDocument

Python Class Name: PersonToDocument"""
        in md
    )

    print(person_schema.outgoing_relationships)

    assert (
        """## Outgoing Relationships

### RELATIONSHIP_TO_DOCUMENT

Target Label(s): PersonLabelToDocument, TigerLabelToDocument"""
        in md
    )
