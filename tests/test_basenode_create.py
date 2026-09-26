"""Creating and merging nodes, including labels and inherited labels."""

from datetime import datetime
from typing import ClassVar, Optional

import pytest
from models_basenode import PracticeNode
from pydantic import (
    Field,
    ValidationInfo,
    field_validator,
)
from rawresult import raw_labels

from neontology import (
    BaseNode,
    GQLIdentifier,
)
from neontology.graphengines.capabilities import Capability


class PracticeNodeDated(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "pp"
    __primarylabel__: ClassVar[Optional[GQLIdentifier]] = "PracticeNodeDated"

    pp: str

    test_merged: datetime = Field(
        default_factory=datetime.now,
    )

    # created property will only be set 'on create' - when the node is first created
    test_created: Optional[datetime] = Field(default=None, validate_default=True, json_schema_extra={"set_on_create": True})

    @field_validator("test_created")
    def set_test_created_to_merged(cls, value: Optional[datetime], values: ValidationInfo) -> datetime:
        """When the node is first created, we want the created value
        to be set equal to merged.
        Otherwise they will be a tiny amount of time different.
        """
        # if the created value has been manually set, don't override it
        if value is None:
            return values.data["test_merged"]
        else:
            return value


def test_set_pp_field_valid():
    tn = PracticeNode(pp="Some Value")

    assert tn.pp == "Some Value"


def test_engine_dict_internals():
    # verify that an error is raised when we
    # execute a function which depends on the db
    with pytest.raises(RuntimeError):
        PracticeNode.match_nodes()

    tn = PracticeNode(pp="Some Value")

    # should be able to call these functions without an engine connection
    assert tn._engine_dict() == {"pp": "Some Value"}


def test_get_merge_parameters():
    tn = PracticeNode(pp="Some Value")

    assert tn._get_merge_parameters() == {
        "always_set": {"pp": "Some Value"},
        "pp": "Some Value",
        "set_on_create": {},
        "set_on_match": {},
    }
    assert tn.get_pp() == "Some Value"


def test_create(use_graph):
    """Test that we can create a node in the database."""
    tn = PracticeNode(pp="Test Node")

    tn.create()

    cypher = """
    MATCH (n:PracticeNode)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert result.nodes[0].__primarylabel__ == "PracticeNode"

    assert result.nodes[0].pp == "Test Node"


def _create_again(engine, create):
    """Create a node that already exists, tolerating a backend that refuses to.

    Neontology does not check first - it is for the database to enforce. A backend
    without DUPLICATE_CREATE enforces it either by keying the node on its primary
    property, which overwrites, or with a primary key, which raises.
    """
    if engine.supports(Capability.DUPLICATE_CREATE):
        create()

        return

    try:
        create()

    except RuntimeError:
        pass


def test_create_if_exists(engine, use_graph):
    """Neontology does not check if a node already exists, it is for the user to enforce this at the database level."""
    tn = PracticeNode(pp="Test Node")

    tn.create()

    cypher = """
    MATCH (n:PracticeNode)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert result.nodes[0].__primarylabel__ == "PracticeNode"

    assert result.nodes[0].pp == "Test Node"

    _create_again(engine, tn.create)

    node_count = use_graph.evaluate_query_single("MATCH (n:PracticeNode) WHERE n.pp = 'Test Node' RETURN COUNT(n)")

    # engines without DUPLICATE_CREATE identify nodes by (primary property, label), so
    # the second create either overwrote the first or was refused - one node either way
    expected = 2 if engine.supports(Capability.DUPLICATE_CREATE) else 1

    assert node_count == expected


def test_create_multiple_if_exists(engine, use_graph):
    tn = PracticeNode(pp="Test Node")

    tn.create()

    cypher = """
    MATCH (n:PracticeNode)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert result.nodes[0].__primarylabel__ == "PracticeNode"

    assert result.nodes[0].pp == "Test Node"

    _create_again(engine, lambda: PracticeNode.create_nodes([tn]))

    node_count = use_graph.evaluate_query_single("MATCH (n:PracticeNode) WHERE n.pp = 'Test Node' RETURN COUNT(n)")

    # engines without DUPLICATE_CREATE identify nodes by (primary property, label), so
    # the second create either overwrote the first or was refused - one node either way
    expected = 2 if engine.supports(Capability.DUPLICATE_CREATE) else 1

    assert node_count == expected


def test_no_primary_label():
    """Never declaring a primary label makes a node abstract.

    This used to surface as an AttributeError from reading the missing attribute, which
    said nothing about the class being abstract.
    """

    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    with pytest.raises(NotImplementedError, match="abstract node"):
        SpecialPracticeNode(pp="Test Node")


def test_none_primary_label():
    """Setting the primary label to None makes a node abstract.

    It used to warn that the label was not alphanumeric on the way through, which
    described the wrong problem - the label is absent by design, not malformed.
    """

    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = None
        pp: str

    with pytest.raises(NotImplementedError, match="abstract node"):
        SpecialPracticeNode(pp="Test Node")


@pytest.mark.requires_capability(Capability.SECONDARY_LABELS)
def test_create_multilabel(use_graph):
    class MultipleLabelNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "PrimaryLabel"
        __secondarylabels__: ClassVar[Optional[list]] = ["ExtraLabel1", "ExtraLabel2"]
        pp: str

    tn = MultipleLabelNode(pp="Test Node")

    tn.create()

    cypher = """
    MATCH (n:ExtraLabel1)
    WHERE n.pp = 'Test Node'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert result.nodes[0].__primarylabel__ == "PrimaryLabel"

    # confirm the secondary labels were written to the database

    assert {"ExtraLabel1", "ExtraLabel2"} <= raw_labels(result)

    assert result.nodes[0].pp == "Test Node"
    assert result.nodes[0].__secondarylabels__ == ["ExtraLabel1", "ExtraLabel2"]


@pytest.mark.requires_capability(Capability.SECONDARY_LABELS)
def test_create_multilabel_inheritance(use_graph):
    class Mammal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Mammal"]
        pp: str

    class Human(Mammal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Human"
        pp: str

    tn = Human(pp="Bob")

    tn.create()

    cypher = """
    MATCH (n:Human)
    WHERE n.pp = 'Bob'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert {"Human", "Mammal"} <= raw_labels(result)

    assert result.nodes[0].pp == "Bob"


def test_create_multilabel_inheritance_multiple(use_graph):
    class Animal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
        pp: str

    class Human(Animal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "HumanInherited"
        pp: str

    class Elephant(Animal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Elephant"
        pp: str

    human = Human(pp="Bob")

    human.create()

    elephant = Elephant(pp="Bob")

    # this should create a new node because the primary label is different
    elephant.create()

    elephant2 = Elephant(pp="Bob")

    # this shouldn't create a new node because the primary label is the same
    elephant2.merge()

    cypher = """
    MATCH (n)
    WHERE n.pp = 'Bob'
    RETURN COUNT(n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2


@pytest.mark.requires_capability(Capability.SECONDARY_LABELS)
def test_merge_defined_label_inherited(use_graph):
    class Mammal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Mammal"]
        pp: str

    class Human(Mammal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "HumanDefinedLabel"
        pp: str

    tn = Human(pp="Bob")

    tn.merge()

    cypher = """
    MATCH (n:HumanDefinedLabel)
    WHERE n.pp = 'Bob'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    assert {"HumanDefinedLabel", "Mammal"} <= raw_labels(result)

    assert result.nodes[0].pp == "Bob"


class SpecialPracticeNode(BaseNode):
    __primarylabel__: ClassVar[Optional[str]] = "SpecialTestLabel1"
    __primaryproperty__: ClassVar[str] = "pp"

    pp: str


def test_merge_multiple_defined_label(use_graph):
    tn = SpecialPracticeNode(pp="Special Test Node")

    tn2 = SpecialPracticeNode(pp="Special Test Node2")

    SpecialPracticeNode.merge_nodes([tn, tn2])

    cypher = """
    MATCH (n:SpecialTestLabel1)
    RETURN n
    """

    results = use_graph.evaluate_query(cypher)

    node_pps = set([x.pp for x in results.nodes])

    assert set(["Special Test Node", "Special Test Node2"]) == node_pps


def test_create_multiple_defined_label(use_graph):
    tn = SpecialPracticeNode(pp="Special Test Node")

    tn2 = SpecialPracticeNode(pp="Special Test Node2")

    SpecialPracticeNode.create_nodes([tn, tn2])

    cypher = """
    MATCH (n:SpecialTestLabel1)
    RETURN n
    """

    results = use_graph.evaluate_query(cypher)

    node_pps = set([x.pp for x in results.nodes])

    assert set(["Special Test Node", "Special Test Node2"]) == node_pps


@pytest.mark.requires_capability(Capability.DATETIME_FUNCTIONS)
def test_creation_datetime(use_graph):
    """Check we can manually define the created datetime.

    Then check we can query for it using neo4j DateTime type.
    """
    my_datetime = datetime(year=2022, month=5, day=4, hour=3, minute=21)

    bn = PracticeNodeDated(pp="Test Node", test_created=my_datetime)

    bn.create()

    cypher = """
    MATCH (n:PracticeNodeDated)
    WHERE n.pp = 'Test Node'
    RETURN n.test_created.year
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2022
