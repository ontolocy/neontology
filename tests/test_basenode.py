import json
from datetime import datetime
from enum import Enum
from typing import ClassVar, Optional
from uuid import UUID, uuid4

import pandas as pd
import pytest
from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_serializer,
    field_validator,
)

from neontology import (
    BaseNode,
    BaseRelationship,
    GQLIdentifier,
    related_nodes,
    related_property,
)
from neontology.result import NeontologyResult


class PracticeNode(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "pp"
    __primarylabel__: ClassVar[Optional[GQLIdentifier]] = "PracticeNode"

    pp: str


class PracticeNodeDated(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "pp"
    __primarylabel__: ClassVar[Optional[GQLIdentifier]] = "PracticeNodeDated"

    pp: str

    test_merged: datetime = Field(
        default_factory=datetime.now,
    )

    # created property will only be set 'on create' - when the node is first created
    test_created: Optional[datetime] = Field(
        default=None, validate_default=True, json_schema_extra={"set_on_create": True}
    )

    @field_validator("test_created")
    def set_test_created_to_merged(
        cls, value: Optional[datetime], values: ValidationInfo
    ) -> datetime:
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


def test_create_if_exists(request, use_graph):
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

    tn.create()

    node_count = use_graph.evaluate_query_single(
        "MATCH (n:PracticeNode) WHERE n.pp = 'Test Node' RETURN COUNT(n)"
    )

    if request.node.callspec.id not in ["networkx-engine"]:
        assert node_count == 2

    if request.node.callspec.id in ["networkx-engine"]:
        assert node_count[0]["_"] == 1


def test_create_multiple_if_exists(request, use_graph):
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

    PracticeNode.create_nodes([tn])

    node_count = use_graph.evaluate_query_single(
        "MATCH (n:PracticeNode) WHERE n.pp = 'Test Node' RETURN COUNT(n)"
    )

    if request.node.callspec.id not in ["networkx-engine"]:
        assert node_count == 2

    if request.node.callspec.id in ["networkx-engine"]:
        assert node_count[0]["_"] == 1


def test_no_primary_label():
    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        pp: str

    with pytest.raises(AttributeError):
        SpecialPracticeNode(pp="Test Node")


def test_none_primary_label():
    class SpecialPracticeNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = None
        pp: str

    with pytest.warns(
        UserWarning,
        match="Primary Label should contain only alphanumeric characters and underscores",
    ):
        with pytest.raises(NotImplementedError):
            SpecialPracticeNode(pp="Test Node")


def test_create_multilabel(request, use_graph):
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

    if request.node.callspec.id not in ["networkx-engine"]:

        assert "ExtraLabel1" in result.records_raw[0].values()[0].labels
        assert "ExtraLabel2" in result.records_raw[0].values()[0].labels

    if request.node.callspec.id in ["networkx-engine"]:
        assert "ExtraLabel1" in result.records_raw["n"][0]["__labels__"]
        assert "ExtraLabel2" in result.records_raw["n"][0]["__labels__"]

    assert result.nodes[0].pp == "Test Node"
    assert result.nodes[0].__secondarylabels__ == ["ExtraLabel1", "ExtraLabel2"]


def test_create_multilabel_inheritance(request, use_graph):
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

    if request.node.callspec.id not in ["networkx-engine"]:
        assert "Human" in result.records_raw[0].values()[0].labels
        assert "Mammal" in result.records_raw[0].values()[0].labels

    if request.node.callspec.id in ["networkx-engine"]:
        assert "Human" in result.records_raw["n"][0]["__labels__"]
        assert "Mammal" in result.records_raw["n"][0]["__labels__"]

    assert result.nodes[0].pp == "Bob"


def test_create_multilabel_inheritance_multiple(request, use_graph):
    class Animal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
        pp: str

    class Human(Animal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Human"
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

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result == 2

    if request.node.callspec.id in ["networkx-engine"]:
        assert result[0]["_"] == 2


def test_merge_defined_label_inherited(request, use_graph):
    class Mammal(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __secondarylabels__: ClassVar[Optional[list]] = ["Mammal"]
        pp: str

    class Human(Mammal):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "Human"
        pp: str

    tn = Human(pp="Bob")

    tn.merge()

    cypher = """
    MATCH (n:Human)
    WHERE n.pp = 'Bob'
    RETURN n
    """

    result = use_graph.evaluate_query(cypher)

    if request.node.callspec.id not in ["networkx-engine"]:
        assert "Human" in result.records_raw[0].values()[0].labels
        assert "Mammal" in result.records_raw[0].values()[0].labels

    if request.node.callspec.id in ["networkx-engine"]:
        assert "Human" in result.records_raw["n"][0]["__labels__"]
        assert "Mammal" in result.records_raw["n"][0]["__labels__"]

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


def test_creation_datetime(request, use_graph):
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

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result == 2022

    if request.node.callspec.id in ["networkx-engine"]:
        # grand cypher doesn't do full datetime operations
        assert result[0].year == 2022


def test_match_nodes(use_graph):
    tn = PracticeNode(pp="Special Test Node")

    tn2 = PracticeNode(pp="Special Test Node2")

    PracticeNode.merge_nodes([tn, tn2])

    results = PracticeNode.match_nodes()

    for result in results:
        assert isinstance(result, PracticeNode)

    pps = [x.pp for x in results]

    assert "Special Test Node" in pps

    assert "Special Test Node2" in pps


def test_match_nodes_limit(use_graph):
    tn = PracticeNode(pp="Special Test Node")
    tn.merge()

    tn2 = PracticeNode(pp="Special Test Node2")

    tn2.merge()

    results = PracticeNode.match_nodes(limit=1)

    assert len(results) == 1

    assert results[0].pp in ["Special Test Node", "Special Test Node2"]


def test_match_nodes_skip(use_graph):
    tn = PracticeNode(pp="Special Test Node")
    tn.merge()

    tn2 = PracticeNode(pp="Special Test Node2")

    tn2.merge()

    results1 = PracticeNode.match_nodes(limit=1)

    assert len(results1) == 1

    assert results1[0].pp in ["Special Test Node", "Special Test Node2"]

    results2 = PracticeNode.match_nodes(limit=1, skip=1)

    assert len(results2) == 1

    # make sure we have actually skipped through the results
    assert results1[0].pp != results2[0].pp


def test_match_nodes_with_filters_basic(use_graph):
    """Test basic filtering functionality."""
    # Create test nodes
    nodes = [
        PracticeNode(pp="Test Node 1"),
        PracticeNode(pp="Another Test Node"),
        PracticeNode(pp="Node with different name"),
        PracticeNode(pp="Test Node 2"),
    ]
    PracticeNode.merge_nodes(nodes)

    # Test exact match (default behavior)
    results = PracticeNode.match_nodes(filters={"pp": "Test Node 1"})
    assert len(results) == 1
    assert results[0].pp == "Test Node 1"

    # Test with no filters (should return all)
    results = PracticeNode.match_nodes()
    assert len(results) == 4


def test_match_nodes_with_string_filters(request, use_graph):
    """Test various string filter operations."""

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNode"
        id: str
        name: str
        description: str
        code: str

    nodes = [
        TestNode(
            id="1", name="Aspartame", description="Sweetener product", code="E951"
        ),
        TestNode(id="2", name="Sucrose", description="Natural sweetener", code="E473"),
        TestNode(
            id="3",
            name="aspartame synthetic",
            description="Artificial sweetener",
            code="E951",
        ),
        TestNode(
            id="4", name="Stevia", description="Plant-based sweetener", code="E960"
        ),
    ]
    TestNode.merge_nodes(nodes)

    # Test exact match (default)
    results = TestNode.match_nodes(filters={"name": "Aspartame"})
    assert len(results) == 1
    assert results[0].id == "1"

    if request.node.callspec.id not in ["networkx-engine"]:

        # Test icontains
        results = TestNode.match_nodes(filters={"name__icontains": "aspart"})
        assert len(results) == 2
        assert sorted([x.id for x in results]) == ["1", "3"]

    elif request.node.callspec.id == "networkx-engine":
        with pytest.raises(NotImplementedError):
            TestNode.match_nodes(filters={"name__icontains": "aspart"})

    # Test case-sensitive contains
    results = TestNode.match_nodes(filters={"name__contains": "Aspart"})
    assert len(results) == 1
    assert results[0].id == "1"

    if request.node.callspec.id not in ["networkx-engine"]:
        # Test iexact
        results = TestNode.match_nodes(filters={"name__iexact": "aspartame synthetic"})
        assert len(results) == 1
        assert results[0].id == "3"

    elif request.node.callspec.id == "networkx-engine":
        with pytest.raises(NotImplementedError):
            results = TestNode.match_nodes(
                filters={"name__iexact": "aspartame synthetic"}
            )

    # Test startswith and istartswith

    results = TestNode.match_nodes(filters={"description__startswith": "Sweetener"})
    assert len(results) == 1
    assert results[0].id == "1"

    if request.node.callspec.id not in ["networkx-engine"]:
        results = TestNode.match_nodes(filters={"description__istartswith": "plant"})
        assert len(results) == 1
        assert results[0].id == "4"

    elif request.node.callspec.id == "networkx-engine":
        with pytest.raises(NotImplementedError):
            results = TestNode.match_nodes(
                filters={"description__istartswith": "plant"}
            )


def test_match_nodes_with_numeric_filters(use_graph):
    """Test numeric filter operations."""

    class Product(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "Product"
        id: str
        name: str
        price: float
        stock: int
        rating: float

    products = [
        Product(id="1", name="Product A", price=9.99, stock=100, rating=4.5),
        Product(id="2", name="Product B", price=19.99, stock=50, rating=3.8),
        Product(id="3", name="Product C", price=5.99, stock=200, rating=4.2),
        Product(id="4", name="Product D", price=14.99, stock=75, rating=4.7),
    ]
    Product.merge_nodes(products)

    # Test gt, lt, gte, lte
    results = Product.match_nodes(filters={"price__lt": 10})
    assert len(results) == 2
    assert sorted([x.id for x in results]) == ["1", "3"]

    results = Product.match_nodes(filters={"stock__gte": 100})
    assert len(results) == 2
    assert sorted([x.id for x in results]) == ["1", "3"]

    # Test combination of numeric filters
    results = Product.match_nodes(
        filters={"price__gt": 5, "price__lt": 15, "rating__gte": 4.6}
    )
    assert len(results) == 1
    assert results[0].id == "4"


def test_match_nodes_with_in_filter(use_graph):
    """Test the __in filter operation."""

    class Item(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "Item"
        id: str
        code: str
        category: str

    items = [
        Item(id="1", code="A100", category="Electronics"),
        Item(id="2", code="B200", category="Clothing"),
        Item(id="3", code="C300", category="Electronics"),
        Item(id="4", code="D400", category="Home"),
    ]
    Item.merge_nodes(items)

    # Test __in with codes
    results = Item.match_nodes(filters={"code__in": ["A100", "C300", "D400"]})
    assert len(results) == 3
    assert sorted([x.id for x in results]) == ["1", "3", "4"]

    # Test __in with categories
    results = Item.match_nodes(filters={"category__in": ["Electronics", "Home"]})
    assert len(results) == 3


def test_match_nodes_with_boolean_filter(use_graph):
    """Test filtering on boolean fields."""

    class User(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "User"
        id: str
        name: str
        active: bool
        admin: bool

    users = [
        User(id="1", name="Alice", active=True, admin=True),
        User(id="2", name="Bob", active=True, admin=False),
        User(id="3", name="Charlie", active=False, admin=False),
        User(id="4", name="Dana", active=True, admin=True),
    ]
    User.merge_nodes(users)

    # Test boolean filters
    results = User.match_nodes(filters={"active": True})
    assert len(results) == 3

    results = User.match_nodes(filters={"admin": False})
    assert len(results) == 2

    # Test combination of boolean and other filters
    results = User.match_nodes(filters={"active": True, "admin": True})
    assert len(results) == 2


def test_match_nodes_with_datetime_filter(request, use_graph):
    """Test filtering on datetime fields."""
    if request.node.callspec.id in ["networkx-engine"]:

        pytest.skip("NetworkxEngine does not support datetime comparison.")

    class Event(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "Event"
        id: str
        name: str
        start_date: datetime
        end_date: datetime

    date1 = datetime(2023, 1, 1)
    date2 = datetime(2023, 2, 1)
    date3 = datetime(2023, 3, 1)
    date4 = datetime(2023, 4, 1)

    events = [
        Event(id="1", name="Event 1", start_date=date1, end_date=date2),
        Event(id="2", name="Event 2", start_date=date2, end_date=date3),
        Event(id="3", name="Event 3", start_date=date3, end_date=date4),
        Event(id="4", name="Event 4", start_date=date1, end_date=date4),
    ]
    Event.merge_nodes(events)

    # Test datetime filters
    results = Event.match_nodes(filters={"start_date__lt": date3})
    assert len(results) == 3
    assert sorted([x.id for x in results]) == ["1", "2", "4"]

    # Test between dates
    results = Event.match_nodes(
        filters={"start_date__gte": date2, "end_date__lte": date3}
    )
    assert len(results) == 1
    assert sorted([x.id for x in results]) == ["2"]


def test_match_nodes_with_combined_filters(request, use_graph):
    """Test combination of different filter types."""

    class Product(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "Product"
        id: str
        name: str
        category: str
        price: float
        stock: int
        active: bool

    products = [
        Product(
            id="1",
            name="Laptop",
            category="Electronics",
            price=999.99,
            stock=10,
            active=True,
        ),
        Product(
            id="2",
            name="Smartphone",
            category="Electronics",
            price=699.99,
            stock=20,
            active=True,
        ),
        Product(
            id="3",
            name="Desk",
            category="Furniture",
            price=199.99,
            stock=5,
            active=False,
        ),
        Product(
            id="4",
            name="Chair",
            category="Furniture",
            price=99.99,
            stock=15,
            active=True,
        ),
        Product(
            id="5",
            name="Tablet",
            category="Electronics",
            price=399.99,
            stock=0,
            active=False,
        ),
    ]
    Product.merge_nodes(products)

    # Test complex filter combination
    results = Product.match_nodes(
        filters={
            "category": "Electronics",
            "price__lt": 800,
            "stock__gt": 0,
            "active": True,
        }
    )
    assert len(results) == 1
    assert results[0].id == "2"

    if request.node.callspec.id in ["networkx-engine"]:

        pytest.skip("NetworkxEngine does not support case insensitive matching.")

    # Test another combination
    results = Product.match_nodes(
        filters={
            "name__icontains": "tablet",
            "category__iexact": "electronics",
            "stock__lte": 10,
        }
    )
    assert len(results) == 1
    assert results[0].id == "5"


def test_match_nodes_with_pagination_and_filters_icontains(request, use_graph):
    """Test combination of filters with pagination parameters."""
    if request.node.callspec.id in ["networkx-engine"]:

        pytest.skip("NetworkxEngine does not support case insensitive matching.")
    # Create test nodes
    nodes = [PracticeNode(pp=f"Test Node {i}") for i in range(1, 11)]
    PracticeNode.merge_nodes(nodes)

    # Test with filter and limit
    results = PracticeNode.match_nodes(filters={"pp__icontains": "test node"}, limit=3)
    assert len(results) == 3

    # Test with filter, skip and limit
    results = PracticeNode.match_nodes(
        filters={"pp__icontains": "test node"}, limit=2, skip=2
    )
    assert len(results) == 2
    assert all("Test Node" in x.pp for x in results)


def test_match_nodes_with_pagination_and_filters_contains(use_graph):
    """Test combination of filters with pagination parameters."""
    # Create test nodes
    nodes = [PracticeNode(pp=f"Test Node {i}") for i in range(1, 11)]
    PracticeNode.merge_nodes(nodes)

    # Test with filter and limit
    results = PracticeNode.match_nodes(filters={"pp__contains": "Test Node"}, limit=3)
    assert len(results) == 3

    # Test with filter, skip and limit
    results = PracticeNode.match_nodes(
        filters={"pp__contains": "Test Node"}, limit=2, skip=2
    )
    assert len(results) == 2
    assert all("Test Node" in x.pp for x in results)


def test_match_nodes_with_special_values(use_graph):
    """Test filtering with special values like None, empty strings, etc."""

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNode"
        id: str
        name: Optional[str] = None
        description: str = ""
        code: Optional[str] = None

    nodes = [
        TestNode(id="1", name="Product A", description="", code="A100"),
        TestNode(id="2", name=None, description="Valid description", code=None),
        TestNode(id="3", name="Product C", description="", code="C300"),
        TestNode(id="4", name="", description="Another desc", code="D400"),
    ]
    TestNode.merge_nodes(nodes)

    # Test filtering on None values
    results = TestNode.match_nodes(filters={"name__isnull": True})
    assert len(results) == 1
    assert results[0].id == "2"

    # Test filtering on empty strings
    results = TestNode.match_nodes(filters={"name": ""})
    assert len(results) == 1
    assert results[0].id == "4"

    # Test filtering on empty string field
    results = TestNode.match_nodes(filters={"description": ""})
    assert len(results) == 2


def test_match_nodes_with_enum_filter(use_graph):
    """Test filtering on enum fields."""

    class Product(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "Product"
        id: str
        name: str
        status: SampleEnum

    products = [
        Product(id="1", name="Product 1", status=SampleEnum.VALUE1),
        Product(id="2", name="Product 2", status=SampleEnum.VALUE2),
        Product(id="3", name="Product 3", status=SampleEnum.VALUE1),
        Product(id="4", name="Product 4", status=SampleEnum.VALUE3),
    ]
    Product.merge_nodes(products)

    # Test filtering on enum values
    results = Product.match_nodes(filters={"status": SampleEnum.VALUE1})
    assert len(results) == 2

    # Test with enum value as string (since we serialize to string)
    results = Product.match_nodes(filters={"status": "value1"})
    assert len(results) == 2


def test_match_nodes_with_list_filters(request, use_graph):
    """Test filtering on list fields."""
    if request.node.callspec.id in ["networkx-engine"]:

        pytest.skip("NetworkxEngine does not support list types.")

    class Product(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "Product"
        id: str
        name: str
        tags: list[str]
        categories: list[str]

    products = [
        Product(id="1", name="Product 1", tags=["tag1", "tag2"], categories=["cat1"]),
        Product(id="2", name="Product 2", tags=["tag2", "tag3"], categories=["cat2"]),
        Product(
            id="3", name="Product 3", tags=["tag1", "tag3"], categories=["cat1", "cat2"]
        ),
    ]
    Product.merge_nodes(products)

    # Note: Filtering on list fields directly isn't supported by Neo4j in a straightforward way,
    # so we might need to implement special handling or use Cypher functions like ANY()
    # For now, we'll test that the basic match works (though it's comparing lists directly)

    # Test exact match on list (order matters)
    results = Product.match_nodes(filters={"tags": ["tag1", "tag2"]})
    assert len(results) == 1
    assert results[0].id == "1"

    # Test contains on list (requires custom handling)
    # This won't work with the current implementation, so we'll skip it
    # In a full implementation, we'd need to add special handling for list fields
    pass


def test_match_nodes_with_unsupported_filter(request, use_graph):
    """Test handling of unsupported filter types."""
    if request.node.callspec.id in ["networkx-engine"]:

        pytest.skip("NetworkxEngine does not support undefined filters.")

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNode"
        id: str
        name: str

    node = TestNode(id="1", name="Test Node")
    node.merge()

    with pytest.raises(ValueError):
        TestNode.match_nodes(filters={"name__unsupported": "Test"})


def test_match_nodes_with_empty_filters(use_graph):
    """Test behavior with empty filters dictionary."""
    nodes = [
        PracticeNode(pp="Test Node 1"),
        PracticeNode(pp="Test Node 2"),
    ]
    PracticeNode.merge_nodes(nodes)

    # Test with empty filters (should return all nodes)
    results = PracticeNode.match_nodes(filters={})
    assert len(results) == 2

    # Test with None as filters (should return all nodes)
    results = PracticeNode.match_nodes(filters=None)
    assert len(results) == 2


def test_match_nodes_with_complex_types(request, use_graph):
    """Test filtering on complex types like UUID, datetime, etc."""
    if request.node.callspec.id in ["networkx-engine"]:

        pytest.skip("NetworkxEngine does not support complex types.")

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNode"
        id: UUID
        name: str
        created_at: datetime
        modified_at: datetime

    now = datetime.now()
    later = now.replace(hour=now.hour + 1)
    uuid1 = UUID("12345678123456781234567812345678")
    uuid2 = UUID("87654321876543218765432187654321")

    nodes = [
        TestNode(id=uuid1, name="Node 1", created_at=now, modified_at=later),
        TestNode(id=uuid2, name="Node 2", created_at=now, modified_at=later),
    ]
    TestNode.merge_nodes(nodes)

    # Test UUID filtering
    results = TestNode.match_nodes(filters={"id": str(uuid1)})
    assert len(results) == 1
    assert results[0].name == "Node 1"

    # Test datetime filtering
    results = TestNode.match_nodes(filters={"created_at__lte": now})
    assert len(results) == 2

    results = TestNode.match_nodes(filters={"modified_at__gt": now})
    assert len(results) == 2


def test_match_nodes_filter_on_primary_property(use_graph):
    """Test filtering on the primary property."""
    nodes = [
        PracticeNode(pp="Node 1"),
        PracticeNode(pp="Node 2"),
        PracticeNode(pp="Node 3"),
    ]
    PracticeNode.merge_nodes(nodes)

    # Test exact match on primary property
    results = PracticeNode.match_nodes(filters={"pp": "Node 2"})
    assert len(results) == 1
    assert results[0].pp == "Node 2"

    # Test contains on primary property
    results = PracticeNode.match_nodes(filters={"pp__contains": "Node"})
    assert len(results) == 3

    # Test startswith on primary property
    results = PracticeNode.match_nodes(filters={"pp__startswith": "Node"})
    assert len(results) == 3


def test_match_node(use_graph):
    tn = PracticeNode(pp="Special Test Node")

    tn.create()

    result = PracticeNode.match("Special Test Node")

    assert isinstance(result, PracticeNode)

    assert "Special Test Node" == result.pp


def test_match_none(use_graph):
    bn = PracticeNode(pp="Test Node")
    bn.create()

    result = PracticeNode.match("Does Not Exist")

    assert result is None


def test_match_many_none(use_graph):
    class PracticeNode2(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[str] = "PracticeNode2"

        pp: str

    bn = PracticeNode2(pp="Test Node")
    bn.create()

    results = PracticeNode.match_nodes()

    assert results == []


def test_delete_node(use_graph):
    tn = PracticeNode(pp="Special Test Node")

    tn.create()

    result = PracticeNode.match("Special Test Node")

    assert isinstance(result, PracticeNode)

    PracticeNode.delete("Special Test Node")

    result = PracticeNode.match("Special Test Node")

    assert result is None


class ModelTestString(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelString"
    pp: str
    test_prop_string: str


class ModelTestInt(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelInt"
    pp: str
    test_prop_int: int


class ModelTestTuple(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelTuple"
    pp: str
    test_prop_tuple: tuple


class ModelTestSet(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelSet"
    pp: str
    test_prop_set: set


class ModelTestUUID(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelUUID"
    pp: str
    test_prop_uuid: UUID


class ModelTestDateTime(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelDateTime"
    pp: str
    test_prop_datetime: datetime


class ModelTestStringList(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelStringList"
    pp: str
    test_prop_list: list


class ModelTestIntListExplicit(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TestModelIntListExplicit"
    pp: str
    test_prop_int_list_exp: list[int]


@pytest.mark.parametrize(
    "test_model,test_prop,input_value,expected_value",
    [
        (ModelTestString, "test_prop_string", "hello world", ["hello world"]),
        (ModelTestInt, "test_prop_int", 5071, [5071]),
        (
            ModelTestTuple,
            "test_prop_tuple",
            ("hello", "world"),
            [
                ("hello", "world"),
                ["hello", "world"],
            ],  # some engines support tuples, some don't
        ),
        (
            ModelTestSet,
            "test_prop_set",
            {"foo", "bar"},
            [
                ["bar", "foo"],
                ["foo", "bar"],
                {"foo", "bar"},
            ],  # some engines support sets, some don't
        ),
        (
            ModelTestUUID,
            "test_prop_uuid",
            UUID("32d4a4cb-29c3-4aa8-9b55-7790431819e3"),
            [
                UUID("32d4a4cb-29c3-4aa8-9b55-7790431819e3"),
                "32d4a4cb-29c3-4aa8-9b55-7790431819e3",
            ],
        ),
        (
            ModelTestDateTime,
            "test_prop_datetime",
            datetime(year=1984, month=1, day=2),
            [datetime(year=1984, month=1, day=2)],
        ),
        (
            ModelTestStringList,
            "test_prop_list",
            ["foo", "bar"],
            [["foo", "bar"]],
        ),
        (
            ModelTestIntListExplicit,
            "test_prop_int_list_exp",
            [1, 2, 3],
            [[1, 2, 3]],
        ),
    ],
)
def test_property_types(use_graph, test_model, test_prop, input_value, expected_value):
    pp = "test_node"

    input_data = {"pp": pp}
    input_data[test_prop] = input_value

    testmodel = test_model(**input_data)

    testmodel.create()

    result = test_model.match(pp)

    assert result.model_dump()[test_prop] == input_value

    cypher = f"""
    MATCH (n:{test_model.__primarylabel__})
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    # in the case of sets, we may get the result back ordered one of two ways
    # therefore, we check that the result is one of the expected values rather
    assert cypher_result.nodes[0].model_dump()[test_prop] in expected_value


def test_empty_list_property(use_graph):
    class TestModelListProp(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel1"
        pp: str
        list_prop: list

    pp = "test_node"

    testmodel = TestModelListProp(list_prop=[], pp=pp)

    testmodel.create()

    result = TestModelListProp.match(pp)

    assert result.list_prop == []


def test_set_on_match(use_graph):
    """Check that we successfully identify field to set on match"""

    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel2"
        pp: str = "test_node"
        only_set_on_match: Optional[str] = Field(
            json_schema_extra={"set_on_match": True}, default=None
        )
        normal_field: str

    test_node = TestModel(only_set_on_match="Foo", normal_field="Bar", pp="test_node")
    test_node.merge()

    cypher = """
    MATCH (n:TestModel2)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    assert cypher_result.nodes[0].only_set_on_match is None
    assert cypher_result.nodes[0].normal_field == "Bar"

    test_node2 = TestModel(only_set_on_match="Foo", normal_field="Bar", pp="test_node")
    test_node2.merge()

    cypher_result2 = use_graph.evaluate_query(cypher)

    assert cypher_result2.nodes[0].only_set_on_match == "Foo"


def test_set_on_create(use_graph):
    """Check that we successfully identify field to set on match"""

    class TestModel(BaseNode):
        __primaryproperty__: ClassVar[str] = "pp"
        __primarylabel__: ClassVar[Optional[str]] = "TestModel3"
        pp: str = "test_node"
        only_set_on_create: str = Field(json_schema_extra={"set_on_create": True})
        normal_field: str

    test_node = TestModel(only_set_on_create="Foo", normal_field="Bar", pp="test_node")
    test_node.merge()

    cypher = """
    MATCH (n:TestModel3)
    WHERE n.pp = 'test_node'
    RETURN n
    """

    cypher_result = use_graph.evaluate_query(cypher)

    assert cypher_result.nodes[0].only_set_on_create == "Foo"
    assert cypher_result.nodes[0].normal_field == "Bar"

    test_node2 = TestModel(only_set_on_create="Fee", normal_field="Fi", pp="test_node")
    test_node2.merge()

    cypher_result2 = use_graph.evaluate_query(cypher)

    assert cypher_result2.nodes[0].only_set_on_create == "Foo"
    assert cypher_result2.nodes[0].normal_field == "Fi"


class Person(BaseNode):
    __primaryproperty__: ClassVar[str] = "identifier"
    __primarylabel__: ClassVar[str] = (
        "PersonLabel1"  # optionally specify the label to use
    )

    name: str
    age: int
    identifier: Optional[str] = Field(default=None, validate_default=True)

    @field_validator("identifier")
    def set_identifier(cls, v, values):
        if v is None:
            v = f"{values.data['name']}_{values.data['age']}"

        return v


def test_merge_df_with_duplicates(use_graph):
    people_records = [
        {"name": "arthur", "age": 70},
        {"name": "betty", "age": 65},
        {"name": "betty", "age": 65},
        {"name": "ted", "age": 50},
        {"name": "betty", "age": 75},
        {"name": "arthur", "age": 70},
    ]

    people_df = pd.DataFrame.from_records(people_records)

    results = Person.merge_df(people_df)

    names = [x.name for x in results]

    assert names == ["arthur", "betty", "betty", "ted", "betty", "arthur"]

    assert results[0].identifier == results[5].identifier

    assert results[1].identifier == results[2].identifier

    assert results[1].identifier != results[4].identifier

    assert results[3].name == "ted"


class Person2(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[str] = (
        "PersonLabel2"  # optionally specify the label to use
    )

    name: str
    age: int
    favorite_colors: Optional[list] = None


def test_merge_df_with_lists(use_graph):
    people_records = [
        {"name": "arthur", "age": 70, "favorite_colors": ["red"]},
        {"name": "betty", "age": 65, "favorite_colors": ["red", "blue"]},
        {"name": "ted", "age": 50, "favorite_colors": []},
        {"name": "ben", "age": 75},
    ]

    people_df = pd.DataFrame.from_records(people_records)

    Person2.merge_df(people_df, deduplicate=False)

    arthur = Person2.match("arthur")
    assert arthur.favorite_colors == ["red"]

    betty = Person2.match("betty")
    assert betty.favorite_colors == ["red", "blue"]

    ted = Person2.match("ted")
    assert ted.favorite_colors == []

    ben = Person2.match("ben")
    assert ben.favorite_colors is None


def test_get_count(request, use_graph):
    people_records = [
        {"name": "arthur", "age": 70, "favorite_colors": ["red"]},
        {"name": "betty", "age": 65, "favorite_colors": ["red", "blue"]},
        {"name": "ted", "age": 50, "favorite_colors": []},
        {"name": "ben", "age": 75},
    ]

    people_df = pd.DataFrame.from_records(people_records)

    Person2.merge_df(people_df, deduplicate=False)

    assert Person2.get_count() == 4
    assert Person2.get_count(filters={"age__gt": 60}) == 3
    assert Person2.get_count(filters={"name__contains": "et"}) == 1


def test_get_count_none(use_graph):
    assert not Person2.get_count()


def test_merge_empty_df():
    df = pd.DataFrame()

    result = PracticeNode.merge_df(df)

    assert len(result) == 0
    assert isinstance(result, pd.Series)


class SampleEnum(str, Enum):
    VALUE1 = "value1"
    VALUE2 = "value2"
    VALUE3 = "value3"


class AugmentedPerson(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "name"
    __primarylabel__: ClassVar[GQLIdentifier] = "AugmentedPerson"

    name: str
    optional_enum: Optional[SampleEnum] = Field(
        default_factory=lambda: SampleEnum.VALUE1
    )

    @field_serializer("optional_enum")
    def serialize_enum(self, value: Optional[SampleEnum]) -> Optional[str]:
        return value.value if value is not None else None

    @related_nodes
    def followers(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN o"

    @related_property
    def follower_count(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN COUNT(o)"

    @property
    @related_property
    def follower_names(self):
        return "MATCH (#ThisNode)<-[:AUGMENTED_PERSON_FOLLOWS]-(o) RETURN COLLECT(DISTINCT o.name)"


class AugmentedPersonRelationship(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "AUGMENTED_PERSON_FOLLOWS"

    source: AugmentedPerson
    target: AugmentedPerson

    follow_tag: Optional[str] = None


def test_get_related_node_methods():
    assert set(AugmentedPerson.get_related_node_methods().keys()) == {
        "followers",
    }


def test_get_related_prop_methods():
    # note that only decorated methods, not properties are returned
    assert set(AugmentedPerson.get_related_property_methods().keys()) == {
        "follower_count",
    }


def test_node_schema():
    schema = AugmentedPerson.neontology_schema()

    assert schema.properties[0].name == "name"
    assert schema.properties[0].required is True
    assert schema.outgoing_relationships[0].name == "AUGMENTED_PERSON_FOLLOWS"


def test_node_schema_json():

    schema_json = AugmentedPerson.neontology_schema().model_dump_json()

    schema_dict = json.loads(schema_json)

    assert schema_dict["properties"][1]["type_annotation"]["core_type"] == "SampleEnum"

    assert schema_dict["properties"][1]["type_annotation"]["enum_values"] == [
        "value1",
        "value2",
        "value3",
    ]


def test_node_schema_md():
    schema = AugmentedPerson.neontology_schema()

    schema_md = schema.md_node_table()

    assert "| Property Name | Type | Required |" in schema_md
    assert "| name | str | True |" in schema_md


def test_rels_schema_md():
    schema = AugmentedPerson.neontology_schema()

    schema_md = schema.md_rel_tables(heading_level=4)

    assert "#### AUGMENTED_PERSON_FOLLOWS" in schema_md
    assert "| follow_tag | Optional[str] | False |" in schema_md


def test_related_nodes(request, use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(
        source=alice, target=bob, follow_tag="test-tag"
    )
    follows.merge()

    follows2 = AugmentedPersonRelationship(
        source=bob, target=alice, follow_tag="second-tag"
    )
    follows2.merge()

    alice_rels = alice.get_related()

    related_nodes = [x for x in alice_rels.nodes if x.get_pp() != alice.get_pp()]

    assert len(related_nodes) == 1
    assert related_nodes[0].name == "Bob"

    # grand cypher has limited support for relationship property queries
    if request.node.callspec.id not in ["networkx-engine"]:

        bobs_followers = bob.get_related(
            relationship_types=["AUGMENTED_PERSON_FOLLOWS"],
            incoming=True,
            outgoing=False,
            relationship_properties={"follow_tag": "test-tag"},
        )

        assert len(bobs_followers.nodes) == 2  # this will include bob himself
        assert bobs_followers.nodes[0].name == "Alice"

        bobs_rels = bob.get_related(incoming=True, distinct=True)

        assert len(bobs_rels.nodes) == 2
        assert bobs_rels.nodes[0].name == "Alice"

        assert len(bobs_rels.relationships) == 2


def test_related_nodes_unmerged(use_graph):
    alice = AugmentedPerson(name="Alice")

    alice_rels = alice.get_related()

    assert len(alice_rels.nodes) == 0


def test_related_nodes_no_rels(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    alice_rels = alice.get_related()

    assert len(alice_rels.nodes) == 0


def test_retrieve_property(request, use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    follows = AugmentedPersonRelationship(
        source=alice, target=bob, follow_tag="test-tag"
    )
    follows.merge()

    if request.node.callspec.id not in ["networkx-engine"]:
        assert bob.follower_count() == 1
        assert bob.follower_names == ["Alice"]

    if request.node.callspec.id in ["networkx-engine"]:
        # grand cypher behaves differently for returning specific values
        assert bob.follower_count()[0]["_"] == 1


def test_retrieve_property_none(request, use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    assert not bob.follower_count()

    if request.node.callspec.id not in ["networkx-engine"]:
        assert not bob.follower_names


def test_retrieve_nodes_none(use_graph):
    alice = AugmentedPerson(name="Alice")
    alice.merge()

    bob = AugmentedPerson(name="Bob")
    bob.merge()

    followers = bob.followers()

    assert len(followers) == 0


class ComplexPerson(BaseNode):
    __primaryproperty__: ClassVar[str] = "identifier"
    __primarylabel__: ClassVar[str] = (
        "PersonLabel1"  # optionally specify the label to use
    )

    name: str = Field(default_factory=uuid4)
    age: int
    favorite_colors: list = ["red", "green", "blue"]
    favorite_numbers: list = [1, 2, 3]
    extra_str1: UUID = Field(default_factory=uuid4)
    extra_str2: UUID = Field(default_factory=uuid4)

    identifier: Optional[str] = Field(default=None, validate_default=True)

    @field_validator("identifier")
    def set_identifier(cls, v, values):
        if v is None:
            v = f"{values.data['name']}_{values.data['age']}"

        return v

    @field_serializer("extra_str1", "extra_str2")
    def serialize_to_str(self, v: UUID):
        return str(v)


def test_create_mass_nodes(use_graph, benchmark):
    people_records = [{"age": x, "name": uuid4().hex} for x in range(1000)]

    people_df = pd.DataFrame.from_records(people_records)

    benchmark(ComplexPerson.merge_df, people_df)

    assert Person.get_count() == 1000


class UserWithAliases(BaseNode):
    __primaryproperty__: ClassVar[str] = "userName"
    __primarylabel__: ClassVar[str] = "AliasedUser"
    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        populate_by_name=True,  # allow population by name and alias
        extra="ignore",  # allow data to be passed in to aliased fields
    )
    user_name: str = Field(alias="userName")
    some_other_property: Optional[str] = Field(None, alias="otherProperty")


def test_aliased_properties(request, use_graph):
    user1: UserWithAliases = UserWithAliases(userName="User1")
    user2: UserWithAliases = UserWithAliases(
        user_name="User2", some_other_property="alpha"
    )
    user3: UserWithAliases = UserWithAliases(userName="User3", otherProperty="beta")
    assert user1.user_name == "User1"
    assert user3.some_other_property == "beta"

    user1.merge()
    user2.merge()
    user3.merge()

    cypher = """
    MATCH (n:AliasedUser)
    RETURN n
    ORDER BY n.userName ASC
    """

    result: NeontologyResult = use_graph.evaluate_query(cypher)
    assert result.nodes[0].user_name == "User1"
    assert hasattr(result.nodes[0], "userName") == False

    assert result.nodes[0].some_other_property is None
    assert result.nodes[1].some_other_property == "alpha"
    assert result.nodes[2].user_name == "User3"
    assert result.nodes[1].user_name == "User2"

    if request.node.callspec.id not in ["networkx-engine"]:
        assert result.records_raw[0][0]["userName"] == "User1"
        assert result.records_raw[0][0]["otherProperty"] is None

        assert result.records_raw[1][0]["otherProperty"] == "alpha"
        assert result.records_raw[2][0]["otherProperty"] == "beta"

    if request.node.callspec.id in ["networkx-engine"]:

        assert result.records_raw["n"][0]["userName"] == "User1"
        assert result.records_raw["n"][0]["otherProperty"] is None

        assert result.records_raw["n"][1]["otherProperty"] == "alpha"
        assert result.records_raw["n"][2]["otherProperty"] == "beta"
