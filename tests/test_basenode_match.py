"""Matching, filtering, pagination and deletion."""

from datetime import datetime
from typing import ClassVar, Optional
from uuid import UUID

import pytest
from models_basenode import PracticeNode, SampleEnum

from neontology import (
    BaseNode,
)
from neontology.graphengines.capabilities import Capability


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


def test_match_nodes_with_string_filters(engine, use_graph):
    """Test various string filter operations."""

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNode"
        id: str
        name: str
        description: str
        code: str

    nodes = [
        TestNode(id="1", name="Aspartame", description="Sweetener product", code="E951"),
        TestNode(id="2", name="Sucrose", description="Natural sweetener", code="E473"),
        TestNode(
            id="3",
            name="aspartame synthetic",
            description="Artificial sweetener",
            code="E951",
        ),
        TestNode(id="4", name="Stevia", description="Plant-based sweetener", code="E960"),
    ]
    TestNode.merge_nodes(nodes)

    # Test exact match (default)
    results = TestNode.match_nodes(filters={"name": "Aspartame"})
    assert len(results) == 1
    assert results[0].id == "1"

    if engine.supports(Capability.CASE_INSENSITIVE_FILTERS):
        results = TestNode.match_nodes(filters={"name__icontains": "aspart"})
        assert len(results) == 2
        assert sorted([x.id for x in results]) == ["1", "3"]

    else:
        with pytest.raises(NotImplementedError):
            TestNode.match_nodes(filters={"name__icontains": "aspart"})

    # Test case-sensitive contains
    results = TestNode.match_nodes(filters={"name__contains": "Aspart"})
    assert len(results) == 1
    assert results[0].id == "1"

    if engine.supports(Capability.CASE_INSENSITIVE_FILTERS):
        results = TestNode.match_nodes(filters={"name__iexact": "aspartame synthetic"})
        assert len(results) == 1
        assert results[0].id == "3"

    else:
        with pytest.raises(NotImplementedError):
            TestNode.match_nodes(filters={"name__iexact": "aspartame synthetic"})

    # Test startswith and istartswith

    results = TestNode.match_nodes(filters={"description__startswith": "Sweetener"})
    assert len(results) == 1
    assert results[0].id == "1"

    if engine.supports(Capability.CASE_INSENSITIVE_FILTERS):
        results = TestNode.match_nodes(filters={"description__istartswith": "plant"})
        assert len(results) == 1
        assert results[0].id == "4"

    else:
        with pytest.raises(NotImplementedError):
            TestNode.match_nodes(filters={"description__istartswith": "plant"})


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
    results = Product.match_nodes(filters={"price__gt": 5, "price__lt": 15, "rating__gte": 4.6})
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


@pytest.mark.requires_capability(Capability.DATETIME_FILTERS)
def test_match_nodes_with_datetime_filter(use_graph):
    """Test filtering on datetime fields."""

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
    results = Event.match_nodes(filters={"start_date__gte": date2, "end_date__lte": date3})
    assert len(results) == 1
    assert sorted([x.id for x in results]) == ["2"]


@pytest.mark.requires_capability(Capability.CASE_INSENSITIVE_FILTERS)
def test_match_nodes_with_combined_filters(use_graph):
    """Test combination of different filter types."""

    class Product(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "ProductDatetimeFilter"
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


@pytest.mark.requires_capability(Capability.CASE_INSENSITIVE_FILTERS)
def test_match_nodes_with_pagination_and_filters_icontains(use_graph):
    """Test combination of filters with pagination parameters."""
    # Create test nodes
    nodes = [PracticeNode(pp=f"Test Node {i}") for i in range(1, 11)]
    PracticeNode.merge_nodes(nodes)

    # Test with filter and limit
    results = PracticeNode.match_nodes(filters={"pp__icontains": "test node"}, limit=3)
    assert len(results) == 3

    # Test with filter, skip and limit
    results = PracticeNode.match_nodes(filters={"pp__icontains": "test node"}, limit=2, skip=2)
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
    results = PracticeNode.match_nodes(filters={"pp__contains": "Test Node"}, limit=2, skip=2)
    assert len(results) == 2
    assert all("Test Node" in x.pp for x in results)


def test_match_nodes_with_special_values(use_graph):
    """Test filtering with special values like None, empty strings, etc."""

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNodePagination"
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
        __primarylabel__: ClassVar[str] = "ProductCombinedFilter"
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


@pytest.mark.requires_capability(Capability.LIST_PROPERTY_FILTERS)
def test_match_nodes_with_list_filters(use_graph):
    """Test filtering on list fields."""

    class Product(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "ProductListFilter"
        id: str
        name: str
        tags: list[str]
        categories: list[str]

    products = [
        Product(id="1", name="Product 1", tags=["tag1", "tag2"], categories=["cat1"]),
        Product(id="2", name="Product 2", tags=["tag2", "tag3"], categories=["cat2"]),
        Product(id="3", name="Product 3", tags=["tag1", "tag3"], categories=["cat1", "cat2"]),
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


def test_match_nodes_with_unsupported_filter(use_graph):
    """Test handling of unsupported filter types."""

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNodeUnsupportedFilter"
        id: str
        name: str

    node = TestNode(id="1", name="Test Node")
    node.merge()

    # the shared implementation raises ValueError for an unknown lookup; the
    # networkx override rejects it as NotImplementedError before reaching that
    with pytest.raises((ValueError, NotImplementedError)):
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


@pytest.mark.requires_capability(Capability.COMPLEX_PROPERTY_TYPES)
def test_match_nodes_with_complex_types(use_graph):
    """Test filtering on complex types like UUID, datetime, etc."""

    class TestNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "id"
        __primarylabel__: ClassVar[str] = "TestNodeComplexTypes"
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
