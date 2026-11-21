"""Names which are reserved words in Cypher.

A label, relationship type or property spelled as a reserved word - `Order`, `END`,
`end`, `order` - has to work on every engine. Neo4j, Memgraph and NetworkX accept them
as written; Ladybug reserves some of them, so every name it is given is quoted, through
`GraphEngineBase.identifier()`. These cover each kind of query Neontology writes.
"""

from typing import ClassVar, Optional

import pytest
from pydantic import Field

from neontology import BaseNode, BaseRelationship, Capability, get_ontology_schema
from neontology.tools import import_records


class Order(BaseNode):
    __primarylabel__: ClassVar[str] = "Order"
    __primaryproperty__: ClassVar[str] = "order"

    order: str
    end: Optional[str] = None


class ReservedItem(BaseNode):
    __primarylabel__: ClassVar[str] = "ReservedItem"
    __primaryproperty__: ClassVar[str] = "id"

    id: str


class End(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "END"

    source: Order
    target: ReservedItem

    end: Optional[str] = Field(default=None, json_schema_extra={"merge_on": True})
    order: Optional[int] = None


def test_reserved_words_as_names(use_graph):
    use_graph.initialise_graph(get_ontology_schema(models=[Order, ReservedItem, End]))

    # writing nodes
    Order(order="o1", end="soon").merge()
    Order(order="o2", end="later").create()
    Order.merge_records([{"order": "o3", "end": "never"}])
    Order.merge_records([{"order": "o3"}], partial=True)
    ReservedItem.merge_records([{"id": "i1"}, {"id": "i2"}])

    # reading them
    o1 = Order.match("o1")
    assert o1 is not None and o1.end == "soon"

    assert [node.order for node in Order.match_nodes(filters={"end": "never"})] == ["o3"]
    assert Order.get_count(filters={"end__icontains": "SOON"}) == 1

    # writing relationships, on a merge_on property and a property set on them
    End(source=Order(order="o1"), target=ReservedItem(id="i1"), end="first", order=1).merge()
    End.merge_records([{"source": "o1", "target": "i2", "end": "second", "order": 2}])

    # reading them
    assert sorted(rel.order or 0 for rel in End.match_relationships()) == [1, 2]
    assert End.get_count() == 2

    related = Order(order="o1").get_related(relationship_types=["END"])
    assert sorted(node.id for node in related.nodes if isinstance(node, ReservedItem)) == ["i1", "i2"]

    # importing, which checks endpoints with a query of its own
    import_records(
        [
            {"LABEL": "Order", "order": "o4", "end": "eventually"},
            {
                "RELATIONSHIP_TYPE": "END",
                "SOURCE_LABEL": "Order",
                "TARGET_LABEL": "ReservedItem",
                "SOURCE": "o4",
                "TARGET": "i1",
                "end": "third",
            },
        ],
        error_on_unmatched=True,
    )

    o4 = Order.match("o4")
    assert o4 is not None and o4.end == "eventually"
    assert End.get_count() == 3

    # deleting
    Order.delete("o2")
    assert Order.match("o2") is None


@pytest.mark.requires_capability(Capability.RELATIONSHIP_PROPERTY_QUERIES)
def test_reserved_words_as_relationship_properties_to_match(use_graph):
    """get_related() passes the values to match as parameters, which are named apart from them."""
    Order(order="o1").merge()
    ReservedItem.merge_records([{"id": "i1"}, {"id": "i2"}])
    End.merge_records([{"source": "o1", "target": "i1", "end": "first"}, {"source": "o1", "target": "i2", "end": "second"}])

    related = Order(order="o1").get_related(relationship_types=["END"], relationship_properties={"end": "first"})

    assert [node.id for node in related.nodes if isinstance(node, ReservedItem)] == ["i1"]
