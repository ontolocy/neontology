"""Temporal properties survive a round trip through every engine.

The neo4j driver, which the Neo4j and Memgraph engines both use, returns its own temporal
types - `neo4j.time.DateTime`, `Date`, `Time` and `Duration` - and pydantic accepts none of
them for `datetime`, `date`, `time` and `timedelta` fields. They must be converted back,
inside lists as well as on their own, or a model can write a value it cannot read back.
`create()` and `merge()` build the node they return from the database, so they failed
after the write had already happened.
"""

from datetime import date, datetime, time, timedelta, timezone
from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship


class TemporalNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "pp"
    __primarylabel__: ClassVar[Optional[str]] = "TemporalNode"

    pp: str
    naive_datetime: Optional[datetime] = None
    aware_datetime: Optional[datetime] = None
    day: Optional[date] = None
    clock: Optional[time] = None
    gap: Optional[timedelta] = None
    negative_gap: Optional[timedelta] = None
    datetimes: Optional[list[datetime]] = None
    days: Optional[list[date]] = None
    clocks: Optional[list[time]] = None
    gaps: Optional[list[timedelta]] = None


class TemporalRel(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "TEMPORAL_REL"

    source: TemporalNode
    target: TemporalNode
    since: Optional[date] = None
    lasted: Optional[timedelta] = None
    seen: Optional[list[datetime]] = None


VALUES = {
    "naive_datetime": datetime(2024, 5, 6, 7, 8, 9, 123456),
    "aware_datetime": datetime(2024, 5, 6, 7, 8, 9, 123456, tzinfo=timezone.utc),
    "day": date(2024, 5, 6),
    "clock": time(7, 8, 9, 123456),
    "gap": timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5000),
    "negative_gap": -timedelta(hours=3, microseconds=250),
    "datetimes": [datetime(2024, 5, 6, 7, 8, 9), datetime(2025, 1, 2, 3, 4, 5)],
    "days": [date(2024, 5, 6), date(2025, 1, 2)],
    "clocks": [time(7, 8, 9), time(10, 11, 12)],
    "gaps": [timedelta(hours=1), timedelta(days=2, seconds=30)],
}


@pytest.mark.parametrize("field", list(VALUES))
class TestNodeRoundTrip:
    """Each temporal type on its own, so a failure names the type that broke."""

    def test_create_returns_the_value(self, use_graph, field):
        node = TemporalNode(pp="created", **{field: VALUES[field]}).create()

        assert getattr(node, field) == VALUES[field]

    def test_merge_returns_the_value(self, use_graph, field):
        merged = TemporalNode(pp="merged", **{field: VALUES[field]}).merge()

        assert getattr(merged[0], field) == VALUES[field]

    def test_match_reads_the_value_back(self, use_graph, field):
        TemporalNode(pp="matched", **{field: VALUES[field]}).create()

        assert getattr(TemporalNode.match("matched"), field) == VALUES[field]


def test_relationship_temporal_properties_round_trip(use_graph):
    source = TemporalNode(pp="source")
    target = TemporalNode(pp="target")

    source.merge()
    target.merge()

    since = date(2020, 2, 29)
    lasted = timedelta(hours=5, seconds=7)
    seen = [datetime(2021, 3, 4, 5, 6, 7), datetime(2022, 8, 9, 10, 11, 12)]

    TemporalRel(source=source, target=target, since=since, lasted=lasted, seen=seen).merge()

    rels = TemporalRel.match_relationships()

    assert len(rels) == 1
    assert (rels[0].since, rels[0].lasted, rels[0].seen) == (since, lasted, seen)


def test_a_duration_of_months_is_left_as_the_drivers_type():
    """A month has no fixed length, so there is no timedelta that means the same thing.

    Neontology never writes one - a timedelta has no months - but a graph written some
    other way can hold one, and inventing a length for it would silently change the data.
    """
    from neo4j.time import Duration

    from neontology.graphengines.bolt import convert_neo4j_types

    months = Duration(months=1, days=2)

    assert convert_neo4j_types({"period": months})["period"] is months
