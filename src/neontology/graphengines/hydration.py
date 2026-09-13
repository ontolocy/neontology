"""Building Neontology results from what a graph engine returns.

Every engine returns results in its own structure. Each reads its rows into the
engine-neutral values below - `RawNode`, `RawRelationship` and `RawPath` - and
`build_result` builds those into Neontology nodes, relationships and paths. Doing that in
one place is what makes the same query give the same result on every engine:

- `records` has one entry per row returned. Anything which cannot be built - a node with no
  defined class, a relationship whose class or nodes are missing, a path including such a
  relationship - is left out of its record, with a warning.
- `nodes`, `relationships` and `paths` hold each distinct one once, in the order first seen.
  Distinct means the database's identity - the `key` an engine gives - never equal
  contents, so parallel relationships with the same properties are both kept.
- each node and relationship is built once per result, so a relationship's source and
  target are the very nodes in the result, and something on several rows is the same
  object on each.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Hashable, Iterable, Optional, Sequence, TypeVar

from ..registry import _inherits_from
from ..result import NeontologyResult

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship, RelationshipTypeData

BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")


def node_class_for_labels(labels: Iterable[str], node_classes: dict[str, type[BaseNodeT]]) -> Optional[type[BaseNodeT]]:
    """Choose the class to build a node from the graph as, from its labels.

    A node carries the primary labels of several classes when a subclass carries its
    ancestors' labels - an employee node is also a :Person. It is built as the most
    derived of them: the one class which inherits from all the others. Every engine builds
    nodes through this, so they agree on which class a node is.

    When there is no such class - none of the labels belong to a known class, or they
    belong to classes unrelated to each other - the node cannot be built and a warning
    says so. A node whose labels differ from those its class declares is still built, with
    a warning.

    Args:
        labels (Iterable[str]): the node's labels, as read from the graph.
        node_classes (dict[str, type[BaseNodeT]]): the classes to choose from, by primary label.

    Returns:
        Optional[type[BaseNodeT]]: the class to build the node as, or None if there is no single answer.
    """
    node_labels = set(labels)

    known_labels = node_labels.intersection(node_classes)

    candidates = {node_classes[label] for label in known_labels}

    most_derived = [candidate for candidate in candidates if all(_inherits_from(candidate, other) for other in candidates)]

    if len(most_derived) != 1:
        if not candidates:
            reason = f"No defined node class has any of the labels {node_labels}"

        else:
            reason = "Those labels belong to classes which do not inherit from one another"

        warnings.warn(f"Unexpected primary labels returned: {known_labels}. {reason}, so the node is left out of the results.")

        return None

    node_class = most_derived[0]

    if node_labels != set(node_class._all_labels()):
        warnings.warn(f"Unexpected secondary labels returned: {node_labels - {node_class.__primarylabel__}}")

    return node_class


@dataclass(frozen=True, eq=False)
class RawNode:
    """A node as an engine returned it, before it is built into a Neontology node.

    Attributes:
        key (Hashable): the node's identity in the database. Values sharing a key are the
            same node.
        labels (Iterable[str]): the node's labels.
        properties (dict[str, Any]): the node's properties, as native Python values.
    """

    key: Hashable
    labels: Iterable[str]
    properties: dict[str, Any]


@dataclass(frozen=True, eq=False)
class RawRelationship:
    """A relationship as an engine returned it, before it is built.

    Attributes:
        key (Hashable): the relationship's identity in the database. Values sharing a key
            are the same relationship.
        type (str): the relationship type.
        properties (dict[str, Any]): the relationship's properties, as native Python values.
        source (Optional[RawNode]): the source node, or None if the result does not include it.
        target (Optional[RawNode]): the target node, or None if the result does not include it.
    """

    key: Hashable
    type: str
    properties: dict[str, Any]
    source: Optional[RawNode]
    target: Optional[RawNode]


@dataclass(frozen=True, eq=False)
class RawPath:
    """A path as an engine returned it: its relationships, in order.

    Attributes:
        relationships (Sequence[RawRelationship]): the steps along the path.
    """

    relationships: Sequence[RawRelationship]


class _ResultBuilder:
    """Builds each node and relationship of one result once, and remembers the outcome.

    Something which cannot be built is remembered as None, so it is warned about once
    rather than on every row it appears on.
    """

    def __init__(self, node_classes: dict[str, type[BaseNode]], relationship_classes: dict[str, RelationshipTypeData]):
        self._node_classes = node_classes
        self._relationship_classes = relationship_classes

        self._nodes: dict[Hashable, Optional[BaseNode]] = {}
        self._relationships: dict[Hashable, Optional[BaseRelationship]] = {}

    def node(self, raw: RawNode) -> Optional[BaseNode]:
        """Build a node, or return the one already built for it.

        Args:
            raw (RawNode): the node to build.

        Returns:
            Optional[BaseNode]: the node, or None if no single class matches its labels.
        """
        if raw.key not in self._nodes:
            node_class = node_class_for_labels(raw.labels, self._node_classes)

            self._nodes[raw.key] = None if node_class is None else node_class(**raw.properties)

        return self._nodes[raw.key]

    def relationship(self, raw: RawRelationship) -> Optional[BaseRelationship]:
        """Build a relationship, or return the one already built for it.

        Args:
            raw (RawRelationship): the relationship to build.

        Returns:
            Optional[BaseRelationship]: the relationship, or None if it cannot be built.
        """
        if raw.key not in self._relationships:
            self._relationships[raw.key] = self._build_relationship(raw)

        return self._relationships[raw.key]

    def _build_relationship(self, raw: RawRelationship) -> Optional[BaseRelationship]:
        """Build a relationship from its class and its already built nodes.

        Args:
            raw (RawRelationship): the relationship to build.

        Returns:
            Optional[BaseRelationship]: the relationship, or None - with a warning saying
                why - if its class or either of its nodes is missing.
        """
        # get rather than indexing: the classes given may be a plain dict without this type
        type_data = self._relationship_classes.get(raw.type)

        if not type_data:
            warnings.warn(
                f"Could not find a class for the {raw.type} relationship type, so it is left out of the results."
                " Is the class defined, and are its source and target node classes resolved?"
            )

            return None

        if raw.source is None or raw.target is None:
            warnings.warn(
                f"{raw.type} relationship type query did not include nodes."
                " To get neontology relationships, return source and target nodes as part of result."
            )

            return None

        source = self.node(raw.source)
        target = self.node(raw.target)

        if source is None or target is None:
            warnings.warn(f"{raw.type} relationship left out of the results: its source or target node could not be built.")

            return None

        return type_data.relationship_class(source=source, target=target, **raw.properties)

    def path(self, column: str, raw: RawPath) -> Optional[list[BaseRelationship]]:
        """Build a path from its relationships.

        A path including a relationship which cannot be built is left out entirely: a gap
        in it, or a shorter path, would both misrepresent what the graph holds.

        Args:
            column (str): the column the path was returned in, for the warning.
            raw (RawPath): the path to build.

        Returns:
            Optional[list[BaseRelationship]]: the path's relationships in order, or None if
                it has none or any of them cannot be built.
        """
        steps = [self.relationship(step) for step in raw.relationships]

        if not steps:
            return None

        if any(step is None for step in steps):
            warnings.warn(f"Path '{column}' left out of the results: it includes a relationship which could not be built.")

            return None

        return steps  # type: ignore[return-value]


def build_result(
    records_raw: Any,
    rows: Iterable[dict[str, Any]],
    node_classes: dict[str, type[BaseNode]],
    relationship_classes: dict[str, RelationshipTypeData],
) -> NeontologyResult:
    """Build an engine's rows into a NeontologyResult.

    Args:
        records_raw (Any): the engine's own result, kept verbatim on the NeontologyResult.
        rows (Iterable[dict[str, Any]]): one dictionary per row returned, from column name to
            value. Only RawNode, RawRelationship and RawPath values are built; anything else
            is available through `records_raw`.
        node_classes (dict[str, type[BaseNode]]): node classes by primary label.
        relationship_classes (dict[str, RelationshipTypeData]): relationship type data by type.

    Returns:
        NeontologyResult: the records, and the distinct nodes, relationships and paths in them.
    """
    builder = _ResultBuilder(node_classes or {}, relationship_classes or {})

    records = []

    # keyed by database identity, so each appears once, in the order first seen
    nodes: dict[Hashable, BaseNode] = {}
    relationships: dict[Hashable, BaseRelationship] = {}
    paths: dict[tuple, list[BaseRelationship]] = {}

    for row in rows:
        record: dict[str, dict] = {"nodes": {}, "relationships": {}, "paths": {}}

        for column, value in row.items():
            if isinstance(value, RawNode):
                node = builder.node(value)

                if node is not None:
                    record["nodes"][column] = node
                    nodes.setdefault(value.key, node)

            elif isinstance(value, RawRelationship):
                relationship = builder.relationship(value)

                if relationship is not None:
                    record["relationships"][column] = relationship
                    relationships.setdefault(value.key, relationship)

            elif isinstance(value, RawPath):
                path = builder.path(column, value)

                if path is not None:
                    record["paths"][column] = path
                    paths.setdefault(tuple(step.key for step in value.relationships), path)

        records.append(record)

    return NeontologyResult(
        records_raw=records_raw,
        records=records,
        nodes=list(nodes.values()),
        relationships=list(relationships.values()),
        paths=list(paths.values()),
    )
