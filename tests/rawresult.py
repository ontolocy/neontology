"""Helpers for reading engine-native query results in tests.

`NeontologyResult.records_raw` is deliberately the driver's own structure, so it
differs between engines by design rather than because of a missing capability.
These helpers keep that knowledge in one place instead of branching on the engine
at every assertion.
"""

from typing import Any


def raw_labels(result: Any, key: str = "n", index: int = 0) -> set:
    """Return the labels of a node in a raw result, whichever engine produced it.

    Args:
        result (Any): a NeontologyResult.
        key (str): the query alias the node was returned under. Defaults to "n".
        index (int): which record to read. Defaults to 0.

    Returns:
        set: the node's labels.
    """
    raw = result.records_raw

    # grand-cypher returns a dict of columns keyed by query alias
    if isinstance(raw, dict):
        return set(raw[key][index]["__labels__"])

    # the neo4j driver returns a list of records holding driver Node objects
    return set(raw[index].values()[0].labels)


def raw_property(result: Any, prop: str, key: str = "n", index: int = 0) -> Any:
    """Return a node property from a raw result, whichever engine produced it.

    Args:
        result (Any): a NeontologyResult.
        prop (str): the property name as stored in the graph.
        key (str): the query alias the node was returned under. Defaults to "n".
        index (int): which record to read. Defaults to 0.

    Returns:
        Any: the property value.
    """
    raw = result.records_raw

    if isinstance(raw, dict):
        return raw[key][index][prop]

    return raw[index][0][prop]
