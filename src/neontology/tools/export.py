"""Writing a graph back out as content, in the format the importers read.

`neontology_dump()` already produces records, split into nodes and edges. What this
adds is the form a repository is usually written in: a record per node, carrying the
relationships that leave it, so what comes out looks like what a person would have
written by hand.
"""

import json
from pathlib import Path
from typing import Any, Optional, Union

import yaml

from .records import RELATIONSHIP_CONTROL_KEYS


def export_records(result: Any, exclude_none: bool = True) -> list[dict[str, Any]]:
    """Describe a query result as content, a record per node.

    Each node becomes a record, and each relationship is declared under the node it
    leaves, in that node's `RELATIONSHIPS_OUT`. A query has to return the nodes at both
    ends of a relationship for the relationship to be built at all, so a query which
    returns only relationships exports nothing.

    Args:
        result (Any): a NeontologyResult, from a query returning nodes and relationships.
        exclude_none (bool): leave out properties which have no value, so what is
            written says only what was set. Defaults to True.

    Returns:
        list[dict[str, Any]]: the records, in the order the nodes were returned.
    """
    records: dict[tuple, dict[str, Any]] = {}

    for node in result.nodes:
        dumped = node.neontology_dump(exclude_none=exclude_none)

        records[(node.__primarylabel__, node.get_pp())] = dumped

    for relationship in result.relationships:
        source = relationship.source

        # a relationship is only built when the result held the nodes at both of its
        # ends, so its source is among the records above - unless a caller built the
        # result itself, in which case there is nothing to declare the relationship under
        record = records.get((source.__primarylabel__, source.get_pp()))

        if record is None:
            continue

        dumped = relationship.neontology_dump(exclude_none=exclude_none)

        # the node declaring it is the source of everything in the block, and the
        # target is named rather than being the relationship's own property
        declared = {k: v for k, v in dumped.items() if k not in RELATIONSHIP_CONTROL_KEYS}

        declared["RELATIONSHIP_TYPE"] = dumped["RELATIONSHIP_TYPE"]
        declared["TARGET_LABEL"] = dumped["TARGET_LABEL"]
        declared["TARGETS"] = [dumped["TARGET"]]

        record.setdefault("RELATIONSHIPS_OUT", []).append(declared)

    return list(records.values())


def _serialisable(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert records to values which json and yaml can write.

    Pydantic already knows how to represent a model's values as JSON, so they are taken
    through it rather than each type being handled here.

    Args:
        records (list[dict[str, Any]]): the records to convert.

    Returns:
        list[dict[str, Any]]: the same records, holding only plain values.
    """
    return json.loads(json.dumps(records, default=str))


def export_json(
    result: Any,
    path: Union[str, Path],
    exclude_none: bool = True,
    indent: Optional[int] = 2,
) -> list[dict[str, Any]]:
    """Write a query result to a JSON file, in the format `import_json` reads.

    Args:
        result (Any): a NeontologyResult, from a query returning nodes and relationships.
        path (Union[str, Path]): the file to write.
        exclude_none (bool): leave out properties which have no value. Defaults to True.
        indent (Optional[int]): how far to indent the JSON. Defaults to 2.

    Returns:
        list[dict[str, Any]]: the records written.
    """
    records = _serialisable(export_records(result, exclude_none=exclude_none))

    Path(path).write_text(json.dumps(records, indent=indent))

    return records


def export_yaml(
    result: Any,
    path: Union[str, Path],
    exclude_none: bool = True,
) -> list[dict[str, Any]]:
    """Write a query result to a YAML file, in the format `import_yaml` reads.

    Args:
        result (Any): a NeontologyResult, from a query returning nodes and relationships.
        path (Union[str, Path]): the file to write.
        exclude_none (bool): leave out properties which have no value. Defaults to True.

    Returns:
        list[dict[str, Any]]: the records written.
    """
    records = _serialisable(export_records(result, exclude_none=exclude_none))

    Path(path).write_text(yaml.safe_dump(records, sort_keys=False))

    return records
