"""The shape of the content Neontology imports, and how it is read.

This layer turns whatever was written - a file, a list, a single record - into node and
relationship records ready to be merged, without touching the graph. Nothing here
writes, so the same code validates content and imports it.
"""

import difflib
import re
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from pydantic import BaseModel, model_validator

from ..utils import get_node_types, get_rels_by_type
from .errors import ErrorCollector, ImportContentError
from .origin import RecordOrigin, SourcedRecord

# Keys which say how to build the graph rather than describing a property of a model.
# Everything else in a record is a property of the node or relationship it describes.
NODE_CONTROL_KEYS = frozenset({"LABEL", "RELATIONSHIPS_OUT"})

RELATIONSHIP_CONTROL_KEYS = frozenset(
    {
        "RELATIONSHIP_TYPE",
        "SOURCE_LABEL",
        "TARGET_LABEL",
        "TARGET_PROPERTY",
        "SOURCE",
        "TARGET",
        "TARGETS",
        "TARGET_NODES",
        # the lowercase spelling of SOURCE and TARGET, deprecated and removed in v4
        "source",
        "target",
    }
)

# the keys naming the far end of a relationship, in the order they are read
TARGET_KEYS = ("TARGET_NODES", "TARGETS", "TARGET")

# accepted as aliases of the canonical uppercase endpoint keys
LEGACY_ENDPOINT_KEYS = {"source": "SOURCE", "target": "TARGET"}

# a key written the way the control keys are written, so a mistyped one can be told
# apart from an ordinary property and reported as the mistake it is
_CONTROL_KEY_SHAPE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass
class ImportContext:
    """What one import run needs to carry across the records it reads.

    Attributes:
        collector: gathers problems, or raises them as they are found.
        legacy_endpoint_keys: the records which spelled an endpoint in lowercase, so
            the deprecation can be reported once for the run rather than once per record.
    """

    collector: ErrorCollector = field(default_factory=ErrorCollector)
    legacy_endpoint_keys: list[RecordOrigin] = field(default_factory=list)


def node_class_for_label(label: Optional[str]) -> type:
    """Get the node class carrying a primary label.

    Args:
        label (Optional[str]): the primary label named by a record.

    Returns:
        type: the node class.

    Raises:
        ImportContentError: if no node class carries that label.
    """
    node_types = get_node_types()

    if label not in node_types:
        raise ImportContentError(
            f"No node class has the primary label {label!r}, so records using it cannot be imported."
            " A model class is registered when it is defined, so check the label is spelled as the"
            " class declares it, and that the module defining the class has been imported."
        )

    return node_types[label]


def relationship_class_for_type(rel_type: Optional[str]) -> type:
    """Get the relationship class carrying a relationship type.

    Args:
        rel_type (Optional[str]): the relationship type named by a record.

    Returns:
        type: the relationship class.

    Raises:
        ImportContentError: if no relationship class has that type.
    """
    rel_types = get_rels_by_type()

    # a defaultdict, so membership has to be asked rather than reading and checking
    if rel_type not in rel_types:
        raise ImportContentError(
            f"No relationship class has the relationship type {rel_type!r}, so records using it cannot"
            " be imported. Is the class defined, and are its source and target node classes resolved?"
        )

    return rel_types[rel_type].relationship_class


def check_control_keys(raw: dict[str, Any], reserved: frozenset, model_class: type) -> None:
    """Check a record for keys meant to be control keys but not spelled like any of them.

    A mistyped control key is otherwise read as a property, and reported as a field the
    model does not have - which is true, but says nothing about the mistake.

    Args:
        raw (dict[str, Any]): the record as it was written.
        reserved (frozenset): the control keys a record of this kind may use.
        model_class (type): the model the record describes, whose own fields are
            properties however they are spelled.

    Raises:
        ImportContentError: naming the nearest control key, where there is one.
    """
    for key in raw:
        if key in reserved or key in model_class.model_fields:
            continue

        if not _CONTROL_KEY_SHAPE.match(key):
            continue

        suggestions = difflib.get_close_matches(key, sorted(reserved), n=1, cutoff=0.6)

        did_you_mean = f" Did you mean {suggestions[0]!r}?" if suggestions else ""

        raise ImportContentError(
            f"{key!r} is not one of the keys a record uses to say how to build the graph, and"
            f" {model_class.__name__} has no property by that name.{did_you_mean}"
        )


def normalise_endpoint_keys(raw: dict[str, Any], context: ImportContext, origin: RecordOrigin) -> dict[str, Any]:
    """Rewrite the lowercase endpoint keys to their canonical uppercase spelling.

    The format is uppercase throughout. `source` and `target` are deprecated aliases,
    accepted so content written against earlier versions - and dumps taken from them -
    still import. They are removed in v4.

    Args:
        raw (dict[str, Any]): one raw relationship record.
        context (ImportContext): the run, which notes that the alias was used.
        origin (RecordOrigin): where the record came from.

    Returns:
        dict[str, Any]: the record, with its endpoint keys spelled canonically.
    """
    normalised = dict(raw)

    used_alias = False

    for alias, canonical in LEGACY_ENDPOINT_KEYS.items():
        if alias not in normalised:
            continue

        used_alias = True

        value = normalised.pop(alias)

        if canonical not in normalised:
            normalised[canonical] = value

    # noted once for the record, not once per key, so the count is of records
    if used_alias is True:
        context.legacy_endpoint_keys.append(origin)

    return normalised


class NeontologyNodeRaw(BaseModel):
    """Pydantic model for a Neontology node raw record."""

    LABEL: str


class NeontologyRelationshipRaw(BaseModel):
    """Pydantic model for a Neontology relationship raw record.

    This describes a relationship record once its targets have been expanded, so it
    names a single source and a single target. `TARGETS` and `TARGET_NODES` are read
    before that, by `expand_relationship`.
    """

    RELATIONSHIP_TYPE: str
    SOURCE_LABEL: str
    TARGET_LABEL: str
    SOURCE: Optional[Any] = None
    TARGET: Optional[Any] = None
    TARGET_PROPERTY: Optional[str] = None


class NeontologyNodeRecord(BaseModel):
    """Pydantic model for transforming a raw Neontology node record."""

    input_record: NeontologyNodeRaw
    label: Optional[str] = None
    output_record: Optional[dict[str, Any]] = None

    # a node declared inside a relationship's TARGET_NODES, rather than by a record of
    # its own. Inline records bring a node into the graph without it needing an entry
    # of its own, so any number of them may name the same node - unlike definitions.
    inline: bool = False

    origin: RecordOrigin = RecordOrigin(source="<records>")

    @model_validator(mode="before")
    def populate_node_fields(cls, data):
        """Use the raw input data (from input_record) to populate the fields of the model."""
        if not data.get("label"):
            data["label"] = data.get("input_record", {}).get("LABEL")

        if not data.get("output_record"):
            data["output_record"] = {k: v for k, v in data.get("input_record", {}).items() if k not in NODE_CONTROL_KEYS}

        return data


class NeontologyRelationshipRecord(BaseModel):
    """Pydantic model for transforming a raw Neontology relationship record."""

    input_record: NeontologyRelationshipRaw
    relationship_type: Optional[str] = None

    target_prop: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    source: Optional[Any] = None
    target: Optional[Any] = None

    output_record: Optional[dict] = None

    origin: RecordOrigin = RecordOrigin(source="<records>")

    @model_validator(mode="before")
    def populate_rel_fields(cls, data: dict) -> dict:
        """Use the raw input data (from input_record) to populate the fields of the model.

        Args:
            data: dict of raw model fields before parsing.

        Returns:
            dict: processed dict of model fields ready for Pydantic validation.
        """
        raw = data.get("input_record", {})

        if not data.get("relationship_type"):
            data["relationship_type"] = raw.get("RELATIONSHIP_TYPE")

        if not data.get("source_label"):
            data["source_label"] = raw.get("SOURCE_LABEL")

        if not data.get("target_label"):
            data["target_label"] = raw.get("TARGET_LABEL")

        if not data.get("target_prop"):
            data["target_prop"] = raw.get("TARGET_PROPERTY")

        if not data.get("source"):
            data["source"] = raw.get("SOURCE")

        if not data.get("target"):
            data["target"] = raw.get("TARGET")

        if not data.get("output_record"):
            # merge_records takes the endpoints as lowercase source and target - that is
            # the model layer's record convention, which the content format does not share
            data["output_record"] = {
                **{k: v for k, v in raw.items() if k not in RELATIONSHIP_CONTROL_KEYS},
                "source": raw.get("SOURCE"),
                "target": raw.get("TARGET"),
            }

        return data

    @model_validator(mode="after")
    def populate_fields(self):
        """Use the raw input data (from input_record) to populate the fields of the model."""
        # if an explicit TARGET_PROPERTY wasn't passed in, use the primary property
        if self.target_prop is None:
            self.target_prop = node_class_for_label(self.target_label).__primaryproperty__

        return self


def primary_property_value(node_record: NeontologyNodeRecord) -> Any:
    """Get the primary property value identifying the node a record describes.

    Read from the record where it is declared - which is the usual case, and the one
    other content relies on to point at this node. Where the model computes it instead,
    the record is hydrated so the computed value can be read.

    Args:
        node_record (NeontologyNodeRecord): the record describing the node.

    Returns:
        Any: the value of the node's primary property.
    """
    node_class = node_class_for_label(node_record.label)
    pp = node_class.__primaryproperty__
    declared = node_record.output_record or {}

    if pp in declared:
        return declared[pp]

    return node_class(**declared).get_pp()


def _node_record(
    raw: dict[str, Any],
    origin: RecordOrigin,
    inline: bool = False,
) -> NeontologyNodeRecord:
    """Build one node record, checking it is shaped like a node record.

    Args:
        raw (dict[str, Any]): the record as it was written.
        origin (RecordOrigin): where it came from.
        inline (bool): whether it was declared inside a relationship's TARGET_NODES.

    Returns:
        NeontologyNodeRecord: the record.

    Raises:
        ImportContentError: if it uses a key no node record has.
    """
    if inline is True and "RELATIONSHIPS_OUT" in raw:
        raise ImportContentError(
            "A node defined inline in TARGET_NODES cannot declare RELATIONSHIPS_OUT. Give the node a"
            " record of its own to relate it to anything further."
        )

    check_control_keys(raw, NODE_CONTROL_KEYS, node_class_for_label(raw.get("LABEL")))

    return NeontologyNodeRecord(input_record=raw, inline=inline, origin=origin)


def expand_relationship(
    raw: dict[str, Any],
    source_value: Any,
    source_label: Optional[str],
    origin: RecordOrigin,
    context: ImportContext,
) -> tuple[list[NeontologyNodeRecord], list[NeontologyRelationshipRecord]]:
    """Expand one relationship record into a record per target it names.

    A relationship record names its targets in one of three ways, which behave the same
    whether the record stands on its own or is nested under a node's RELATIONSHIPS_OUT:
    `TARGET` for a single node, `TARGETS` for several, and `TARGET_NODES` for nodes
    defined inline, which the import brings into the graph rather than matching.

    Args:
        raw (dict[str, Any]): one raw relationship record.
        source_value (Any): the value identifying the source node.
        source_label (Optional[str]): the primary label of the source node.
        origin (RecordOrigin): where the record came from.
        context (ImportContext): the run it is part of.

    Returns:
        tuple: the node records the relationship defines inline, and one relationship
        record per target.

    Raises:
        ImportContentError: if the record names no target at all, or has no source.
    """
    raw = normalise_endpoint_keys(raw, context, origin)

    raw["SOURCE_LABEL"] = source_label

    check_control_keys(raw, RELATIONSHIP_CONTROL_KEYS, relationship_class_for_type(raw.get("RELATIONSHIP_TYPE")))

    if not any(key in raw for key in TARGET_KEYS):
        raise ImportContentError(
            f"A {raw.get('RELATIONSHIP_TYPE')!r} relationship record names no target. Give it a TARGET,"
            " a list of TARGETS, or TARGET_NODES to define the nodes at the far end inline."
        )

    if source_value is None:
        raise ImportContentError(f"A {raw.get('RELATIONSHIP_TYPE')!r} relationship record has no SOURCE to relate from.")

    inline_nodes = []
    targets = []

    for position, entry in enumerate(raw.pop("TARGET_NODES", None) or []):
        node_record = _node_record(entry, origin.nested(f"TARGET_NODES[{position}]"), inline=True)

        inline_nodes.append(node_record)
        targets.append(primary_property_value(node_record))

    targets += raw.pop("TARGETS", None) or []

    if "TARGET" in raw:
        targets.append(raw.pop("TARGET"))

    relationships = [
        NeontologyRelationshipRecord(
            input_record={**raw, "SOURCE": source_value, "TARGET": target},
            origin=origin,
        )
        for target in targets
    ]

    return inline_nodes, relationships


def _process_sub_records(
    source_node_record: NeontologyNodeRecord,
    subrecords: list[dict[str, Any]],
    origin: RecordOrigin,
    context: ImportContext,
) -> tuple[list[NeontologyNodeRecord], list[NeontologyRelationshipRecord]]:
    """Expand the relationships declared under a node record's RELATIONSHIPS_OUT.

    Args:
        source_node_record (NeontologyNodeRecord): the record declaring them, which is
            the source of every one of them.
        subrecords (list[dict[str, Any]]): the raw relationship records.
        origin (RecordOrigin): where the declaring record came from.
        context (ImportContext): the run it is part of.

    Returns:
        tuple: the node records defined inline, and the relationship records.
    """
    source_pp = primary_property_value(source_node_record)

    output_nodes = []
    output_rels = []

    for position, record in enumerate(subrecords):
        nested_origin = origin.nested(f"RELATIONSHIPS_OUT[{position}]")

        with context.collector.catching(nested_origin):
            new_nodes, new_rels = expand_relationship(
                record,
                source_pp,
                source_node_record.label,
                nested_origin,
                context,
            )

            output_nodes += new_nodes
            output_rels += new_rels

    return output_nodes, output_rels


def iter_raw_records(entry: Union[list, dict[str, Any]], origin: RecordOrigin) -> list[SourcedRecord]:
    """Read the node and relationship records out of one piece of input.

    Accepts a single record, a list of records, or the container `neontology_dump()`
    produces, which splits them into `nodes` and `edges`. Lists may be nested, because
    a file holding several documents each holding a list of records reads that way.

    Args:
        entry (Union[list, dict[str, Any]]): one piece of input.
        origin (RecordOrigin): where it came from.

    Returns:
        list[SourcedRecord]: the records it holds, each with its own origin.
    """
    if isinstance(entry, dict):
        # the container form is only recognised where the record does not identify
        # itself as a node or a relationship, so a model with a property called
        # 'nodes' or 'edges' is still read as the record it is
        if "LABEL" in entry or "RELATIONSHIP_TYPE" in entry:
            return [SourcedRecord(data=entry, origin=origin)]

        if "nodes" in entry or "edges" in entry:
            held = list(entry.get("nodes") or []) + list(entry.get("edges") or [])

            return [SourcedRecord(data=x, origin=origin.at(position)) for position, x in enumerate(held)]

        return [SourcedRecord(data=entry, origin=origin)]

    records: list[SourcedRecord] = []

    for position, item in enumerate(entry):
        records += iter_raw_records(item, origin.at(position))

    return records


def _unidentified_record_message(record: dict[str, Any]) -> str:
    """Say why a record could not be read as a node or a relationship.

    A record is identified by LABEL or RELATIONSHIP_TYPE, so the usual reason for one
    being neither is that the key identifying it is mistyped - which is worth saying,
    rather than only that the record could not be placed.

    Args:
        record (dict[str, Any]): the record as it was written.

    Returns:
        str: the message to report.
    """
    identifiers = ["LABEL", "RELATIONSHIP_TYPE"]

    for key in record:
        suggestions = difflib.get_close_matches(key, identifiers, n=1, cutoff=0.6)

        if suggestions and key not in identifiers:
            return (
                f"A record uses {key!r}, which is not how a record says what it is."
                f" Did you mean {suggestions[0]!r}? A node record has a LABEL and a relationship"
                f" record a RELATIONSHIP_TYPE: {record!r}"
            )

    return (
        "A record has neither LABEL (which makes it a node) nor RELATIONSHIP_TYPE (which"
        f" makes it a relationship), so there is nothing to import it as: {record!r}"
    )


def prepare_records(
    input_records: Union[list, dict[str, Any]],
    origin: RecordOrigin,
    context: ImportContext,
) -> tuple[list[NeontologyNodeRecord], list[NeontologyRelationshipRecord]]:
    """Turn raw input into node and relationship records, ready to be merged.

    Args:
        input_records (Union[list, dict[str, Any]]): one piece of input.
        origin (RecordOrigin): where it came from.
        context (ImportContext): the run it is part of.

    Returns:
        tuple: the node records and the relationship records it describes.
    """
    return prepare_sourced_records(iter_raw_records(input_records, origin), context)


def prepare_sourced_records(
    sourced: list[SourcedRecord],
    context: ImportContext,
) -> tuple[list[NeontologyNodeRecord], list[NeontologyRelationshipRecord]]:
    """Turn records which already know where they came from into records to merge.

    Args:
        sourced (list[SourcedRecord]): the raw records, each with its origin.
        context (ImportContext): the run they are part of.

    Returns:
        tuple: the node records and the relationship records they describe.
    """
    nodes: list[NeontologyNodeRecord] = []
    relationships: list[NeontologyRelationshipRecord] = []

    for entry in sourced:
        record = entry.data
        origin = entry.origin

        with context.collector.catching(origin):
            if "LABEL" in record:
                rel_records = record.pop("RELATIONSHIPS_OUT", None)

                node_record = _node_record(record, origin)

                nodes.append(node_record)

                if rel_records:
                    new_nodes, new_rels = _process_sub_records(node_record, rel_records, origin, context)

                    nodes += new_nodes
                    relationships += new_rels

            elif "RELATIONSHIP_TYPE" in record:
                # normalised here rather than twice, so using a deprecated endpoint key
                # is noted once for the record rather than once per pass over it
                record = normalise_endpoint_keys(record, context, origin)

                new_nodes, new_rels = expand_relationship(
                    record,
                    record.get("SOURCE"),
                    record.get("SOURCE_LABEL"),
                    origin,
                    context,
                )

                nodes += new_nodes
                relationships += new_rels

            else:
                raise ImportContentError(_unidentified_record_message(record))

    return nodes, relationships
