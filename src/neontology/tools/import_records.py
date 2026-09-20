"""Merging prepared content into the graph, and checking it before anything is written."""

import logging
import warnings
from collections import Counter, defaultdict
from collections.abc import Iterator, Sequence
from copy import deepcopy
from typing import Any, Callable, Optional

from ..gql import gql_identifier_adapter
from ..graphconnection import GraphConnection
from .errors import (
    ConflictingNodeRecordError,
    DuplicateNodeDefinitionError,
    ErrorCollector,
    ImportContentError,
)
from .origin import RecordOrigin, SourcedRecord
from .records import (
    ImportContext,
    NeontologyNodeRecord,
    NeontologyRelationshipRecord,
    iter_raw_records,
    node_class_for_label,
    prepare_sourced_records,
    primary_property_value,
    relationship_class_for_type,
)
from .report import ImportReport

logger = logging.getLogger(__name__)


def _combine_node_records(
    input_records: list[NeontologyNodeRecord],
    context: ImportContext,
) -> list[NeontologyNodeRecord]:
    """Combine the records describing each node into one record per node.

    A node may be described more than once: by its definition, and by any number of
    inline records naming it as the target of a relationship. Combining them before
    anything is written is what makes the result independent of the order the content
    was read in - an inline record naming only the primary property would otherwise be
    hydrated into a full model and overwrite the definition's properties with defaults.

    Args:
        input_records (list[NeontologyNodeRecord]): every node record, in the order read.
        context (ImportContext): the run they are part of.

    Returns:
        list[NeontologyNodeRecord]: one record per node, in order of first appearance.
    """
    grouped: dict[tuple, list[NeontologyNodeRecord]] = {}

    for record in input_records:
        with context.collector.catching(record.origin):
            grouped.setdefault((record.label, primary_property_value(record)), []).append(record)

    combined = []

    for (label, pp_value), group in grouped.items():
        with context.collector.catching(group[0].origin):
            combined.append(_combine_one_node(label, pp_value, group))

    return combined


def _combine_one_node(label: Optional[str], pp_value: Any, group: list[NeontologyNodeRecord]) -> NeontologyNodeRecord:
    """Combine every record describing one node into a single record.

    Args:
        label (Optional[str]): the node's primary label.
        pp_value (Any): the node's primary property value.
        group (list[NeontologyNodeRecord]): the records describing it.

    Returns:
        NeontologyNodeRecord: one record carrying every property the group declares.

    Raises:
        DuplicateNodeDefinitionError: if more than one of them is a definition.
        ConflictingNodeRecordError: if two of them give a property different values.
    """
    if len(group) == 1:
        return group[0]

    definitions = [x for x in group if x.inline is False]

    if len(definitions) > 1:
        where = "; ".join(str(x.origin) for x in definitions)

        raise DuplicateNodeDefinitionError(
            f"{label} {pp_value!r} is defined {len(definitions)} times, but a node has one definition."
            f" Defined in: {where}."
            " Nodes named inline by a relationship (in TARGET_NODES) may repeat freely; records at the"
            " top level of the content define a node, so only one of them may describe it."
        )

    combined: dict[str, Any] = {}
    declared_by: dict[str, RecordOrigin] = {}

    for record in group:
        for prop, value in (record.output_record or {}).items():
            if prop in combined and combined[prop] != value:
                raise ConflictingNodeRecordError(
                    f"Records describing {label} {pp_value!r} give {prop!r} two different values:"
                    f" {combined[prop]!r} in {declared_by[prop]}, and {value!r} in {record.origin}."
                    " Records for one node are combined, so they have to agree - preferring either"
                    " value would make the graph depend on the order the content was read in."
                )

            combined[prop] = value
            declared_by[prop] = record.origin

    return NeontologyNodeRecord(
        input_record={"LABEL": label, **combined},
        inline=all(x.inline for x in group),
        # the definition where there is one, so the combined record is reported against
        # the record a reader would go and edit
        origin=(definitions or group)[0].origin,
    )


def _locate_failure(
    error: Exception,
    records: Sequence[dict],
    origins: Sequence[RecordOrigin],
    hydrate: Callable[[dict], Any],
    context: ImportContext,
) -> None:
    """Report a failed write against the record which caused it, where one did.

    Records are written in batches, so a model which does not validate fails the batch
    rather than naming itself. Each record in the batch is hydrated here - on the
    failure path only, so a successful import pays nothing for it - to find which one it
    was, and report it with the file and entry it came from.

    A batch can also fail for reasons no single record explains, such as the database
    rejecting the write. That cannot be narrowed down here, so it is reported against
    the batch, saying what to run to locate it.

    Args:
        error (Exception): what the write raised.
        records (Sequence[dict]): the records in the failed batch.
        origins (Sequence[RecordOrigin]): where each of them came from.
        hydrate (Callable[[dict], Any]): builds one record's model, as the write does.
        context (ImportContext): the run they are part of.

    Raises:
        ImportContentError: if no single record accounts for the failure.
    """
    found = False

    for record, origin in zip(records, origins):
        try:
            hydrate(record)

        except Exception as exc:  # noqa: PERF203 - the failure path, once per batch
            found = True
            context.collector.fail(origin, exc)

    if found is True:
        return

    sources = sorted({origin.source for origin in origins})

    raise ImportContentError(
        f"Writing {len(records)} records failed, and no single record accounts for it, so it is the"
        f" write itself rather than the content: {error}."
        f" The batch covered: {', '.join(sources)}."
        " Run the import with validate_only=True to check the content against your models."
    ) from error


def _import_nodes(
    input_records: list[NeontologyNodeRecord],
    context: ImportContext,
    report: ImportReport,
    batch_size: Optional[int] = None,
) -> None:
    """Merge node records into the graph, one node at a time however often it is described.

    Args:
        input_records (list[NeontologyNodeRecord]): the combined node records to merge.
        context (ImportContext): the run they are part of.
        report (ImportReport): counts what was merged.
        batch_size (Optional[int]): how many records to merge per query, or None for all.
    """
    mapped_records = defaultdict(list)

    for record in input_records:
        mapped_records[record.label].append(record)

    for label, node_records in mapped_records.items():
        node_class = node_class_for_label(label)

        records = [x.output_record for x in node_records]
        origins = [x.origin for x in node_records]

        for batch, batch_origins in _batches_with_origins(records, origins, batch_size):
            try:
                node_class.merge_records(batch)

            except Exception as exc:
                _locate_failure(exc, batch, batch_origins, lambda x, cls=node_class: cls(**x), context)

        report.nodes[label] = report.nodes.get(label, 0) + len(records)


def _batches_with_origins(
    records: Sequence[dict],
    origins: Sequence[RecordOrigin],
    batch_size: Optional[int],
) -> Iterator[tuple[Sequence[dict], Sequence[RecordOrigin]]]:
    """Split records into batches, keeping each record's origin alongside it.

    Args:
        records (Sequence[dict]): the records to write.
        origins (Sequence[RecordOrigin]): where each of them came from.
        batch_size (Optional[int]): the maximum size of each batch, or None for one batch.

    Yields:
        tuple: each batch's records, and their origins.
    """
    size = batch_size if batch_size else len(records) or 1

    for start in range(0, len(records), size):
        yield records[start : start + size], origins[start : start + size]  # noqa: E203


def _known_endpoints(node_records: list[NeontologyNodeRecord]) -> dict[tuple, set]:
    """Index what the content being imported will itself put in the graph.

    A relationship may point at a node defined inline, or in a file read after it, so an
    endpoint resolves against the content as well as against the graph. This matters most
    when validating, where nothing has been written yet.

    Args:
        node_records (list[NeontologyNodeRecord]): the combined node records.

    Returns:
        dict[tuple, set]: the values each label declares, by label and property.
    """
    known: dict[tuple, set] = defaultdict(set)

    for record in node_records:
        for prop, value in (record.output_record or {}).items():
            try:
                known[(record.label, prop)].add(value)

            except TypeError:
                # an unhashable property value cannot identify a node anyway
                continue

    return known


def _check_endpoints(
    input_records: list[NeontologyRelationshipRecord],
    node_records: list[NeontologyNodeRecord],
    error_on_unmatched: bool,
    context: ImportContext,
    report: ImportReport,
) -> None:
    """Check that every relationship endpoint identifies exactly one node.

    Both ends are checked: a relationship is merged by matching its source *and* its
    target, so an endpoint which does not resolve means no relationship is created at
    all - previously without anything being reported for the source.

    An endpoint resolves against the graph or against the content being imported, since
    the content brings its own nodes with it. One query is issued per label and property
    being looked up, rather than one per relationship.

    Args:
        input_records (list[NeontologyRelationshipRecord]): the relationship records.
        node_records (list[NeontologyNodeRecord]): the nodes the content itself defines.
        error_on_unmatched (bool): raise rather than warn where an endpoint does not resolve.
        context (ImportContext): the run they are part of.
        report (ImportReport): collects what did not resolve.
    """
    required: dict[tuple, dict[Any, list[RecordOrigin]]] = defaultdict(lambda: defaultdict(list))

    for record in input_records:
        with context.collector.catching(record.origin):
            source_class = node_class_for_label(record.source_label)

            required[(record.source_label, source_class.__primaryproperty__)][record.source].append(record.origin)
            required[(record.target_label, record.target_prop)][record.target].append(record.origin)

    known = _known_endpoints(node_records)

    gc = GraphConnection()

    for (label, prop), wanted in required.items():
        gql_identifier_adapter.validate_strings(label)
        gql_identifier_adapter.validate_strings(prop)

        cypher = f"""
            MATCH (n:{label})
            WHERE n.{prop} IN $values
            RETURN COLLECT(n.{prop})
            """

        found = Counter(gc.evaluate_query_single(cypher, {"values": list(wanted)}) or [])

        being_imported = known.get((label, prop), set())

        for value, origins in wanted.items():
            matched = found[value]

            if matched == 1 or (matched == 0 and value in being_imported):
                continue

            if matched == 0:
                message = (
                    f"No {label} node has {prop} = {value!r}, and the content being imported does not"
                    " define one, so relationships to or from it cannot be created. Ensure primary"
                    " properties are explicitly set or deterministic."
                )

            else:
                message = f"{matched} {label} nodes have {prop} = {value!r}, so it does not identify one node to relate."

            report.unresolved.append(f"{origins[0]}: {message}")

            if error_on_unmatched is True:
                context.collector.fail(origins[0], ImportContentError(message))

            else:
                logger.warning("%s: %s", origins[0], message)


def _import_relationships(
    input_records: list[NeontologyRelationshipRecord],
    context: ImportContext,
    report: ImportReport,
    batch_size: Optional[int] = None,
) -> None:
    """Merge relationship records into the graph.

    Args:
        input_records (list[NeontologyRelationshipRecord]): the relationship records to merge.
        context (ImportContext): the run they are part of.
        report (ImportReport): counts what was merged.
        batch_size (Optional[int]): how many records to merge per query, or None for all.
    """
    for (rel_type, target_prop, source_label, target_label), rel_entries in _group_relationships(input_records).items():
        rel_class = relationship_class_for_type(rel_type)

        source_type = node_class_for_label(source_label)
        target_type = node_class_for_label(target_label)

        records = [x.output_record for x in rel_entries]
        origins = [x.origin for x in rel_entries]

        def hydrate(record, rel_class=rel_class, source_type=source_type, target_type=target_type, target_prop=target_prop):
            return _hydrate_relationship(record, rel_class, source_type, target_type, target_prop)

        for batch, batch_origins in _batches_with_origins(records, origins, batch_size):
            try:
                rel_class.merge_records(
                    batch,
                    source_type=source_type,
                    target_type=target_type,
                    target_prop=target_prop,
                )

            except Exception as exc:
                _locate_failure(exc, batch, batch_origins, hydrate, context)

        report.relationships[rel_type] = report.relationships.get(rel_type, 0) + len(records)


def _group_relationships(
    input_records: list[NeontologyRelationshipRecord],
) -> dict[tuple, list[NeontologyRelationshipRecord]]:
    """Group relationship records by everything the merge query cannot vary per row.

    Args:
        input_records (list[NeontologyRelationshipRecord]): the records to group.

    Returns:
        dict[tuple, list]: the records, by relationship type, target property and labels.
    """
    grouped: dict[tuple, list[NeontologyRelationshipRecord]] = defaultdict(list)

    for record in input_records:
        grouped[
            (
                record.relationship_type,
                record.target_prop,
                record.source_label,
                record.target_label,
            )
        ].append(record)

    return grouped


def _hydrate_relationship(
    record: dict,
    rel_class: type,
    source_type: type,
    target_type: type,
    target_prop: Optional[str],
) -> Any:
    """Build one relationship's model the way merging it does.

    Used to find which record in a failed batch was at fault, so this has to construct
    the model exactly as `BaseRelationship.merge_records` does - the endpoints as
    partial nodes carrying only the property they are matched on.

    Args:
        record (dict): the relationship record.
        rel_class (type): the relationship class.
        source_type (type): the source node class.
        target_type (type): the target node class.
        target_prop (Optional[str]): the property the target is matched on.

    Returns:
        Any: the relationship.
    """
    hydrated = dict(record)

    source_prop = source_type.__primaryproperty__
    resolved_target_prop = target_prop or target_type.__primaryproperty__

    hydrated["source"] = source_type.model_construct(**{source_prop: record["source"]})
    hydrated["target"] = target_type.model_construct(**{resolved_target_prop: record["target"]})

    return rel_class(**hydrated)


def _validate_records(
    node_records: list[NeontologyNodeRecord],
    rel_records: list[NeontologyRelationshipRecord],
    context: ImportContext,
) -> None:
    """Check every record against the model it describes, without writing anything.

    Args:
        node_records (list[NeontologyNodeRecord]): the combined node records.
        rel_records (list[NeontologyRelationshipRecord]): the relationship records.
        context (ImportContext): the run they are part of.
    """
    for record in node_records:
        with context.collector.catching(record.origin):
            node_class_for_label(record.label)(**(record.output_record or {}))

    for rel_record in rel_records:
        with context.collector.catching(rel_record.origin):
            _hydrate_relationship(
                rel_record.output_record or {},
                relationship_class_for_type(rel_record.relationship_type),
                node_class_for_label(rel_record.source_label),
                node_class_for_label(rel_record.target_label),
                rel_record.target_prop,
            )


def _warn_legacy_endpoint_keys(context: ImportContext) -> None:
    """Warn once for the run where records spelled an endpoint in lowercase.

    Args:
        context (ImportContext): the run.
    """
    if not context.legacy_endpoint_keys:
        return

    count = len(context.legacy_endpoint_keys)

    warnings.warn(
        f"{count} relationship record{'s' if count != 1 else ''} used the lowercase 'source' or"
        f" 'target' key, first at {context.legacy_endpoint_keys[0]}. The content format spells"
        " these SOURCE and TARGET; the lowercase keys are deprecated and are removed in v4.",
        DeprecationWarning,
        stacklevel=3,
    )


def import_sourced_records(
    sourced: list[SourcedRecord],
    validate_only: bool = False,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    batch_size: Optional[int] = None,
    report: Optional[ImportReport] = None,
) -> ImportReport:
    """Import records which already know which file and entry they came from.

    This is what the file importers use, so anything reported about a record can name
    where it was written.

    Args:
        sourced (list[SourcedRecord]): the raw records, each with its origin.
        validate_only (bool): check the content without populating the graph.
        check_unmatched (bool): check relationship endpoints resolve.
        error_on_unmatched (bool): raise rather than warn where they do not.
        batch_size (Optional[int]): how many records to write per query.
        report (Optional[ImportReport]): a report to add to, if one is already started.

    Returns:
        ImportReport: what the import did.
    """
    report = report if report is not None else ImportReport()
    report.validated_only = validate_only

    context = ImportContext(collector=ErrorCollector(collect=validate_only))

    input_nodes, input_rels = prepare_sourced_records(sourced, context)

    combined_nodes = _combine_node_records(input_nodes, context)

    _warn_legacy_endpoint_keys(context)

    if validate_only is True:
        _validate_records(combined_nodes, input_rels, context)

        if check_unmatched is True:
            _check_endpoints(input_rels, combined_nodes, error_on_unmatched, context, report)

        for record in combined_nodes:
            report.nodes[record.label] = report.nodes.get(record.label, 0) + 1

        for rel_record in input_rels:
            rel_type = rel_record.relationship_type or "?"
            report.relationships[rel_type] = report.relationships.get(rel_type, 0) + 1

        context.collector.raise_if_any()

        return report

    _import_nodes(combined_nodes, context, report, batch_size)

    if check_unmatched is True:
        _check_endpoints(input_rels, combined_nodes, error_on_unmatched, context, report)

    _import_relationships(input_rels, context, report, batch_size)

    return report


def import_records(
    records: list,
    validate_only: bool = False,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    batch_size: Optional[int] = None,
) -> ImportReport:
    """Import 'Neontology' records into the graph database.

    Every node is written before any relationship is, so a relationship may refer to a
    node described anywhere in the input, whatever order the records are given in.

    Args:
        records (list): a list of dictionaries representing nodes and relationships.
        validate_only (bool, optional): check the content against your models and report
            every problem found, without populating the graph. Defaults to False.
        check_unmatched (bool, optional): Check relationship endpoints exist, warn if not.
            Defaults to True.
        error_on_unmatched (bool, optional): When checking relationship endpoints,
            raise an error if the node is neither in the graph nor in the content.
            Defaults to False.
        batch_size (int, optional): how many records to write per query. Defaults to
            None, which writes each label and relationship type in one query. It limits
            the size of each write, not what a relationship can refer to.

    Returns:
        ImportReport: what the import did - how many of each label and relationship type
        were merged, and any endpoints which did not resolve.

    Raises:
        ImportValidationError: with `validate_only`, listing every problem found.
        ImportContentError: otherwise, for the first problem found.
    """
    fresh_records = deepcopy(records)

    origin = RecordOrigin(source="<records>")

    sourced: list[SourcedRecord] = []

    for position, entry in enumerate(fresh_records):
        # the usual call passes one piece of input holding all the records, and its
        # position says nothing worth reporting - the records' own positions do
        entry_origin = origin if len(fresh_records) == 1 else origin.at(position)

        sourced += iter_raw_records(entry, entry_origin)

    return import_sourced_records(
        sourced,
        validate_only=validate_only,
        check_unmatched=check_unmatched,
        error_on_unmatched=error_on_unmatched,
        batch_size=batch_size,
    )
