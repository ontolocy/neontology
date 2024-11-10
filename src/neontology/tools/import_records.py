import logging
from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, model_validator

from ..gql import gql_identifier_adapter
from ..graphconnection import GraphConnection
from ..utils import get_node_types, get_rels_by_type

logger = logging.getLogger(__name__)


def nested_dict(n, type):
    if n == 1:
        return defaultdict(type)
    else:
        return defaultdict(lambda: nested_dict(n - 1, type))


class NeontologyNodeRaw(BaseModel):
    LABEL: str


class NeontologyRelationshipRaw(BaseModel):
    RELATIONSHIP_TYPE: str
    TARGET_LABEL: str
    TARGET_NODES: Optional[List[NeontologyNodeRaw]] = None
    TARGETS: Optional[List[str]] = None
    TARGET_PROPERTY: Optional[str] = None


class NeontologyNodeRecord(BaseModel):
    input_record: NeontologyNodeRaw
    label: str
    output_record: dict

    @model_validator(mode="before")
    def populate_node_fields(cls, data):
        if not data.get("label"):
            data["label"] = data.get("input_record", {}).get("LABEL")

        if not data.get("output_record"):
            data["output_record"] = {
                k: v
                for k, v in data.get("input_record", {}).items()
                if k not in ["LABEL"]
            }

        return data


class NeontologyRelationshipRecord(BaseModel):
    input_record: NeontologyRelationshipRaw
    relationship_type: str
    target_prop: Optional[str] = None
    source_label: str
    target_label: str
    source: Any
    target: Any
    relationship_properties: dict
    output_record: dict

    @model_validator(mode="before")
    def populate_rel_fields(cls, data):
        if not data.get("RELATIONSHIP_TYPE"):
            data["relationship_type"] = data.get("input_record", {}).get(
                "RELATIONSHIP_TYPE"
            )

        if not data.get("source_label"):
            data["source_label"] = data.get("input_record", {}).get("SOURCE_LABEL")

        if not data.get("target_label"):
            data["target_label"] = data.get("input_record", {}).get("TARGET_LABEL")

        if not data.get("target_prop"):
            data["target_prop"] = data.get("input_record", {}).get("TARGET_PROPERTY")

        if not data.get("source"):
            data["source"] = data.get("input_record", {}).get("source")

        if not data.get("target"):
            data["target"] = data.get("input_record", {}).get("target")

        if not data.get("relationship_properties"):
            data["relationship_properties"] = {
                k: v
                for k, v in data.get("input_record", {}).items()
                if k
                not in [
                    "SOURCE_LABEL",
                    "TARGET_LABEL",
                    "TARGET_PROPERTY",
                    "RELATIONSHIP_TYPE",
                    "TARGET_NODES",
                    "TARGETS",
                    "source",
                    "target",
                ]
            }

        if not data.get("output_record"):
            data["output_record"] = {
                k: v
                for k, v in data.get("input_record", {}).items()
                if k
                not in [
                    "SOURCE_LABEL",
                    "TARGET_LABEL",
                    "TARGET_PROPERTY",
                    "RELATIONSHIP_TYPE",
                    "TARGET_NODES",
                    "TARGETS",
                ]
            }

        return data

    @model_validator(mode="after")
    def populate_fields(self):
        node_types = get_node_types()
        self.target_prop = node_types[self.target_label].__primaryproperty__

        return self


def _import_nodes(input_records: List[NeontologyNodeRecord]):
    mapped_records = defaultdict(list)
    node_types = get_node_types()

    for record in input_records:
        mapped_records[record.label].append(record.output_record)

    for label, node_records in mapped_records.items():
        node_class = node_types[label]
        node_class.merge_records(node_records)


def _import_relationships(
    input_records: List[NeontologyRelationshipRecord],
    check_unmatched: bool,
    error_on_unmatched: bool,
):
    # if we're meant to warn on unmatched, we need to look up source and target

    mapped_records = nested_dict(4, list)
    rel_types = get_rels_by_type()
    node_types = get_node_types()

    for record in input_records:
        rel_type = record.relationship_type

        # when we merge relationship records, we pass in target prop and source prop
        # we also need to hydrate based on the given source and target labels
        # therefore we need to group together records which share those properties
        mapped_records[rel_type][record.target_prop][record.source_label][
            record.target_label
        ].append(record)

    for rel_type, rel_records_by_prop in mapped_records.items():
        for target_prop, rel_records_by_source_label in rel_records_by_prop.items():
            for (
                source_label,
                rel_records_by_target_label,
            ) in rel_records_by_source_label.items():
                for target_label, rel_entries in rel_records_by_target_label.items():
                    if check_unmatched is True:
                        for rel_entry in rel_entries:
                            gc = GraphConnection()

                            gql_identifier_adapter.validate_strings(target_label)
                            gql_identifier_adapter.validate_strings(target_prop)

                            cypher = f"""
                                MATCH (n:{target_label})
                                WHERE n.{target_prop} = $val
                                RETURN n
                                """

                            params = {"val": rel_entry.target}

                            result = gc.evaluate_query(cypher, params)

                            if len(result.nodes) > 1:
                                message = (
                                    f"Matched {len(result.nodes)} on {rel_type} for {target_label}"
                                    f" WHERE {target_prop} = {rel_entry.target}"
                                )

                                if error_on_unmatched is True:
                                    raise ValueError(message)

                                logger.warning(message)

                            elif len(result.nodes) == 0:
                                error_msg = f"No target node for {rel_type} to {rel_entry.target}"

                                if error_on_unmatched is True:
                                    raise ValueError(error_msg)

                                logger.warning(error_msg)

                    source_type = node_types[source_label]
                    target_type = node_types[target_label]

                    rel_class = rel_types[rel_type].relationship_class

                    output_records = [x.output_record for x in rel_entries]

                    rel_class.merge_records(
                        output_records,
                        source_type=source_type,
                        target_type=target_type,
                        target_prop=target_prop,
                    )


def _process_sub_records(
    source_node_record: NeontologyNodeRecord, subrecords
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    node_types = get_node_types()

    output_nodes = []
    output_rels = []

    # hydrate the source node to get the pp
    # then dump it

    source_label = source_node_record.label
    source_class = node_types[source_label]
    source_node = source_class(**source_node_record.output_record)
    source_node_pk = source_node.get_pp()

    # iterate through the subrecords

    for record in subrecords:
        target_prop = record.get("TARGET_PROPERTY")
        target_class = node_types[record["TARGET_LABEL"]]

        rel_targets = []

        # handle new nodes that need to be created
        # we need to hydrate these to get the pp
        # then dump them
        if "TARGET_NODES" in record:
            for entry in record["TARGET_NODES"]:
                target_node_record = NeontologyNodeRecord(input_record=entry)
                tgt_node = target_class(**target_node_record.output_record)
                rel_targets.append(tgt_node.get_pp())
                output_nodes.append(target_node_record)

        if "TARGETS" in record:
            rel_targets += record["TARGETS"]

        # now do the relationships
        # convert to neontology style
        rel_dict = record
        rel_dict["source"] = source_node_pk
        rel_dict["SOURCE_LABEL"] = source_label

        if target_prop:
            rel_dict["TARGET_PROPERTY"] = target_prop

        output_raw_records = [{**rel_dict, **{"target": x}} for x in rel_targets]

        output_rels += [
            NeontologyRelationshipRecord(input_record=x) for x in output_raw_records
        ]

    # returns a set of records to be used as input to import_records
    return output_nodes, output_rels


def _prepare_records(
    input_records: Union[List[Dict[str, Any]], Dict[str, Any]],
) -> tuple:
    input_records = input_records.copy()

    if isinstance(input_records, dict):
        # handle the situation where we've just been passed a single record
        if "nodes" not in input_records and "edges" not in input_records:
            raw_records = [input_records]

        # handle the situation where we've got 'link data'
        # which consists of node records and link/relationship records
        else:
            raw_records = input_records.get("nodes", [])
            raw_records += input_records.get("edges", [])

    else:
        raw_records = input_records.copy()

    input_nodes = []
    input_relationships = []

    for record in raw_records:
        if "LABEL" in record.keys():
            # pull out relationships
            # append the input_node
            # now parse the relationships
            rel_records = record.pop("RELATIONSHIPS_OUT", None)

            node_record = NeontologyNodeRecord(input_record=record)

            input_nodes.append(node_record)

            if rel_records:
                logger.warning(
                    (
                        "Importing relationships which are sub-records to a Node. "
                        "Note that this requires associated nodes to have explicit or deterministic primary keys."
                    )
                )

                new_nodes, new_rels = _process_sub_records(node_record, rel_records)
                input_nodes += new_nodes
                input_relationships += new_rels

        elif "RELATIONSHIP_TYPE" in record.keys():
            input_relationships.append(
                NeontologyRelationshipRecord(input_record=record)
            )

        else:
            raise ValueError(
                (
                    "Input record does not have LABEL (for node) "
                    "or RELATIONSHIP_TYPE (for relationship). %s",
                    record,
                )
            )

    return input_nodes, input_relationships


def import_records(
    records: list,
    validate_only: bool = False,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
) -> None:
    input_nodes = []
    input_rels = []

    fresh_records = deepcopy(records)

    for entry in fresh_records:
        prepared_nodes, prepared_rels = _prepare_records(entry)

        input_nodes += prepared_nodes
        input_rels += prepared_rels

    if validate_only is False:
        _import_nodes(input_nodes)
        _import_relationships(input_rels, check_unmatched, error_on_unmatched)
