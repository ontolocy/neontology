from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel

from ..gql import gql_identifier_adapter, int_adapter
from ..result import NeontologyResult

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship


class GraphEngineBase:
    _supported_types: ClassVar[Any] = (
        list,
        bool,
        int,
        bytearray,
        float,
        str,
        bytes,
        date,
        time,
        datetime,
        timedelta,
    )

    def __init__(self, config: Optional[dict]) -> None:
        """Initialise connection to the engine

        Args:
            config (Optional[dict]): _description_
        """
        pass

    @classmethod
    def _export_type_converter(cls, value: Any) -> Any:
        if isinstance(value, dict):
            raise TypeError("Neontology doesn't support dict types for properties.")

        elif isinstance(value, (tuple, set)):
            new_value = list(value)
            return cls._export_type_converter(new_value)

        elif isinstance(value, list):
            if len(value) == 0:
                return []
            # items in a list must all be the same type
            item_type = type(value[0])
            for item in value:
                if isinstance(item, item_type) is False:
                    raise TypeError(
                        "For neo4j, all items in a list must be of the same type."
                    )

            return [cls._export_type_converter(x) for x in value]

        elif isinstance(value, cls._supported_types) is False:
            return str(value)

        else:
            return value

    @classmethod
    def export_dict_converter(cls, original_dict: Dict[str, Any]) -> Dict[str, Any]:
        """_summary_

        Args:
            export_dict (Dict[str, Any]): _description_

        Returns:
            Dict[str, Any]: _description_
        """

        export_dict = original_dict.copy()

        for k, v in export_dict.items():
            export_dict[k] = cls._export_type_converter(v)

        return export_dict

    def verify_connection(self) -> bool:
        raise NotImplementedError

    def close_connection(self) -> None:
        raise NotImplementedError

    def evaluate_query(
        self,
        cypher: str,
        params: Dict[str, Any] = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        raise NotImplementedError

    def evaluate_query_single(self, cypher: str, params: Dict[str, Any]) -> Any:
        raise NotImplementedError

    def apply_constraint(self, label: str, property: str) -> None:
        raise NotImplementedError

    def drop_constraint(self, constraint_name: str) -> None:
        raise NotImplementedError

    def get_constraints(self) -> list:
        raise NotImplementedError

    def create_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> List["BaseNode"]:
        """
        Args:
            labels (list): a list of labels to give created nodes
            pp_key (str): the primary property for the nodes
            properties (list): A list of dictionaries representing each node to be created.
                two keys with associated values pp (the value to assign the primary property)
                and props (dict with key value pairs for all other properties).

        Returns:
            list: list of created Nodes
        """

        label_identifiers = [gql_identifier_adapter.validate_strings(x) for x in labels]

        cypher = f"""
        UNWIND $node_list AS node
        create (n:{":".join(label_identifiers)} {{{gql_identifier_adapter.validate_strings(pp_key)}: node.pp}})
        SET n += node.props
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def merge_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> List["BaseNode"]:
        """_summary_

        Args:
            labels (list): _description_
            pp_key (str): _description_
            properties (list): A list of dictionaries representing each node to be created.
                four keys with associated values: pp (the value to assign the primary property)
                set_on_match, set_on_create and always_set (dicts with key value pairs for all other properties).

        Returns:
            list: list of merged Nodes
        """

        label_identifiers = [gql_identifier_adapter.validate_strings(x) for x in labels]

        cypher = f"""
        UNWIND $node_list AS node
        MERGE (n:{":".join(label_identifiers)} {{{gql_identifier_adapter.validate_strings(pp_key)}: node.pp}})
        ON MATCH SET n += node.set_on_match
        ON CREATE SET n += node.set_on_create
        SET n += node.always_set
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def delete_nodes(self, label: str, pp_key: str, pp_values: List[Any]) -> None:
        cypher = f"""
        UNWIND $pp_values AS pp
        MATCH (n:{gql_identifier_adapter.validate_strings(label)})
        WHERE n.{gql_identifier_adapter.validate_strings(pp_key)} = pp
        DETACH DELETE n
        """

        params = {"pp_values": pp_values}

        self.evaluate_query_single(cypher, params)

    def merge_relationships(
        self,
        source_label: str,
        target_label: str,
        source_prop: str,
        target_prop: str,
        rel_type: str,
        merge_on_props: List[str],
        rel_props: List[dict],
    ) -> None:
        # build a string of properties to merge on "prop_name: $prop_name"
        merge_props = ", ".join(
            [
                f"{gql_identifier_adapter.validate_strings(x)}: rel.{x}"
                for x in merge_on_props
            ]
        )

        cypher = f"""
        UNWIND $rel_list AS rel
        MATCH (source:{gql_identifier_adapter.validate_strings(source_label)})
        WHERE source.{gql_identifier_adapter.validate_strings(source_prop)} = rel.source_prop
        MATCH (target:{gql_identifier_adapter.validate_strings(target_label)})
        WHERE target.{gql_identifier_adapter.validate_strings(target_prop)} = rel.target_prop
        MERGE (source)-[r:{gql_identifier_adapter.validate_strings(rel_type)} {{ {merge_props} }}]->(target)
        ON MATCH SET r += rel.set_on_match
        ON CREATE SET r += rel.set_on_create
        SET r += rel.always_set
        """

        params = {"rel_list": rel_props}

        self.evaluate_query_single(cypher, params)

    def match_nodes(
        self,
        node_class: type["BaseNode"],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> List["BaseNode"]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[List[B]]: A list of node instances.
        """

        cypher = f"""
        MATCH(n:{node_class.__primarylabel__})
        RETURN n
        """

        params = {}

        if skip:
            cypher += " SKIP $skip "
            params["skip"] = int_adapter.validate_python(skip)

        if limit:
            cypher += " LIMIT $limit "
            params["limit"] = int_adapter.validate_python(limit)

        result = self.evaluate_query(
            cypher, params, node_classes={node_class.__primarylabel__: node_class}
        )

        return result.nodes

    def match_relationships(
        self,
        relationship_class: type["BaseRelationship"],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> List["BaseRelationship"]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[List[B]]: A list of node instances.
        """

        from ..utils import get_node_types, get_rels_by_type

        cypher = f"""
        MATCH (n)-[r:{relationship_class.__relationshiptype__}]->(o)
        RETURN DISTINCT n, r, o
        """

        params = {}

        if skip:
            cypher += " SKIP $skip "
            params["skip"] = int_adapter.validate_python(skip)

        if limit:
            cypher += " LIMIT $limit "
            params["limit"] = int_adapter.validate_python(limit)

        rel_types = get_rels_by_type()
        node_classes = get_node_types()

        result = self.evaluate_query(
            cypher,
            params,
            node_classes=node_classes,
            relationship_classes=rel_types,
        )

        return result.relationships


class GraphEngineConfig(BaseModel):
    engine: ClassVar[type[GraphEngineBase]]
