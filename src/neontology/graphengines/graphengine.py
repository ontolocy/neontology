from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar

from dotenv import load_dotenv
from pydantic import BaseModel, model_validator

from ..gql import gql_identifier_adapter, int_adapter
from ..result import NeontologyResult

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship

BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")
BaseRelationshipT = TypeVar("BaseRelationshipT", bound="BaseRelationship")


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

    def __init__(self, config: Optional["GraphEngineConfig"]) -> None:
        """Initialise the graph engine.

        Args:
            config (Optional[dict]): GraphEngine configuration
        """
        pass

    @classmethod
    def _export_type_converter(cls, value: Any) -> Any:
        """Convert a value to a type supported by the graph engine.

        This method is used to ensure that values are converted to types that the graph engine can handle.

        Args:
            value (Any): The value to convert.

        Returns:
            Any: The converted value, or the original value if it is already of a supported type.

        Raises:
            TypeError: If the value is a dict, or if it is a list with mixed types.
        """
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
                    raise TypeError("For neo4j, all items in a list must be of the same type.")

            return [cls._export_type_converter(x) for x in value]

        elif value is None:
            return None

        elif isinstance(value, cls._supported_types) is False:
            return str(value)

        else:
            return value

    @classmethod
    def export_dict_converter(cls, original_dict: dict[str, Any]) -> dict[str, Any]:
        """Convert types in a dictionary to those supported by the graph engine.

        Args:
            original_dict (dict[str, Any]): The original dictionary to convert.

        Returns:
            dict[str, Any]: A new dictionary with values converted to types supported by the graph engine.
        """
        export_dict = original_dict.copy()

        for k, v in export_dict.items():
            export_dict[k] = cls._export_type_converter(v)

        return export_dict

    def verify_connection(self) -> bool:
        """Verify the connection to the graph engine.

        Returns:
            bool: True if the connection is valid, False otherwise.
        """
        raise NotImplementedError

    def close_connection(self) -> None:
        """Close the connection to the graph engine."""
        raise NotImplementedError

    def evaluate_query(
        self,
        cypher: str,
        params: dict[str, Any] = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        """Evaluate a Cypher query against the database.

        Args:
            cypher (str): The Cypher query to execute.
            params (dict[str, Any], optional): Parameters for the Cypher query. Defaults to {}.
            node_classes (dict, optional): Mapping of node labels to their classes. Defaults to {}.
            relationship_classes (dict, optional): Mapping of relationship types to their classes. Defaults to {}.

        Returns:
            NeontologyResult: The result of the query execution, containing nodes and relationships.
        """
        raise NotImplementedError

    def evaluate_query_single(self, cypher: str, params: dict[str, Any]) -> Any:
        """Evaluate a query which returns a single result.

        Args:
            cypher (str): The Cypher query to execute.
            params (dict[str, Any]): Parameters for the Cypher query.

        Returns:
            Any: The result of the query execution.
        """
        raise NotImplementedError

    def apply_constraint(self, label: str, property: str) -> None:
        """Apply a constraint to a label and property in the database.

        Args:
            label (str): The label to apply the constraint to.
            property (str): The property to apply the constraint on.
        """
        raise NotImplementedError

    def drop_constraint(self, constraint_name: str) -> None:
        """Drop a constraint from the database.

        Args:
            constraint_name (str): The name of the constraint to drop.
        """
        raise NotImplementedError

    def get_constraints(self) -> list:
        """Get a list of constraints in the database.

        Returns:
            list: A list of constraints in the database.
        """
        raise NotImplementedError

    def create_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Create nodes with specified labels and properties.

        Args:
            labels (list): a list of labels to give created nodes
            pp_key (str): the primary property for the nodes
            properties (list): A list of dictionaries representing each node to be created.
                two keys with associated values pp (the value to assign the primary property)
                and props (dict with key value pairs for all other properties).
            node_class (type[BaseNodeT]): the type of nodes to create

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

    def merge_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Merge nodes with specified labels and property.

        Args:
            labels (list): _description_
            pp_key (str): _description_
            properties (list): A list of dictionaries representing each node to be created.
                four keys with associated values: pp (the value to assign the primary property)
                set_on_match, set_on_create and always_set (dicts with key value pairs for all other properties).
            node_class (type["BaseNode"]): The class of the nodes to be merged.

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

    def delete_nodes(self, label: str, pp_key: str, pp_values: list[Any]) -> None:
        """Delete nodes with a specific label and primary property value.

        Args:
            label (str): The label of the nodes to delete.
            pp_key (str): The primary property key to match on.
            pp_values (list[Any]): A list of primary property values to match on for deletion.
        """
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
        merge_on_props: list[str],
        rel_props: list[dict],
    ) -> None:
        """Merge relationships between nodes in the database.

        Args:
            source_label (str): The label of the source node.
            target_label (str): The label of the target node.
            source_prop (str): The property of the source node to match on.
            target_prop (str): The property of the target node to match on.
            rel_type (str): The type of relationship to create or merge.
            merge_on_props (list[str]): A list of properties to merge on.
            rel_props (list[dict]): A list of dictionaries representing each relationship to be merged.
                Each dictionary should contain keys for `source_prop`, `target_prop`, and any additional properties
                to set on the relationship.
        """
        # build a string of properties to merge on "prop_name: $prop_name"
        merge_props = ", ".join([f"{gql_identifier_adapter.validate_strings(x)}: rel.{x}" for x in merge_on_props])

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

    def _filters_to_where_clause(self, filters: Optional[dict] = None) -> tuple[Optional[str], dict]:
        """Convert a dictionary of filters into a WHERE clause and parameter dictionary for a query.

        Args:
            filters (dict | None): A dictionary of filters. Each key is a field name possibly followed
                                by '__' and a lookup type (e.g., 'exact', 'contains'). The value is
                                the filter value. If None, returns an empty WHERE clause.

        Returns:
            tuple: A tuple containing the WHERE clause string and a dictionary of parameters.
        """
        params = {}
        where_clauses = []
        where_clause = None
        if filters:
            for key, value in filters.items():
                if "__" in key:
                    field_name, lookup_type = key.split("__")
                else:
                    field_name, lookup_type = key, "exact"
                param_name = f"filter_{field_name}_{lookup_type}"
                params[param_name] = value
                if lookup_type == "exact":
                    clause = f"n.{field_name} = ${param_name}"
                elif lookup_type == "iexact":
                    clause = f"toLower(n.{field_name}) = toLower(${param_name})"
                elif lookup_type == "contains":
                    clause = f"n.{field_name} CONTAINS ${param_name}"
                elif lookup_type == "icontains":
                    clause = f"toLower(n.{field_name}) CONTAINS toLower(${param_name})"
                elif lookup_type == "startswith":
                    clause = f"n.{field_name} STARTS WITH ${param_name}"
                elif lookup_type == "istartswith":
                    clause = f"toLower(n.{field_name}) STARTS WITH toLower(${param_name})"
                elif lookup_type in ("gt", "lt", "gte", "lte"):
                    operator = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}[lookup_type]

                    clause = f"n.{field_name} {operator} ${param_name}"
                elif lookup_type == "in":
                    clause = f"n.{field_name} IN ${param_name}"
                elif lookup_type == "isnull":
                    clause = f"n.{field_name} IS NULL" if value else f"n.{field_name} IS NOT NULL"

                else:
                    raise ValueError(f"Invalid filter: {lookup_type}")

                where_clauses.append(clause)
            where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        return where_clause, params

    def match_nodes(
        self,
        node_class: type,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        filters: Optional[dict] = None,
    ) -> list:
        """Match nodes based on the given node class, limit, skip, and filters.

        Args:
            node_class (type): The class of the nodes to match.
            limit (int | None): The maximum number of nodes to return. If None, all matching nodes are returned.
            skip (int | None): The number of nodes to skip before collecting the result set. If None, no nodes are skipped.
            filters (dict | None): A dictionary of filters to apply. If None, no filters are applied.

        Returns:
            list: A list of nodes that match the given criteria.
        """
        cypher = f"MATCH (n:{node_class.__primarylabel__})"
        where_clause, params = self._filters_to_where_clause(filters)
        if where_clause:
            cypher += where_clause
        cypher += " RETURN n"
        if skip is not None:
            cypher += " SKIP $skip"
            params["skip"] = skip
        if limit is not None:
            cypher += " LIMIT $limit"
            params["limit"] = limit

        result = self.evaluate_query(cypher, params, node_classes={node_class.__primarylabel__: node_class})

        return result.nodes

    def get_count(
        self,
        node_class: type,
        filters: Optional[dict] = None,
    ) -> int:
        """Get the count of nodes based on the given node class and filters.

        Args:
            node_class (type): The class of the nodes to count.
            filters (dict | None): A dictionary of filters to apply. If None, no filters are applied.

        Returns:
            int: The count of nodes that match the given criteria.
        """
        cypher = f"MATCH (n:{node_class.__primarylabel__})"
        where_clause, params = self._filters_to_where_clause(filters)
        if where_clause:
            cypher += where_clause
        cypher += " RETURN COUNT(DISTINCT n)"
        return self.evaluate_query_single(cypher, params)

    def match_relationships(
        self,
        relationship_class: type[BaseRelationshipT],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list[BaseRelationshipT]:
        """Get relationships of this type from the database.

        Run a MATCH cypher query to retrieve any Relationships of this type.

        Args:
            relationship_class (type["BaseRelationshipT"]): the type of relationship to match on
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            list["BaseRelationshipT"]: A list of relationships.
        """
        from ..utils import get_node_types, get_rels_by_type

        cypher = f"""
        MATCH (n)-[r:{relationship_class.__relationshiptype__}]->(o)
        RETURN n, r, o
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
    """Base class for Graph Engine configuration."""

    engine: ClassVar[type[GraphEngineBase]]

    env_fields: ClassVar[dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def populate_defaults(cls, data: Any) -> Any:
        """Populate configuration with environment variables.

        Where no values are provided, attempt to load them from environment variables.
        """
        load_dotenv()

        for field, env_var in cls.env_fields.items():
            if not data.get(field):
                value = os.getenv(env_var)
                if value is None:
                    raise ValueError(f"No value provided for {field} field and no {env_var} environment variable set.")
                data[field] = value
        return data
