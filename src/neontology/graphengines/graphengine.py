from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Optional

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
        elif value is None:
            return None
        elif isinstance(value, cls._supported_types) is False:
            return str(value)

        else:
            return value

    @classmethod
    def export_dict_converter(cls, original_dict: dict[str, Any]) -> dict[str, Any]:
        """_summary_

        Args:
            export_dict (dict[str, Any]): _description_

        Returns:
            dict[str, Any]: _description_
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
        params: dict[str, Any] = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        raise NotImplementedError

    def evaluate_query_single(self, cypher: str, params: dict[str, Any]) -> Any:
        raise NotImplementedError

    def apply_constraint(self, label: str, property: str) -> None:
        raise NotImplementedError

    def drop_constraint(self, constraint_name: str) -> None:
        raise NotImplementedError

    def get_constraints(self) -> list:
        raise NotImplementedError

    def create_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> list["BaseNode"]:
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

        element_id_prop_name = getattr(node_class, "__elementidproperty__", None)
        if node_class.__primaryproperty__ == element_id_prop_name:
            pp_cypher = ""
        else:
            pp_cypher = (
                f"{{{gql_identifier_adapter.validate_strings(pp_key)}: node.pp}}"
            )

        cypher = f"""
        UNWIND $node_list AS node
        create (n:{":".join(label_identifiers)} {pp_cypher})
        SET n += node.props
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def merge_nodes(
        self,
        labels: list[str],
        pp_key: str,
        properties: list[dict],
        node_class: type["BaseNode"],
    ) -> list["BaseNode"]:
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

        element_id_prop_name: str | None = getattr(
            node_class, "__elementidproperty__", None
        )
        if node_class.__primaryproperty__ == element_id_prop_name:
            return self._merge_element_id_nodes(
                label_identifiers, pp_key, properties, node_class, element_id_prop_name
            )

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

    def _merge_element_id_nodes(
        self,
        label_identifiers: list[str],
        pp_key: str,
        properties: list[dict],
        node_class: type["BaseNode"],
        element_id_prop_name: str,
    ) -> list["BaseNode"]:
        """Manually merge element ID nodes
        Args:
            labels (list): _description_
            pp_key (str): _description_
            properties (list): A list of dictionaries representing each node to be created.
                four keys with associated values: pp (the value to assign the primary property)
                set_on_match, set_on_create and always_set (dicts with key value pairs for all other properties).
            node_class (type[BaseNode]): class of nodes being merged
            element_id_prop_name (str): property of class used as an element id

        Returns:
            list: list of merged Nodes
        """
        result_list = []
        for node_prop in properties:
            match = self.match_node(node_prop["pp"], node_class)
            if not match:
                create_props = node_prop["always_set"] | node_prop["set_on_create"]
                node_details = [{"pp": node_prop["pp"], "props": create_props}]
                result_list.extend(
                    self.create_nodes(
                        label_identifiers, pp_key, node_details, node_class
                    )
                )
            else:
                cypher = f"""
                MATCH (n:{":".join(label_identifiers)})
                WHERE {self._where_elementId_cypher()}
                SET n += $set_on_match
                SET n += $always_set
                RETURN n
                """
                results = self.evaluate_query(
                    cypher, node_prop, {node_class.__primarylabel__: node_class}
                )
                result_list.extend(results.nodes)
        return result_list

    @staticmethod
    def _where_elementId_cypher() -> str:
        return "elementId(n) = $pp"

    def delete_nodes(self, label: str, pp_key: str, pp_values: list[Any]) -> None:
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
        rel_class: type["BaseRelationship"],
    ) -> NeontologyResult:
        # build a string of properties to merge on "prop_name: $prop_name"
        merge_props = ", ".join(
            [
                f"{gql_identifier_adapter.validate_strings(x)}: rel.{x}"
                for x in merge_on_props
            ]
        )

        if rel_props[0]["source_element_id_prop"] == source_prop:
            source_match_cypher = """elementId(source)"""
        else:
            source_match_cypher = (
                f"""source.{gql_identifier_adapter.validate_strings(source_prop)}"""
            )
        if rel_props[0]["target_element_id_prop"] == target_prop:
            target_match_cypher = """elementId(target)"""
        else:
            target_match_cypher = (
                f"""target.{gql_identifier_adapter.validate_strings(target_prop)}"""
            )
        cypher = f"""
        UNWIND $rel_list AS rel
        MATCH (source:{gql_identifier_adapter.validate_strings(source_label)})
        WHERE {source_match_cypher} = rel.source_prop
        MATCH (target:{gql_identifier_adapter.validate_strings(target_label)})
        WHERE {target_match_cypher} = rel.target_prop
        MERGE (source)-[r:{gql_identifier_adapter.validate_strings(rel_type)} {{ {merge_props} }}]->(target)
        ON MATCH SET r += rel.set_on_match
        ON CREATE SET r += rel.set_on_create
        SET r += rel.always_set
        RETURN r, source, target
        """

        params = {"rel_list": rel_props}

        from ..utils import get_node_types, get_rels_by_type

        rel_types = get_rels_by_type(rel_class)
        node_classes = get_node_types(rel_class.model_fields["source"].annotation)
        if (
            rel_class.model_fields["source"].annotation
            != rel_class.model_fields["target"].annotation
        ):
            node_classes.update(
                get_node_types(rel_class.model_fields["target"].annotation)
            )

        return self.evaluate_query(
            cypher, params, node_classes=node_classes, relationship_classes=rel_types
        )

    def match_node(self, pp: str, node_class: type[BaseNode]) -> Optional[BaseNode]:
        """MATCH a single node of this type with the given primary property.

        Args:
            pp (str): The value of the primary property (pp) to match on.
            node_class (type[BaseNode]): Class of the node to match

        Returns:
            Optional[B]: If the node exists, return it as an instance.
        """
        element_id_prop_name = getattr(node_class, "__elementidproperty__", None)
        if node_class.__primaryproperty__ == element_id_prop_name:
            match_cypher = "elementId(n)"
        else:
            match_cypher = f"n.{node_class.__primaryproperty__}"

        cypher = f"""
        MATCH (n:{node_class.__primarylabel__})
        WHERE {match_cypher} = $pp
        RETURN n
        """

        params = {"pp": pp}

        result = self.evaluate_query(
            cypher, params, node_classes={node_class.__primarylabel__: node_class}
        )

        if result.nodes:
            return result.nodes[0]

        else:
            return None

    def match_nodes(
        self,
        node_class: type["BaseNode"],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list["BaseNode"]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[list[B]]: A list of node instances.
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
    ) -> list["BaseRelationship"]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[list[B]]: A list of node instances.
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

        rel_types = get_rels_by_type(relationship_class)
        node_classes = get_node_types(
            relationship_class.model_fields["source"].annotation
        )
        if (
            relationship_class.model_fields["source"].annotation
            != relationship_class.model_fields["target"].annotation
        ):
            node_classes.update(
                get_node_types(relationship_class.model_fields["target"].annotation)
            )

        result = self.evaluate_query(
            cypher,
            params,
            node_classes=node_classes,
            relationship_classes=rel_types,
        )

        return result.relationships


class GraphEngineConfig(BaseModel):
    engine: ClassVar[type[GraphEngineBase]]
