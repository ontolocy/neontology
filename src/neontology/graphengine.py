from typing import Any, Optional

from .result import NeontologyResult


class GraphEngineBase:
    def __init__(self, config: Optional[dict]) -> None:
        """Initialise connection to the engine

        Args:
            config (Optional[dict]): _description_
        """
        pass

    def verify_connection(self) -> bool:
        raise NotImplementedError

    def close_connection(self) -> None:
        raise NotImplementedError

    def evaluate_query(
        self, cypher, params={}, node_classes={}, relationship_classes={}
    ) -> NeontologyResult:
        raise NotImplementedError

    def evaluate_query_single(self, cypher, params) -> Any:
        raise NotImplementedError

    def apply_constraint(self, label: str, property: str) -> None:
        raise NotImplementedError

    def drop_constraint(self, constraint_name: str) -> None:
        raise NotImplementedError

    def get_constraints(self) -> list:
        raise NotImplementedError

    def create_nodes(
        self, labels: list, pp_key: str, properties: list, node_class
    ) -> list:
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

        cypher = f"""
        UNWIND $node_list AS node
        create (n:{":".join(labels)} {{{pp_key}: node.pp}})
        SET n += node.props
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def merge_nodes(
        self, labels: list, pp_key: str, properties: list, node_class
    ) -> list:
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

        cypher = f"""
        UNWIND $node_list AS node
        MERGE (n:{":".join(labels)} {{{pp_key}: node.pp}})
        ON MATCH SET n += node.set_on_match
        ON CREATE SET n += node.set_on_create
        SET n += node.always_set
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def delete_nodes(self, label, pp_key, pp_values) -> None:
        cypher = f"""
        UNWIND $pp_values AS pp
        MATCH (n:{label})
        WHERE n.{pp_key} = pp
        DETACH DELETE n
        """

        params = {"pp_values": pp_values}

        self.evaluate_query_single(cypher, params)

    def merge_relationships(
        self,
        source_label,
        target_label,
        source_prop,
        target_prop,
        rel_type,
        merge_on_props,
        rel_props,
    ) -> None:
        # build a string of properties to merge on "prop_name: $prop_name"
        merge_props = ", ".join([f"{x}: rel.{x}" for x in merge_on_props])

        cypher = f"""
        UNWIND $rel_list AS rel
        MATCH (source:{source_label})
        WHERE source.{source_prop} = rel.source_prop
        MATCH (target:{target_label})
        WHERE target.{target_prop} = rel.target_prop
        MERGE (source)-[r:{rel_type} {{ {merge_props} }}]->(target)
        ON MATCH SET r += rel.set_on_match
        ON CREATE SET r += rel.set_on_create
        SET r += rel.always_set
        """

        params = {"rel_list": rel_props}

        self.evaluate_query_single(cypher, params)

    def match_nodes(
        self,
        node_class,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list:
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
        ORDER BY n.created DESC
        """

        params = {}

        if skip:
            cypher += " SKIP $skip "
            params["skip"] = skip

        if limit:
            cypher += " LIMIT $limit "
            params["limit"] = limit

        result = self.evaluate_query(
            cypher, params, node_classes={node_class.__primarylabel__: node_class}
        )

        return result.nodes
