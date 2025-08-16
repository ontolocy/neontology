from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Optional, TypeVar
from warnings import warn

from .graphengines import MemgraphConfig, Neo4jConfig
from .graphengines.graphengine import GraphEngineBase, GraphEngineConfig
from .result import NeontologyResult

if TYPE_CHECKING:
    from .basenode import BaseNode
    from .baserelationship import BaseRelationship

logger = logging.getLogger(__name__)


BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")
BaseRelationshipT = TypeVar("BaseRelationshipT", bound="BaseRelationship")


class GraphConnection(object):
    """Class for managing connections to Neo4j."""

    _instance = None

    def __new__(
        cls,
        config: Optional[GraphEngineConfig] = None,
    ) -> "GraphConnection":
        """Make sure we only have a single connection to the GraphDatabase.

        This connection then gets used by all instances.

        Args:
            config: GraphEngineConfig to setup the desired GraphEngine

        Returns:
            GraphConnection: Instance of the connection
        """
        if cls._instance is None:
            cls._instance = object.__new__(cls)

            if GraphConnection._instance:
                try:
                    GraphConnection._instance.engine = config.engine(config)

                except Exception as exc:
                    GraphConnection._instance = None

                    raise RuntimeError(
                        (
                            "Error: connection not established. Have you run init_neontology?"
                            f" Underlying exception: {type(exc).__name__}"
                        )
                    ) from exc

                # capture all currently defined types of node and relationship
                from .utils import get_node_types, get_rels_by_type

                cls.global_nodes = get_node_types()
                cls.global_rels = get_rels_by_type()

            else:
                GraphConnection._instance = None

        return cls._instance

    def __init__(
        self,
        config: Optional[GraphEngineConfig] = None,
    ) -> None:
        if self._instance:
            self.engine: GraphEngineBase = self._instance.engine

        if self.engine.verify_connection() is False:
            raise RuntimeError("Error: connection not established. Have you run init_neontology?")

    @classmethod
    def change_engine(
        cls,
        config: GraphEngineConfig,
    ) -> None:
        """Change the graph engine used by Neontology.

        This method allows changing the graph engine configuration after Neontology has been initialized.

        Args:
            config (GraphEngineConfig): The new configuration for the graph engine.

        Raises:
            RuntimeError: If Neontology has not been initialized yet.
        """
        if not cls._instance:
            raise RuntimeError("Error: Can't change the engine without initializing Neontology first.")

        cls._instance.engine.close_connection()
        cls._instance.engine = config.engine(config)

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        """Evaluate a Cypher query against the graph database which returns a single result.

        Calls the underlying engine's evaluate_query_single method.

        Args:
            cypher (str): The Cypher query to execute.
            params (dict): Parameters to pass to the Cypher query.

        Returns:
            Optional[Any]: The single result of the query execution, or None if no result.
        """
        return self.engine.evaluate_query_single(cypher, params)

    def evaluate_query(
        self,
        cypher: str,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
        refresh_classes: bool = True,
    ) -> NeontologyResult:
        """Evaluate a Cypher query against the graph database.

        Args:
            cypher (str): The Cypher query to execute.
            params (dict): Parameters to pass to the Cypher query.
            node_classes (dict): Optional dictionary of node classes to use.
            relationship_classes (dict): Optional dictionary of relationship classes to use.
            refresh_classes (bool): Whether to refresh the global node and relationship types.

        Returns:
            NeontologyResult: The result of the query execution.
        """
        if refresh_classes is True:
            from .utils import get_node_types, get_rels_by_type

            # capture all currently defined types of node and relationship
            self.global_nodes = get_node_types()
            self.global_rels = get_rels_by_type()

        if not node_classes:
            node_classes = self.global_nodes

        if not relationship_classes:
            relationship_classes = self.global_rels

        return self.engine.evaluate_query(cypher, params, node_classes, relationship_classes)

    def create_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Create nodes in the graph database.

        Calls the underlying engine's create_nodes method to create new nodes.

        Args:
            labels (list): List of labels for the nodes to create.
            pp_key (str): The property key to match on.
            properties (list): List of properties to set on the nodes.
            node_class (type[BaseNode]): The class of the node to create.

        Returns:
            list[BaseNode]: List of created nodes.
        """
        return self.engine.create_nodes(labels, pp_key, properties, node_class)

    def merge_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list["BaseNodeT"]:
        """Merge nodes in the graph database.

        Calls the underlying engine's merge_nodes method to create or update nodes.

        Args:
            labels (list): List of labels for the nodes to merge.
            pp_key (str): The property key to match on.
            properties (list): List of properties to set on the nodes.
            node_class (type[BaseNode]): The class of the node to merge.

        Returns:
            list[BaseNode]: List of merged nodes.
        """
        return self.engine.merge_nodes(labels, pp_key, properties, node_class)

    def match_nodes(
        self,
        node_class: type[BaseNodeT],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        filters: Optional[dict] = None,
    ) -> list[BaseNodeT]:
        """Match nodes in the graph database with optional filtering.

        Calls the underlying engine's match_nodes method to retrieve nodes.

        Args:
            node_class (type[BaseNode]): The class of the node to match.
            filters (dict, optional): Dictionary of filters using Django-like syntax:
                - {"name": "exact_value"} → exact match (case-sensitive)
                - {"name__icontains": "part"} → case-insensitive contains
                - {"name__exact": "Value"} → exact match (case-sensitive)
                - {"name__iexact": "value"} → exact match (case-insensitive)
                - {"quantity__gt": 100} → greater than
                - {"date__lt": some_date} → less than
                Defaults to None.
            limit (Optional[int]): Maximum number of nodes to return.
            skip (Optional[int]): Number of nodes to skip.

        Returns:
            list[BaseNode]: List of matched nodes.
        """
        return self.engine.match_nodes(node_class, limit=limit, skip=skip, filters=filters)

    def get_count(
        self,
        node_class: type,
        filters: Optional[dict] = None,
    ) -> int:
        """Get the count of nodes of a specific type in the graph database with optional filtering.

        Args:
            node_class (type): The class of the node to count.
            filters (dict | None): Dictionary of filters using Django-like syntax.

        Returns:
            int: Count of matched nodes.
        """
        return self.engine.get_count(node_class, filters=filters)

    def match_relationships(
        self,
        relationship_class: type[BaseRelationshipT],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list[BaseRelationshipT]:
        """Match relationships in the graph database.

        Calls the underlying engine's match_relationships method to retrieve relationships.

        Args:
            relationship_class (type[BaseRelationshipT]): The class of the relationship to match.
            limit (Optional[int]): Maximum number of relationships to return.
            skip (Optional[int]): Number of relationships to skip.

        Returns:
            list[BaseRelationshipT]: List of matched relationships.
        """
        return self.engine.match_relationships(relationship_class, limit, skip)

    def delete_nodes(self, label: str, pp_key: str, pp_values: list) -> None:
        """Delete nodes from the graph database.

        Calls the underlying engine's delete_nodes method to remove nodes.

        Args:
            label (str): The label of the nodes to delete.
            pp_key (str): The property key to match on.
            pp_values (list): The list of property values to match for deletion.
        """
        self.engine.delete_nodes(label, pp_key, pp_values)

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
        """Merge relationships between nodes in the graph database.

        Args:
            source_label (str): The label of the source node.
            target_label (str): The label of the target node.
            source_prop (str): The property of the source node to match on.
            target_prop (str): The property of the target node to match on.
            rel_type (str): The type of relationship to merge.
            merge_on_props (list[str]): Properties to use for merging relationships.
            rel_props (list[dict]): Properties to set on the relationship.
        """
        self.engine.merge_relationships(
            source_label,
            target_label,
            source_prop,
            target_prop,
            rel_type,
            merge_on_props,
            rel_props,
        )

    def close(self) -> None:
        """Close the connection to the graph database."""
        self.engine.close_connection()


def init_neontology(config: Optional[GraphEngineConfig] = None, **kwargs) -> None:
    """Initialise neontology."""
    graph_engines = {
        "NEO4J": Neo4jConfig,
        "MEMGRAPH": MemgraphConfig,
    }

    try:
        from .graphengines import NetworkxConfig

        graph_engines["NETWORKX"] = NetworkxConfig

    except ImportError:
        pass

    if "neo4j_uri" in kwargs or "neo4j_username" in kwargs or "neo4j_password" in kwargs:
        warn(
            (
                "Neo4j keyword arguments in init_neontology are being deprecated "
                "- use config dictionary instead. Read the docs for new syntax."
            ),
            DeprecationWarning,
            stacklevel=2,
        )

        neo4j_config = {}

        if kwargs.get("neo4j_uri"):
            neo4j_config["uri"] = kwargs.get("neo4j_uri")

        if kwargs.get("neo4j_username"):
            neo4j_config["username"] = kwargs.get("neo4j_username")

        if kwargs.get("neo4j_password"):
            neo4j_config["password"] = kwargs.get("neo4j_password")

        config = Neo4jConfig(**neo4j_config)

    if config is None:
        graph_engine = os.getenv("NEONTOLOGY_ENGINE")

        if graph_engine:
            logger.info(f"No GraphConfig provided, using defaults based on specified engine: {graph_engine}.")
            config = graph_engines[graph_engine]()

        else:
            logger.info("No GraphConfig provided and no Graph Engine specified, using Neo4j.")
            config = Neo4jConfig()

    GraphConnection(config)
    logger.info("Neontology initialized.")
