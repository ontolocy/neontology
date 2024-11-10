from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, List, Optional
from warnings import warn

from .graphengines import MemgraphConfig, Neo4jConfig
from .graphengines.graphengine import GraphEngineBase, GraphEngineConfig
from .result import NeontologyResult

if TYPE_CHECKING:
    from .basenode import BaseNode
    from .baserelationship import BaseRelationship

logger = logging.getLogger(__name__)


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
                    GraphConnection._instance.engine = config.engine(config)  # type: ignore[union-attr,arg-type]

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
            raise RuntimeError(
                "Error: connection not established. Have you run init_neontology?"
            )

    @classmethod
    def change_engine(
        cls,
        config: GraphEngineConfig,
    ) -> None:
        if not cls._instance:
            raise RuntimeError(
                "Error: Can't change the engine without initializing Neontology first."
            )

        cls._instance.engine.close_connection()
        cls._instance.engine = config.engine(config)

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        return self.engine.evaluate_query_single(cypher, params)

    def evaluate_query(
        self,
        cypher: str,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
        refresh_classes: bool = True,
    ) -> NeontologyResult:
        if refresh_classes is True:
            from .utils import get_node_types, get_rels_by_type

            # capture all currently defined types of node and relationship
            self.global_nodes = get_node_types()
            self.global_rels = get_rels_by_type()

        if not node_classes:
            node_classes = self.global_nodes

        if not relationship_classes:
            relationship_classes = self.global_rels

        return self.engine.evaluate_query(
            cypher, params, node_classes, relationship_classes
        )

    def create_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> List["BaseNode"]:
        return self.engine.create_nodes(labels, pp_key, properties, node_class)

    def merge_nodes(
        self, labels: list, pp_key: str, properties: list, node_class: type["BaseNode"]
    ) -> List["BaseNode"]:
        return self.engine.merge_nodes(labels, pp_key, properties, node_class)

    def match_nodes(
        self,
        node_class: type["BaseNode"],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> List["BaseNode"]:
        return self.engine.match_nodes(node_class, limit, skip)

    def match_relationships(
        self,
        relationship_class: type["BaseRelationship"],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> List["BaseRelationship"]:
        return self.engine.match_relationships(relationship_class, limit, skip)

    def delete_nodes(self, label: str, pp_key: str, pp_values: list) -> None:
        self.engine.delete_nodes(label, pp_key, pp_values)

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
        self.engine.close_connection()


def init_neontology(config: Optional[GraphEngineConfig] = None, **kwargs) -> None:
    """Initialise neontology."""

    graph_engines = {
        "NEO4J": Neo4jConfig,
        "MEMGRAPH": MemgraphConfig,
    }

    if (
        "neo4j_uri" in kwargs
        or "neo4j_username" in kwargs
        or "neo4j_password" in kwargs
    ):
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
            logger.info(
                f"No GraphConfig provided, using defaults based on specified engine: {graph_engine}."
            )
            config = graph_engines[graph_engine]()

        else:
            logger.info(
                "No GraphConfig provided and no Graph Engine specified, using Neo4j."
            )
            config = Neo4jConfig()

    GraphConnection(config)
    logger.info("Neontology initialized.")
