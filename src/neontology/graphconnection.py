from typing import Optional


from .graph_engines import Neo4jEngine
from .graphengine import GraphEngineBase


class GraphConnection(object):
    """Class for managing connections to Neo4j."""

    _instance = None

    def __new__(
        cls,
        graph_config: Optional[dict] = None,
        graph_engine_class: Optional[type[GraphEngineBase]] = None,
    ) -> "GraphConnection":
        """Make sure we only have a single connection to the GraphDatabase.

        This connection then gets used by all instances.

        Args:
            neo4j_uri (Optional[str], optional): Neo4j URI to connect to. Defaults to None.
            neo4j_username (Optional[str], optional): Neo4j username. Defaults to None.
            neo4j_password (Optional[str], optional): Neo4j password. Defaults to None.

        Returns:
            GraphConnection: Instance of the connection
        """

        if cls._instance is None:
            cls._instance = object.__new__(cls)

            if GraphConnection._instance:
                try:
                    GraphConnection._instance.engine = graph_engine_class(graph_config)

                except Exception:
                    GraphConnection._instance = None
                    raise RuntimeError(
                        "Error: connection not established. Have you run init_neontology?"
                    )

                # capture all currently defined types of node and relationship
                from .utils import get_node_types, get_rels_by_type

                cls.global_nodes = get_node_types()
                cls.global_rels = get_rels_by_type()

            else:
                GraphConnection._instance = None

        return cls._instance

    def __init__(
        self,
        graph_config: dict = {},
        graph_engine_class: Optional[type[GraphEngineBase]] = None,
    ) -> None:
        if self._instance:
            self.engine: GraphEngineBase = self._instance.engine

        if self.engine.verify_connection() is False:
            raise RuntimeError(
                "Error: connection not established. Have you run init_neontology?"
            )

    @classmethod
    def change_engine(cls, graph_config, graph_engine_class):
        cls._instance.engine.close_connection()
        cls._instance.engine = graph_engine_class(graph_config)

    def evaluate_query_single(self, cypher, params={}):
        return self.engine.evaluate_query_single(cypher, params)

    def evaluate_query(
        self,
        cypher,
        params={},
        node_classes={},
        relationship_classes={},
        refresh_classes: bool = True,
    ):
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
        self, labels: list, pp_key: str, properties: list, node_class
    ) -> list:
        return self.engine.create_nodes(labels, pp_key, properties, node_class)

    def merge_nodes(
        self, labels: list, pp_key: str, properties: list, node_class
    ) -> list:
        return self.engine.merge_nodes(labels, pp_key, properties, node_class)

    def match_nodes(
        self,
        node_class,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list:
        return self.engine.match_nodes(node_class, limit, skip)

    def delete_nodes(self, label, pp_key, pp_values):
        self.engine.delete_nodes(label, pp_key, pp_values)

    def merge_relationships(
        self,
        source_label,
        target_label,
        source_prop,
        target_prop,
        rel_type,
        merge_on_props,
        rel_props,
    ):
        self.engine.merge_relationships(
            source_label,
            target_label,
            source_prop,
            target_prop,
            rel_type,
            merge_on_props,
            rel_props,
        )

    def close(self):
        self.engine.close_connection()


def init_neontology(
    graph_config: dict = {},
    graph_engine_class: Optional[type[GraphEngineBase]] = None,
    neo4j_uri: Optional[str] = None,
    neo4j_username: Optional[str] = None,
    neo4j_password: Optional[str] = None,
) -> None:
    """Initialise neontology."""

    if neo4j_uri or neo4j_username or neo4j_password:
        raise TypeError(
            "Neo4j keyword arguments no longer supported by init_neontology - use graph_config instead. Read the docs for new syntax."
        )

    if graph_engine_class is None:
        graph_engine_class = Neo4jEngine

    GraphConnection(graph_config, graph_engine_class)
    print("Neontology initialised.")
