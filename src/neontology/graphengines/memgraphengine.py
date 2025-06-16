from typing import Any, ClassVar, Optional

from neo4j import GraphDatabase
from neo4j import Result as Neo4jResult

from ..result import NeontologyResult
from .graphengine import GraphEngineBase, GraphEngineConfig
from .neo4jengine import neo4j_records_to_neontology_records


class MemgraphEngine(GraphEngineBase):
    """Graph engine for a Memgraph database.

    Note. the memgraph engine is very similar to the Neo4j engine as they
        can both use the same Neo4j driver
    """

    def __init__(self, config: "MemgraphConfig") -> None:
        """Initialise connection to the Memgraph database.

        Args:
            config (MemgraphConfig): Takes a MemgraphConfig object.
        """
        self.driver = GraphDatabase.driver(
            config.connection_uri,
            auth=(config.connection_username, config.connection_password),
        )

    def verify_connection(self) -> bool:
        """Verify the connection to the Memgraph database.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            self.driver.verify_connectivity()
            return True
        except:  # noqa: E722
            return False

    def close_connection(self) -> None:
        """Close the connection to the Memgraph database."""
        try:
            self.driver.close()
        except AttributeError:
            pass

    def evaluate_query(
        self,
        cypher: str,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        """Evaluate a Cypher query and return the results as Neontology records.

        Args:
            cypher (str): query to evaluate.
            params (dict, optional): parameters to pass through. Defaults to {}.
            node_classes (dict, optional): mapping of labels to node classes used for populating with results. Defaults to {}.
            relationship_classes (dict, optional): mapping of relationship types to classes used for populating with results.
                Defaults to {}.

        Returns:
            NeontologyResult: Result object containing the records, nodes, relationships, and paths.
        """
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records, nodes, rels, paths = neo4j_records_to_neontology_records(
            neo4j_records, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=neo4j_records,
            records=neontology_records,
            nodes=nodes,
            relationships=rels,
            paths=paths,
        )

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        """Evaluate a Cypher query which returns a single result.

        Args:
            cypher (str): query to evaluate.
            params (dict, optional): parameters to pass through. Defaults to {}.

        Returns:
            Optional[Any]: Query result, or None if no result is found.
        """
        result = self.driver.execute_query(cypher, parameters_=params, result_transformer_=Neo4jResult.single)

        if result:
            return result.value()

        else:
            return None


class MemgraphConfig(GraphEngineConfig):
    """Configuration for a Neo4j graph engine."""

    engine: ClassVar[type[MemgraphEngine]] = MemgraphEngine
    uri: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    env_fields: ClassVar[dict[str, str]] = {
        "uri": "MEMGRAPH_URI",
        "username": "MEMGRAPH_USERNAME",
        "password": "MEMGRAPH_PASSWORD",
    }

    # Properties that guarantee non-None values for type checking
    @property
    def connection_uri(self) -> str:
        """Get the URI, guaranteed to be non-None after validation."""
        if self.uri is None:
            raise ValueError("URI should be set by validator")

        return self.uri

    @property
    def connection_username(self) -> str:
        """Get the username, guaranteed to be non-None after validation."""
        if self.username is None:
            raise ValueError("Username should be set by validator")

        return self.username

    @property
    def connection_password(self) -> str:
        """Get the password, guaranteed to be non-None after validation."""
        if self.password is None:
            raise ValueError("Password should be set by validator")

        return self.password
