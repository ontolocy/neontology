import os
from typing import Any, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j import Result as Neo4jResult

from ..result import NeontologyResult
from .graphengine import GraphEngineBase
from .neo4jengine import neo4j_records_to_neontology_records


class MemgraphEngine(GraphEngineBase):
    """
    Note. the memgraph engine is very similar to the Neo4j engine as they
        can both use the same Neo4j driver
    Args:
        Neo4jEngine (_type_): _description_
    """

    def __init__(self, config: dict) -> None:
        """Initialise connection to the engine

        Args:
            config (Optional[dict]): _description_
        """

        # try to load environment variables from .env file
        load_dotenv()

        memgraph_uri = config.get("memgraph_uri", os.getenv("MEMGRAPH_URI"))

        memgraph_username = config.get("memgraph_username", os.getenv("MEMGRAPH_USER"))

        memgraph_password = config.get(
            "memgraph_password", os.getenv("MEMGRAPH_PASSWORD")
        )

        self.driver = GraphDatabase.driver(  # type: ignore
            memgraph_uri,
            auth=(memgraph_username, memgraph_password),
        )

    def verify_connection(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except:  # noqa: E722
            return False

    def close_connection(self) -> None:
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
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records, nodes, rels = neo4j_records_to_neontology_records(
            neo4j_records, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=neo4j_records,
            records=neontology_records,
            nodes=nodes,
            relationships=rels,
        )

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        result = self.driver.execute_query(
            cypher, parameters_=params, result_transformer_=Neo4jResult.single
        )

        if result:
            return result.value()

        else:
            return None
