import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase, Neo4jDriver
from neo4j import Record as Neo4jRecord
from neo4j import Result as Neo4jResult
from neo4j import Transaction as Neo4jTransaction

from .result import NeontologyResult, neo4j_records_to_neontology_records


class GraphConnection(object):
    """Class for managing connections to Neo4j."""

    _instance = None

    def __new__(
        cls,
        neo4j_uri: Optional[str] = None,
        neo4j_username: Optional[str] = None,
        neo4j_password: Optional[str] = None,
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
                    driver = GraphConnection._instance.driver = GraphDatabase.driver(  # type: ignore
                        neo4j_uri, auth=(neo4j_username, neo4j_password)
                    )
                    driver.verify_connectivity()

                    from .utils import get_node_types, get_rels_by_type

                    # capture all possible types of node and relationship
                    cls.global_nodes = get_node_types()
                    cls.global_rels = get_rels_by_type()

                except Exception as error:
                    print(
                        "Error: connection not established. Have you run init_neontology? {}".format(
                            error
                        )
                    )
                    GraphConnection._instance = None

            else:
                GraphConnection._instance = None

        return cls._instance

    def __del__(self) -> None:
        """Close the driver gracefully when the class gets deleted."""

        self.driver.close()

    def __init__(
        self,
        neo4j_uri: Optional[str] = None,
        neo4j_username: Optional[str] = None,
        neo4j_password: Optional[str] = None,
    ) -> None:
        if self._instance:
            self.driver: Neo4jDriver = self._instance.driver

    def run_transaction_single(
        self, tx: Neo4jTransaction, query: str, params: Dict[str, Any]
    ) -> Optional[Neo4jRecord]:
        """Run a transaction which is expected to return a single result.

        Args:
            tx (Neo4jTransaction): Neo4j Transaction object
            query (str): cypher query to run
            params (Dict[str, Any]): Parameters to pass to the query

        Returns:
            Optional[Neo4jRecord]: The result
        """

        return tx.run(query, **params).single()

    def run_transaction_many(
        self, tx: Neo4jTransaction, query: str, params: Dict[str, Any]
    ) -> List[Neo4jRecord]:
        """Run a transation which is expected to return multiple nodes.

        Args:
            tx (Neo4jTransaction): Neo4j Transaction object
            query (str): cypher query to run
            params (Dict[str, Any]): parameters to pass the query

        Returns:
            List[Neo4jRecord]: a list of the results
        """

        return [record for record in tx.run(query, **params)]

    def cypher_write(self, cypher: str, params: Dict[str, Any] = {}) -> None:
        """Execute a write transaction.

        Args:
            cypher (str): cypher query
            params (Dict[str, Any]): parameters to pass to the query
        """

        with self.driver.session() as session:
            session.execute_write(self.run_transaction_single, cypher, params)

    def cypher_write_single(self, cypher: str, params: Dict[str, Any] = {}) -> None:
        """Execute a write transaction.

        Args:
            cypher (str): cypher query
            params (Dict[str, Any]): parameters to pass to the query
        """

        with self.driver.session() as session:
            return session.execute_write(self.run_transaction_single, cypher, params)

    def cypher_write_many(self, cypher: str, params: Dict[str, Any] = {}) -> None:
        """Execute a write transaction.

        Args:
            cypher (str): cypher query
            params (Dict[str, Any]): parameters to pass to the query
        """

        with self.driver.session() as session:
            return session.execute_write(self.run_transaction_many, cypher, params)

    def cypher_read(
        self, cypher: str, params: Dict[str, Any] = {}
    ) -> Optional[Neo4jRecord]:
        """Run a cypher read only query which is expected to return a single result.

        Args:
            cypher (str): cypher query string
            params (Dict[str, Any]): parameters to pass to the query

        Returns:
            Neo4jRecord: the resulting Neo4j 'Record', or None
        """

        with self.driver.session() as session:
            return session.execute_read(self.run_transaction_single, cypher, params)

    def cypher_read_many(
        self, cypher: str, params: Dict[str, Any] = {}
    ) -> List[Neo4jRecord]:
        """Run a cypher read query which will return multiple records.

        Args:
            cypher (str): cypher string to run
            params (Dict[str, Any]): parameters to pass to the query

        Returns:
            List[Neo4jRecord]: A list of Neo4j 'Records' returned by the query.
        """

        with self.driver.session() as session:
            return session.execute_read(self.run_transaction_many, cypher, params)

    def apply_constraint(self, label: str, property: str) -> None:
        cypher = f"""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (n:{label})
        REQUIRE n.{property} IS UNIQUE
        """

        self.cypher_write(cypher)

    def evaluate_query_single(self, cypher, params={}):
        result = self.driver.execute_query(
            cypher, parameters_=params, result_transformer_=Neo4jResult.single
        )

        if result:
            return result.value()

        else:
            return None

    def evaluate_query(self, cypher, params={}):
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records = neo4j_records_to_neontology_records(
            neo4j_records, self.global_nodes, self.global_rels
        )

        return NeontologyResult(
            records=neo4j_records, neontology_records=neontology_records
        )


def init_neontology(
    neo4j_uri: Optional[str] = None,
    neo4j_username: Optional[str] = None,
    neo4j_password: Optional[str] = None,
) -> None:
    """Initialise neontology.

    If connection properties are explicitly passed in, use these.
    If not, attempt to load from enviornment variables (optionally in a .env file.)

    Args:
        neo4j_uri (Optional[str], optional): Neo4j URI to connect to. Defaults to None.
        neo4j_username (Optional[str], optional): Neo4j username. Defaults to None.
        neo4j_password (Optional[str], optional): Neo4j password. Defaults to None.
    """

    # try to load environment variables from .env file
    load_dotenv()

    if neo4j_uri is None:
        neo4j_uri = os.getenv("NEO4J_URI")

    if neo4j_password is None:
        neo4j_password = os.getenv("NEO4J_PASSWORD")

    if neo4j_username is None:
        neo4j_username = os.getenv("NEO4J_USERNAME")

    GraphConnection(neo4j_uri, neo4j_username, neo4j_password)
