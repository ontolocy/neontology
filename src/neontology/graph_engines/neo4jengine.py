import os

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j import Result as Neo4jResult

from ..graphengine import GraphEngineBase
from ..result import NeontologyResult, neo4j_records_to_neontology_records


class Neo4jEngine(GraphEngineBase):
    def __init__(self, config: dict) -> None:
        """Initialise connection to the engine

        Args:
            config (Optional[dict]): _description_
        """

        # try to load environment variables from .env file
        load_dotenv()

        neo4j_uri = config.get("neo4j_uri", os.getenv("NEO4J_URI"))

        neo4j_username = config.get("neo4j_username", os.getenv("NEO4J_USERNAME"))

        neo4j_password = config.get("neo4j_password", os.getenv("NEO4J_PASSWORD"))

        self.driver = GraphDatabase.driver(  # type: ignore
            neo4j_uri,
            auth=(neo4j_username, neo4j_password),
        )

    def verify_connection(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except:
            return False

    def close_connection(self) -> None:
        try:
            self.driver.close()
        except AttributeError:
            pass

    def evaluate_query(
        self, cypher, params={}, node_classes={}, relationship_classes={}
    ):
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records, nodes, rels = neo4j_records_to_neontology_records(
            neo4j_records, node_classes, relationship_classes
        )

        return NeontologyResult(
            records=neo4j_records,
            neontology_records=neontology_records,
            nodes=nodes,
            relationships=rels,
        )

    def evaluate_query_single(self, cypher, params={}):
        result = self.driver.execute_query(
            cypher, parameters_=params, result_transformer_=Neo4jResult.single
        )

        if result:
            return result.value()

        else:
            return None

    def apply_constraint(self, label: str, property: str) -> None:
        cypher = f"""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (n:{label})
        REQUIRE n.{property} IS UNIQUE
        """

        self.evaluate_query_single(cypher)

    def drop_constraint(self, constraint_name: str) -> None:
        drop_cypher = f"""
        DROP CONSTRAINT {constraint_name}
        """
        self.evaluate_query_single(drop_cypher)

    def get_constraints(self) -> list:
        get_constraints_query = """
        SHOW CONSTRAINTS yield name
        RETURN COLLECT(DISTINCT name)
        """

        constraints = self.evaluate_query_single(get_constraints_query)

        if constraints:
            return constraints

        else:
            return []
