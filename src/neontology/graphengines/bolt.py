"""What the engines reached through the neo4j driver have in common.

Neo4j and Memgraph both speak Bolt and are both reached through the neo4j driver, so
connecting, running queries and reading results back are the same for each. How each
database names and reports constraints and indexes differs, and stays on the engines.
"""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

from neo4j import GraphDatabase
from neo4j import Record as Neo4jRecord
from neo4j import Result as Neo4jResult
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Path as Neo4jPath
from neo4j.graph import Relationship as Neo4jRelationship
from neo4j.time import Date as Neo4jDate
from neo4j.time import DateTime as Neo4jDateTime
from neo4j.time import Duration as Neo4jDuration
from neo4j.time import Time as Neo4jTime
from typing_extensions import LiteralString

from ..result import NeontologyResult
from .graphengine import GraphEngineBase
from .hydration import RawNode, RawPath, RawRelationship, build_result

if TYPE_CHECKING:
    from .memgraphengine import MemgraphConfig
    from .neo4jengine import Neo4jConfig


def _to_native(value: Any) -> Any:
    """Convert one value returned by the neo4j driver to its native Python equivalent.

    The driver has its own temporal types, which pydantic does not accept for datetime,
    date, time and timedelta fields. Lists are converted item by item, since a list
    property holds the same driver types.

    Args:
        value (Any): a property value as the driver returned it.

    Returns:
        Any: the native equivalent, or the value unchanged if it needs no conversion.
    """
    if isinstance(value, (Neo4jDateTime, Neo4jDate, Neo4jTime)):
        return value.to_native()

    # a month has no fixed length, so a duration counting months has no timedelta
    # equivalent. Neontology never writes one, and inventing a length would change the data.
    if isinstance(value, Neo4jDuration) and value.months == 0:
        # the parts carry their own signs, so sub-microsecond precision is truncated
        # toward zero rather than floored, which would move a negative duration
        microseconds = abs(value.nanoseconds) // 1000 * (1 if value.nanoseconds >= 0 else -1)

        return timedelta(days=value.days, seconds=value.seconds, microseconds=microseconds)

    if isinstance(value, list):
        return [_to_native(item) for item in value]

    return value


def convert_neo4j_types(input_dict: dict) -> dict:
    """Convert neo4j driver types in a dictionary of properties to native Python types.

    Args:
        input_dict (dict): properties as the driver returned them.

    Returns:
        dict: the same properties, with driver temporal types converted.
    """
    return {key: _to_native(value) for key, value in input_dict.items()}


def bolt_rows(records: list[Neo4jRecord]) -> Iterator[dict[str, Any]]:
    """Read driver records as rows of engine-neutral values, for `build_result`.

    The driver identifies nodes and relationships by element id. A relationship's source
    and target only carry their labels and properties when the result also returns those
    nodes, anywhere in it; otherwise the driver knows nothing but their ids, and they are
    passed on as missing. A path carries its own nodes.

    Args:
        records (list[Neo4jRecord]): the driver's records.

    Yields:
        dict[str, Any]: each row, from column name to the nodes, relationships and paths in it,
            and every other value, converted to native Python types.
    """
    raw_nodes: dict[str, RawNode] = {}
    raw_relationships: dict[str, RawRelationship] = {}

    def node(value: Neo4jNode) -> RawNode:
        raw = raw_nodes.get(value.element_id)

        if raw is None:
            raw = raw_nodes[value.element_id] = RawNode(
                key=value.element_id,
                labels=value.labels,
                properties=convert_neo4j_types(dict(value)),
            )

        return raw

    def endpoint(value: Optional[Neo4jNode]) -> Optional[RawNode]:
        # a node the result does not return is known only by its id, without its labels
        if value is None or not value.labels:
            return None

        return node(value)

    def relationship(value: Neo4jRelationship) -> RawRelationship:
        raw = raw_relationships.get(value.element_id)

        if raw is None:
            raw = raw_relationships[value.element_id] = RawRelationship(
                key=value.element_id,
                type=value.type,
                properties=convert_neo4j_types(dict(value)),
                source=endpoint(value.start_node),
                target=endpoint(value.end_node),
            )

        return raw

    for record in records:
        row: dict[str, Any] = {}

        for column, value in record.items():
            if isinstance(value, Neo4jNode):
                row[column] = node(value)

            elif isinstance(value, Neo4jRelationship):
                row[column] = relationship(value)

            elif isinstance(value, Neo4jPath):
                row[column] = RawPath(relationships=[relationship(step) for step in value.relationships])

            else:
                row[column] = _to_native(value)

        yield row


class BoltEngine(GraphEngineBase):
    """Connection and query mechanics for an engine reached through the neo4j driver."""

    def __init__(self, config: Union["Neo4jConfig", "MemgraphConfig"]) -> None:
        """Connect to the database.

        Args:
            config (Union[Neo4jConfig, MemgraphConfig]): the engine's connection details.
        """
        self.driver = GraphDatabase.driver(
            config.connection_uri,
            auth=(config.connection_username, config.connection_password),
        )

    def verify_connection(self) -> bool:
        """Verify the connection to the database.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            self.driver.verify_connectivity()
            return True
        except:  # noqa: E722
            return False

    def close_connection(self) -> None:
        """Close the connection to the database."""
        try:
            self.driver.close()
        except AttributeError:
            pass

    def evaluate_query(
        self,
        cypher: LiteralString,
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

        return build_result(result.records, bolt_rows(result.records), node_classes, relationship_classes)

    def evaluate_query_single(self, cypher: LiteralString, params: dict = {}) -> Optional[Any]:
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
