from __future__ import annotations

import logging
import os
import warnings
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence, TypeVar, Union

from .graphengines import MemgraphConfig, Neo4jConfig
from .graphengines.capabilities import Capability
from .graphengines.dbschema import Constraint, Index
from .graphengines.graphengine import GraphEngineBase, GraphEngineConfig
from .result import NeontologyResult

if TYPE_CHECKING:
    from .basenode import BaseNode
    from .baserelationship import BaseRelationship

logger = logging.getLogger(__name__)


BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")
BaseRelationshipT = TypeVar("BaseRelationshipT", bound="BaseRelationship")


def _close_quietly(engine: GraphEngineBase) -> None:
    """Close an engine that never became the live one, without masking the real error.

    An engine that failed to verify has usually still opened a driver. It is discarded
    either way, so a failure to close it is logged rather than raised - the caller needs
    the connection error, not this one.

    Args:
        engine (GraphEngineBase): the engine to close.
    """
    try:
        engine.close_connection()

    except Exception:  # pragma: no cover - depends on the driver's failure mode
        logger.debug("Failed to close a discarded engine.", exc_info=True)


class GraphConnection(object):
    """Class for managing the connection to the graph database."""

    _instance: Optional["GraphConnection"] = None

    # set in __new__ on the instance it publishes, declared here so it is annotated
    # without __init__ having to reassign it
    engine: GraphEngineBase

    def __new__(cls, *args: Any, **kwargs: Any) -> "GraphConnection":
        """Return the connection established by `init_neontology`.

        Takes no arguments. It used to accept a config, which was honoured on the first
        call in a process and silently ignored on every later one - so the signature
        promised to define the connection and did not. Establishing a connection is
        `init_neontology`'s job alone.

        Args:
            *args: rejected, so a config passed here is a loud error rather than a
                silent no-op.
            **kwargs: rejected, as above.

        Returns:
            GraphConnection: the one connection.

        Raises:
            TypeError: if any argument is passed.
            RuntimeError: if no connection has been established yet.
        """
        if args or kwargs:
            raise TypeError(
                "GraphConnection() takes no arguments - it returns the connection that"
                " init_neontology(config) established. To connect, or to reconnect with"
                " different settings, call init_neontology(config)."
            )

        if cls._instance is None:
            raise RuntimeError("Error: connection not established. Have you run init_neontology?")

        return cls._instance

    @classmethod
    def _establish(cls, config: GraphEngineConfig) -> "GraphConnection":
        """Connect using the given config, replacing any existing connection.

        The new engine is built and verified before the live one is touched, so a config
        that cannot connect leaves the existing connection working rather than destroying
        the one it was meant to replace. Nothing is published until it is known good, so
        a failure part way through cannot leave a broken connection cached.

        Where a connection already exists the engine is swapped on it rather than a new
        instance being created, so anything already holding a GraphConnection keeps
        working.

        Args:
            config (GraphEngineConfig): the engine configuration to connect with.

        Returns:
            GraphConnection: the connection.

        Raises:
            RuntimeError: if a connection could not be established.
        """
        try:
            new_engine = config.engine(config)

            # Verify here, where the connection is actually established, rather than on
            # every access. GraphConnection() is used throughout the library as a way to
            # reach the singleton - including once per model dump - so verifying there
            # cost a database round trip for every one of those.
            connected = new_engine.verify_connection()

        except Exception as exc:
            raise RuntimeError(
                f"Error: could not connect using the given {type(config).__name__}. Underlying exception: {type(exc).__name__}"
            ) from exc

        if connected is False:
            _close_quietly(new_engine)

            raise RuntimeError(f"Error: could not connect using the given {type(config).__name__}.")

        if cls._instance is None:
            instance = object.__new__(cls)
            instance.engine = new_engine

            cls._instance = instance

        else:
            previous_engine = cls._instance.engine
            cls._instance.engine = new_engine

            _close_quietly(previous_engine)

        # capture all currently defined types of node and relationship
        from .utils import get_node_types, get_rels_by_type

        cls.global_nodes = get_node_types()
        cls.global_rels = get_rels_by_type()

        return cls._instance

    @classmethod
    def change_engine(
        cls,
        config: GraphEngineConfig,
    ) -> None:
        """Change the graph engine used by Neontology.

        Deprecated since v3.0: `init_neontology(config)` now does exactly this, so there
        is one way to say which database to talk to rather than two.

        Args:
            config (GraphEngineConfig): The new configuration for the graph engine.

        Raises:
            RuntimeError: If Neontology has not been initialized yet.
        """
        warnings.warn(
            "GraphConnection.change_engine is deprecated and will be removed in v4. Use init_neontology(config) instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        if not cls._instance:
            raise RuntimeError("Error: Can't change the engine without initializing Neontology first.")

        cls._establish(config)

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
                - {"tags__in": ["a", "b"]} → value in list
                - {"name__isnull": True} → property is null (False for IS NOT NULL)

                Also supported: __gte, __lte, __contains, __startswith, __istartswith.
                A key containing "__" must end in one of these lookups; field names are
                validated as identifiers.
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
        """Close the connection to the graph database.

        The singleton is cleared as well as the driver, so `init_neontology()` can
        establish a fresh connection afterwards. Leaving it in place meant a closed
        connection could never be replaced: `init_neontology()` handed back the same
        dead instance and every query raised from the closed driver.
        """
        self.engine.close_connection()

        GraphConnection._instance = None

    def supports(self, capability: Capability) -> bool:
        """Report whether the current engine supports a capability.

        Constraints and indexes are backend features, so portable code checks here
        before calling rather than catching the error.

        Args:
            capability (Capability): the capability to check.

        Returns:
            bool: True if the engine supports it.
        """
        return self.engine.supports(capability)

    def apply_constraints(self, node_types: Iterable[type[BaseNodeT]]) -> list[Constraint]:
        """Apply uniqueness constraints for the given node types.

        Args:
            node_types (Iterable[type[BaseNode]]): the node classes to constrain.

        Returns:
            list[Constraint]: the constraints applied.
        """
        return self.engine.apply_constraints(node_types)

    def auto_constrain(self) -> list[Constraint]:
        """Apply a uniqueness constraint for every node type currently defined.

        Node types are discovered from the class hierarchy, so a class has to be
        imported or defined before this runs to be covered. Only primary labels are
        constrained - secondary labels are not.

        Returns:
            list[Constraint]: the constraints applied.
        """
        from .utils import get_node_types

        return self.apply_constraints(list(get_node_types().values()))

    def get_constraints(self) -> list[Constraint]:
        """Get the constraints defined in the graph database.

        Returns:
            list[Constraint]: every constraint the database reports.
        """
        return self.engine.get_constraints()

    def drop_constraint(self, constraint: Constraint) -> None:
        """Drop a constraint from the graph database.

        Args:
            constraint (Constraint): a constraint as returned by `get_constraints()`.
        """
        self.engine.drop_constraint(constraint)

    def apply_index(self, label: str, properties: Union[str, Sequence[str], None] = None) -> None:
        """Index a label/property combination without requiring uniqueness.

        Args:
            label (str): the node label to index.
            properties (Union[str, Sequence[str], None]): one property name, several for
                a composite index, or none for a label-only index.
        """
        self.engine.apply_index(label, properties)

    def get_indexes(self) -> list[Index]:
        """Get the indexes defined in the graph database.

        Returns:
            list[Index]: the indexes a caller can manage.
        """
        return self.engine.get_indexes()

    def drop_index(self, index: Index) -> None:
        """Drop an index from the graph database.

        Args:
            index (Index): an index as returned by `get_indexes()`.
        """
        self.engine.drop_index(index)


def init_neontology(config: Optional[GraphEngineConfig] = None) -> None:
    """Initialise neontology.

    Args:
        config (Optional[GraphEngineConfig]): configuration for the engine to connect
            with. If not given, the NEONTOLOGY_ENGINE environment variable selects the
            engine and its own environment variables supply the connection details,
            defaulting to Neo4j.
    """
    graph_engines = {
        "NEO4J": Neo4jConfig,
        "MEMGRAPH": MemgraphConfig,
    }

    try:
        from .graphengines import NetworkxConfig

        graph_engines["NETWORKX"] = NetworkxConfig

    except ImportError:
        pass

    if config is None:
        graph_engine = os.getenv("NEONTOLOGY_ENGINE")

        if graph_engine:
            logger.info(f"No GraphConfig provided, using defaults based on specified engine: {graph_engine}.")

            config_class = graph_engines.get(graph_engine)

            if config_class is None:
                available = ", ".join(sorted(graph_engines))
                hint = (
                    " Install the optional 'grand' extra (pip install neontology[grand]) to use NETWORKX."
                    if graph_engine == "NETWORKX"
                    else ""
                )
                raise ValueError(f"Unknown graph engine '{graph_engine}'. Available engines: {available}.{hint}")

            config = config_class()

        else:
            logger.info("No GraphConfig provided and no Graph Engine specified, using Neo4j.")
            config = Neo4jConfig()

    GraphConnection._establish(config)
    logger.info("Neontology initialized.")
