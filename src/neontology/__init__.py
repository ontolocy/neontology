# flake8: noqa

from .basenode import BaseNode, related_nodes, related_property
from .baserelationship import BaseRelationship
from .gql import GQLIdentifier, gql_identifier_adapter
from .graphconnection import GraphConnection, init_neontology
from .graphengines.capabilities import Capability, CapabilityNotSupportedError
from .graphengines.dbschema import Constraint, ConstraintType, Index, Table
from .graphengines.memgraphengine import MemgraphConfig
from .graphengines.neo4jengine import Neo4jConfig
from .neontologywarning import NeontologyWarning
from .registry import (
    DuplicateLabelError,
    DuplicateLabelWarning,
    InheritedLabelWarning,
    Registry,
    registry,
)
from .result import NeontologyResult
from .schema import get_ontology_schema
from .utils import (
    auto_constrain_neo4j,
    get_node_types,
    get_rels_by_source,
    get_rels_by_target,
    get_rels_by_type,
)

__all__ = [
    # BaseNode
    "BaseNode",
    "related_nodes",
    "related_property",
    # BaseRelationship
    "BaseRelationship",
    # GraphConnection
    "init_neontology",
    "GraphConnection",
    # Schema management
    "Constraint",
    "ConstraintType",
    "Index",
    "Table",
    "Capability",
    "CapabilityNotSupportedError",
    # Deprecated, removed in v4
    "auto_constrain_neo4j",
    # Model registry
    "get_node_types",
    "get_rels_by_type",
    "get_rels_by_source",
    "get_rels_by_target",
    "registry",
    "Registry",
    "DuplicateLabelWarning",
    "InheritedLabelWarning",
    "DuplicateLabelError",
    "NeontologyWarning",
    # Describing the ontology
    "get_ontology_schema",
    # Query results
    "NeontologyResult",
    # GQL
    "GQLIdentifier",
    "gql_identifier_adapter",
    # Engines
    "Neo4jConfig",
    "MemgraphConfig",
]
