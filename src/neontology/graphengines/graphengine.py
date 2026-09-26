from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Optional, Sequence, TypeVar, Union

from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, model_validator

from ..gql import gql_identifier_adapter, non_negative_int_adapter
from ..registry import registry
from ..result import NeontologyResult
from .capabilities import Capability, CapabilityNotSupportedError
from .dbschema import Constraint, ConstraintType, Index, SchemaObject

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..baserelationship import BaseRelationship
    from ..schema import NodeSchema, OntologySchema

BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")
BaseRelationshipT = TypeVar("BaseRelationshipT", bound="BaseRelationship")


class GraphEngineBase:
    # engines declare what they support - an omission means unsupported, so a
    # capability added to the vocabulary is never silently claimed
    supported_capabilities: ClassVar[frozenset[Capability]] = frozenset(Capability)

    # extra context for the error raised when an unsupported capability is asked for,
    # so an engine can explain *why* rather than only that it cannot
    capability_hints: ClassVar[dict[Capability, str]] = {}

    # whether a uniqueness constraint carries an index of its own, as Neo4j's do. Where it
    # does, apply_indexes() leaves unique properties to the constraint
    uniqueness_constraints_are_indexed: ClassVar[bool] = False

    _supported_types: ClassVar[Any] = (
        list,
        bool,
        int,
        bytearray,
        float,
        str,
        bytes,
        date,
        time,
        datetime,
        timedelta,
    )

    def __init__(self, config: Optional["GraphEngineConfig"]) -> None:
        """Initialise the graph engine.

        Args:
            config (Optional[dict]): GraphEngine configuration
        """
        pass

    @classmethod
    def supports(cls, capability: Capability) -> bool:
        """Report whether this engine supports a given capability.

        Args:
            capability (Capability): the capability to check.

        Returns:
            bool: True if this engine supports it.

        Raises:
            TypeError: if given something that is not a Capability, so that a typo
                is an error rather than a silently unsupported feature.
        """
        if not isinstance(capability, Capability):
            raise TypeError(f"Expected a Capability, got {capability!r}")

        return capability in cls.supported_capabilities

    def label_pattern(self, label: str) -> str:
        """Build the label fragment that matches every node of a class, for a MATCH pattern.

        A class is matched by its primary label, because a subclass node carries its
        ancestors' labels as well as its own. A backend where that is not true - where a
        node has exactly one label - overrides this to name the class and its subclasses
        instead, which is why every class scoped query goes through here rather than
        interpolating `__primarylabel__` itself.

        Args:
            label (str): the class' primary label.

        Returns:
            str: the fragment to follow the node variable with, such as ":Person".
        """
        return f":{gql_identifier_adapter.validate_strings(label)}"

    @classmethod
    def _export_type_converter(cls, value: Any) -> Any:
        """Convert a value to a type supported by the graph engine.

        This method is used to ensure that values are converted to types that the graph engine can handle.

        Args:
            value (Any): The value to convert.

        Returns:
            Any: The converted value, or the original value if it is already of a supported type.

        Raises:
            TypeError: If the value is a dict, or if it is a list with mixed types.
        """
        if isinstance(value, dict):
            raise TypeError("Neontology doesn't support dict types for properties.")

        elif isinstance(value, (tuple, set)):
            new_value = list(value)
            return cls._export_type_converter(new_value)

        elif isinstance(value, list):
            if len(value) == 0:
                return []
            # items in a list must all be the same type
            item_type = type(value[0])
            for item in value:
                if isinstance(item, item_type) is False:
                    raise TypeError("For neo4j, all items in a list must be of the same type.")

            return [cls._export_type_converter(x) for x in value]

        elif value is None:
            return None

        elif isinstance(value, cls._supported_types) is False:
            return str(value)

        else:
            return value

    @classmethod
    def export_dict_converter(cls, original_dict: dict[str, Any]) -> dict[str, Any]:
        """Convert types in a dictionary to those supported by the graph engine.

        Args:
            original_dict (dict[str, Any]): The original dictionary to convert.

        Returns:
            dict[str, Any]: A new dictionary with values converted to types supported by the graph engine.
        """
        export_dict = original_dict.copy()

        for k, v in export_dict.items():
            export_dict[k] = cls._export_type_converter(v)

        return export_dict

    def verify_connection(self) -> bool:
        """Verify the connection to the graph engine.

        Returns:
            bool: True if the connection is valid, False otherwise.
        """
        raise NotImplementedError

    def close_connection(self) -> None:
        """Close the connection to the graph engine."""
        raise NotImplementedError

    def evaluate_query(
        self,
        cypher: str,
        params: dict[str, Any] = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        """Evaluate a Cypher query against the database.

        Args:
            cypher (str): The Cypher query to execute.
            params (dict[str, Any], optional): Parameters for the Cypher query. Defaults to {}.
            node_classes (dict, optional): Mapping of node labels to their classes. Defaults to {}.
            relationship_classes (dict, optional): Mapping of relationship types to their classes. Defaults to {}.

        Returns:
            NeontologyResult: The result of the query execution, containing nodes and relationships.
        """
        raise NotImplementedError

    def evaluate_query_single(self, cypher: str, params: dict[str, Any]) -> Any:
        """Evaluate a query which returns a single result.

        Args:
            cypher (str): The Cypher query to execute.
            params (dict[str, Any]): Parameters for the Cypher query.

        Returns:
            Any: The result of the query execution.
        """
        raise NotImplementedError

    def _require(self, capability: Capability) -> None:
        """Raise unless this engine supports a capability.

        Every constraint and index method guards on this, so an unsupported backend
        gives one clear error naming itself and the capability, rather than a bare
        NotImplementedError from wherever the call happened to land.

        Args:
            capability (Capability): the capability the caller needs.

        Raises:
            CapabilityNotSupportedError: if this engine does not support it.
        """
        if self.supports(capability):
            return

        message = f"{type(self).__name__} does not support {capability.value}."

        hint = self.capability_hints.get(capability)

        if hint:
            message += f"\n\n{hint}"

        message += (
            f"\n\nCheck GraphConnection().supports(Capability.{capability.name})"
            " before calling. See docs/graph-engines.md for the capability matrix."
        )

        raise CapabilityNotSupportedError(message)

    def _unimplemented(self, method: str, capability: Capability) -> NotImplementedError:
        """Build the error for an engine that claims a capability but lacks the method.

        Reaching this is a bug in the engine rather than something a caller can act on,
        so it is deliberately not a CapabilityNotSupportedError.

        Args:
            method (str): the method that should have been overridden.
            capability (Capability): the capability the engine declares.

        Returns:
            NotImplementedError: the error to raise.
        """
        return NotImplementedError(f"{type(self).__name__} declares {capability.value} but does not implement {method}().")

    # -- constraints -------------------------------------------------------

    def apply_uniqueness_constraint(self, label: str, properties: Union[str, Sequence[str]]) -> None:
        """Require a label/property combination to be unique.

        Applying the same constraint twice is a no-op on every engine that supports
        constraints, so callers do not have to check first.

        Args:
            label (str): the node label to constrain.
            properties (Union[str, Sequence[str]]): one property name, or several for a
                composite constraint.

        Raises:
            NotImplementedError: if the engine declares CONSTRAINTS without implementing this.
        """
        self._require(Capability.CONSTRAINTS)

        raise self._unimplemented("apply_uniqueness_constraint", Capability.CONSTRAINTS)

    def get_constraints(self) -> list[Constraint]:
        """Get the constraints defined in the database.

        Returns:
            list[Constraint]: every constraint the database reports, including any
                neontology did not create.

        Raises:
            NotImplementedError: if the engine declares CONSTRAINTS without implementing this.
        """
        self._require(Capability.CONSTRAINTS)

        raise self._unimplemented("get_constraints", Capability.CONSTRAINTS)

    def drop_constraint(self, constraint: Constraint) -> None:
        """Drop a constraint.

        Takes a Constraint as returned by `get_constraints()` rather than a name,
        because Memgraph does not name constraints and identifies them by pattern.

        Args:
            constraint (Constraint): the constraint to drop.

        Raises:
            NotImplementedError: if the engine declares CONSTRAINTS without implementing this.
        """
        self._require(Capability.CONSTRAINTS)

        raise self._unimplemented("drop_constraint", Capability.CONSTRAINTS)

    @staticmethod
    def _describe(node_types: Iterable[type[BaseNode]], action: str) -> list[NodeSchema]:
        """Describe node classes to constrain or index, checking them all before anything is applied.

        Args:
            node_types (Iterable[type[BaseNode]]): the node classes.
            action (str): what they are described for, to explain the error.

        Returns:
            list[NodeSchema]: their descriptions.

        Raises:
            ValueError: if a node type is abstract, with no primary label.
        """
        nodes = []

        for node_type in node_types:
            if node_type._is_abstract():
                raise ValueError(f"{node_type.__name__} is abstract, so it has no primary label to {action}.")

            nodes.append(node_type.neontology_schema())

        return nodes

    @staticmethod
    def _unique_properties(node: NodeSchema) -> list[str]:
        """Get the properties a described node requires to be unique.

        Args:
            node (NodeSchema): the node class, described.

        Returns:
            list[str]: its primary property, then any property tagged `unique`.
        """
        return list(dict.fromkeys([node.primary_property, *(prop.name for prop in node.properties if prop.unique)]))

    def apply_constraints(self, node_types: Iterable[type[BaseNode]]) -> list[Constraint]:
        """Apply the uniqueness constraints the given node types declare.

        Each node type's primary property is constrained to be unique under its primary
        label, and so is every property tagged `unique` with `json_schema_extra`. That is
        the same decision on every backend, so it lives here rather than being repeated
        per engine. An engine that can apply a batch in one statement can override this.

        Args:
            node_types (Iterable[type[BaseNode]]): the node classes to constrain.

        Returns:
            list[Constraint]: the constraints applied.

        Raises:
            ValueError: if a node type is abstract, with no primary label.
        """
        # guard up front rather than relying on the loop below to reach
        # apply_uniqueness_constraint - asking an engine that has no constraints is an
        # error even when the caller passes no node types
        self._require(Capability.CONSTRAINTS)

        return self._apply_constraints(self._describe(node_types, "constrain"))

    def _apply_constraints(self, nodes: Iterable[NodeSchema]) -> list[Constraint]:
        """Apply the uniqueness constraints described node classes declare.

        Args:
            nodes (Iterable[NodeSchema]): concrete node classes, described.

        Returns:
            list[Constraint]: the constraints applied.
        """
        constraints = [
            Constraint(label=node.label, properties=(prop,), constraint_type=ConstraintType.UNIQUENESS)
            for node in nodes
            for prop in self._unique_properties(node)
        ]

        for constraint in constraints:
            self.apply_uniqueness_constraint(constraint.label, constraint.properties)

        return constraints

    # -- indexes -----------------------------------------------------------

    def apply_index(self, label: str, properties: Union[str, Sequence[str], None] = None) -> None:
        """Index a label/property combination, without requiring uniqueness.

        Applying the same index twice is a no-op on every engine that supports
        indexes, so callers do not have to check first.

        Args:
            label (str): the node label to index.
            properties (Union[str, Sequence[str], None]): one property name, several for
                a composite index, or none for a label-only index where the backend
                supports one.

        Raises:
            NotImplementedError: if the engine declares INDEXES without implementing this.
        """
        self._require(Capability.INDEXES)

        raise self._unimplemented("apply_index", Capability.INDEXES)

    def get_indexes(self) -> list[Index]:
        """Get the indexes defined in the database.

        Only indexes a caller could manage are reported. Indexes backing a constraint
        and indexes the database maintains for itself are excluded, because dropping
        either is not something a caller can meaningfully do - and a teardown loop over
        this list would otherwise destroy them.

        Returns:
            list[Index]: the manageable indexes.

        Raises:
            NotImplementedError: if the engine declares INDEXES without implementing this.
        """
        self._require(Capability.INDEXES)

        raise self._unimplemented("get_indexes", Capability.INDEXES)

    def drop_index(self, index: Index) -> None:
        """Drop an index.

        Takes an Index as returned by `get_indexes()` rather than a name, because
        Memgraph does not name indexes and identifies them by pattern. Dropping an
        index that does not exist is a no-op.

        Args:
            index (Index): the index to drop.

        Raises:
            NotImplementedError: if the engine declares INDEXES without implementing this.
        """
        self._require(Capability.INDEXES)

        raise self._unimplemented("drop_index", Capability.INDEXES)

    def apply_indexes(self, node_types: Iterable[type[BaseNode]]) -> list[Index]:
        """Apply the indexes the given node types declare.

        Each property tagged `index` with `json_schema_extra` is indexed under its node
        type's primary label, and so is each property required to be unique - the primary
        property and any tagged `unique` - so looking one up is fast too. Where the
        database's uniqueness constraints carry their own index, unique properties are
        left to `apply_constraints()`: Neo4j refuses a constraint on a property a plain
        index already covers.

        Args:
            node_types (Iterable[type[BaseNode]]): the node classes to index.

        Returns:
            list[Index]: the indexes applied.

        Raises:
            ValueError: if a node type is abstract, with no primary label.
        """
        # guarded up front, as in apply_constraints
        self._require(Capability.INDEXES)

        return self._apply_indexes(self._describe(node_types, "index"))

    def _apply_indexes(self, nodes: Iterable[NodeSchema]) -> list[Index]:
        """Apply the indexes described node classes declare.

        Args:
            nodes (Iterable[NodeSchema]): concrete node classes, described.

        Returns:
            list[Index]: the indexes applied.
        """
        indexes = []

        for node in nodes:
            unique = self._unique_properties(node)
            tagged = [prop.name for prop in node.properties if prop.index]

            if self.uniqueness_constraints_are_indexed:
                properties = [prop for prop in tagged if prop not in unique]

            else:
                properties = list(dict.fromkeys([*unique, *tagged]))

            indexes += [Index(label=node.label, properties=(prop,)) for prop in properties]

        for index in indexes:
            self.apply_index(index.label, index.properties)

        return indexes

    # -- initialising a graph ------------------------------------------------

    def initialise_graph(self, schema: OntologySchema) -> list[SchemaObject]:
        """Prepare the database for an ontology.

        Everything the ontology declares that this engine supports is applied - here, the
        constraints and indexes `apply_constraints()` and `apply_indexes()` apply, for each
        concrete node class - and anything it does not support is skipped, so this can be
        called on any engine. It only ever adds, so it is safe to run again.

        The ontology is passed as its description, relationships included, so an engine
        whose database must be given its schema before it can be used can override this to
        build that schema from the same data.

        Args:
            schema (OntologySchema): the ontology to prepare the database for.

        Returns:
            list[SchemaObject]: what was applied.
        """
        nodes = [node for node in schema.nodes if not node.abstract]

        applied: list[SchemaObject] = []

        if self.supports(Capability.CONSTRAINTS):
            applied += self._apply_constraints(nodes)

        if self.supports(Capability.INDEXES):
            applied += self._apply_indexes(nodes)

        return applied

    def create_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Create nodes with specified labels and properties.

        Args:
            labels (list): a list of labels to give created nodes
            pp_key (str): the primary property for the nodes
            properties (list): A list of dictionaries representing each node to be created.
                two keys with associated values pp (the value to assign the primary property)
                and props (dict with key value pairs for all other properties).
            node_class (type[BaseNodeT]): the type of nodes to create

        Returns:
            list: list of created Nodes
        """
        label_identifiers = [gql_identifier_adapter.validate_strings(x) for x in labels]

        cypher = f"""
        UNWIND $node_list AS node
        create (n:{":".join(label_identifiers)} {{{gql_identifier_adapter.validate_strings(pp_key)}: node.pp}})
        SET n += node.props
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def merge_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Merge nodes with specified labels and property.

        Args:
            labels (list): _description_
            pp_key (str): _description_
            properties (list): A list of dictionaries representing each node to be created.
                four keys with associated values: pp (the value to assign the primary property)
                set_on_match, set_on_create and always_set (dicts with key value pairs for all other properties).
            node_class (type["BaseNode"]): The class of the nodes to be merged.

        Returns:
            list: list of merged Nodes
        """
        primary_label = gql_identifier_adapter.validate_strings(node_class.__primarylabel__)

        other_labels = [gql_identifier_adapter.validate_strings(x) for x in labels if x != node_class.__primarylabel__]

        # MERGE on the primary label alone: with the primary property, that is what
        # identifies a node. Merging on every label stopped matching existing nodes as soon
        # as a model's other labels changed, and created duplicates instead. The other
        # labels are added afterwards, which never removes a label already on the node.
        set_labels = f"SET n:{':'.join(other_labels)}" if other_labels else ""

        cypher = f"""
        UNWIND $node_list AS node
        MERGE (n:{primary_label} {{{gql_identifier_adapter.validate_strings(pp_key)}: node.pp}})
        ON MATCH SET n += node.set_on_match
        ON CREATE SET n += node.set_on_create
        SET n += node.always_set
        {set_labels}
        RETURN n
        """

        params = {"node_list": properties}

        node_classes = {node_class.__primarylabel__: node_class}

        results = self.evaluate_query(cypher, params, node_classes)

        return results.nodes

    def delete_nodes(self, label: str, pp_key: str, pp_values: list[Any]) -> None:
        """Delete nodes with a specific label and primary property value.

        Args:
            label (str): The label of the nodes to delete.
            pp_key (str): The primary property key to match on.
            pp_values (list[Any]): A list of primary property values to match on for deletion.
        """
        cypher = f"""
        UNWIND $pp_values AS pp
        MATCH (n{self.label_pattern(label)})
        WHERE n.{gql_identifier_adapter.validate_strings(pp_key)} = pp
        DETACH DELETE n
        """

        params = {"pp_values": pp_values}

        self.evaluate_query_single(cypher, params)

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
        """Merge relationships between nodes in the database.

        Args:
            source_label (str): The label of the source node.
            target_label (str): The label of the target node.
            source_prop (str): The property of the source node to match on.
            target_prop (str): The property of the target node to match on.
            rel_type (str): The type of relationship to create or merge.
            merge_on_props (list[str]): A list of properties to merge on.
            rel_props (list[dict]): A list of dictionaries representing each relationship to be merged.
                Each dictionary should contain keys for `source_prop`, `target_prop`, and any additional properties
                to set on the relationship.
        """
        # build a string of properties to merge on "prop_name: $prop_name"
        merge_props = ", ".join([f"{gql_identifier_adapter.validate_strings(x)}: rel.{x}" for x in merge_on_props])

        cypher = f"""
        UNWIND $rel_list AS rel
        MATCH (source:{gql_identifier_adapter.validate_strings(source_label)})
        WHERE source.{gql_identifier_adapter.validate_strings(source_prop)} = rel.source_prop
        MATCH (target:{gql_identifier_adapter.validate_strings(target_label)})
        WHERE target.{gql_identifier_adapter.validate_strings(target_prop)} = rel.target_prop
        MERGE (source)-[r:{gql_identifier_adapter.validate_strings(rel_type)} {{ {merge_props} }}]->(target)
        ON MATCH SET r += rel.set_on_match
        ON CREATE SET r += rel.set_on_create
        SET r += rel.always_set
        """

        params = {"rel_list": rel_props}

        self.evaluate_query_single(cypher, params)

    # the lookups a filter key may end in. A key whose final `__`-segment is not one of
    # these is treated as a plain field name (exact match), so a property whose own name
    # contains `__` still works.
    _FILTER_LOOKUPS: ClassVar[frozenset[str]] = frozenset(
        {
            "exact",
            "iexact",
            "contains",
            "icontains",
            "startswith",
            "istartswith",
            "gt",
            "lt",
            "gte",
            "lte",
            "in",
            "isnull",
        }
    )

    def _split_filter_key(self, key: str) -> tuple[str, str]:
        """Split a filter key into a validated field name and a lookup type.

        The field name is interpolated into the query string, so it is validated as a
        GQL identifier - this is what stops a filter key being used to inject Cypher.

        A key with `__` must end in a recognised lookup: a `created__startswit` typo is
        an error rather than a silent exact-match that quietly returns nothing. Splitting
        from the right also means a three-part key like `a__b__gt` reads as field `a__b`
        with the `gt` lookup, rather than crashing the old two-way split.

        Args:
            key (str): the filter key, e.g. "name" or "created__gt".

        Returns:
            tuple[str, str]: the field name and the lookup type.

        Raises:
            ValueError: if the lookup is unrecognised or the field name is not a valid identifier.
        """
        if "__" in key:
            field_name, _, lookup_type = key.rpartition("__")

            if lookup_type not in self._FILTER_LOOKUPS:
                raise ValueError(
                    f"Invalid filter lookup {lookup_type!r} in key {key!r}."
                    f" Supported lookups: {', '.join(sorted(self._FILTER_LOOKUPS))}."
                )
        else:
            field_name, lookup_type = key, "exact"

        try:
            gql_identifier_adapter.validate_strings(field_name)
        except ValidationError as exc:
            raise ValueError(f"Invalid filter field {field_name!r}: field names must be alphanumeric identifiers.") from exc

        return field_name, lookup_type

    def _filters_to_where_clause(self, filters: Optional[dict] = None) -> tuple[Optional[str], dict]:
        """Convert a dictionary of filters into a WHERE clause and parameter dictionary for a query.

        Args:
            filters (dict | None): A dictionary of filters. Each key is a field name possibly followed
                                by '__' and a lookup type (e.g., 'exact', 'contains', 'isnull'). The
                                value is the filter value. If None, returns an empty WHERE clause.

        Returns:
            tuple: A tuple containing the WHERE clause string and a dictionary of parameters.
        """
        params = {}
        where_clauses = []
        where_clause = None
        if filters:
            for key, value in filters.items():
                field_name, lookup_type = self._split_filter_key(key)
                param_name = f"filter_{field_name}_{lookup_type}"
                params[param_name] = value
                if lookup_type == "exact":
                    clause = f"n.{field_name} = ${param_name}"
                elif lookup_type == "iexact":
                    clause = f"toLower(n.{field_name}) = toLower(${param_name})"
                elif lookup_type == "contains":
                    clause = f"n.{field_name} CONTAINS ${param_name}"
                elif lookup_type == "icontains":
                    clause = f"toLower(n.{field_name}) CONTAINS toLower(${param_name})"
                elif lookup_type == "startswith":
                    clause = f"n.{field_name} STARTS WITH ${param_name}"
                elif lookup_type == "istartswith":
                    clause = f"toLower(n.{field_name}) STARTS WITH toLower(${param_name})"
                elif lookup_type in ("gt", "lt", "gte", "lte"):
                    operator = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}[lookup_type]

                    clause = f"n.{field_name} {operator} ${param_name}"
                elif lookup_type == "in":
                    clause = f"n.{field_name} IN ${param_name}"
                elif lookup_type == "isnull":
                    clause = f"n.{field_name} IS NULL" if value else f"n.{field_name} IS NOT NULL"

                else:
                    raise ValueError(f"Invalid filter: {lookup_type}")

                where_clauses.append(clause)
            where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        return where_clause, params

    def match_nodes(
        self,
        node_class: type,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        filters: Optional[dict] = None,
    ) -> list:
        """Match nodes based on the given node class, limit, skip, and filters.

        Args:
            node_class (type): The class of the nodes to match.
            limit (int | None): The maximum number of nodes to return. If None, all matching nodes are returned.
            skip (int | None): The number of nodes to skip before collecting the result set. If None, no nodes are skipped.
            filters (dict | None): A dictionary of filters to apply. If None, no filters are applied.

        Returns:
            list: A list of nodes that match the given criteria.
        """
        # checked before anything is asked of the database
        if skip is not None:
            skip = non_negative_int_adapter.validate_python(skip)
        if limit is not None:
            limit = non_negative_int_adapter.validate_python(limit)

        cypher = f"MATCH (n{self.label_pattern(node_class.__primarylabel__)})"
        where_clause, params = self._filters_to_where_clause(filters)
        if where_clause:
            cypher += where_clause
        cypher += " RETURN n"
        if skip is not None:
            cypher += " SKIP $skip"
            params["skip"] = skip
        if limit is not None:
            cypher += " LIMIT $limit"
            params["limit"] = limit

        # subclasses carrying this label match the query too, and come back as themselves
        result = self.evaluate_query(cypher, params, node_classes=registry.result_classes(node_class))

        return result.nodes

    def get_count(
        self,
        node_class: type,
        filters: Optional[dict] = None,
    ) -> int:
        """Get the count of nodes based on the given node class and filters.

        Args:
            node_class (type): The class of the nodes to count.
            filters (dict | None): A dictionary of filters to apply. If None, no filters are applied.

        Returns:
            int: The count of nodes that match the given criteria.
        """
        cypher = f"MATCH (n{self.label_pattern(node_class.__primarylabel__)})"
        where_clause, params = self._filters_to_where_clause(filters)
        if where_clause:
            cypher += where_clause
        cypher += " RETURN COUNT(DISTINCT n)"
        return self.evaluate_query_single(cypher, params)

    def match_relationships(
        self,
        relationship_class: type[BaseRelationshipT],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list[BaseRelationshipT]:
        """Get relationships of this type from the database.

        Run a MATCH cypher query to retrieve any Relationships of this type.

        Args:
            relationship_class (type["BaseRelationshipT"]): the type of relationship to match on
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            list["BaseRelationshipT"]: A list of relationships.
        """
        from ..utils import get_node_types, get_rels_by_type

        cypher = f"""
        MATCH (n)-[r:{gql_identifier_adapter.validate_strings(relationship_class.__relationshiptype__)}]->(o)
        RETURN n, r, o
        """

        params = {}

        if skip:
            cypher += " SKIP $skip "
            params["skip"] = non_negative_int_adapter.validate_python(skip)

        if limit:
            cypher += " LIMIT $limit "
            params["limit"] = non_negative_int_adapter.validate_python(limit)

        rel_types = get_rels_by_type()
        node_classes = get_node_types()

        result = self.evaluate_query(
            cypher,
            params,
            node_classes=node_classes,
            relationship_classes=rel_types,
        )

        return result.relationships


class GraphEngineConfig(BaseModel):
    """Base class for Graph Engine configuration."""

    engine: ClassVar[type[GraphEngineBase]]

    env_fields: ClassVar[dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def populate_defaults(cls, data: Any) -> Any:
        """Populate configuration with environment variables.

        Where no values are provided, attempt to load them from environment variables.
        """
        load_dotenv()

        for field, env_var in cls.env_fields.items():
            if not data.get(field):
                value = os.getenv(env_var)
                if value is None:
                    raise ValueError(f"No value provided for {field} field and no {env_var} environment variable set.")
                data[field] = value
        return data
