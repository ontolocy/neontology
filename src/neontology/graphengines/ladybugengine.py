"""The LadybugDB graph engine.

LadybugDB (formerly Kùzu) is an embedded property graph database which speaks Cypher.
It runs in the calling process, against a directory on disk or entirely in memory, so
there is no server to reach and no connection to keep alive.

It differs from the Bolt engines in two ways that shape everything here.

It is **schema first**: every label is a table with typed columns, declared before
anything is written. `initialise_graph()` builds those tables from an `OntologySchema`,
which is what that hook exists for. A write to a label with no table is an error naming
the label, unless the config asks for `auto_create`.

A node belongs to **exactly one table**, so it carries exactly one label - hence
`SECONDARY_LABELS` is not supported. Several labels in a pattern are a *union* in
Ladybug rather than an intersection, which is what makes `label_pattern()` able to
match a class together with its subclasses.

Two properties of Ladybug's parameter binding drive the way queries are built:

- A `STRUCT` parameter takes its type from the values given, so a property which is
  `None` in every row of a batch binds as `STRING` and will not assign to an `INT64`
  column. Every value is therefore written through `CAST(... AS <column type>)`, read
  from the table itself.
- A batch is bound as one `LIST[STRUCT]`, so every row must carry the same keys, and an
  empty list - or an empty dictionary nested inside a row - cannot be bound at all. Rows
  are flat, and an empty batch never reaches the database.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Optional, Sequence, TypeVar

import ladybug as lb
from dotenv import load_dotenv
from pydantic import model_validator

from ..gql import gql_identifier_adapter
from ..result import NeontologyResult
from .capabilities import Capability
from .dbschema import SchemaEntity, SchemaObject, Table
from .graphengine import GraphEngineBase, GraphEngineConfig
from .hydration import RawNode, RawPath, RawRelationship, build_result

if TYPE_CHECKING:
    from ..basenode import BaseNode
    from ..schema import NodeSchema, OntologySchema, PropertySchema, RelationshipSchema

BaseNodeT = TypeVar("BaseNodeT", bound="BaseNode")

# the keys Ladybug puts on a returned node, relationship or path, which are its own
# structure rather than model properties
_NODE_KEYS = frozenset({"_ID", "_LABEL"})
_REL_KEYS = frozenset({"_ID", "_LABEL", "_SRC", "_DST"})

# JSON Schema "format" values that name a Ladybug type of their own. A time has none -
# Ladybug has no TIME type - so it is stored as its ISO string and parsed back by
# pydantic, which is also why `time` is left out of `_supported_types` below.
_FORMAT_TYPES = {
    "date-time": "TIMESTAMP",
    "date": "DATE",
    "duration": "INTERVAL",
    "uuid": "UUID",
    "binary": "BLOB",
    "time": "STRING",
}

_JSON_TYPES = {
    "string": "STRING",
    "integer": "INT64",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
}


def ladybug_type(json_schema: dict) -> str:
    """Choose the Ladybug column type for a property, from its pydantic JSON Schema.

    Anything with no clear equivalent becomes a `STRING`, which is what
    `GraphEngineBase._export_type_converter` does with a value whose type the engine
    does not support: it writes `str(value)`.

    Args:
        json_schema (dict): the property's JSON Schema, as `PropertySchema` carries it.

    Returns:
        str: the column type, such as "STRING", "INT64" or "STRING[]".
    """
    if not isinstance(json_schema, dict):
        return "STRING"

    # Optional[X] is anyOf[X, null]: the column is nullable either way, so describe it
    # by the branch that is not null
    branches = json_schema.get("anyOf") or json_schema.get("oneOf")

    if branches:
        for branch in branches:
            if isinstance(branch, dict) and branch.get("type") != "null":
                return ladybug_type(branch)

        return "STRING"

    schema_format = json_schema.get("format")

    if schema_format in _FORMAT_TYPES:
        return _FORMAT_TYPES[schema_format]

    schema_type = json_schema.get("type")

    if schema_type == "array":
        return f"{ladybug_type(json_schema.get('items', {}))}[]"

    if schema_type in _JSON_TYPES:
        return _JSON_TYPES[schema_type]

    # an Enum or Literal arrives as a bare list of values, with no type of its own
    values = json_schema.get("enum") or json_schema.get("const")

    if isinstance(values, list) and values:
        if all(isinstance(value, bool) for value in values):
            return "BOOLEAN"

        if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
            return "INT64"

        if all(isinstance(value, float) for value in values):
            return "DOUBLE"

    return "STRING"


def _identity(value: dict) -> tuple:
    """Read Ladybug's internal id as something hashable, to identify a node or relationship.

    Args:
        value (dict): an `_ID` value, which Ladybug returns as {"table": int, "offset": int}.

    Returns:
        tuple: the same identity as a tuple.
    """
    return (value.get("table"), value.get("offset"))


def _is_node(value: Any) -> bool:
    """Whether a returned value is a node: Ladybug's node keys, without a relationship's."""
    return isinstance(value, dict) and _NODE_KEYS <= value.keys() and "_SRC" not in value


def _is_relationship(value: Any) -> bool:
    """Whether a returned value is a relationship: it names the nodes at its ends."""
    return isinstance(value, dict) and "_SRC" in value and "_DST" in value


def _is_path(value: Any) -> bool:
    """Whether a returned value is a path: Ladybug returns its nodes and relationships."""
    return isinstance(value, dict) and "_NODES" in value and "_RELS" in value


def _model_keys(model_class: Optional[type]) -> Optional[set[str]]:
    """The property keys a model class accepts, addressed the way the graph stores them.

    Args:
        model_class (Optional[type]): a node or relationship class, or None if unknown.

    Returns:
        Optional[set[str]]: the keys, or None where the class is unknown and nothing
            can be filtered.
    """
    if model_class is None:
        return None

    return {(field.alias or name) for name, field in model_class.model_fields.items()}


def _properties(value: dict, internal: frozenset, keys: Optional[set[str]]) -> dict[str, Any]:
    """Read the model properties out of a node or relationship Ladybug returned.

    Two things are dropped. Ladybug's own keys, which are its structure rather than
    properties. And every `None`: a column exists on every row of its table whether or
    not a value was written, and a pattern naming several labels pads each row with the
    other tables' columns - where the Bolt engines simply return no property at all.
    Leaving them out is what makes a model's defaults apply as they do elsewhere, and
    models forbid properties they do not declare.

    Args:
        value (dict): the node or relationship as Ladybug returned it.
        internal (frozenset): the keys Ladybug uses for its own structure.
        keys (Optional[set[str]]): the property keys the model class accepts, where its
            class is known.

    Returns:
        dict[str, Any]: the properties to build the model from.
    """
    return {
        key: item for key, item in value.items() if key not in internal and item is not None and (keys is None or key in keys)
    }


def ladybug_rows(
    rows: Sequence[Sequence[Any]],
    columns: Sequence[str],
    node_classes: dict,
    relationship_classes: dict,
) -> list[dict[str, Any]]:
    """Read Ladybug's rows as engine-neutral values, for `build_result`.

    Ladybug identifies a node and a relationship by an internal id, and gives a
    relationship the ids of the nodes at its ends rather than the nodes themselves. As
    on the other engines, an end is only known when the result returns that node
    somewhere - here or on a path - so every node in the result is read first and the
    relationships are resolved against them.

    Args:
        rows (Sequence[Sequence[Any]]): the result's rows, each a value per column.
        columns (Sequence[str]): the column names, in the same order.
        node_classes (dict): node classes by primary label, to know which properties
            each node's class declares.
        relationship_classes (dict): relationship type data by relationship type.

    Returns:
        list[dict[str, Any]]: one dictionary per row, from column name to the nodes,
            relationships and paths in it, and every other value.
    """
    raw_nodes: dict[tuple, RawNode] = {}
    raw_relationships: dict[tuple, RawRelationship] = {}

    def node(value: dict) -> RawNode:
        key = _identity(value["_ID"])

        raw = raw_nodes.get(key)

        if raw is None:
            label = value["_LABEL"]
            node_class = node_classes.get(label)

            # a Ladybug node holds one label; the class knows the rest, so a model
            # declaring secondary labels is still built as itself rather than warned about
            labels = node_class._all_labels() if node_class is not None else [label]

            raw = raw_nodes[key] = RawNode(
                key=key,
                labels=labels,
                properties=_properties(value, _NODE_KEYS, _model_keys(node_class)),
            )

        return raw

    def relationship(value: dict) -> RawRelationship:
        key = _identity(value["_ID"])

        raw = raw_relationships.get(key)

        if raw is None:
            rel_type = value["_LABEL"]
            type_data = relationship_classes.get(rel_type)
            rel_class = getattr(type_data, "relationship_class", None)

            raw = raw_relationships[key] = RawRelationship(
                key=key,
                type=rel_type,
                properties=_properties(value, _REL_KEYS, _model_keys(rel_class)),
                source=raw_nodes.get(_identity(value["_SRC"])),
                target=raw_nodes.get(_identity(value["_DST"])),
            )

        return raw

    # every node the result holds, read before any relationship, so an end that is in
    # the result is found whichever column or row it was returned in
    for row in rows:
        for value in row:
            if _is_node(value):
                node(value)

            elif _is_path(value):
                for step in value["_NODES"]:
                    node(step)

    built: list[dict[str, Any]] = []

    for row in rows:
        record: dict[str, Any] = {}

        for column, value in zip(columns, row):
            if _is_node(value):
                record[column] = node(value)

            elif _is_relationship(value):
                record[column] = relationship(value)

            elif _is_path(value):
                record[column] = RawPath(relationships=[relationship(step) for step in value["_RELS"]])

            else:
                record[column] = value

        built.append(record)

    return built


class LadybugEngine(GraphEngineBase):
    """An embedded LadybugDB database, on disk or in memory."""

    # Ladybug speaks full Cypher, so it diverges only where its storage model does:
    # one label per node, a primary key instead of constraints, and no secondary
    # indexes. Everything not named in Capability works normally.
    supported_capabilities: ClassVar[frozenset[Capability]] = frozenset(
        {
            Capability.GRAPH_MUTATIONS,
            Capability.RETURN_STAR,
            Capability.DATETIME_FILTERS,
            Capability.LIST_PROPERTY_FILTERS,
            Capability.COMPLEX_PROPERTY_TYPES,
            Capability.COLLECT_DISTINCT,
            Capability.RELATIONSHIP_PROPERTY_QUERIES,
            Capability.MULTI_PATTERN_PATHS,
        }
    )

    capability_hints: ClassVar[dict[Capability, str]] = {
        Capability.SECONDARY_LABELS: (
            "A Ladybug node lives in the table named by its primary label and carries no"
            " other label, so a query naming a secondary or inheritable label directly"
            " finds nothing. Class scoped queries - match_nodes(), match(), get_count(),"
            " related_nodes() - still find subclasses, because the engine matches a class"
            " as the union of its own and its subclasses' primary labels."
        ),
        Capability.DUPLICATE_CREATE: (
            "Every node table is keyed on its class' primary property, so creating a"
            " second node with a value already present violates that key. Use merge()."
        ),
        Capability.CONSTRAINTS: (
            "Ladybug has no CREATE CONSTRAINT. Uniqueness of the primary property is the"
            " node table's primary key, declared by initialise_graph(); other properties"
            " tagged unique are not enforced."
        ),
        Capability.INDEXES: (
            "Ladybug has no general secondary indexes - the primary key is indexed by the"
            " table itself, and the full text and vector indexes are extensions neontology"
            " does not manage."
        ),
        Capability.UNDECLARED_SCHEMA: (
            "Ladybug is schema first, so a query naming a label, relationship type or"
            " property with nothing behind it is an error rather than a pattern matching"
            " nothing. Declare your models' tables with initialise_graph()."
        ),
        Capability.TIMEZONE_AWARE_DATETIMES: ("Ladybug's TIMESTAMP holds no offset, so an aware datetime is read back naive."),
        Capability.DATETIME_FUNCTIONS: (
            "Ladybug reads a date part with date_part('year', n.created) rather than with"
            " an accessor on the value, so n.created.year does not bind."
        ),
    }

    # datetime.time has no Ladybug type and cannot be bound as a parameter, so it is
    # written as its ISO string - which pydantic parses back into a time. The rest of
    # the base list binds natively.
    _supported_types: ClassVar[Any] = (
        list,
        bool,
        int,
        bytearray,
        float,
        str,
        bytes,
        date,
        datetime,
        timedelta,
    )

    def __init__(self, config: "LadybugConfig") -> None:
        """Open the database.

        Args:
            config (LadybugConfig): where the database lives, and whether to declare
                tables on write.
        """
        self.database = lb.Database(config.db_path)
        self.driver = lb.Connection(self.database)

        self.auto_create = config.auto_create

        self._tables: Optional[dict[str, str]] = None
        self._columns: dict[str, dict[str, str]] = {}

    # -- connection ---------------------------------------------------------

    def verify_connection(self) -> bool:
        """Verify the database is open and answering.

        Returns:
            bool: True if it is.
        """
        try:
            return self._run("RETURN 1") == [[1]]

        except Exception:
            return False

    def close_connection(self) -> None:
        """Close the connection and the database, releasing an on-disk database's lock."""
        for handle in ("driver", "database"):
            target = getattr(self, handle, None)

            if target is None:
                continue

            try:
                target.close()

            except Exception:  # pragma: no cover - closing twice, or an already dead handle
                pass

            setattr(self, handle, None)

    # -- running queries ----------------------------------------------------

    def _run(self, cypher: str, params: Optional[dict] = None) -> list[list[Any]]:
        """Run a query and read every row.

        Args:
            cypher (str): the query.
            params (Optional[dict]): its parameters.

        Returns:
            list[list[Any]]: the rows, each a value per column.
        """
        return self._execute(cypher, params).get_all()

    def _execute(self, cypher: str, params: Optional[dict] = None) -> Any:
        """Run a query and return Ladybug's result object.

        Args:
            cypher (str): the query.
            params (Optional[dict]): its parameters.

        Returns:
            Any: the Ladybug QueryResult.
        """
        return self.driver.execute(cypher, parameters=params) if params else self.driver.execute(cypher)

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
            node_classes (dict, optional): mapping of labels to node classes used for populating
                with results. Defaults to {}.
            relationship_classes (dict, optional): mapping of relationship types to classes used
                for populating with results. Defaults to {}.

        Returns:
            NeontologyResult: the records, and the nodes, relationships and paths in them.
        """
        result = self._execute(cypher, params)

        columns = result.get_column_names()
        rows = result.get_all()

        return build_result(
            rows,
            ladybug_rows(rows, columns, node_classes or {}, relationship_classes or {}),
            node_classes,
            relationship_classes,
        )

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        """Evaluate a Cypher query which returns a single result.

        Args:
            cypher (str): query to evaluate.
            params (dict, optional): parameters to pass through. Defaults to {}.

        Returns:
            Optional[Any]: the first value of the first row, or None if there are no rows.
        """
        rows = self._run(cypher, params)

        if not rows or not rows[0]:
            return None

        return rows[0][0]

    # -- the catalog --------------------------------------------------------

    def _catalog(self) -> dict[str, str]:
        """The tables the database holds, by name, mapped to "NODE" or "REL".

        Returns:
            dict[str, str]: the tables.
        """
        if self._tables is None:
            self._tables = {row[1]: row[2] for row in self._run("CALL show_tables() RETURN *")}

        return self._tables

    def _table_columns(self, table: str) -> dict[str, str]:
        """The columns of a table, by name, mapped to their Ladybug type.

        Args:
            table (str): the table name, already validated as an identifier.

        Returns:
            dict[str, str]: the columns.
        """
        if table not in self._columns:
            self._columns[table] = {row[1]: row[2] for row in self._run(f"CALL table_info('{table}') RETURN *")}

        return self._columns[table]

    def _pairs(self, table: str) -> set[tuple[str, str]]:
        """The FROM/TO node table pairs a relationship table connects.

        Args:
            table (str): the relationship table name, already validated as an identifier.

        Returns:
            set[tuple[str, str]]: the pairs.
        """
        return {(row[0], row[1]) for row in self._run(f"CALL show_connection('{table}') RETURN *")}

    def _ddl(self, statement: str) -> None:
        """Run a DDL statement and forget what was known about the schema.

        Args:
            statement (str): the statement to run.
        """
        self._run(statement)

        self._tables = None
        self._columns = {}

    def _has_table(self, name: str) -> bool:
        """Whether a table exists, re-reading the catalog once before saying it does not.

        Args:
            name (str): the table name.

        Returns:
            bool: True if the database has it.
        """
        if name in self._catalog():
            return True

        self._tables = None

        return name in self._catalog()

    def _require_table(self, name: str, kind: str) -> None:
        """Raise unless a table exists, explaining how to declare it.

        Args:
            name (str): the label or relationship type.
            kind (str): "node" or "relationship", for the message.

        Raises:
            RuntimeError: if the table has not been declared.
        """
        if self._has_table(name):
            return

        raise RuntimeError(
            f"LadybugDB has no table for the {name} {kind} type."
            " Ladybug is schema first, so tables are declared before anything is written:"
            " call GraphConnection().initialise_graph() once your models are defined."
            " To declare them as they are first written instead, connect with"
            " LadybugConfig(auto_create=True)."
        )

    # -- declaring the schema -----------------------------------------------

    def label_pattern(self, label: str) -> str:
        """Match a class as the union of its own and its subclasses' primary labels.

        A Ladybug node carries one label, so a subclass node is not also labelled with
        its parent's label the way it is on Neo4j. Several labels in a Ladybug pattern
        are a union rather than an intersection, so naming the class and its subclasses
        matches exactly the nodes the parent's label matches elsewhere.

        Labels with no table are left out: Ladybug rejects a pattern naming one, where
        the other engines simply match nothing.

        Args:
            label (str): the class' primary label.

        Returns:
            str: the fragment to follow the node variable with, such as ":Person:Employee".

        Raises:
            RuntimeError: if the class itself has no table.
        """
        return ":" + ":".join(self._class_tables(label))

    def _class_tables(self, label: str) -> list[str]:
        """The tables holding a class' nodes: its own, then its subclasses' that exist.

        Args:
            label (str): the class' primary label.

        Returns:
            list[str]: the table names, the class' own first.

        Raises:
            RuntimeError: if the class itself has no table.
        """
        from ..utils import get_node_types

        primary = gql_identifier_adapter.validate_strings(label)

        self._require_table(primary, "node")

        node_class = get_node_types().get(primary)

        if node_class is None:
            return [primary]

        return [primary] + [
            other
            for other in get_node_types(node_class)
            if other != primary and self._has_table(gql_identifier_adapter.validate_strings(other))
        ]

    def _column_definitions(self, properties: Iterable["PropertySchema"]) -> dict[str, str]:
        """Read a described class' properties as validated column names and Ladybug types.

        Args:
            properties (Iterable[PropertySchema]): the class' properties, described.

        Returns:
            dict[str, str]: column name to Ladybug type.
        """
        return {gql_identifier_adapter.validate_strings(prop.name): ladybug_type(prop.json_schema) for prop in properties}

    def _declare_node_table(self, node: "NodeSchema") -> Table:
        """Declare - or bring up to date - the node table for a described node class.

        Args:
            node (NodeSchema): a concrete node class, described.

        Returns:
            Table: the table the database now holds.

        Raises:
            ValueError: if the class has no primary label or no primary property.
        """
        if node.label is None or node.primary_property is None:
            raise ValueError(f"{node.class_name} is abstract, so it has no node table to declare.")

        label = gql_identifier_adapter.validate_strings(node.label)
        primary = gql_identifier_adapter.validate_strings(node.primary_property)

        columns = self._column_definitions(node.properties)

        if not self._has_table(label):
            # the primary property is declared like any other column and then named as the
            # key, so a composite definition would be the same statement with more names
            definitions = ", ".join(f"{name} {column_type}" for name, column_type in columns.items())

            self._ddl(f"CREATE NODE TABLE IF NOT EXISTS {label} ({definitions}, PRIMARY KEY ({primary}))")

        else:
            self._add_columns(label, columns)

        return Table(
            label=label,
            properties=(primary,),
            entity=SchemaEntity.NODE,
            columns=tuple(columns),
        )

    def _declare_relationship_table(self, relationship: "RelationshipSchema") -> Optional[Table]:
        """Declare - or bring up to date - the relationship table for a described class.

        A relationship table names the node tables it may connect, so it needs one
        FROM/TO pair for each combination of source and target class. Both lists are
        already expanded over subclasses by `RelationshipSchema`. A pair whose node
        tables are not both declared is left out rather than failing the whole
        initialisation: a schema can describe more classes than this database holds.

        Args:
            relationship (RelationshipSchema): the relationship class, described.

        Returns:
            Optional[Table]: the table the database now holds, or None if no pair of its
                node tables exists.
        """
        rel_type = gql_identifier_adapter.validate_strings(relationship.relationship_type)

        columns = self._column_definitions(relationship.properties)

        pairs = [
            (source, target)
            for source in map(gql_identifier_adapter.validate_strings, relationship.source_labels)
            for target in map(gql_identifier_adapter.validate_strings, relationship.target_labels)
            if self._has_table(source) and self._has_table(target)
        ]

        if not pairs:
            return None

        if not self._has_table(rel_type):
            definitions = ", ".join(
                [f"FROM {source} TO {target}" for source, target in pairs]
                + [f"{name} {column_type}" for name, column_type in columns.items()]
            )

            self._ddl(f"CREATE REL TABLE IF NOT EXISTS {rel_type} ({definitions})")

        else:
            self._add_columns(rel_type, columns)

            for source, target in set(pairs) - self._pairs(rel_type):
                self._ddl(f"ALTER TABLE {rel_type} ADD FROM {source} TO {target}")

        return Table(
            label=rel_type,
            entity=SchemaEntity.RELATIONSHIP,
            columns=tuple(columns),
        )

    def _add_columns(self, table: str, columns: dict[str, str]) -> None:
        """Add any column a table does not have yet.

        This is what makes initialise_graph() safe to run again after a property has been
        added to a model - CREATE TABLE IF NOT EXISTS alone would leave the table as it was.

        Args:
            table (str): the table name, already validated.
            columns (dict[str, str]): the columns it should have, and their types.
        """
        existing = self._table_columns(table)

        for name, column_type in columns.items():
            if name not in existing:
                self._ddl(f"ALTER TABLE {table} ADD IF NOT EXISTS {name} {column_type}")

    def initialise_graph(self, schema: "OntologySchema") -> list[SchemaObject]:
        """Declare the node and relationship tables an ontology needs.

        Ladybug is schema first, so this is what prepares a database for your models.
        Node tables are declared before relationship tables, because a relationship table
        names the node tables it connects. Running it again adds what is missing - a new
        class' table, a new property's column, a new pair of ends - and changes nothing
        else, so it is how a database follows a model as it grows.

        Constraints and indexes are not applied: a node table's primary key is its class'
        primary property, declared here, and Ladybug has no secondary indexes.

        Args:
            schema (OntologySchema): the ontology to prepare the database for.

        Returns:
            list[SchemaObject]: the node and relationship tables now declared.
        """
        applied: list[SchemaObject] = [self._declare_node_table(node) for node in schema.nodes if not node.abstract]

        for relationship in schema.relationships:
            table = self._declare_relationship_table(relationship)

            if table is not None:
                applied.append(table)

        return applied

    # -- writing ------------------------------------------------------------

    def _ensure_node_table(self, node_class: type) -> None:
        """Make sure a node class has its table, declaring it if the config allows.

        Args:
            node_class (type): the node class about to be written.
        """
        label = gql_identifier_adapter.validate_strings(node_class.__primarylabel__)

        if self._has_table(label):
            return

        if not self.auto_create:
            self._require_table(label, "node")

        self._declare_node_table(node_class.neontology_schema())

    def _ensure_relationship_table(self, rel_type: str, source_label: str, target_label: str) -> None:
        """Make sure a relationship type has its table, declaring it if the config allows.

        Every pair the relationship class allows is declared, not only the one being
        written: the endpoints are matched as a class and its subclasses, so a
        relationship declared between two parent classes has to be able to connect any
        of their subclasses' tables. Pairs whose node tables do not exist yet are left
        out and added by a later call, once those tables do.

        Args:
            rel_type (str): the relationship type.
            source_label (str): the source node's primary label.
            target_label (str): the target node's primary label.
        """
        from ..utils import get_rels_by_type

        rel_type = gql_identifier_adapter.validate_strings(rel_type)

        if not self.auto_create:
            self._require_table(rel_type, "relationship")

            return

        type_data = get_rels_by_type().get(rel_type)

        if type_data is None:  # pragma: no cover - the class is what asked for the write
            self._require_table(rel_type, "relationship")

            return

        from ..schema import _relationship_schema

        described = _relationship_schema(type_data.relationship_class, type_data)

        # the ends being written are included whatever the class declares, since they
        # are what this write needs
        described = described.model_copy(
            update={
                "source_labels": list(dict.fromkeys([*described.source_labels, source_label])),
                "target_labels": list(dict.fromkeys([*described.target_labels, target_label])),
            }
        )

        self._declare_relationship_table(described)

    def _casts(self, table: str, keys: Iterable[str], variable: str) -> dict[str, str]:
        """Build the CAST expression for each property, to its column's declared type.

        A batch is bound as one LIST[STRUCT] whose field types come from the values, so a
        property that is None in every row would bind as STRING and refuse to assign to a
        typed column. Casting to the column's own type makes the assignment well defined
        whatever the batch happens to hold.

        Args:
            table (str): the table being written, already validated.
            keys (Iterable[str]): the property keys to write.
            variable (str): the UNWIND variable the row is bound to.

        Returns:
            dict[str, str]: property name to the expression that reads it.

        Raises:
            RuntimeError: if the table has no column for one of the properties.
        """
        columns = self._table_columns(table)

        expressions = {}

        for key in keys:
            name = gql_identifier_adapter.validate_strings(key)

            column_type = columns.get(name)

            if column_type is None:
                raise RuntimeError(
                    f"LadybugDB's {table} table has no {name} column."
                    " The model has changed since the table was declared - run"
                    " GraphConnection().initialise_graph() to bring the database up to date."
                )

            expressions[name] = f"CAST({variable}.{name} AS {column_type})"

        return expressions

    @staticmethod
    def _set_clause(keyword: str, variable: str, expressions: dict[str, str]) -> str:
        """Build one SET clause, or nothing where there is nothing to set.

        Args:
            keyword (str): "SET", "ON MATCH SET" or "ON CREATE SET".
            variable (str): the node or relationship variable to set on.
            expressions (dict[str, str]): property name to the expression to set it to.

        Returns:
            str: the clause, or an empty string.
        """
        if not expressions:
            return ""

        assignments = ", ".join(f"{variable}.{name} = {expression}" for name, expression in expressions.items())

        return f"{keyword} {assignments}"

    def create_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Create nodes with the given properties.

        Only the primary label is written: a Ladybug node belongs to one table, so the
        secondary and inheritable labels the other engines add are not applied here.

        Args:
            labels (list): the labels the class declares. Only its primary label is used.
            pp_key (str): the primary property for the nodes.
            properties (list): one dictionary per node, with `pp` and `props`.
            node_class (type[BaseNodeT]): the type of nodes to create.

        Returns:
            list: the created nodes.
        """
        if not properties:
            return []

        self._ensure_node_table(node_class)

        label = gql_identifier_adapter.validate_strings(node_class.__primarylabel__)
        primary = gql_identifier_adapter.validate_strings(pp_key)

        # the primary property reaches here as `pp`, and depending on the caller is in
        # `props` as well - create() pops it out, create_nodes() leaves it in. Writing it
        # under its own name makes the row the same either way
        rows = [{**(entry.get("props") or {}), primary: entry["pp"]} for entry in properties]

        expressions = self._casts(label, rows[0], "node")

        # the primary property is written by the CREATE pattern, not set afterwards
        key_expression = expressions.pop(primary)

        cypher = f"""
        UNWIND $node_list AS node
        CREATE (n:{label} {{{primary}: {key_expression}}})
        {self._set_clause("SET", "n", expressions)}
        RETURN n
        """

        results = self.evaluate_query(cypher, {"node_list": rows}, {node_class.__primarylabel__: node_class})

        return results.nodes

    def merge_nodes(self, labels: list, pp_key: str, properties: list, node_class: type[BaseNodeT]) -> list[BaseNodeT]:
        """Merge nodes on their primary label and primary property.

        Args:
            labels (list): the labels the class declares. Only its primary label is used.
            pp_key (str): the primary property for the nodes.
            properties (list): one dictionary per node, with `pp`, `always_set`,
                `set_on_match` and `set_on_create`.
            node_class (type[BaseNodeT]): the class of the nodes to be merged.

        Returns:
            list: the merged nodes.
        """
        if not properties:
            return []

        self._ensure_node_table(node_class)

        label = gql_identifier_adapter.validate_strings(node_class.__primarylabel__)
        primary = gql_identifier_adapter.validate_strings(pp_key)

        # one flat row per node: a nested empty dictionary cannot be bound, and the three
        # buckets partition the properties, so flattening them cannot lose or shadow one
        rows = [
            {
                **(entry.get("always_set") or {}),
                **(entry.get("set_on_match") or {}),
                **(entry.get("set_on_create") or {}),
                primary: entry["pp"],
            }
            for entry in properties
        ]

        def clause(keyword: str, bucket: str) -> str:
            keys = [key for key in (properties[0].get(bucket) or {}) if key != primary]

            return self._set_clause(keyword, "n", self._casts(label, keys, "node"))

        key_expression = self._casts(label, [primary], "node")[primary]

        cypher = f"""
        UNWIND $node_list AS node
        MERGE (n:{label} {{{primary}: {key_expression}}})
        {clause("ON MATCH SET", "set_on_match")}
        {clause("ON CREATE SET", "set_on_create")}
        {clause("SET", "always_set")}
        RETURN n
        """

        results = self.evaluate_query(cypher, {"node_list": rows}, {node_class.__primarylabel__: node_class})

        return results.nodes

    def delete_nodes(self, label: str, pp_key: str, pp_values: list[Any]) -> None:
        """Delete nodes with a specific label and primary property value.

        Args:
            label (str): the label of the nodes to delete.
            pp_key (str): the primary property key to match on.
            pp_values (list[Any]): the primary property values to delete.
        """
        if not pp_values or not self._has_table(gql_identifier_adapter.validate_strings(label)):
            return

        super().delete_nodes(label, pp_key, pp_values)

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
            source_label (str): the label of the source node.
            target_label (str): the label of the target node.
            source_prop (str): the property of the source node to match on.
            target_prop (str): the property of the target node to match on.
            rel_type (str): the type of relationship to create or merge.
            merge_on_props (list[str]): the properties that identify the relationship.
            rel_props (list[dict]): one dictionary per relationship, with `source_prop`,
                `target_prop`, `always_set`, `set_on_match` and `set_on_create`.
        """
        if not rel_props:
            return

        source = gql_identifier_adapter.validate_strings(source_label)
        target = gql_identifier_adapter.validate_strings(target_label)

        self._ensure_relationship_table(rel_type, source, target)

        rel_table = gql_identifier_adapter.validate_strings(rel_type)
        source_key = gql_identifier_adapter.validate_strings(source_prop)
        target_key = gql_identifier_adapter.validate_strings(target_prop)

        rows = [
            {
                **(entry.get("always_set") or {}),
                **(entry.get("set_on_match") or {}),
                **(entry.get("set_on_create") or {}),
                "source_prop": entry["source_prop"],
                "target_prop": entry["target_prop"],
            }
            for entry in rel_props
        ]

        merge_expressions = self._casts(rel_table, merge_on_props, "rel")

        merge_pattern = (
            " {" + ", ".join(f"{name}: {expression}" for name, expression in merge_expressions.items()) + "}"
            if merge_expressions
            else ""
        )

        def clause(keyword: str, bucket: str) -> str:
            keys = [key for key in (rel_props[0].get(bucket) or {}) if key not in merge_expressions]

            return self._set_clause(keyword, "r", self._casts(rel_table, keys, "rel"))

        # An end declared as a parent class has to reach a subclass node, which on
        # Ladybug lives in a table of its own. Matching the ends as a union the way a
        # read does is not available here - Ladybug will not create a relationship
        # whose ends are bound by several labels, since that does not say which pair of
        # tables to write into - so the write is run once per concrete pair instead.
        # A pair the relationship table does not connect is left out rather than
        # erroring, and a pair holding no matching nodes simply writes nothing, which
        # together give the same result as one query over the union.
        connected = self._pairs(rel_table)

        pairs = [
            (source_table, target_table)
            for source_table in self._class_tables(source)
            for target_table in self._class_tables(target)
            if (source_table, target_table) in connected
        ]

        if not pairs:
            raise RuntimeError(
                f"LadybugDB's {rel_table} table does not connect {source} to {target}."
                " Declare the relationship's ends with GraphConnection().initialise_graph()."
            )

        for source_table, target_table in pairs:
            # each end's value is cast to that table's own column type
            source_cast = f"CAST(rel.source_prop AS {self._table_columns(source_table)[source_key]})"
            target_cast = f"CAST(rel.target_prop AS {self._table_columns(target_table)[target_key]})"

            cypher = f"""
            UNWIND $rel_list AS rel
            MATCH (source:{source_table})
            WHERE source.{source_key} = {source_cast}
            MATCH (target:{target_table})
            WHERE target.{target_key} = {target_cast}
            MERGE (source)-[r:{rel_table}{merge_pattern}]->(target)
            {clause("ON MATCH SET", "set_on_match")}
            {clause("ON CREATE SET", "set_on_create")}
            {clause("SET", "always_set")}
            """

            self.evaluate_query_single(cypher, {"rel_list": rows})


class LadybugConfig(GraphEngineConfig):
    """Configuration for an embedded LadybugDB database.

    Args:
        db_path (str): where the database lives. ":memory:", the default, keeps it in
            memory for the life of the process; anything else is a path on disk, which
            Ladybug creates if it is not there - though its parent directory has to
            exist already. Taken from the LADYBUG_PATH environment variable where it is
            not given.
        auto_create (bool): whether to declare a table the first time something is
            written to it. Off by default: Ladybug is schema first, so
            `initialise_graph()` declares the schema and a write to an undeclared label
            is an error saying so. Turn it on to have tables follow the models instead.
    """

    engine: ClassVar[type[GraphEngineBase]] = LadybugEngine

    db_path: str = ":memory:"
    auto_create: bool = False

    @model_validator(mode="before")
    @classmethod
    def populate_path(cls, data: Any) -> Any:
        """Take the database path from the environment where it was not given.

        `env_fields` is not used for this: the base validator raises when a field it
        lists has no value and no environment variable, and an embedded database has a
        sensible default instead.

        Args:
            data (Any): the values the config is being built from.

        Returns:
            Any: the same values, with db_path filled in where the environment names one.
        """
        if isinstance(data, dict) and not data.get("db_path"):
            load_dotenv()

            from_env = os.getenv("LADYBUG_PATH")

            if from_env:
                data["db_path"] = from_env

        return data
