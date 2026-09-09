"""Descriptions of the schema objects a graph database can hold.

These are *database* schema objects - constraints and indexes that live in the
backend - as opposed to the model schemas in `neontology.schema_utils`, which
describe Pydantic classes.

They are read types. Callers ask for a constraint or an index through a named
method (`apply_uniqueness_constraint`, `apply_index`), and get these back from
`get_constraints()` / `get_indexes()`. The value read back is what gets passed to
`drop_constraint()` / `drop_index()`, because the two backends identify schema
objects differently: Neo4j names them, Memgraph identifies them by pattern. A name
alone would not round trip on Memgraph, and a pattern alone loses information on
Neo4j, so the whole description travels.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Sequence, Union

from pydantic import BaseModel


class ConstraintType(str, Enum):
    """The kind of guarantee a constraint makes."""

    # A label/property combination is unique. The only kind neontology creates,
    # and the only kind supported by every engine that supports constraints.
    UNIQUENESS = "uniqueness"

    # The property must be present. Memgraph community supports this; on Neo4j it
    # requires Enterprise Edition.
    EXISTENCE = "existence"

    # Unique *and* present. Neo4j Enterprise only.
    NODE_KEY = "node_key"

    # Something the database reported that neontology does not model. Kept so
    # get_constraints() can describe a database faithfully rather than hiding
    # constraints it did not create - those still need to be listed and dropped.
    OTHER = "other"


class SchemaEntity(str, Enum):
    """Whether a schema object applies to nodes or to relationships."""

    NODE = "node"
    RELATIONSHIP = "relationship"


def normalise_properties(properties: Union[str, Sequence[str], None]) -> tuple[str, ...]:
    """Accept a single property name or a sequence of them.

    Args:
        properties (Union[str, Sequence[str], None]): one property name, several, or none.

    Returns:
        tuple[str, ...]: the property names as a tuple.
    """
    if properties is None:
        return ()

    if isinstance(properties, str):
        return (properties,)

    return tuple(properties)


class SchemaObject(BaseModel):
    """Shared identity for a constraint or an index.

    Args:
        label (str): the node label or relationship type it applies to.
        properties (tuple[str, ...]): the properties it covers, in order. Empty for
            Memgraph's label-only indexes.
        entity (SchemaEntity): whether it applies to nodes or relationships.
        name (Optional[str]): the database's name for it, where the database names
            them. Memgraph does not, so this is None there.
    """

    label: str
    properties: tuple[str, ...] = ()
    entity: SchemaEntity = SchemaEntity.NODE
    name: Optional[str] = None


class Constraint(SchemaObject):
    """A constraint that exists in the database.

    Args:
        constraint_type (ConstraintType): the guarantee it makes.
    """

    constraint_type: ConstraintType = ConstraintType.UNIQUENESS


class Index(SchemaObject):
    """An index that exists in the database.

    Only indexes neontology could meaningfully manage are reported - see
    `GraphEngineBase.get_indexes`.
    """
