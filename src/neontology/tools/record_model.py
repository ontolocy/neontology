"""A pydantic type for the records `import_records()` reads, built from your models.

A program producing content - a language model with structured output, most often - can be
held to it: `record_model().model_json_schema()` is the JSON Schema of a valid list of
records, and the type parses what was produced, ready to import. It describes the simplest
form of the format only - flat records, one node or relationship each, with a single
`SOURCE` and `TARGET` - since the more of the format a schema allows, the worse constrained
decoding does with it.
"""

from __future__ import annotations

from copy import copy
from typing import Any, Iterable, Literal, Optional, Union, cast

from pydantic import BaseModel, ConfigDict, RootModel, create_model

from ..basenode import BaseNode
from ..baserelationship import BaseRelationship
from ..utils import generate_relationship_type_data, get_node_types, get_rels_by_type

# a key the import does not know is an error there, so it is one here
_RECORD_CONFIG = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


# Literal and Union built from values known only at runtime - the labels and types of
# whichever models are defined - which a type annotation cannot spell, hence __getitem__


def _literal(values: Iterable[str]) -> Any:
    """`Literal` of some values, built at runtime."""
    return cast(Any, Literal).__getitem__(tuple(values))


def _union(types: list[Any]) -> Any:
    """One type accepting any of several, or the type itself where there is only one."""
    return types[0] if len(types) == 1 else cast(Any, Union).__getitem__(tuple(types))


def _record_fields(model: type[BaseModel], skip: tuple[str, ...] = ()) -> dict[str, Any]:
    """A model's fields, named by the key a record gives each - its alias, where it has one."""
    fields: dict[str, Any] = {}

    for name, field in model.model_fields.items():
        if name in skip:
            continue

        # named by its key and the alias dropped, so a record dumps with the keys the import reads
        record_field = copy(field)
        record_field.alias = record_field.validation_alias = record_field.serialization_alias = None

        fields[field.alias or name] = (field.annotation, record_field)

    return fields


def _primary_property_type(node_classes: Iterable[type[BaseNode]]) -> Any:
    """The type of the primary property of any of some node classes, which end a relationship."""
    types: list[Any] = []

    for node_class in node_classes:
        annotation = node_class.model_fields[node_class.__primaryproperty__].annotation

        if annotation not in types:
            types.append(annotation)

    return _union(types)


def _node_record(node_class: type[BaseNode]) -> type[BaseModel]:
    label = cast(str, node_class.__primarylabel__)

    return create_model(
        f"{node_class.__name__}Record",
        __config__=_RECORD_CONFIG,
        __doc__=node_class.__doc__,
        LABEL=(_literal([label]), ...),
        **_record_fields(node_class),
    )


def _relationship_record(rel_class: type[BaseRelationship]) -> type[BaseModel]:
    data = generate_relationship_type_data(rel_class)

    sources = get_node_types(data.source_class)
    targets = get_node_types(data.target_class)

    return create_model(
        f"{rel_class.__name__}Record",
        __config__=_RECORD_CONFIG,
        __doc__=rel_class.__doc__,
        RELATIONSHIP_TYPE=(_literal([cast(str, rel_class.__relationshiptype__)]), ...),
        SOURCE_LABEL=(_literal(sources), ...),
        TARGET_LABEL=(_literal(targets), ...),
        SOURCE=(_primary_property_type(sources.values()), ...),
        TARGET=(_primary_property_type(targets.values()), ...),
        **_record_fields(rel_class, skip=("source", "target")),
    )


def record_model(models: Optional[Iterable[type[Union[BaseNode, BaseRelationship]]]] = None) -> type[RootModel]:
    """Build a pydantic type for a list of import records, describing your models.

    Each record is a node - `LABEL` and its properties - or a relationship - its
    `RELATIONSHIP_TYPE`, the `SOURCE_LABEL` and `TARGET_LABEL` its class allows, `SOURCE`
    and `TARGET` primary property values, and its properties - each typed as the model
    declares it, with no key the import would reject. Records parsed with it dump straight
    into `import_records()`:

        Records = record_model()
        records = Records.model_validate(produced)
        import_records([record.model_dump(exclude_unset=True) for record in records.root])

    It checks the shape of the content rather than everything the import does: your
    models' validators are not part of it, and endpoints are not resolved. Import with
    `validate_only=True` for that.

    Args:
        models (Optional[Iterable[type]]): the node and relationship classes records may
            describe. Abstract node classes are left out, having no label to write.
            Defaults to every model defined.

    Returns:
        type[RootModel]: the type of a list of records. Its `model_json_schema()` is the
        JSON Schema of one.

    Raises:
        ValueError: if there is no concrete node or relationship class to describe.
    """
    if models is None:
        node_classes = list(get_node_types().values())
        rel_classes = [data.relationship_class for data in get_rels_by_type().values()]

    else:
        models = list(models)
        node_classes = [m for m in models if issubclass(m, BaseNode) and not m._is_abstract()]
        rel_classes = [m for m in models if issubclass(m, BaseRelationship) and not m._is_abstract()]

    records = [_node_record(n) for n in node_classes]
    records += [_relationship_record(r) for r in rel_classes]

    if not records:
        raise ValueError("There is no concrete node or relationship class to describe records of.")

    # subscripted at runtime, for the reason _literal() is
    root = cast(Any, RootModel)[cast(Any, list)[_union(records)]]

    return create_model("NeontologyRecords", __base__=root)
