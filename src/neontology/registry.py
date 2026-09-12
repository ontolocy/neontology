"""The registry of node and relationship types Neontology knows about.

Model classes register themselves here as they are defined, through
`__pydantic_init_subclass__` on `BaseNode` and `BaseRelationship`. Nothing has to be
declared or passed in: defining a class is what registers it, which is what makes
querying "just work" - results come back as the models you defined.

Registering at definition time rather than discovering at read time is what makes that
dependable:

- a label claimed twice is reported *when the second class is defined*, naming both,
  rather than being resolved silently by whichever the class hierarchy happened to be
  walked into first
- the registry holds references, so a model defined inside a function does not quietly
  disappear from it when the garbage collector runs
- the answer is built once as classes are defined, rather than recomputed by walking
  `__subclasses__()` on every query

A class still has to be imported or defined before a query runs for its results to come
back typed - that is a property of Python, not of the registry.
"""

from __future__ import annotations

import warnings
from collections import defaultdict
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .basenode import BaseNode
    from .baserelationship import BaseRelationship, RelationshipTypeData


class DuplicateLabelWarning(UserWarning):
    """Two different model classes claim the same primary label or relationship type.

    Only one of them can be used to build results, so the other's data comes back as the
    wrong class. Filter or escalate this like any warning::

        warnings.simplefilter("error", DuplicateLabelWarning)
    """


class InheritedLabelWarning(UserWarning):
    """A subclass has inherited a concrete primary label instead of declaring its own.

    The subclass takes over that label in the registry, so rows written as the parent
    come back as the subclass, with defaults invented for fields that were never stored.
    Give the subclass its own `__primarylabel__`, or set `__primarylabel__ = None` to
    make it abstract.
    """


class DuplicateLabelError(ValueError):
    """Raised instead of `DuplicateLabelWarning` when the registry is in strict mode."""


def _describe(cls: type) -> str:
    """Identify a class by module and qualified name, for error messages.

    Args:
        cls (type): the class to describe.

    Returns:
        str: something like "myapp.models.Person".
    """
    return f"{cls.__module__}.{cls.__qualname__}"


def _is_redefinition(existing: type, new: type) -> bool:
    """Report whether two classes are the same definition, defined twice.

    Re-running a notebook cell or reloading a module produces a genuinely different
    class object for the same source definition. That is a replacement, not a clash, and
    must not be reported as one - otherwise interactive use becomes unusable.

    Args:
        existing (type): the class already registered.
        new (type): the class being registered.

    Returns:
        bool: True if these are the same definition.
    """
    return existing.__module__ == new.__module__ and existing.__qualname__ == new.__qualname__


class Registry:
    """The model classes Neontology knows about, keyed by label and relationship type."""

    #: When True, a label claimed by two different classes raises `DuplicateLabelError`
    #: instead of warning. Off by default, because a clash that an application has been
    #: living with should not stop it from importing. Set it before your models are
    #: imported, which is when registration happens::
    #:
    #:     from neontology import registry
    #:
    #:     registry.strict = True
    strict: bool

    def __init__(self) -> None:
        self._nodes: dict[str, type[BaseNode]] = {}
        self._relationships: dict[str, type[BaseRelationship]] = {}

        # RelationshipTypeData is derived from the relationship class and the node
        # hierarchy, so it is built on demand and dropped whenever a node registers
        self._rel_data: dict[str, RelationshipTypeData] = {}

        self.strict = False

    def _report_clash(self, kind: str, key: str, existing: type, new: type) -> None:
        """Warn, or raise in strict mode, that two classes claim the same key.

        Args:
            kind (str): what the key is, for the message ("primary label").
            key (str): the label or relationship type claimed twice.
            existing (type): the class already registered.
            new (type): the class claiming it now.

        Raises:
            DuplicateLabelError: if the registry is in strict mode.
        """
        message = (
            f"{kind} {key!r} is claimed by both {_describe(existing)} and {_describe(new)}."
            f" Only one of them can be used to build query results, so data written as one"
            f" will come back as the other. Give them distinct names."
        )

        if self.strict:
            raise DuplicateLabelError(message)

        warnings.warn(message, DuplicateLabelWarning, stacklevel=2)

    def register_node(self, cls: type[BaseNode]) -> None:
        """Register a node class under its primary label.

        Called as the class is defined. Abstract classes - those with no primary label -
        are skipped, exactly as they are skipped by type discovery.

        Args:
            cls (type[BaseNode]): the node class being defined.
        """
        # an "abstract" node has no label and never goes in the graph. Both spellings
        # count: __primarylabel__ = None, and never declaring it at all.
        label = getattr(cls, "__primarylabel__", None)

        if label is None:
            return

        existing = self._nodes.get(label)

        if "__primarylabel__" not in cls.__dict__:
            # inherited a concrete label rather than declaring one. Reported on its own
            # rather than as a clash: the cause and the fix are specific.
            warnings.warn(
                (
                    f"{_describe(cls)} inherits __primarylabel__ {label!r} from"
                    f" {_describe(cls.__mro__[1])} rather than declaring its own, and takes over"
                    f" that label. Nodes written as {label!r} will come back as {cls.__name__}."
                    f" Give it its own __primarylabel__, or set __primarylabel__ = None to make"
                    f" it abstract."
                ),
                InheritedLabelWarning,
                stacklevel=2,
            )

        elif existing is not None and existing is not cls and not _is_redefinition(existing, cls):
            self._report_clash("primary label", label, existing, cls)

        self._nodes[label] = cls

        # a new node class can change which subclasses a relationship's source and
        # target expand to, so anything derived from the hierarchy is now stale
        self._rel_data.clear()

    def register_relationship(self, cls: type[BaseRelationship]) -> None:
        """Register a relationship class under its relationship type.

        Called as the class is defined. Abstract relationships - those with no
        relationship type - are skipped.

        Args:
            cls (type[BaseRelationship]): the relationship class being defined.
        """
        rel_type = getattr(cls, "__relationshiptype__", None)

        if rel_type is None:
            return

        existing = self._relationships.get(rel_type)

        if existing is not None and existing is not cls and not _is_redefinition(existing, cls):
            self._report_clash("relationship type", rel_type, existing, cls)

        self._relationships[rel_type] = cls
        self._rel_data.pop(rel_type, None)

    def nodes(self, base_type: Optional[type] = None) -> dict[str, type[BaseNode]]:
        """Get the registered node classes, keyed by primary label.

        Args:
            base_type (Optional[type]): if given, only classes of this type. Passing an
                abstract node class is how you scope to one branch of your models.

        Returns:
            dict[str, type[BaseNode]]: node classes by primary label.
        """
        if base_type is None:
            return dict(self._nodes)

        return {label: cls for label, cls in self._nodes.items() if issubclass(cls, base_type)}

    def relationships(self, base_type: Optional[type] = None) -> dict[str, RelationshipTypeData]:
        """Get the registered relationship types, keyed by relationship type.

        Source and target are resolved here rather than at registration, because a
        relationship may be defined before the node classes it points at - those
        annotations are unresolved `ForwardRef`s until `model_rebuild()` runs. A
        relationship whose nodes are not yet resolved is left out and tried again next
        time, so it appears as soon as it can be built.

        Args:
            base_type (Optional[type]): if given, only relationships of this type.

        Returns:
            dict[str, RelationshipTypeData]: relationship type data by relationship type.
        """
        from .utils import _resolved_relationship_nodes, generate_relationship_type_data

        # a defaultdict, as this has always returned, so reading a relationship type
        # that is not defined gives an empty entry rather than a KeyError
        resolved: dict[str, RelationshipTypeData] = defaultdict(dict)  # type: ignore[arg-type]

        for rel_type, cls in self._relationships.items():
            if base_type is not None and not issubclass(cls, base_type):
                continue

            cached = self._rel_data.get(rel_type)

            if cached is not None:
                resolved[rel_type] = cached
                continue

            nodes = _resolved_relationship_nodes(cls)

            if nodes is None:
                continue

            data = generate_relationship_type_data(cls, nodes)

            self._rel_data[rel_type] = data
            resolved[rel_type] = data

        return resolved


#: The registry every model registers itself into.
registry = Registry()
