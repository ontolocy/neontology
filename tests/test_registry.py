"""Model classes register themselves as they are defined.

These cover the registry itself. The behaviour it replaced - type discovery by walking
`__subclasses__()` - is still covered by the existing tests in test_utils.py, which are
deliberately unchanged so they prove the rewiring did not alter what is discovered.

Labels are passed as values rather than written as literals wherever two classes need to
claim the same one, because `test_no_duplicate_primary_labels_in_the_suite` scans the
test files for repeated literal labels.
"""

import gc
import warnings
from typing import ClassVar, Optional

import pytest

from neontology import (
    Capability,
    DuplicateLabelError,
    DuplicateLabelWarning,
    InheritedLabelWarning,
    registry,
)
from neontology.basenode import BaseNode
from neontology.baserelationship import BaseRelationship
from neontology.utils import get_node_types, get_rels_by_type


def _node(name: str, base: type = BaseNode, **namespace):
    """Build a node class dynamically, so labels can be passed rather than written."""
    namespace.setdefault("__annotations__", {"pp": str})
    namespace.setdefault("__primaryproperty__", "pp")

    return type(name, (base,), namespace)


def _rel(name: str, source: type, target: type, rel_type: Optional[str], base: type = BaseRelationship):
    """Build a relationship class dynamically."""
    return type(
        name,
        (base,),
        {"__annotations__": {"source": source, "target": target}, "__relationshiptype__": rel_type},
    )


class TestRegistration:
    """Defining a class is what puts it in the registry."""

    def test_node_is_registered_when_defined(self):
        label = "RegistryDefinedNode"

        _node("DefinedNode", __primarylabel__=label)

        assert get_node_types()[label].__name__ == "DefinedNode"

    def test_relationship_is_registered_when_defined(self):
        node = _node("RelHomeNode", __primarylabel__="RegistryRelHomeNode")

        _rel("DefinedRel", node, node, "REGISTRY_DEFINED_REL")

        assert "REGISTRY_DEFINED_REL" in get_rels_by_type()

    def test_abstract_nodes_are_not_registered(self):
        """Both spellings of abstract: an explicit None, and never declaring one."""
        explicit = _node("AbstractExplicit", __primarylabel__=None)
        implicit = _node("AbstractImplicit")

        registered = get_node_types().values()

        assert explicit not in registered
        assert implicit not in registered

    def test_abstract_relationships_are_not_registered(self):
        node = _node("AbstractRelNode", __primarylabel__="RegistryAbstractRelNode")

        abstract = _rel("AbstractRel", node, node, None)

        assert abstract not in [data.relationship_class for data in get_rels_by_type().values()]

    def test_scoping_by_base_type(self):
        """Passing an abstract parent scopes discovery to that branch."""
        parent = _node("ScopedParent", __primarylabel__=None)

        _node("ScopedA", base=parent, __primarylabel__="RegistryScopedA")
        _node("ScopedB", base=parent, __primarylabel__="RegistryScopedB")

        assert set(get_node_types(parent)) == {"RegistryScopedA", "RegistryScopedB"}


class TestClashes:
    """A label claimed twice is reported when the second class is defined."""

    def test_duplicate_primary_label_warns_naming_both(self):
        label = "RegistryDuplicateLabel"

        _node("DuplicateOne", __primarylabel__=label)

        with pytest.warns(DuplicateLabelWarning) as caught:
            _node("DuplicateTwo", __primarylabel__=label)

        message = str(caught[0].message)

        assert label in message
        assert "DuplicateOne" in message
        assert "DuplicateTwo" in message

    def test_duplicate_relationship_type_warns(self):
        node = _node("DuplicateRelNode", __primarylabel__="RegistryDuplicateRelNode")
        rel_type = "REGISTRY_DUPLICATE_REL"

        _rel("DuplicateRelOne", node, node, rel_type)

        with pytest.warns(DuplicateLabelWarning, match=rel_type):
            _rel("DuplicateRelTwo", node, node, rel_type)

    def test_strict_mode_raises_instead_of_warning(self):
        label = "RegistryStrictLabel"

        _node("StrictOne", __primarylabel__=label)

        registry.strict = True

        try:
            with pytest.raises(DuplicateLabelError, match=label):
                _node("StrictTwo", __primarylabel__=label)

        finally:
            registry.strict = False

    def test_redefining_the_same_class_is_not_a_clash(self):
        """Re-running a notebook cell or reloading a module must stay quiet.

        Each pass builds a different class object for the same source definition, which
        is a replacement rather than two models fighting over one label.
        """
        label = "RegistryRedefinedLabel"

        def define():
            return _node("Redefined", __primarylabel__=label)

        define()

        with _no_registry_warnings():
            define()
            define()

        assert get_node_types()[label].__name__ == "Redefined"


class TestInheritedLabels:
    """A subclass that inherits a concrete label takes it over, silently, without this."""

    def test_inheriting_a_concrete_label_warns(self):
        parent = _node("InheritParent", __primarylabel__="RegistryInheritParent")

        with pytest.warns(InheritedLabelWarning) as caught:
            _node("InheritChild", base=parent)

        message = str(caught[0].message)

        assert "RegistryInheritParent" in message
        assert "InheritChild" in message

    def test_declaring_its_own_label_does_not_warn(self):
        parent = _node("OwnLabelParent", __primarylabel__="RegistryOwnLabelParent")

        with _no_registry_warnings():
            _node("OwnLabelChild", base=parent, __primarylabel__="RegistryOwnLabelChild")

    def test_re_abstracting_a_subclass_does_not_warn(self):
        """Setting the label back to None is the documented way to make a class abstract."""
        parent = _node("ReAbstractParent", __primarylabel__="RegistryReAbstractParent")

        with _no_registry_warnings():
            child = _node("ReAbstractChild", base=parent, __primarylabel__=None)

        assert child not in get_node_types().values()


class TestLifetime:
    """The registry holds references, so models do not disappear when gc runs."""

    def test_a_model_defined_in_a_function_stays_registered(self):
        """Discovery by __subclasses__() held only weak references.

        A model defined inside a function and not referenced elsewhere vanished from
        discovery whenever the garbage collector happened to run.
        """
        label = "RegistryTransientNode"

        def define_and_discard():
            _node("Transient", __primarylabel__=label)

        define_and_discard()

        gc.collect()

        assert label in get_node_types()


class TestForwardReferences:
    """A relationship may be defined before the nodes it points at."""

    def test_unresolved_relationship_appears_once_rebuilt(self):
        class RegistryForwardRel(BaseRelationship):
            __relationshiptype__: ClassVar[Optional[str]] = "REGISTRY_FORWARD_REL"

            source: "RegistryForwardNode"
            target: "RegistryForwardNode"

        # registered, but not resolvable yet - and asking must not raise
        assert "REGISTRY_FORWARD_REL" not in get_rels_by_type()

        class RegistryForwardNode(BaseNode):
            __primaryproperty__: ClassVar[str] = "pp"
            __primarylabel__: ClassVar[Optional[str]] = "RegistryForwardNode"

            pp: str

        RegistryForwardRel.model_rebuild()

        resolved = get_rels_by_type()

        assert resolved["REGISTRY_FORWARD_REL"].source_class is RegistryForwardNode

    def test_a_new_node_class_updates_relationship_subclasses(self):
        """Cached relationship data must not go stale when a subclass is defined."""
        parent = _node("SubExpandParent", __primarylabel__=None)
        _node("SubExpandA", base=parent, __primarylabel__="RegistrySubExpandA")

        rel = _rel("SubExpandRel", parent, parent, "REGISTRY_SUB_EXPAND")

        assert rel  # defined for the side effect of registering

        before = {c.__primarylabel__ for c in get_rels_by_type()["REGISTRY_SUB_EXPAND"].all_source_classes}

        _node("SubExpandB", base=parent, __primarylabel__="RegistrySubExpandB")

        after = {c.__primarylabel__ for c in get_rels_by_type()["REGISTRY_SUB_EXPAND"].all_source_classes}

        assert before == {"RegistrySubExpandA"}
        assert after == {"RegistrySubExpandA", "RegistrySubExpandB"}


class TestLabelHandling:
    """The resolved label set and label validation."""

    def test_all_labels_is_primary_then_secondary(self):
        node = _node(
            "AllLabelsNode",
            __primarylabel__="RegistryAllLabelsNode",
            __secondarylabels__=["RegistryExtraOne", "RegistryExtraTwo"],
        )

        assert node._all_labels() == ["RegistryAllLabelsNode", "RegistryExtraOne", "RegistryExtraTwo"]

    def test_secondary_labels_are_validated(self):
        """Secondary labels reach cypher too, so they are held to the same standard."""
        node = _node(
            "BadSecondaryNode",
            __primarylabel__="RegistryBadSecondaryNode",
            __secondarylabels__=["not a valid label!"],
        )

        with pytest.warns(UserWarning, match="Secondary Label"):
            node(pp="x")


class TestDeprecations:
    """refresh_classes is meaningless now the registry is never stale."""

    @pytest.mark.requires_capability(Capability.UNDECLARED_SCHEMA)
    def test_refresh_classes_is_deprecated(self, use_graph):
        with pytest.warns(DeprecationWarning, match="refresh_classes"):
            use_graph.evaluate_query("MATCH (n:RegistryNeverCreatedNode) RETURN n", refresh_classes=True)

    @pytest.mark.requires_capability(Capability.UNDECLARED_SCHEMA)
    def test_not_passing_refresh_classes_does_not_warn(self, use_graph):
        import warnings as warnings_module

        with warnings_module.catch_warnings(record=True) as caught:
            warnings_module.simplefilter("always")

            use_graph.evaluate_query("MATCH (n:RegistryNeverCreatedNode) RETURN n")

        assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []


class _no_registry_warnings:
    """Assert no registry warning is raised inside the block."""

    def __enter__(self):
        import warnings as warnings_module

        self._manager = warnings_module.catch_warnings(record=True)
        self._caught = self._manager.__enter__()

        warnings_module.simplefilter("always")

        return self._caught

    def __exit__(self, *exc_info):
        registry_warnings = [w for w in self._caught if issubclass(w.category, (DuplicateLabelWarning, InheritedLabelWarning))]

        self._manager.__exit__(*exc_info)

        assert not registry_warnings, f"unexpected registry warnings: {[str(w.message) for w in registry_warnings]}"

        return False


class TestAbstractModels:
    """Abstract models cannot be instantiated, however they are spelled."""

    def test_both_spellings_of_abstract_node_raise_the_same_error(self):
        """Never declaring a label used to raise AttributeError instead."""
        explicit = _node("AbstractRaisesExplicit", __primarylabel__=None)
        implicit = _node("AbstractRaisesImplicit")

        for abstract in (explicit, implicit):
            with pytest.raises(NotImplementedError, match="abstract node"):
                abstract(pp="x")

    def test_abstract_node_does_not_warn_about_its_label(self):
        """The label is absent by design, not malformed.

        Instantiating one used to warn that the primary label was not alphanumeric,
        which described the wrong problem.
        """
        abstract = _node("AbstractQuiet", __primarylabel__=None)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            with pytest.raises(NotImplementedError):
                abstract(pp="x")

        assert [w for w in caught if "alphanumeric" in str(w.message)] == []

    def test_abstract_relationship_raises_and_does_not_warn(self):
        node = _node("AbstractRelHome", __primarylabel__="RegistryAbstractRelHome")
        abstract = _rel("AbstractRelRaises", node, node, None)

        source = node(pp="a")
        target = node(pp="b")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            with pytest.raises(NotImplementedError, match="abstract"):
                abstract(source=source, target=target)

        assert [w for w in caught if "alphanumeric" in str(w.message)] == []

    def test_a_malformed_primary_label_still_warns(self):
        """Skipping the check for abstract models must not skip it for real ones."""
        node = _node("MalformedLabelNode", __primarylabel__="Not A Valid Label!")

        with pytest.warns(UserWarning, match="Primary Label should contain"):
            node(pp="x")

    def test_a_malformed_relationship_type_still_warns(self):
        node = _node("MalformedRelHome", __primarylabel__="RegistryMalformedRelHome")
        rel = _rel("MalformedRel", node, node, "Not A Valid Type!")

        with pytest.warns(UserWarning, match="Relationship type should contain"):
            rel(source=node(pp="a"), target=node(pp="b"))


class TestConnectionPropertiesAreDeprecated:
    """Which models you have defined is not a property of the connection."""

    def test_global_nodes_is_deprecated(self, use_graph):
        with pytest.warns(DeprecationWarning, match="global_nodes"):
            result = use_graph.global_nodes

        assert result == get_node_types()

    def test_global_rels_is_deprecated(self, use_graph):
        with pytest.warns(DeprecationWarning, match="global_rels"):
            result = use_graph.global_rels

        assert result == get_rels_by_type()

    @pytest.mark.requires_capability(Capability.UNDECLARED_SCHEMA)
    def test_querying_does_not_use_the_deprecated_properties(self, use_graph):
        """Internal use had to move off them, or every query would warn."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            use_graph.evaluate_query("MATCH (n:RegistryNeverCreatedNode) RETURN n")

        assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []


class TestPackageExports:
    """The recommended API should be reachable from the package root."""

    def test_type_lookups_are_importable_from_neontology(self):
        import neontology

        for name in ("get_node_types", "get_rels_by_type", "get_rels_by_source", "get_rels_by_target"):
            assert hasattr(neontology, name), f"{name} should be exported from neontology"
            assert name in neontology.__all__

    def test_looking_up_models_does_not_need_a_connection(self):
        """The point of keeping these as functions rather than connection properties."""
        import neontology

        assert callable(neontology.get_node_types)


class TestDefiningAModelDuringALookup:
    """A model class defined while a lookup is running must not break the lookup.

    With threads, another thread's import can define a class while a query is looking up
    models. Here the class is defined from inside the lookup itself, which does the same
    thing to the registry, deterministically.
    """

    @staticmethod
    def _base_type_defining(define):
        """A base type to scope a lookup with, which defines a model the first time it is asked."""

        class DefinesAModel:
            defined = False

            def __subclasscheck__(self, subclass):
                if not DefinesAModel.defined:
                    DefinesAModel.defined = True
                    define()

                return False

        return DefinesAModel()

    def test_node_lookup(self):
        _node("LookupExistingNode", __primarylabel__="RegistryLookupExistingNode")

        base = self._base_type_defining(lambda: _node("LookupNewNode", __primarylabel__="RegistryLookupNewNode"))

        assert get_node_types(base) == {}

    def test_relationship_lookup(self):
        node = _node("LookupRelHome", __primarylabel__="RegistryLookupRelHome")
        _rel("LookupExistingRel", node, node, "REGISTRY_LOOKUP_EXISTING_REL")

        base = self._base_type_defining(lambda: _rel("LookupNewRel", node, node, "REGISTRY_LOOKUP_NEW_REL"))

        assert get_rels_by_type(base) == {}
