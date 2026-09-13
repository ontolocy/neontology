"""Labels shared across a class hierarchy.

A node carries every label its class declares: its primary label, its secondary labels,
and any inheritable labels declared by it or an ancestor. When a subclass carries an
ancestor's primary label - employees are also :Person - the graph reads as an ontology
natively, so `MATCH (p:Person)` finds employees too.

That only works if Neontology can still tell which class a node is. These tests pin that:

- a node is built as the most derived class its labels allow
- queries scoped to a class return its subclasses, built as themselves
- merging identifies a node by primary label and primary property alone, so changing a
  model's other labels updates existing nodes rather than duplicating them
- a class carrying the primary label of a class it does not inherit from is reported
  when it is defined, because its nodes would match queries for a class they are not

Labels needed by more than one class are passed as values through `_node`, because
`test_no_duplicate_primary_labels_in_the_suite` scans the test files for literal labels.
"""

import warnings
from typing import ClassVar, Optional

import pytest
from rawresult import raw_labels

from neontology import BaseNode, BaseRelationship, DuplicateLabelError, DuplicateLabelWarning, registry


def _node(name: str, base: type = BaseNode, **namespace):
    """Build a node class dynamically, so labels can be passed rather than written."""
    namespace.setdefault("__annotations__", {"pp": str})
    namespace.setdefault("__primaryproperty__", "pp")

    return type(name, (base,), namespace)


class _no_label_warnings:
    """Assert nothing inside the block warns about labels.

    Covers both hydration ("Unexpected ... labels returned") and registration
    (DuplicateLabelWarning), which are the two places a hierarchy could be misreported.
    """

    def __enter__(self):
        self._manager = warnings.catch_warnings(record=True)
        self._caught = self._manager.__enter__()

        warnings.simplefilter("always")

        return self._caught

    def __exit__(self, *exc_info):
        label_warnings = [
            str(w.message)
            for w in self._caught
            if issubclass(w.category, DuplicateLabelWarning) or "label" in str(w.message).lower()
        ]

        self._manager.__exit__(*exc_info)

        assert not label_warnings, f"unexpected label warnings: {label_warnings}"

        return False


# A subclass carrying its parent's primary label as a secondary label. This is the shape
# that used to break: the node's labels matched two registered classes, so it was dropped
# from general queries and built as the parent - failing validation - by scoped ones.


class LblPerson(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "LblPerson"

    name: str


class LblEmployee(LblPerson):
    __primarylabel__: ClassVar[Optional[str]] = "LblEmployee"
    __secondarylabels__: ClassVar[list[str]] = ["LblPerson"]

    employer: str = "Acme"


class LblKnows(BaseRelationship):
    __relationshiptype__: ClassVar[Optional[str]] = "LBL_KNOWS"

    source: LblPerson
    target: LblPerson


# Labels accumulated down a hierarchy with __inheritablelabels__.


class LblLivingThing(BaseNode):
    """Abstract: it has no label of its own, but everything below it is an organism."""

    __primaryproperty__: ClassVar[str] = "name"
    __inheritablelabels__: ClassVar[list[str]] = ["LblOrganism"]

    name: str


class LblAnimal(LblLivingThing):
    __primarylabel__: ClassVar[Optional[str]] = "LblAnimal"
    __inheritablelabels__: ClassVar[list[str]] = ["LblAnimal"]


class LblDog(LblAnimal):
    __primarylabel__: ClassVar[Optional[str]] = "LblDog"
    __secondarylabels__: ClassVar[list[str]] = ["LblPet"]

    breed: Optional[str] = None


class LblPuppy(LblDog):
    __primarylabel__: ClassVar[Optional[str]] = "LblPuppy"
    __secondarylabels__: ClassVar[list[str]] = ["LblYoung"]


def _by_name(nodes: list) -> dict:
    """Map node name to class name, which is what hydration is being tested on."""
    return {node.name: type(node).__name__ for node in nodes}


class TestBuildingTheMostDerivedClass:
    """A node whose labels match several classes is built as the most derived one."""

    @pytest.fixture
    def people(self, use_graph):
        bob = LblPerson(name="bob")
        alice = LblEmployee(name="alice", employer="Initech")

        bob.create()
        alice.create()

        return bob, alice

    def test_a_general_query_builds_the_subclass(self, use_graph, people):
        with _no_label_warnings():
            result = use_graph.evaluate_query("MATCH (n:LblPerson) RETURN n")

        assert _by_name(result.nodes) == {"bob": "LblPerson", "alice": "LblEmployee"}

    def test_match_on_the_parent_returns_the_subclass(self, people):
        with _no_label_warnings():
            alice = LblPerson.match("alice")

        assert type(alice) is LblEmployee
        assert alice.employer == "Initech"

    def test_match_nodes_on_the_parent_returns_each_as_itself(self, people):
        with _no_label_warnings():
            nodes = LblPerson.match_nodes()

        assert _by_name(nodes) == {"bob": "LblPerson", "alice": "LblEmployee"}

    def test_count_on_the_parent_agrees_with_match_nodes(self, people):
        assert LblPerson.get_count() == len(LblPerson.match_nodes()) == 2

    def test_match_nodes_on_the_subclass_is_unaffected(self, people):
        with _no_label_warnings():
            nodes = LblEmployee.match_nodes()

        assert _by_name(nodes) == {"alice": "LblEmployee"}

    def test_related_nodes_include_the_subclass(self, people):
        bob, alice = people

        LblKnows(source=bob, target=alice).merge()

        with _no_label_warnings():
            result = bob.get_related()

        assert _by_name(result.nodes) == {"bob": "LblPerson", "alice": "LblEmployee"}
        assert len(result.relationships) == 1
        assert type(result.relationships[0].target) is LblEmployee

    def test_match_relationships_includes_the_subclass(self, people):
        bob, alice = people

        LblKnows(source=bob, target=alice).merge()

        with _no_label_warnings():
            rels = LblKnows.match_relationships()

        assert len(rels) == 1
        assert type(rels[0].source) is LblPerson
        assert type(rels[0].target) is LblEmployee


class TestInheritableLabels:
    """__inheritablelabels__ are carried by the declaring class and all its descendants."""

    def test_labels_accumulate_down_the_hierarchy(self):
        assert set(LblAnimal._all_labels()) == {"LblAnimal", "LblOrganism"}
        assert set(LblDog._all_labels()) == {"LblDog", "LblPet", "LblAnimal", "LblOrganism"}
        assert set(LblPuppy._all_labels()) == {"LblPuppy", "LblYoung", "LblAnimal", "LblOrganism"}

    def test_secondary_labels_still_replace_rather_than_accumulate(self):
        """Declaring __secondarylabels__ replaces the parent's, as it always has."""
        assert "LblPet" not in LblPuppy._all_labels()

    def test_secondary_labels_are_still_inherited_when_not_redeclared(self):
        child = _node("SecondaryInheritChild", base=LblDog, __primarylabel__="LblSecondaryInheritChild")

        assert "LblPet" in child._all_labels()

    def test_primary_label_comes_first_and_labels_are_not_repeated(self):
        for model in (LblAnimal, LblDog, LblPuppy):
            labels = model._all_labels()

            assert labels[0] == model.__primarylabel__
            assert len(labels) == len(set(labels))

    def test_secondary_and_inheritable_labels_combine_on_one_class(self):
        node = _node(
            "CombinedLabels",
            __primarylabel__="LblCombinedLabels",
            __secondarylabels__=["LblCombinedSecondary"],
            __inheritablelabels__=["LblCombinedInheritable"],
        )

        assert node._all_labels() == ["LblCombinedLabels", "LblCombinedSecondary", "LblCombinedInheritable"]

    def test_inheritable_labels_are_validated(self):
        """They reach cypher like any other label, so are held to the same standard."""
        node = _node(
            "BadInheritable",
            __primarylabel__="LblBadInheritable",
            __inheritablelabels__=["not a valid label!"],
        )

        with pytest.warns(UserWarning, match="should contain only alphanumeric"):
            node(pp="x")

    def test_schema_lists_every_label(self):
        schema = LblPuppy.neontology_schema(include_outgoing_rels=False)

        assert set(schema.secondary_labels) == {"LblYoung", "LblAnimal", "LblOrganism"}

    def test_written_nodes_carry_every_label(self, use_graph):
        LblPuppy(name="rex").create()
        LblPuppy(name="fido").merge()

        for name in ("rex", "fido"):
            result = use_graph.evaluate_query("MATCH (n:LblPuppy) WHERE n.name = $name RETURN n", {"name": name})

            assert raw_labels(result) == {"LblPuppy", "LblYoung", "LblAnimal", "LblOrganism"}

    def test_an_ancestor_label_finds_every_descendant(self, use_graph):
        LblAnimal(name="generic").create()
        LblDog(name="rex").create()
        LblPuppy(name="fido").create()

        expected = {"generic": "LblAnimal", "rex": "LblDog", "fido": "LblPuppy"}

        with _no_label_warnings():
            assert _by_name(LblAnimal.match_nodes()) == expected
            assert _by_name(use_graph.evaluate_query("MATCH (n:LblOrganism) RETURN n").nodes) == expected

        assert LblAnimal.get_count() == 3

        # LblDog does not list its own label as inheritable, so puppies are not :LblDog -
        # each class decides whether its label passes down
        assert LblDog.get_count() == 1


def _evolving_model(secondary: list):
    """Define the same model with different secondary labels, as editing it would."""

    class LblEvolving(BaseNode):
        __primaryproperty__: ClassVar[str] = "name"
        __primarylabel__: ClassVar[Optional[str]] = "LblEvolving"
        __secondarylabels__: ClassVar[list[str]] = secondary

        name: str
        version: int = 0

    return LblEvolving


class TestMergeIdentity:
    """A node is identified by its primary label and primary property, not its full label set."""

    def test_adding_a_label_to_a_model_updates_existing_nodes(self, use_graph):
        _evolving_model([])(name="x", version=1).merge()

        with warnings.catch_warnings():
            # the node is returned before the new label is visible to hydration on some
            # engines; what matters here is what is in the graph afterwards
            warnings.simplefilter("ignore")

            _evolving_model(["LblAddedLabel"])(name="x", version=2).merge()

        result = use_graph.evaluate_query("MATCH (n:LblEvolving) WHERE n.name = 'x' RETURN n")

        assert len(result.records) == 1
        assert raw_labels(result) == {"LblEvolving", "LblAddedLabel"}
        assert result.nodes[0].version == 2

    def test_merge_does_not_remove_labels(self, use_graph):
        """Merging adds the model's labels; it never strips labels already on the node."""
        _evolving_model(["LblKeptLabel"])(name="y").merge()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            _evolving_model([])(name="y").merge()

            result = use_graph.evaluate_query("MATCH (n:LblEvolving) WHERE n.name = 'y' RETURN n")

        assert len(result.records) == 1
        assert raw_labels(result) == {"LblEvolving", "LblKeptLabel"}

    def test_merging_a_subclass_twice_does_not_duplicate(self, use_graph):
        LblEmployee(name="alice").merge()
        LblEmployee(name="alice").merge()

        assert LblEmployee.get_count(filters={"name": "alice"}) == 1


class TestDeletingThroughAHierarchy:
    """delete() removes whatever match() would find for that label and key."""

    def test_deleting_through_the_parent_removes_the_subclass_node(self, use_graph):
        LblEmployee(name="erin").create()

        LblPerson.delete("erin")

        assert LblEmployee.get_count(filters={"name": "erin"}) == 0

    def test_deleting_the_subclass_leaves_a_parent_node_with_the_same_key(self, use_graph):
        LblPerson(name="sam").create()
        LblEmployee(name="sam").create()

        LblEmployee.delete("sam")

        assert LblEmployee.get_count(filters={"name": "sam"}) == 0
        assert _by_name(LblPerson.match_nodes()) == {"sam": "LblPerson"}


class TestCarryingAnotherClassesLabel:
    """A primary label may only be carried by subclasses of the class that owns it."""

    def test_carrying_an_unrelated_classes_label_warns_naming_both(self):
        _node("CarriedOwner", __primarylabel__="LblCarriedOwner")

        with pytest.warns(DuplicateLabelWarning) as caught:
            _node("CarriedStranger", __primarylabel__="LblCarriedStranger", __secondarylabels__=["LblCarriedOwner"])

        message = str(caught[0].message)

        assert "LblCarriedOwner" in message
        assert "CarriedOwner" in message
        assert "CarriedStranger" in message

    def test_defining_the_owner_after_the_carrier_warns(self):
        _node("LateCarrier", __primarylabel__="LblLateCarrier", __secondarylabels__=["LblLateOwner"])

        with pytest.warns(DuplicateLabelWarning, match="LblLateOwner"):
            _node("LateOwner", __primarylabel__="LblLateOwner")

    def test_an_inheritable_label_is_checked_too(self):
        _node("InheritedOwner", __primarylabel__="LblInheritedOwner")

        abstract = _node("InheritedAbstract", __inheritablelabels__=["LblInheritedOwner"])

        with pytest.warns(DuplicateLabelWarning, match="LblInheritedOwner"):
            _node("InheritedStranger", base=abstract, __primarylabel__="LblInheritedStranger")

    def test_strict_mode_raises(self):
        _node("StrictCarriedOwner", __primarylabel__="LblStrictCarriedOwner")

        registry.strict = True

        try:
            with pytest.raises(DuplicateLabelError, match="LblStrictCarriedOwner"):
                _node(
                    "StrictCarriedStranger",
                    __primarylabel__="LblStrictCarriedStranger",
                    __secondarylabels__=["LblStrictCarriedOwner"],
                )

        finally:
            registry.strict = False

    def test_a_subclass_may_carry_its_parents_label(self):
        parent = _node("CarryParent", __primarylabel__="LblCarryParent")

        with _no_label_warnings():
            _node("CarryChild", base=parent, __primarylabel__="LblCarryChild", __secondarylabels__=["LblCarryParent"])

    def test_a_descendant_may_inherit_an_ancestors_label(self):
        grandparent = _node(
            "CarryGrandparent", __primarylabel__="LblCarryGrandparent", __inheritablelabels__=["LblCarryGrandparent"]
        )

        with _no_label_warnings():
            parent = _node("CarryMiddle", base=grandparent, __primarylabel__="LblCarryMiddle")
            _node("CarryGrandchild", base=parent, __primarylabel__="LblCarryGrandchild")

    def test_redefining_the_owner_does_not_report_its_existing_subclasses(self):
        """Re-running a notebook cell defining the parent must stay quiet.

        The subclass defined earlier still inherits from the previous definition of the
        parent, which is the same model rather than an unrelated one.
        """

        def define_parent():
            return _node("RedefinedCarryParent", __primarylabel__="LblRedefinedCarryParent")

        parent = define_parent()

        _node(
            "RedefinedCarryChild",
            base=parent,
            __primarylabel__="LblRedefinedCarryChild",
            __secondarylabels__=["LblRedefinedCarryParent"],
        )

        with _no_label_warnings():
            define_parent()

    def test_a_carrier_redefined_without_the_label_is_not_reported(self):
        """Only the current definition of a class counts, as after editing a notebook cell."""

        def define_carrier(secondary):
            return _node("RedefinedCarrier", __primarylabel__="LblRedefinedCarrier", __secondarylabels__=secondary)

        define_carrier(["LblRedefinedCarriedOwner"])
        define_carrier([])

        with _no_label_warnings():
            _node("RedefinedCarriedOwner", __primarylabel__="LblRedefinedCarriedOwner")
