# Advanced Usage

## Use multiple labels

Nodes can carry labels beyond their primary label. There are two ways to add them, which
differ in what happens when a class is subclassed.

### Secondary labels

`__secondarylabels__` lists extra labels for a class:

```python
class ElephantNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Elephant"
    __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
    name: str

ellie = ElephantNode(name="Ellie")
```

This is an ordinary class attribute: a subclass which does not declare its own inherits its
parent's, and a subclass which declares its own **replaces** its parent's.

### Inheritable labels

`__inheritablelabels__` lists labels carried by the class *and every class that inherits
from it*. A subclass cannot replace them - its own labels are added alongside.

This is how to build a hierarchy which reads naturally in the graph itself. A class listing
its own primary label as inheritable passes that label down to all of its subclasses:

```python
class Person(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Person"
    __inheritablelabels__: ClassVar[list[str]] = ["Person"]
    name: str


class Employee(Person):
    __primarylabel__: ClassVar[Optional[str]] = "Employee"   # written as :Employee:Person
    employer: str


class Manager(Employee):
    __primarylabel__: ClassVar[Optional[str]] = "Manager"    # written as :Manager:Person
```

`Manager` is not labelled `:Employee`, because `Employee` does not list its own label as
inheritable: each class decides whether its label passes down. An abstract class can declare
inheritable labels too, which labels a whole branch of your models without the abstract
class needing a label of its own.

### Querying a hierarchy

A node is built as the most derived class its labels allow, so a `:Employee:Person` node
comes back as an `Employee` however you query it - `MATCH (p:Person)`, `get_related()`, or
a method on `Person`. Queries on a parent class therefore return its subclasses as
themselves:

```python
Person.match_nodes()   # [Person(name='bob'), Employee(name='alice', employer='Acme')]
Person.match("alice")  # Employee(name='alice', employer='Acme')
Person.get_count()     # 2
```

`delete()` follows the same rule, so `Person.delete("alice")` deletes the employee named
alice.

Only a class which inherits from `Person` may carry the `Person` label. A class carrying the
primary label of a class it does not inherit from - through either kind of label - raises a
`DuplicateLabelWarning` when it is defined, or a `DuplicateLabelError` in
[strict mode](#duplicate-labels), because its nodes would match queries for `Person`
without being people.

### How labels affect identity

A node is identified by its primary label and primary property. `merge()` finds an existing
node on those alone and then adds the class' other labels, so adding a label to a model
updates the nodes already in the graph the next time they are merged, rather than
duplicating them. Merging never removes a label: one taken out of a model stays on nodes
already written, and is reported as unexpected when they are read back.

A class carrying its parent's label shares its parent's identity: `Person` nodes are
identified by the `Person` label and `name`, and employees carry both. A uniqueness
constraint on `Person.name` - such as `auto_constrain()` applies - therefore covers employees
as well. Merging an `Employee` does not turn an existing `Person` with the same name into
one: it creates a new node, which that constraint will reject.

## How Neontology finds your models

Neontology needs to know your model classes in order to turn query results back into
them. You do not have to declare or register anything: **defining a class is what
registers it**.

```python
class Person(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Person"
    name: str
```

From that point on, any query returning a `Person` node comes back as a `Person`.

Because registration happens when the class is defined, **a model has to be imported
before a query runs for its results to come back typed**. If you keep your models in a
`models.py`, importing that module during application startup is enough. Nodes whose
label Neontology does not recognise are left out of `result.nodes` with a warning.

Abstract classes - those with `__primarylabel__ = None`, or which never declare a
`__primarylabel__` at all - exist to share properties between models. They are never
registered, because they are never written to the graph, and instantiating one raises
`NotImplementedError`.

To look up the models Neontology knows about, use `get_node_types()` and
`get_rels_by_type()`. They do not need a database connection, so they work anywhere your
models have been imported:

```python
from neontology import get_node_types, get_rels_by_type

get_node_types()      # {"Person": <class 'Person'>, ...}
get_rels_by_type()    # {"FOLLOWS": RelationshipTypeData(...), ...}
```

Pass an abstract class to either to scope the lookup to that branch of your models -
`get_node_types(MyAbstractBase)`.

### Duplicate labels

Two model classes claiming the same primary label is a problem: only one of them can be
used to build results, so data written as one comes back as the other. Neontology warns
as soon as the second class is defined, naming both:

```text
DuplicateLabelWarning: primary label 'Person' is claimed by both myapp.models.Person
and myapp.other.Person. Only one of them can be used to build query results, so data
written as one will come back as the other. Give them distinct names.
```

To make that an error instead, turn on strict mode before your models are imported:

```python
from neontology import registry

registry.strict = True   # raises DuplicateLabelError instead of warning
```

You can also escalate the warning with Python's own machinery:

```python
import warnings

from neontology import DuplicateLabelWarning

warnings.simplefilter("error", DuplicateLabelWarning)
```

Re-running a notebook cell or reloading a module is not treated as a clash - that is the
same model being defined again, not two models fighting over one label.

### Inherited labels

A subclass which does not declare its own `__primarylabel__` inherits its parent's and
takes over that label, so nodes written as the parent come back as the subclass with
defaults invented for any fields that were never stored. That is rarely intended, so
Neontology raises an `InheritedLabelWarning`. Give the subclass its own label, or set
`__primarylabel__ = None` to make it abstract:

```python
class Animal(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Animal"
    name: str


class Dog(Animal):
    __primarylabel__: ClassVar[Optional[str]] = "Dog"   # its own label
```

## Type Conversion / Serialization

Not all graph databases natively support the same range of types as Python/Pydantic. Therefore, model fields annotated with complex types may need to go through some level of conversion before being written to the database. This can be achieved with Pydantic's [custom serializers](https://docs.pydantic.dev/2.9/concepts/serialization/#custom-serializers).

```
from pydantic import field_serializer
from uuid import UUID

class ElephantNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Elephant"
    __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
    name: str
    id: UUID

    @field_serializer("id")
    def serialize_ref_url(self, id: UUID, _info):
        return str(id)
```

## Retrieving related nodes and properties with BaseNode methods

The power of GQL comes from the ability to quickly traverse relationships to understand what how a node relates to the rest of the graph. Neontology aims to make this easier by helping you run GQL directly from BaseNode models to find related nodes and properties - even if that involves traversing multiple hops to find what you're looking for.

!!! EXPERIMENTAL
    Support for these features is still experimental so may change in the future.

### get_related()

BaseNode subclasses have a `get_related` method which can be used to find nodes and relationships which are related to a BaseNode instance.

If no arguments are given, this function will return all nodes with a direct outgoing relationship from the Node. However you can also specify keyword arguments to be more specific about which relationships you care about. For example:

* `relationship_types` - list of one or more relationship types to look for (such as 'FOLLOWS').
* `target_label` - the label of the target node you want to match on.
* `incoming` - whether to include incoming relationships.
* `outgoing` - whether to include outgoing relationships.
* `limit` - the maximum number of nodes to return.

The return type is a [NeontologyResult object](/queries/#querying-for-neontology-nodes-and-relationships) which will include identified nodes and relationships.

### @related_nodes Decorator

If you write a method on a Node, that returns a cyber/GQL string then adding the `@related_nodes` decorator will evaluate the GQL and return any Nodes returned by the query as Neontology Node objects.

If you use `(#ThisNode)`, it will get replaced with the specific node that the method is called from (based on primary label and primary property).

```python
@related_nodes
def followers(self):
    return "MATCH (#ThisNode)<-[:FOLLOWS]-(o) RETURN o"
```

### @related_properties Decorator

This decorator works like above, but instead of returning nodes, it expects the cypher/GQL to return a single object (such as a string, a list or a dict/mapping). Under the hood, it uses `evaluate_query_single`. Again, use `(#ThisNode)` to match on the given Node.

```python
@property
@related_property
def follower_count(self):
    return "MATCH (#ThisNode)<-[:FOLLOWS]-(o) RETURN COUNT(DISTINCT o)"
```

### Returning parameters

A decorated method may return the query on its own, or a `(query, parameters)` pair if
the query needs parameters:

```python
@related_nodes
def followers_since(self, year):
    return "MATCH (#ThisNode)<-[r:FOLLOWS]-(o) WHERE r.year > $year RETURN o", {"year": year}
```

Returning just the query is the common case; any keyword arguments the method was
called with are used as the parameters.

### Caching results

Both decorators run their query every time the method is called. That is the right
default for a database mapper - the graph can change between calls - but it means a
view that reads the same relationship several times pays for it each time.

For an accessor that takes no arguments, `functools.cached_property` composes with the
decorators and caches the result for the lifetime of the node object:

```python
from functools import cached_property

class PersonNode(BaseNode):
    ...

    @cached_property
    @related_property
    def follower_count(self):
        return "MATCH (#ThisNode)<-[:FOLLOWS]-(o) RETURN COUNT(o)"
```

The query then runs once per node instance. In a web application where a node is
fetched to render a request, that scopes the cache to the request, which is usually
what you want. To recompute, drop the cached value:

```python
person.__dict__.pop("follower_count", None)
```

Two things to be aware of:

- Nothing invalidates the cache when the graph changes. If you merge data and then read
  a cached property on a node object you already had, you will get the previous answer.
  Fetch the node again, or drop the cached value, after writing.
- A cached property is not reported by `get_related_property_methods()`, because the
  attribute on the class is the `cached_property` rather than the decorated function.
  The same is already true of a plain `@property`.

### Example

We can put this all together to add some handy extra functionality to nodes - for example, making it easy to access followers in a social graph.

```python
from neontology import BaseNode

class AugmentedPerson(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "name"
    __primarylabel__: ClassVar[GQLIdentifier] = "AugmentedPerson"

    name: str

    @related_nodes
    def followers(self):
        return "MATCH (#ThisNode)<-[:FOLLOWS]-(o) RETURN o"

    @property
    @related_property
    def follower_count(self):
        return "MATCH (#ThisNode)<-[:FOLLOWS]-(o) RETURN COUNT(DISTINCT o)"

    @property
    @related_property
    def follower_names(self):
        return "MATCH (#ThisNode)<-[:FOLLOWS]-(o) RETURN COLLECT(DISTINCT o.name)"


class FollowsRelationship(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "FOLLOWS"

    source: AugmentedPerson
    target: AugmentedPerson

```

We could then use this like:

```python
alice = AugmentedPerson(name="Alice")
alice.merge()

bob = AugmentedPerson(name="Bob")
bob.merge()

follows = AugmentedPersonRelationship(
    source=alice, target=bob
)
follows.merge()

follows2 = AugmentedPersonRelationship(
    source=bob, target=alice
)
follows2.merge()

# get people Alice follows (this will return Bob)
alice_rels = alice.get_related_nodes(relationship_types=["FOLLOWS"])

print(bob.follower_count)

# 1

print(alice.follower_names)

# ["Bob"]

```

## Set properties on match or on create

When we run MERGE operations with neo4j, sometimes we want to only alter properties under certain circumstances.

!!! NOTE
    From v1.0.0, changes in v2 of Pydantic mean that these properties are now defined in a dict called 'json_schema_extra' rather than directly on the field.

You can control this behaviour in Neontology by passing certain parameters in the 'json_schema_extra' dictionary when you define fields:

```python
from typing import ClassVar, Optional
from pydantic import Field
from neontology import BaseNode

class MyNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "my_id"
        __primarylabel__: ClassVar[Optional[str]] = "MyNode"
        
        my_id: str = "test_node"
        only_set_on_match: Optional[str] = Field(json_schema_extra={"set_on_match": True})
        only_set_on_create: Optional[str] = Field(json_schema_extra={"set_on_create": True})
        normal_field: str
```

!!! NOTE
    Fields which are 'set_on_match' must be optional as they will be None/null when the node is first created.

## Controlling merge relationships

When merging relationships, we might want to merge on certain properties to avoid creating an excessive number of relationships.

!!! NOTE
    From v1.0.0, changes in v2 of Pydantic mean that this property is now defined in a dict called 'json_schema_extra' rather than directly on the field.

To do this use the 'merge_on' key in the 'json_schema_extra' parameter when defining a field.

```python
from typing import ClassVar, Optional
from pydantic import Field
from neontology import BaseRelationship

class MyRel(BaseRelationship):
        __relationshiptype__: ClassVar[Optional[str]] = "MY_RELATIONSHIP_TO"

        source: MyNode
        target: MyNode
        
        prop_to_merge_on: str = Field(json_schema_extra={"merge_on": True})
```

In this example, where a relationship with a given source and target exists with the same value for 'prop_to_merge_on', the relationship will be overwritten. If a new 'prop_to_merge_on' value is given then a new relationship will be created with that value.
