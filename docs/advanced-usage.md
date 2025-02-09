# Advanced Usage

## Use multiple labels

Sometimes you may want to apply additional labels to nodes, beyond just the primary label. Where this is the case, you can add those labels as a list using the class variable `__secondarylabels__`.

```python
class ElephantNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Elephant"
    __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
    name: str

ellie = ElephantNode(name="Ellie")
```

Note that methods such as `.match` use only the primary label.

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
