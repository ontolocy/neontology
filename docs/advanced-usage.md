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

## Running Simple Single Queries

If you want to quickly execute a query which returns a single result, you can use the `execute_query_single` method on a `GraphConnection`. This will return whatever Python type the underlying Neo4j driver returns. For example, if you return a number, or a list of strings, you will get that straight back. If you return a node or relationship, you will get these in the underlying Neo4j driver's [types](https://neo4j.com/docs/python-manual/current/data-types/).

You can also pass in parameters (for example to use as part of a WHERE clause).

```python
from neontology import init_neontology, GraphConnection

init_neontology()

gc = GraphConnection()

cypher_query = """
MATCH (p:Person)
RETURN COLLECT({name: p.name})
"""

result = gc.evaluate_query_single(cypher_query)

print(result)

# [{'name': 'Alice'}, {'name': 'Bob'}]

cypher_query_count = """
MATCH (p:Person)
RETURN COUNT(DISTINCT p)
"""

result_count = gc.evaluate_query_single(cypher_query_count)

print(result_count)

# 2

cypher_query_params = """
MATCH (p:Person)
WHERE p.name = $name
RETURN p.name
"""

params = {"name": "Bob"}

result_params = gc.evaluate_query_single(cypher_query_params, params)

print(result)

# 'Bob'

```

## Querying for Neontology Nodes and Relationships

If you want to run a cypher query and get back the Neo4j nodes and relationships directly as Neontology's Pydantic models, you can use the `evaluate_query` method on a `GraphConnection`.

This will search the Python environment for Neontology for defined nodes and relationships and use them to 'rehydrate' your query results.

```python
from neontology import init_neontology, GraphConnection, BaseNode

init_neontology()

gc = GraphConnection()

class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"
    
    name: str
    age: Optional[int] = None

bob = PersonNode(name="Bob", age=40)
bob.merge()

cypher = "MATCH p RETURN p"

results = gc.evaluate_query(cypher)

print(results.nodes[0].name)

# Bob

```

The returned `NeontologyResult` object has the following properties:

* `records_raw` - the raw records returned by the Neo4j driver
* `records` - the records converted into equivalent Neontology objects
* `nodes` - a list of all the Neontology/Pydantic nodes returned
* `relationships` - a list of all the Neontology/Pydantic relationships returned
* `node_link_data` - a dictionary with 'nodes' and 'links' keys and corresponding values which can be used with other tools such as NetworkX and D3.

## Retrieving related nodes and properties with BaseNode methods

The power of GQL comes from the ability to quickly traverse relationships to understand what how a node relates to the rest of the graph. Neontology aims to make this easier by helping you run GQL directly from BaseNode models to find related nodes and properties - even if that involves traversing multiple hops to find what you're looking for.

!!! EXPERIMENTAL
    Support for these features is still experimental so may change in the future.

### get_related_nodes()

BaseNode subclasses have a `get_related_nodes` method which can be used to find nodes which are related to a BaseNode instance.

If no arguments are given, this function will return all nodes with a direct outgoing relationship from the Node. However you can also specify keyword arguments to be more specific about which relationships you care about. For example:

* `relationship_types` - list of one or more relationship types to look for (such as 'FOLLOWS').
* `target_label` - the label of the target node you want to match on.
* `incoming` - whether to include incoming relationships.
* `outgoing` - whether to include outgoing relationships.
* `limit` - the maximum number of nodes to return.

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

## Type Conversion/Coersion

Not all graph databases natively support the same range of types as Python/Pydantic. Therefore, we do our best to coerce data types to fit into the database appropriately. However, you may see some data loss when converting between complex data types.
