# Usage

This guide will help you quickly start using Python and pandas with Neo4j.

[Jupyter](https://jupyter.org/) notebooks are a great way to get started playing with neontology.

We will build a simple model for ingesting data which looks like this:

```cypher
(p1:Person)-[:FOLLOWS]->(p2:Person)
```

## Defining nodes

Graphs in Neo4j are made up of Nodes and Relationships (or edges). In neontology, both nodes and relationships are defined as Python objects.

To create a new node type to be modeled in your graph, simply create a python type which inherits from neontology's `BaseNode` class (which itself inherits from Pydantic's `BaseModel`).

```python
from typing import ClassVar, Optional
import pandas as pd
from neontology import BaseNode, BaseRelationship, init_neontology

class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"
    
    name: str
    age: Optional[int] = None
```

Lets see what we've defined here:

* `__primarylabel__` - this is the label which will be applied to nodes in the graph
* `__primaryproperty__` - this is the property which neontology will use to MATCH these types of node. We are using the 'name' property.
* `name` this is a field on our model which is of type `str` (a string) - it will map to a property when we create a node.
* `age` another property. In this case it is optional and by default gets set to `None`.

## Defining relationships

We can define relationships by specifying the types node which are used for the source and target of the relationship. We must also specify the string to use for the relationship type when creating it in Neo4j.

```python
class FollowsRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "FOLLOWS"
    
    source: PersonNode
    target: PersonNode
```

Here we've created a relationship which takes a PersonNode as source and another as target. The relationship will be stored in the database with the `FOLLOWS` relationship type.

## Connecting to Neo4j with Python

We use the `init_neontology` method to set up our connection.

```python
NEO4J_URI="neo4j+s://<database id>.databases.neo4j.io"  # neo4j Aura example
NEO4J_USERNAME="neo4j"
NEO4J_PASSWORD="<your password>"

init_neontology(
    neo4j_uri=NEO4J_URI,
    neo4j_username=NEO4J_USERNAME,
    neo4j_password=NEO4J_PASSWORD
)   # initialise the connection to the database
```

## Creating a node

We can now instantiate some node objects and use the `merge()` or `create()` method to push them to the database.

```python
alice = PersonNode(name="Alice", age=40)
alice.create()
bob = PersonNode(name="Bob", age=40)
bob.merge()
```

## Creating the relationship

Similarly, we can now create a relationship between the two nodes.

```python
rel = FollowsRel(source=bob,target=alice)
rel.merge()
```

## Populating Neo4j with pandas data

Often we might have data in a pandas dataframe which we want to push to Neo4j according to the model which we have defined.

neontology makes this easy!

For nodes, just create a dataframe where each column represents the fields/properties of the nodes.

```python
node_records = [{"name": "Freddy", "age": 42}, {"name": "Philipa", "age":42}]
node_df = pd.DataFrame.from_records(node_records)

PersonNode.merge_df(node_df)
```

Similarly, if you have the values for the primary properties of sources and targets, you can push relationships to Neo4j using a pandas dataframe. If the relationship has additional properties, these should also be columns on the dataframe.

```python
rel_records = [
    {"source": "Freddy", "target": "Philipa"},
    {"source": "Alice", "target": "Freddy"}
]
rel_df = pd.DataFrame.from_records(rel_records)

FollowsRel.merge_df(rel_df)
```

## Putting it all together

Running the above code (full example below) should create the following nodes and relationships in Neo4j:

```cypher
(Bob:Person)-[:FOLLOWS]->(Alice:Person)-[:FOLLOWS]->(Freddy:Person)-[:FOLLOWS]->(Philipa:Person)
```

### Neo4j and Python Sample Code

```python
# demo.py
from typing import ClassVar, Optional
import pandas as pd
from neontology import BaseNode, BaseRelationship, init_neontology


class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    age: Optional[int] = None


class FollowsRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "FOLLOWS"

    source: PersonNode
    target: PersonNode


init_neontology()  # initialise the connection to the database

alice = PersonNode(name="Alice", age=40)
alice.create()
bob = PersonNode(name="Bob", age=40)
bob.merge()

rel = FollowsRel(source=bob, target=alice)
rel.merge()

node_records = [{"name": "Freddy", "age": 42}, {"name": "Philipa", "age": 42}]
node_df = pd.DataFrame.from_records(node_records)

PersonNode.merge_df(node_df)

rel_records = [
    {"source": "Freddy", "target": "Philipa"},
    {"source": "Alice", "target": "Freddy"},
]
rel_df = pd.DataFrame.from_records(rel_records)

FollowsRel.merge_df(rel_df)
```
