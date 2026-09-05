# Neontology: Neo4j, Python and Pydantic

> *Easily ingest data into a GQL (Graph Query Language) graph database like Neo4j using Python and Pydantic.*

Neontology is a simple object-graph mapper which lets you use [Pydantic](https://pydantic-docs.helpmanual.io/) models to define Nodes and Relationships. It imposes certain restrictions on how you model data, which aims to make life easier for most users in areas like the construction of knowledge graphs and development of graph database web applications.

Neontology is inspired by projects like py2neo (which is no longer maintained), Beanie and SQLModel.

## Installation

```bash
pip install neontology
```

The core works with plain Python dictionaries. Optional extras add pandas dataframe support and the experimental in-memory NetworkX backend:

```bash
pip install neontology[pandas]
pip install neontology[grand]
pip install neontology[all]
```

## A Simple Example

```python
from typing import ClassVar, Optional
from neontology import BaseNode, BaseRelationship, init_neontology, Neo4jConfig

# We define nodes by inheriting from BaseNode
class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"
    
    name: str
    age: int

# We define relationships by inheriting from BaseRelationship
class FollowsRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "FOLLOWS"
    
    source: PersonNode
    target: PersonNode

# initialise the connection to the database
config = Neo4jConfig(
    uri="neo4j+s://mydatabaseid.databases.neo4j.io", 
    username="neo4j",
    password="<PASSWORD>"
)
init_neontology(config) 

# Define a couple of people
alice = PersonNode(name="Alice", age=40)

bob = PersonNode(name="Bob", age=40)

# Create them in the database
alice.create()
bob.create()

# Create a follows relationship between them
rel = FollowsRel(source=bob,target=alice)
rel.merge()

# We can also create many nodes at once from a list of dictionaries
node_records = [{"name": "Freddy", "age": 42}, {"name": "Philippa", "age": 42}]

PersonNode.merge_records(node_records)

# Relationships work the same way, using the primary property values of the nodes
rel_records = [
    {"source": "Freddy", "target": "Philippa"},
    {"source": "Alice", "target": "Freddy"}
]

FollowsRel.merge_records(rel_records)
```
