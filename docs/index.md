# Neontology: Neo4j, Python and Pydantic

> *Easily ingest data into a GQL (Graph Query Language) graph database like Neo4j using Python, Pydantic and pandas.*

Neontology is a simple object-graph mapper which lets you use [Pydantic](https://pydantic-docs.helpmanual.io/) models to define Nodes and Relationships. It imposes certain restrictions on how you model data, which aims to make life easier for most users in areas like the construction of knowledge graphs and development of graph database web applications.

Neontology is inspired by projects like py2neo (which is no longer maintained), Beanie and SQLModel.

## Installation

```bash
pip install neontology
```

## A Simple Example

```python
from typing import ClassVar, Optional
import pandas as pd
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

# We can also use pandas DataFrames to create multiple nodes
node_records = [{"name": "Freddy", "age": 42}, {"name": "Philippa", "age":42}]
node_df = pd.DataFrame.from_records(node_records)

PersonNode.merge_df(node_df)

# We can also merge relationships from a pandas DataFrame, using the primary property values of the nodes
rel_records = [
    {"source": "Freddy", "target": "Philippa"},
    {"source": "Alice", "target": "Freddy"}
]
rel_df = pd.DataFrame.from_records(rel_records)

FollowsRel.merge_df(rel_df)
```
