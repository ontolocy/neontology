# Getting Started

Easily ingest data into a Neo4j graph database with Python, Pydantic and pandas. Neontology is a simple object-graph mapper which lets you use Pydantic compatible models to define Nodes and Relationships.

## Install Neontology

```bash
pip install neontology
```

## A simple example

```python
from typing import ClassVar
from neontology import BaseNode, BaseRelationship, init_neontology, auto_constrain

NEO4J_URI="neo4j+s://dbid.databases.neo4j.io"
NEO4J_USERNAME="neo4j"
NEO4J_PASSWORD="don't keep your password in version control"

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
init_neontology(
    neo4j_uri=NEO4J_URI,
    neo4j_username=NEO4J_USERNAME,
    neo4j_password=NEO4J_PASSWORD
)   

# Define a couple of people
alice = PersonNode(name="Alice", age=40)

bob = PersonNode(name="Bob", age=40)

# Create them in the database
alice.create()
bob.create()

# Create a follows relationship between them
rel = FollowsRel(source=bob,target=alice)
rel.merge()
```
