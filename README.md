# Neontology: Neo4j, Python and Pydantic

Easily ingest data into a Neo4j graph database with Python, Pydantic and pandas. Neontology is a simple object-graph mapper which lets you use [Pydantic](https://pydantic-docs.helpmanual.io/) models to define Nodes and Relationships. It imposes certain restrictions on how you model data, which aims to make life easier for most users but may provide too many limitations for others. The focus of Neontology is getting data into the database, for running complex queries and accessing data, consider using the Neo4j browser or bloom.

Read the documentation [here](https://neontology.readthedocs.io/en/latest/)

## Note on v1

With v1, we have upgraded to Pydantic v2 which brings some major changes (and improvements!). Read their [migration guide](https://docs.pydantic.dev/2.0/migration/) to see what changes you might need to make to your models.

## Installation

```bash
pip install neontology
```

## Example

```python
from typing import ClassVar, Optional, List
import pandas as pd
from neontology import BaseNode, BaseRelationship, init_neontology, auto_constrain

# We define nodes by inheriting from BaseNode
class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"
    __secondarylabels__: ClassVar[Optional[List]] = ["individual", "somebody"]
    
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

# We can also use pandas DataFrames to create multiple nodes
node_records = [{"name": "Freddy", "age": 42}, {"name": "Philipa", "age":42}]
node_df = pd.DataFrame.from_records(node_records)

PersonNode.merge_df(node_df)

# We can also merge relationships from a pandas DataFrame, using the primary property values of the nodes
rel_records = [
    {"source": "Freddy", "target": "Philipa"},
    {"source": "Alice", "target": "Freddy"}
]
rel_df = pd.DataFrame.from_records(rel_records)

FollowsRel.merge_df(rel_df)
```

## Configuring your graph connection

### With a dotenv file

You can use a `.env` file as below, which should automatically get picked up by neontology.

```txt
# .env
NEO4J_URI=neo4j+s://myneo4j.example.com
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<PASSWORD>
```

With the above environment variables defined, you can just use `init_neontology()` without providing any arguments.

### On initialisation

Alternatively, you can explicitly provide access information:

```python
init_neontology(
    neo4j_uri="neo4j+s://mydatabaseid.databases.neo4j.io",
    neo4j_username="neo4j",
    neo4j_password="password"
)
```
