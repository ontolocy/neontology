# Neontology: Neo4j, Python and Pydantic

[![PyPI - Version](https://img.shields.io/pypi/v/neontology)](https://pypi.org/project/neontology/)
[![Read the Docs](https://img.shields.io/readthedocs/neontology)](https://neontology.readthedocs.io/en/latest/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/neontology)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/ontolocy/neontology/ci.yml)
![GitHub License](https://img.shields.io/github/license/ontolocy/neontology)
[![Pydantic v2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://docs.pydantic.dev/)

> *Easily ingest data into a GQL (Graph Query Language) graph database like Neo4j using Python, Pydantic and pandas.*

Neontology is a simple object-graph mapper which lets you use [Pydantic](https://pydantic-docs.helpmanual.io/) models to define Nodes and Relationships. It imposes certain restrictions on how you model data, which aims to make life easier for most users in areas like the construction of knowledge graphs and development of graph database applications.

Neontology is inspired by projects like py2neo (which is no longer maintained), Beanie and SQLModel.

Read the documentation [here](https://neontology.readthedocs.io/en/latest/).

## Installation

```bash
pip install neontology
```

## Example

```python
from typing import ClassVar, Optional, List
import pandas as pd
from neontology import BaseNode, BaseRelationship, init_neontology

# We define nodes by inheriting from BaseNode
class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"
    __secondarylabels__: ClassVar[Optional[List]] = ["Individual", "Somebody"]
    
    name: str
    age: int

# We define relationships by inheriting from BaseRelationship
class FollowsRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "FOLLOWS"
    
    source: PersonNode
    target: PersonNode

# initialise the connection to the database (default is Neo4j)
init_neontology(
    config = {
        "neo4j_uri": "neo4j+s://mydatabaseid.databases.neo4j.io",
        "neo4j_username": "neo4j",
        "neo4j_password": "password"
    }
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

## Configuring your graph connection

### On initialisation

You can explicitly provide access information as in the example above with a `config` dictionary.

### With a dotenv file

You can use a `.env` file as below, which should automatically get picked up by neontology.

```txt
# .env
NEO4J_URI=neo4j+s://myneo4j.example.com
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<PASSWORD>
```

With the above environment variables defined, you can just use `init_neontology()` without providing any arguments.

### Alternative Graph Engines (Memgraph, Kùzu)

Neontology has experimental support for GQL graph databases other than Neo4j:

* Memgraph

#### Memgraph Engine

[Memgraph](https://memgraph.com/) is a Neo4j compatible database.

```python
from neontology import init_neontology
from neontology.graphengines import MemgraphEngine

init_neontology(
    config={
                "memgraph_uri": "bolt://localhost:9687",
                "memgraph_username": "memgraphuser",
                "memgraph_password": "<MEMGRAPH PASSWORD>",
            },
    engine=MemgraphEngine
)
```

You can also use the following environment variables and just `init_neontology(graph_engine=MemgraphEngine)`:

* `MEMGRAPH_URI`
* `MEMGRAPH_USER`
* `MEMGRAPH_PASSWORD`

## Executing Queries

Neontology has limited support for running GQL/cypher queries.

Using a GraphConnection, you can call `evaluate_query` with a GQL query which returns nodes and relationships and get them back as Neontology Nodes Relationships.

Once neontology is initialized, only one connection to the database is used under the hood which can be accessed with `GraphConnection`.

```python
from neontology import init_neontology, GraphConnection

# Only nodes and relationships that have already been defined can be returned as results.
class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"
    
    name: str
    age: int

class FollowsRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "FOLLOWS"
    
    source: PersonNode
    target: PersonNode


init_neontology()

gc = GraphConnection()

cypher_query = """
MATCH (n)
WHERE n.name = $name
OPTIONAL MATCH (n)-[r:FOLLOWS]-(o:Person)
RETURN r, o
"""

results = gc.evaluate_query(cypher_query, {"name": "bob"})

results.records[0]["nodes"]["o"]    # Get the "o" result of the 1st record as a PersonNode
results.nodes                       # The nodes returned by the query as PersonNode objects
results.relationships               # The relationships as FollowsRel objects

```

For complex queries, data science or visualization/exploration, consider using a native driver or built-in interface (like Neo4j Browser/Bloom, Memgraph Lab or Kùzu Explorer).
