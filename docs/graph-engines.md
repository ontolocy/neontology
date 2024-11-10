# Graph Engines

By default, Neontology is set up to work with a Neo4j backend. However, it can also be configured to use other graph databases, starting with experimental support for Memgraph.

!!! EXPERIMENTAL
    Some of these features are still experimental so may change in the future.

For large or complex queries, data science or visualization/exploration, consider using a native driver or built-in interface (like Neo4j Browser/Bloom or Memgraph Lab).

## Graph Configs

The easiest way to start is to pass a GraphConfig object to init_neontology to set up the backend appropriately. You can set config variables explicitly, or using environment variables.

If you don't explicitly provide a GraphConfig, neontology will default to Neo4j and look for environment variables for configuration.

You can also use the NEONTOLOGY_ENGINE environment variable to set the graph engine (`NEO4J` or `MEMGRAPH`) to use, which will then look for the respective configuration environment variables.

### Neo4j

```python
from neontology import GraphConnection, init_neontology
from neontology.graphengines import Neo4jConfig

config = Neo4jConfig(
        uri="bolt://localhost:7687",    # OR use NEO4J_URI environment variable
        username="neo4j",               # OR use NEO4J_USERNAME environment variable
        password="<PASSWORD>"           # OR use NEO4J_PASSWORD environment variable
    )

init_neontology(config)

gc = GraphConnection()
gc.evaluate_query_single("MATCH (n) RETURN COUNT(n)")
```

### Memgraph

```python
from neontology import GraphConnection, init_neontology
from neontology.graphengines import MemgraphConfig

config = MemgraphConfig(
        uri="bolt://localhost:7687",    # OR use MEMGRAPH_URI environment variable
        username="memgraphuser",        # OR use MEMGRAPH_USERNAME environment variable
        password="<PASSWORD>"           # OR use MEMGRAPH_PASSWORD environment variable
    )

init_neontology(config)

gc = GraphConnection()
gc.evaluate_query_single("MATCH (n) RETURN COUNT(n)")
```

## Graph Engines and Graph Connections

The typical way of using Neontology is to use the `init_neontology` function to initialize the connection to a database and then use the `GraphConnection` class to interact with the graph database elsewhere in your code. Behind the scenes, `GraphConnection` uses a `GraphEngine` for that database connection and you can also use a `GraphEngine` directly.

There are a couple of reasons to use `GraphConnection`:

1. It maintains a single connection to the database, rather than creating a new connection every time you need to talk to the database.
2. It provides a uniform interface regardless of the underlying GraphEngine. This means you can easily swap out the backend in the future if you want to use a different graph database.

You can access the underlying `GraphEngine` at `.engine` and you can access the native Python driver for the graph database at `.engine.driver` if you want to use functionality of the official Neo4j driver.

## Neo4j

If you just use `init_neontology`, Neontology will assume that you have a Neo4j backend. You can also declare this explicitly as above.

### Using the Neo4j driver from Neontology

You can also directly access the [Neo4j driver](https://neo4j.com/docs/api/python-driver/current/index.html) on Neontology's GraphConnection object.

The Neo4j driver has many features and different ways of executing queries, but the below recipe shows how we can write and [execute arbitrary queries](https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Driver.execute_query) with the driver to return data as Python lists / dictionaries. We will use Neo4j's built in support for [map projection](https://neo4j.com/docs/cypher-manual/current/values-and-types/maps/).

```python
import neo4j
from neontology import init_neontology, GraphConnection

init_neontology()

gc = GraphConnection()

cypher_query = """
MATCH (p:Person)
RETURN COLLECT({name: p.name})
"""

result = gc.engine.driver.execute_query(cypher_query, result_transformer_=neo4j.Result.single)

print(result)

# [{'name': 'Alice'}, {'name': 'Bob'}]

```

### Applying constraints

With neo4j, we can constrain label/property pairs to be unique and indexed.

Neontology can automatically apply neo4j constraints for all defined nodes using the `auto_constrain_neo4j` method.

Simply use `auto_constrain_neo4j()` after defining your models and initialising your connection.

Note that auto constrain features only use a model's primary label (not secondary labels if they're defined).

## Memgraph

[Memgraph](https://memgraph.com/) is a Neo4j compatible database.

In addition to configuring explicitly as above, you can also use the following environment variables and just `init_neontology(graph_engine=MemgraphEngine)`:

* `MEMGRAPH_URI`
* `MEMGRAPH_USER`
* `MEMGRAPH_PASSWORD`

```python
from neontology import init_neontology
from neontology.graph_engines import MemgraphEngine

init_neontology(
    engine=MemgraphConfig()
)
```

### Memgraph Driver

Memgraph also uses the Neo4j python driver, so works just like the Neo4j driver in this respect.
