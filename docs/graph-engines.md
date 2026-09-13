# Graph Engines

By default, Neontology is set up to work with a Neo4j backend. However, it can also be configured to use other graph databases, starting with experimental support for Memgraph.

!!! EXPERIMENTAL
    Some of these features are still experimental so may change in the future.

For large or complex queries, data science or visualization/exploration, consider using a native driver or built-in interface (like Neo4j Browser/Bloom or Memgraph Lab).

## Graph Configs

The easiest way to start is to pass a GraphConfig object to init_neontology to set up the backend appropriately. You can set config variables explicitly, or using environment variables.

If you don't explicitly provide a GraphConfig, neontology will default to Neo4j and look for environment variables for configuration.

You can also use the NEONTOLOGY_ENGINE environment variable to set the graph engine to use, which will then look for the respective configuration environment variables:

* `NEO4J`
* `MEMGRAPH`
* `NETWORKX`

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

### NetworkX

Neontology has experimental support for NetworkX as a backend - storing your graph in memory rather than an external database. This is enabled by [grand-cypher](https://github.com/aplbrain/grand-cypher).

This allows you to build property graphs and query them using cypher / graph query language (GQL).

Working with the `NetworkxEngine` requires additional dependencies and works with Python v3.10 and above:

```bash
pip install neontology[grand]
```

Cypher / GQL support with this engine is limited compared to Neo4j so some features of the language may not work and raw query result structures are different. Certain Neontology features are also not implemented with this backend, most notably writing to the graph with a raw query and datetime functionality - see the capability matrix below.

The NetworkX graph is held in memory with no locking, so a query running while another thread writes to the graph can fail with `RuntimeError: dictionary changed size during iteration`. If several threads use the NetworkX engine, don't let them query while another is writing.

### Engine capability matrix

Where an engine cannot offer something, it is named as a capability and declared on the
engine itself. The table below is generated from those declarations and checked by a
test, so it cannot drift - see `Capability` in `neontology.graphengines.capabilities`
for what each one means.

<!-- BEGIN CAPABILITY MATRIX -->
| Capability | Neo4j | Memgraph | NetworkX |
| --- | --- | --- | --- |
| `graph_mutations` | Yes | Yes | No |
| `return_star` | Yes | Yes | No |
| `duplicate_create` | Yes | Yes | No |
| `datetime_filters` | Yes | Yes | No |
| `datetime_functions` | Yes | Yes | No |
| `list_property_filters` | Yes | Yes | No |
| `complex_property_types` | Yes | Yes | No |
| `collect_distinct` | Yes | Yes | No |
| `relationship_property_queries` | Yes | Yes | No |
| `constraints` | Yes | Yes | No |
| `indexes` | Yes | Yes | No |
| `multi_pattern_paths` | Yes | Yes | No |
<!-- END CAPABILITY MATRIX -->

### Constraints and indexes

Constraints and indexes are backend features, so they live on the graph engine and are
gated by the `constraints` and `indexes` capabilities above. Neo4j and Memgraph support
both; the NetworkX backend supports neither.

#### Initialising a graph

To prepare a database for your models - a new database, or one whose models have changed -
initialise it once the models are defined:

```python
from neontology import GraphConnection, init_neontology

init_neontology()

# ... define or import your models ...

GraphConnection().initialise_graph()
```

This applies everything your models declare that the backend supports: a uniqueness
constraint on each node class' primary property, and the constraints and indexes
[declared on your models](#declaring-them-on-your-models). Anything the backend cannot do
is skipped, so the same call works on every engine - on the NetworkX backend it does
nothing. It only ever adds, so it is safe to run again, and it returns the `Constraint` and
`Index` objects it applied.

It is a separate step from `init_neontology()` on purpose. It only covers the models
defined when it runs, creating a constraint fails if data already in the database breaks
it, and changing a database's schema is usually a decision for deployment rather than
something to happen every time an application starts.

To initialise a database for only some of your models - such as those a library exports,
alongside your own - pass a [schema](describing-your-ontology.md) of them:

```python
from neontology import get_ontology_schema

schema = get_ontology_schema(models=[*library.MODELS, Employee, EmployedBy])

GraphConnection().initialise_graph(schema)
```

To start again from a bare schema, drop what the database holds before initialising it.
`get_constraints()` and `get_indexes()` report constraints and indexes Neontology did not
create, too:

```python
gc = GraphConnection()

for constraint in gc.get_constraints():
    gc.drop_constraint(constraint)

for index in gc.get_indexes():
    gc.drop_index(index)

gc.initialise_graph()
```

#### Constraints

To apply uniqueness constraints for particular node types, use
`GraphConnection().apply_constraints([MyNode, MyOtherNode])`. Each one's primary property is
constrained, along with any properties [tagged `unique`](#declaring-them-on-your-models).
Only primary labels are constrained - secondary labels are not.

Constraints can also be applied directly, by label and property:

```python
gc = GraphConnection()

gc.engine.apply_uniqueness_constraint("Person", "email")
gc.engine.apply_uniqueness_constraint("Person", ["first_name", "last_name"])  # composite
```

`get_constraints()` returns `Constraint` objects describing what the database holds,
including constraints Neontology did not create. Pass one back to `drop_constraint()` to
remove it - Neo4j names constraints and Memgraph identifies them by pattern, so the
description travels rather than a name:

```python
for constraint in gc.get_constraints():
    gc.drop_constraint(constraint)
```

#### Indexes

An index speeds up lookups without requiring uniqueness:

```python
gc.apply_index("Person", "email")

for index in gc.get_indexes():
    gc.drop_index(index)
```

`get_indexes()` only reports indexes you can manage. Indexes backing a constraint and
indexes the database maintains for itself (Neo4j's token `LOOKUP` indexes) are excluded,
so a teardown loop over the list cannot destroy them.

Note that Memgraph indexes either a label on its own (`gc.apply_index("Person")`) or a
single label/property pair - it has no composite index, and raises a `ValueError` if
given more than one property. Neo4j requires at least one property.

#### Declaring them on your models

Tag a node property as `unique` or `index` with `json_schema_extra`, as for
[setting properties on match or on create](advanced-usage.md#set-properties-on-match-or-on-create):

```python
from typing import ClassVar, Optional

from pydantic import Field

from neontology import BaseNode


class PersonNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "slug"
    __primarylabel__: ClassVar[Optional[str]] = "Person"

    slug: str
    email: str = Field(json_schema_extra={"unique": True})
    name: str = Field(json_schema_extra={"index": True})
```

Tags have no effect until you [initialise the graph](#initialising-a-graph):

```python
GraphConnection().initialise_graph()  # slug and email are unique, and name is indexed
```

To apply only the constraints or only the indexes, for the node types you pass, use
`apply_constraints([...])` or `apply_indexes([...])`. Unlike `initialise_graph()`, they
raise on a backend without constraints or indexes.

- **A unique property is indexed too**: by its constraint on Neo4j, and on Memgraph, whose
  uniqueness constraints carry no index, by an index of its own. So on Memgraph every
  primary property is indexed as well.
- **Constraints and indexes apply under each class' primary label**, the label `merge()`
  matches on. A property tagged on an abstract class applies to every class inheriting it,
  but uniqueness is per label: it is unique among each class' nodes, not across all of
  them. An `Employee(Person)` carrying `Person` as an
  [inheritable label](advanced-usage.md#inheritable-labels) is covered by `Person`'s
  constraints as well as its own.
- **Only single node properties can be tagged.** Tagging a relationship property raises a
  `TypeError` when the class is defined. Apply composite constraints and indexes directly,
  as above.
- **Removing a tag leaves what it created in place** - drop it with `drop_constraint()` or
  `drop_index()`. On Neo4j, a property that was indexed and is now tagged `unique` needs
  that index dropped before it can be constrained.

#### Unsupported backends

`initialise_graph()` applies only what the backend supports. Otherwise, asking an engine
for something it cannot do - `apply_constraints()` and `apply_indexes()` included - raises
`CapabilityNotSupportedError`, which subclasses `NotImplementedError`. Portable code can
check first:

```python
from neontology import Capability, GraphConnection

gc = GraphConnection()

if gc.supports(Capability.CONSTRAINTS):
    gc.apply_constraints([MyNode])
```

A model with tagged properties works on every backend, since tags only take effect when
they are applied. The NetworkX backend enforces none of them, though: only the primary
property is unique there, because nodes are keyed by it.

```python
from neontology import GraphConnection, init_neontology
from neontology.graphengines import NetworkxConfig

config = NetworkxConfig()

init_neontology(config)

gc = GraphConnection()
gc.evaluate_query("MATCH (n) RETURN n")
```

## Graph Engines and Graph Connections

The typical way of using Neontology is to use the `init_neontology` function to initialize the connection to a database and then use the `GraphConnection` class to interact with the graph database elsewhere in your code. Behind the scenes, `GraphConnection` uses a `GraphEngine` for that database connection and you can also use a `GraphEngine` directly.

There are a couple of reasons to use `GraphConnection`:

1. It maintains a single connection to the database, rather than creating a new connection every time you need to talk to the database.
2. It provides a uniform interface regardless of the underlying GraphEngine. This means you can easily swap out the backend in the future if you want to use a different graph database.

You can access the underlying `GraphEngine` at `.engine` and you can access the native Python driver for the graph database at `.engine.driver` if you want to use functionality of the official Neo4j driver.

### One connection

There is a single connection, and `init_neontology(config)` is the only thing that
establishes it. `GraphConnection()` takes no arguments - it returns the connection that
`init_neontology` set up:

```python
init_neontology(config)     # establishes the connection

gc = GraphConnection()      # returns it, anywhere in your code
```

This is deliberate. The graph driver maintains its own pool of connections and is
designed to be created once and shared, so keeping one of them makes sure you are not
opening more connections than you need.

Calling `init_neontology(config)` again connects using the new config and closes the
previous connection, so it is also how you point Neontology at a different database:

```python
init_neontology(Neo4jConfig(...))
init_neontology(MemgraphConfig(...))   # now talking to Memgraph
```

The new connection is established and checked before the old one is closed, so a config
that cannot connect leaves the existing connection working. `GraphConnection()` returns
the same object either way, so anything holding one keeps working after a reconnect.

Because `init_neontology` replaces the connection, it is an application startup call
rather than something to call defensively - calling it repeatedly will reconnect each
time.

Use `GraphConnection().close()` to close the connection. Neontology can be initialised
again afterwards.

If you need to talk to two databases at once, work with the native driver at
`.engine.driver` rather than trying to hold two Neontology connections.

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

## Memgraph

[Memgraph](https://memgraph.com/) is a Neo4j compatible database.

In addition to configuring explicitly as above, you can also use the following environment variables and just `init_neontology(MemgraphConfig())`:

* `MEMGRAPH_URI`
* `MEMGRAPH_USERNAME`
* `MEMGRAPH_PASSWORD`

```python
from neontology import init_neontology, MemgraphConfig

init_neontology(MemgraphConfig())
```

### Memgraph Driver

Memgraph also uses the Neo4j python driver, so works just like the Neo4j driver in this respect.
