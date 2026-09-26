# Queries

Neontology runs cypher/GQL queries against the connected graph database, and can build what they return into your models.

For large or complex queries, data science or visualization/exploration, consider using a native driver or built-in interface (like Neo4j Browser/Bloom or Memgraph Lab).

## Convenience Methods

When working with Node and Relationship classes, there are some methods defined to help quickly find data in the graph.

### Nodes

#### Match

The `match` method takes a primary property value and returns the associated node.

```python
my_node = MyNode.match("foo")
```

#### Match Nodes

The `match_nodes` method will return nodes with the associated primary label.

```python
my_nodes = MyNode.match_nodes()
```

You can use `limit` and `skip` parameters to control the results.

```python
my_nodes = MyNode.match_nodes(limit=10, skip=20)
```

You can also use Django like filters. Simple filters look for an exact match on a given property, whilst more complex filters are specified with `__`.

For a simple exact match against a given property:

```python
TeamNode.match_nodes(filters={"slogan": "Better than the rest!"})
```

##### String-based filters

**icontains**: Case-insensitive contains

```python
TeamNode.match_nodes(filters={"teamname__icontains": "team"})
```

**contains**: Case-sensitive contains

  ```python
  TeamNode.match_nodes(filters={"teamname__contains": "Team"})
  ```

**iexact**: Case-insensitive exact match

  ```python
  TeamNode.match_nodes(filters={"teamname__iexact": "team a"})
  ```

**startswith**: Case-sensitive startswith

  ```python
  TeamNode.match_nodes(filters={"teamname__startswith": "Tea"})
  ```

**istartswith**: Case-insensitive startswith

  ```python
  TeamNode.match_nodes(filters={"teamname__istartswith": "tea"})
  ```

##### Numeric filters

For numeric fields (if any were present), you could use:

- `gt`: Greater than
- `lt`: Less than
- `gte`: Greater than or equal to
- `lte`: Less than or equal to

##### Boolean filters

For boolean fields, you can simply use the field name with the desired boolean value:

```python
TeamNode.match_nodes(filters={"is_active": True})
```

##### Null checks

To filter based on null values:

```python
TeamNode.match_nodes(filters={"slogan__isnull": True})  # Teams with no slogan
```

##### Combining filters

You can combine multiple filters to create more complex queries:

```python
TeamNode.match_nodes(filters={
    "slogan__icontains": "better",
    "teamname__startswith": "A"
})
```

#### Get Count

You can use the `get_count` method to return the count for that type of node.

```python
MyNode.get_count()
```

`get_count` also takes filters.

## Running Simple Single Queries

If you want to quickly execute a cypher query which returns a **single** value, you can use the `evaluate_query_single` method on a `GraphConnection`. It returns the first value of the first row, as the engine's driver returns it, with nothing built into Neontology objects - or `None` if there are no rows. If you return a number, or a list of strings, you will get that straight back. If you return a node or relationship, you will get the engine's own representation of it - on Neo4j and Memgraph, the driver's [types](https://neo4j.com/docs/python-manual/current/data-types/).

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

print(result_params)

# 'Bob'

```

## Querying for Neontology Nodes and Relationships

If you want to run a cypher query and get back the nodes and relationships directly as Neontology Pydantic objects, you can use the `evaluate_query` method on a `GraphConnection`.

This uses the node and relationship classes you have defined to 'rehydrate' your query results. Model classes register themselves as they are defined, so this works as long as the class has been imported - see [How Neontology finds your models](advanced-usage.md#how-neontology-finds-your-models).

```python
from typing import ClassVar, Optional

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

cypher = "MATCH (p:Person) RETURN p"

results = gc.evaluate_query(cypher)

print(results.nodes[0].name)

# Bob

```

`evaluate_query` returns a `NeontologyResult`, which you can import from `neontology` - for
a type hint, say:

```python
from neontology import GraphConnection, NeontologyResult

def people() -> NeontologyResult:
    return GraphConnection().evaluate_query("MATCH (p:Person) RETURN p")
```

It has the following properties:

- `records_raw` - the raw records returned by the engine's driver, unchanged
- `records` - one entry per row returned, holding that row's `nodes`, `relationships` and `paths` as Neontology objects, and its other `values`, each keyed by the name it was returned as
- `nodes` - each distinct Neontology/Pydantic node returned, once, in the order first seen
- `relationships` - each distinct Neontology/Pydantic relationship returned, once, in the order first seen
- `paths` - each distinct path returned, once, represented as a list of relationships
- `node_link_data` - a dictionary with 'nodes' and 'edges' keys and corresponding values which can be used with other tools such as NetworkX and D3.

`nodes`, `relationships` and `paths` describe *what* the query returned, so something on
several rows is listed once. Distinct means distinct in the database: two parallel
relationships with the same properties are both listed. Each node and relationship is built
once per result, so a relationship's `source` and `target` are the same objects as the nodes
in `nodes`. Use `records` to see what each row held, or `records_raw` for the engine's own
result.

Anything which cannot be built as a Neontology object is left out, with a warning:

- a node whose labels do not match a single defined class
- a relationship whose type has no defined class, or whose source or target node is not
  returned by the query, or cannot be built
- a path including a relationship which cannot be built

Each warning is a `NeontologyWarning`, the base class of every warning Neontology raises
about your models, queries and results. It is a `UserWarning`, so it can be filtered with
Python's own machinery - to make any of them fail a test suite, say:

```python
import warnings

from neontology import NeontologyWarning

warnings.simplefilter("error", NeontologyWarning)
```

Deprecations are raised as `DeprecationWarning` instead.

### Values

Anything a row returns which is not a node, relationship or path - a property, an aggregate,
a literal - is in that record's `values`, keyed by the name it was returned as, so a query
can return nodes and other values side by side:

```python
cypher = """
MATCH (p:Person)
OPTIONAL MATCH (p)<-[:FOLLOWS]-(f:Person)
RETURN p, COUNT(f) AS followers
"""

for record in gc.evaluate_query(cypher).records:
    print(record["nodes"]["p"].name, record["values"]["followers"])

# Alice 1
# Bob 0
```

Name each value with `AS`: without it, the name is the expression as the engine writes it,
which differs between engines. Values are converted to native Python types - a date comes
back as a `datetime.date` on every engine. Nodes and relationships inside a value, such as
`COLLECT(n)`, are not built; return them in their own column to get Neontology objects.
