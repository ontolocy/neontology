# Queries

Neontology has limited support for running cypher/GQL queries against the connected graph database.

!!! EXPERIMENTAL
    Some of these features are still experimental so may change in the future.

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

If you want to quickly execute a cypher query which returns a **single** result, you can use the `execute_query_single` method on a `GraphConnection`. This will return whatever Python type the underlying Neo4j driver returns. For example, if you return a number, or a list of strings, you will get that straight back. If you return a node or relationship, you will get these in the underlying Neo4j driver's [types](https://neo4j.com/docs/python-manual/current/data-types/).

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

If you want to run a cypher query and get back the nodes and relationships directly as Neontology Pydantic objects, you can use the `evaluate_query` method on a `GraphConnection`.

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

- `records_raw` - the raw records returned by the Neo4j driver
- `records` - the records converted into equivalent Neontology objects
- `nodes` - a list of all the Neontology/Pydantic nodes returned
- `paths` - any paths returned, represented as a list of relationships
- `relationships` - a list of all the Neontology/Pydantic relationships returned
- `node_link_data` - a dictionary with 'nodes' and 'edges' keys and corresponding values which can be used with other tools such as NetworkX and D3.
