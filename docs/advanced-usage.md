# Advanced Usage

## Managing configuration with a .env file

It is generally good practice to avoid storing connection details (and especially passwords) in your source code (and version control). Therefore, Neontology supports the use of .env files (or just normal environment variables) for:

* NEO4J_URI
* NEO4J_USERNAME
* NEO4J_PASSWORD

For example:

```txt
# .env
NEO4J_URI=neo4j+s://myneo4j.example.com
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<PASSWORD>
```

## Automatically apply constraints

With neo4j, we can constrain label/property pairs to be unique and indexed.

Neontology can automatically apply neo4j constraints for all defined nodes using the `auto_constrain` method.

Simply use `auto_constrain()` after defining your models and initialising your connection.

## Use multiple labels

Sometimes you may want to apply additional labels to nodes, beyond just the primary label. Where this is the case, you can add those labels as a list using the class variable `__secondarylabels__`.

```python
class ElephantNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[Optional[str]] = "Elephant"
    __secondarylabels__: ClassVar[Optional[list]] = ["Animal"]
    name: str

ellie = ElephantNode(name="Ellie")
```

Note that methods such as `.match` and `auto_constrain` use only the primary label.

## Set neo4j properties on match or on create

When we run MERGE operations with neo4j, sometimes we want to only alter properties under certain circumstances.

!!! NOTE
    From v1.0.0, changes in v2 of Pydantic mean that these properties are now defined in a dict called 'json_schema_extra' rather than directly on the field.

You can control this behaviour in Neontology by passing certain parameters in the 'json_schema_extra' dictionary when you define fields:

```python
from typing import ClassVar, Optional
from pydantic import Field
from neontology import BaseNode

class MyNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "my_id"
        __primarylabel__: ClassVar[Optional[str]] = "MyNode"
        
        my_id: str = "test_node"
        only_set_on_match: Optional[str] = Field(json_schema_extra={"set_on_match": True})
        only_set_on_create: Optional[str] = Field(json_schema_extra={"set_on_create": True})
        normal_field: str
```

!!! NOTE
    Fields which are 'set_on_match' must be optional as they will be None/null when the node is first created.

## Controlling merge relationships

When merging relationships, we might want to merge on certain properties to avoid creating an excessive number of relationships.

!!! NOTE
    From v1.0.0, changes in v2 of Pydantic mean that this property is now defined in a dict called 'json_schema_extra' rather than directly on the field.

To do this use the 'merge_on' key in the 'json_schema_extra' parameter when defining a field.

```python
from typing import ClassVar, Optional
from pydantic import Field
from neontology import BaseRelationship

class MyRel(BaseRelationship):
        __relationshiptype__: ClassVar[Optional[str]] = "MY_RELATIONSHIP_TO"

        source: MyNode
        target: MyNode
        
        prop_to_merge_on: str = Field(json_schema_extra={"merge_on": True})
```

In this example, where a relationship with a given source and target exists with the same value for 'prop_to_merge_on', the relationship will be overwritten. If a new 'prop_to_merge_on' value is given then a new relationship will be created with that value.

## Type Conversion/Coersion

Neo4j doesn't support the same range of types as Python/Pydantic. Therefore, we do our best to coerce data types to fit into neo4j. However, you may see some data loss when converting between complex data types.

## Executing Advanced Cypher Queries with Python

The focus of Neontology is currently ingesting data rather than providing additional ways to query data. Cypher is an incredibly powerful language for doing that already.

If you want to run plain cypher queries, you can do this using the [Neo4j driver](https://neo4j.com/docs/api/python-driver/current/index.html) which can be accessed on Neontology's GraphConnection object.

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

result = gc.driver.execute_query(cypher_query, result_transformer_=neo4j.Result.single)

print(result)

# [{'name': 'Alice'}, {'name': 'Bob'}]

```
