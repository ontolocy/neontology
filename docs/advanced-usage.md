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

## Set neo4j properties on match or on create

When we run MERGE operations with neo4j, sometimes we want to only alter properties under certain circumstances.

You can control this behaviour in Neontology by passing certain parameters when you define fields:

```python
from typing import ClassVar, Optional
from pydantic import Field
from neontology import BaseNode

class MyNode(BaseNode):
        __primaryproperty__: ClassVar[str] = "my_id"
        __primarylabel__: ClassVar[Optional[str]] = "MyNode"
        
        my_id: str = "test_node"
        only_set_on_match: Optional[str] = Field(set_on_match=True)
        only_set_on_create: Optional[str] = Field(set_on_create=True)
        normal_field: str
```

Note that fields 'set_on_match' must be optional as they will bu None/null when the node is first created.

## Controlling merge relationships

When merging relationships, we might want to merge on certain properties to avoid creating an excessive number of relationships.

To do this use the 'merge_on' parameter when defining a field.

```python
from typing import ClassVar, Optional
from pydantic import Field
from neontology import BaseRelationship

class MyRel(BaseRelationship):
        __relationshiptype__: ClassVar[Optional[str]] = "MY_RELATIONSHIP_TO"

        source: MyNode
        target: MyNode
        
        prop_to_merge_on: str = Field(merge_on=True)
```

In this example, where a relationship with a given source and target exists with the same value for 'prop_to_merge_on', the relationship will be overwritten. If a new 'prop_to_merge_on' value is given then a new relationship will be created with that value.

## Type Conversion/Coersion

neo4j doesn't support the same range of types as Python/Pydantic. Therefore, we do our best to coerce datatypes to fit into neo4j. However, you may see some dataloss when converting between complex datatypes.
