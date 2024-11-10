# Neo4j Recipes and Tips

Common Python patterns and use cases you may come across when working with Neo4j or other databases using Neontology.

## Created and Modified Timestamps

A common requirement can be to record when nodes and relationships were created or modified in Neo4j (or any other graph database).

If you're using Neontology, there are a couple of features which can help with this. The below example uses Pydantic and Neontology functionality to set a `created` timestamp when nodes are created and then updates the `merged` timestamp any time the Node is merged/modified.

You can also manually override the values by explicitly setting them when creating/merging the node.

```python
from datetime import datetime
from typing import Optional

from pydantic import Field, field_validator, ValidationInfo

from neontology import BaseNode

class MyBaseNode(BaseNode):
    merged: datetime = Field(
        default_factory=datetime.now, 
    )

    # created property will only be set 'on create' - when the node is first created
    created: Optional[datetime] = Field(
                                    default=None, 
                                    validate_default=True,
                                    json_schema_extra={"set_on_create": True})

    # Use Pydantic's validator functionality to set created off the merged value
    @field_validator("created")
    def set_created_to_merged(
        cls, value: Optional[datetime], values: ValidationInfo
    ) -> datetime:
        """When the node is first created, we want the created value to be set equal to merged.
        Otherwise they will be a tiny amount of time different.
        """

        # set created = merged (which was set to datetime.now())
        if value is None:
            return values.data["merged"]

        # if the created value has been manually set, don't override it
        else:
            return value

```

Neontology v0 and v1 included `merged` and `created` fields with this functionality by default but it was removed in v2 to provide greater flexibility to users.

## Set a property value to a unique ID

Another common pattern is to assign each new node that goes into Neo4j with a unique identifier or key. Again, we can use Pydantic to help with this - combining Python's built-in uuid4 function with Pydantic's support for a [default_factory](https://docs.pydantic.dev/latest/concepts/fields/#default-values) to generate a property value.

```python
from uuid import uuid4

from neontology import BaseNode

class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "PersonLabel"
    __primaryproperty__: ClassVar[str] = "id"
    
    name: str
    age: int
    uuid: str = Field(default_factory=lambda: uuid4().hex)
```

Depending on what you're trying to achieve, you could also use a custom field_validator to help generate an appropriate unique value.
