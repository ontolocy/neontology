# Neo4j Recipes and Tips

Common Python patterns and use cases you may come across when working with Neo4j or other databases using Neontology.

## Created and Modified Timestamps

A common requirement can be to record when nodes and relationships were created or modified in Neo4j (or any other graph database).

If you're using Neontology, there are a couple of features which can help with this. The below example uses Pydantic and Neontology functionality to set a `created` timestamp when nodes are created and then updates the `merged` timestamp any time the Node is merged/modified.

You can also manually override the values by explicitly setting them when creating/merging the node.

```python
from datetime import datetime
from typing import Any, Optional

from pydantic import Field, model_validator

from neontology import BaseNode

class MyBaseNode(BaseNode):
    merged: datetime

    # created property will only be set 'on create' - when the node is first created
    created: Optional[datetime] = Field(default=None, json_schema_extra={"set_on_create": True})

    @model_validator(mode="before")
    @classmethod
    def set_timestamps(cls, data: Any) -> Any:
        """Stamp merged with the current time, and created with the same value.

        Values given explicitly are kept.
        """
        if isinstance(data, dict):
            data = dict(data)
            data.setdefault("merged", datetime.now())
            data.setdefault("created", data["merged"])

        return data
```

The timestamps are supplied by a validator rather than as defaults (`default_factory=datetime.now`) so that they count as given. Merging with `partial=True` - which [importing content](importing-content.md#nodes-already-in-the-graph) does by default - changes an existing node only in the properties it was given, so a `merged` timestamp left to its default would not be updated.

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
    id: str = Field(default_factory=lambda: uuid4().hex)
```

Depending on what you're trying to achieve, you could also use a custom field_validator to help generate an appropriate unique value.

## Ingesting from polars and other dataframes

`merge_records` takes a list of dictionaries, so any dataframe library that can produce
one works without installing an extra. With [polars](https://pola.rs), that is `to_dicts()`:

```python
from typing import ClassVar, Optional

import polars as pl

from neontology import BaseNode


class PersonNode(BaseNode):
    __primarylabel__: ClassVar[str] = "Person"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    age: int


df = pl.DataFrame(
    {
        "name": ["Alice", "Bob", "Alice"],
        "age": [40, 35, 40],
    }
)

nodes = PersonNode.merge_records(df.to_dicts())
```

`merge_records` returns one node per input row, in the order given, so the result lines
up with the dataframe and can be added back as a column:

```python
df = df.with_columns(pl.Series("node", nodes, dtype=pl.Object))
```

Note that the repeated "Alice" row is merged only once, but still gets a node in the
returned list, so the result stays the same length as the input.

Unlike pandas, polars represents missing values as `None` rather than `NaN`, so no
conversion is needed before merging.

The same approach works for any source of dictionaries - `csv.DictReader`, a JSON
payload, a database cursor, or `to_dict(orient="records")` if you do happen to have a
pandas dataframe.

