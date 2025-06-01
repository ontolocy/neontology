# Working with Non-Unique Schemas

## Adding an Element ID field

Add the `ElementIdModel` parent class to your nodes or relationships to add the default `element_id` property, or set your own with `__elementidproperty__`.

```python
class ElementIdModel(BaseModel):
    __elementidproperty__: ClassVar[str] = "element_id"
    element_id: Optional[str] = Field("",json_schema_extra={"never_set": True})
```

## Behavior of an Element ID field

An Element ID field is never set by the Node or Relationship object. It automatically updates from the result of a merge or create.

In this example, the element_id defaults to an empty string, but is set to the graph database elementId() when merge() is called.

```python
from neontology import BaseNode, ElementIdModel
class ElephantNode(BaseNode,ElementIdModel):
    __primaryproperty__: ClassVar[str] = "name"
    __primarylabel__: ClassVar[str] = "Elephant"
    name: str

ellie = ElephantNode (name="Ellie)
assert ellie.element_id == ""
ellie.merge()
assert ellie.element_id != ""
```