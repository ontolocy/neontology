# Topic Ideas

Ideas for topics to cover in the docs.

Type conversion - we do our best to coerce datatypes to fit into neo4j. We should outline that this happens and have a table which shows the mappings. And explain that when you use the model's match method, types should be converted back. But for complex data, we can't guarantee that there will be no data loss. Note that neo4j doesn't support dict types, or complex types like lists of lists.

Merge on, set on match, set on create - how you can choose how merging is done for different properties. Note that properties defined as only being set on match must be optional.

'Abstract' nodes - you may wish to share common properties across different types of node. You can do this by defining node classes where the `__primarylabel__` is set to None.
