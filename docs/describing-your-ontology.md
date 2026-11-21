# Describing Your Ontology

Your node and relationship classes already say what your ontology is: which kinds of node there are, the properties they carry, and how they relate. Neontology can describe them as data, so you can generate documentation, diagrams or other tooling from the models themselves, and keep it in step with the code.

`get_ontology_schema()` returns that description. `neontology.schema` also renders it as Cypher patterns and as a Markdown page for each node, and because the description is plain data, you can render it any other way you like too.

## An example ontology

The examples on this page describe these models:

```python
# myapp/models.py
from datetime import date
from enum import Enum
from typing import ClassVar, Optional

from pydantic import Field

from neontology import BaseNode, BaseRelationship


class Entity(BaseNode):
    """Anything in the ontology with a name."""

    __primaryproperty__: ClassVar[str] = "slug"

    slug: str = Field(description="A unique, URL-safe identifier.")
    name: str


class Person(Entity):
    """Someone who can work for an organisation."""

    __primarylabel__: ClassVar[Optional[str]] = "Person"

    email: str | None = Field(default=None, examples=["ada@example.com"])


class Size(str, Enum):
    SMALL = "small"
    LARGE = "large"


class Organisation(Entity):
    """A company, charity or public body."""

    __primarylabel__: ClassVar[Optional[str]] = "Organisation"

    size: Size | None = None
    founded: date | None = None


class WorksFor(BaseRelationship):
    """Someone's employment at an organisation."""

    __relationshiptype__: ClassVar[Optional[str]] = "WORKS_FOR"

    source: Person
    target: Organisation

    role: str = Field(description="Their job title.", json_schema_extra={"merge_on": True})
    since: date | None = None
```

Descriptions come from docstrings and from `Field(description=...)`, so the more of those you write, the better the generated documentation reads.

## Getting the schema

```python
import myapp.models  # defining the models is what makes them known
from neontology import get_ontology_schema

schema = get_ontology_schema()
```

As with queries, a class is described only once it has been defined, so import your models first. See [How Neontology finds your models](advanced-usage.md#how-neontology-finds-your-models).

The result is an `OntologySchema`, with a list of `nodes` and a list of `relationships`.

Each **node** (`NodeSchema`) has:

| Field | |
| --- | --- |
| `class_name`, `qualified_name` | The class' name, and its module and name, which stays unique if two classes share a name. |
| `label`, `secondary_labels` | Its primary label, and every other label it carries, [inheritable labels](advanced-usage.md#inheritable-labels) included. |
| `abstract` | Whether it is abstract. Abstract classes are described too - `label` is `None` - as they define properties shared by the classes inheriting from them. |
| `description` | Its docstring. |
| `primary_property` | The name of its primary property. |
| `parents` | The qualified names of the node classes it directly inherits from. |
| `properties` | Its properties, inherited ones included, in the order they are defined. |
| `outgoing_relationships`, `incoming_relationships` | The types of relationship it can be the source or target of, including those declared on a class it inherits from. |

Each **relationship** (`RelationshipSchema`) has its `relationship_type`, `class_name`, `qualified_name` and `description`; `source` and `target`, the qualified names of the node classes it is declared with; `source_labels` and `target_labels`, the label of every concrete class which can be at each end - the declared class and any class inheriting from it; and its `properties`.

Each **property** (`PropertySchema`) has:

| Field | |
| --- | --- |
| `name` | The key it is stored under in the graph: its alias, where it has one. |
| `type` | Its type as written in Python, such as `list[str] \| None`. |
| `required`, `nullable` | Whether a value must be given, and whether `None` is accepted. |
| `default` | Its default, or `None` where there isn't one or it comes from a `default_factory`. |
| `description`, `examples` | From `Field(...)`. |
| `allowed_values` | The values an `Enum` or `Literal` allows - for a list, each of its items. |
| `primary_property` | Whether it is the node's primary property. |
| `set_on_create`, `set_on_match`, `merge_on` | Whether it is [only set on create or match](advanced-usage.md#set-properties-on-match-or-on-create), or a relationship is [merged on it](advanced-usage.md#controlling-merge-relationships). |
| `unique`, `index` | Whether it is [tagged to be unique or indexed](graph-engines.md#declaring-them-on-your-models). |
| `json_schema` | Its JSON Schema, as pydantic generates it, including constraints such as `Field(gt=0)` and any keys you add with `json_schema_extra`. |

Types, defaults, allowed values and constraints are all read from pydantic's JSON Schema for the model, so a property is described exactly as pydantic validates it.

### Exporting it as JSON

Every part of the schema is a pydantic model, so it serialises to JSON for tools written in other languages, or to compare between versions of your models:

```python
with open("ontology.json", "w") as f:
    f.write(schema.model_dump_json(indent=2))
```

The `size` property of `Organisation`, for example, is described as:

```json
{
  "name": "size",
  "type": "Size | None",
  "required": false,
  "nullable": true,
  "default": null,
  "description": null,
  "examples": null,
  "allowed_values": [
    "small",
    "large"
  ],
  "primary_property": false,
  "set_on_create": false,
  "set_on_match": false,
  "merge_on": false,
  "unique": false,
  "index": false,
  "json_schema": {
    "anyOf": [
      {
        "enum": [
          "small",
          "large"
        ],
        "title": "Size",
        "type": "string"
      },
      {
        "type": "null"
      }
    ],
    "default": null
  }
}
```

### Describing part of your models, or one class

Pass a node class to describe only the classes inheriting from it. Giving each part of a larger set of models its own abstract base class lets you describe each one separately:

```python
schema = get_ontology_schema(Entity)
```

A relationship is included when a node class described can be at either end of it, even if the class at the other end is not described.

To describe exactly the classes you choose, pass them as `models` - node and relationship classes alike:

```python
schema = get_ontology_schema(models=[Person, Organisation, WorksFor])
```

Each node's relationships are then limited to those among the classes given, and a class given twice is described once. The same schema can [initialise a database](graph-engines.md#initialising-a-graph) for just those models.

A library defining models of its own is best exporting them as a list - `MODELS = [Person, Organisation, WorksFor]` - rather than a schema of them. A schema describes the models defined when it is built, so one built by the library would miss what the application using it defines later, and lists of models combine where schemas do not: `get_ontology_schema(models=[*library.MODELS, Employee])`.

To describe one class, call `neontology_schema()` on it. It returns the same `NodeSchema` or `RelationshipSchema` as the class' entry in the full schema:

```python
Person.neontology_schema()
WorksFor.neontology_schema()
```

## Cypher patterns, for writing queries

`cypher_patterns()` describes the ontology in the notation Cypher queries are written in: a pattern for each node class, with its labels and its properties' types, and one for each relationship between the labels at its ends. It is the form to put in front of a language model writing queries against your graph - far shorter than the JSON, and in the notation the model is writing - and a quick way to see the shape of an ontology yourself. Run the queries a model writes with `evaluate_query(..., read_only=True)`, so one which writes is refused - see [read-only access](graph-engines.md#read-only-access).

```python
from neontology import GraphConnection
from neontology.schema import cypher_patterns

print(cypher_patterns(schema, capabilities=GraphConnection().engine.supported_capabilities))
```

```text
(:Person {slug: str PP, name: str, email: str})
  // Someone who can work for an organisation.
  // slug: A unique, URL-safe identifier.
(:Organisation {slug: str PP, name: str, size: "small"|"large", founded: date})
  // A company, charity or public body.
  // slug: A unique, URL-safe identifier.

(:Person)-[:WORKS_FOR {role: str, since: date}]->(:Organisation)
  // Someone's employment at an organisation.
  // role: Their job title.
```

This is Cypher-like notation for describing the graph, rather than Cypher to run. Each node's [primary property](usage.md#defining-nodes) - the property neontology identifies and merges it on, and so the one worth matching it on - is marked `PP`. Say so when you put the patterns in front of a model, since `PP` is neontology's own shorthand. A property with a fixed set of values lists them in place of its type, since filter values are where a model writing queries is most likely to guess. Abstract classes are left out, because their properties appear on the classes inheriting them, and `descriptions=False` leaves out the `//` lines.

An end of a relationship names the fewest labels that find every node it can be. Where every class there carries one label, such as an [inheritable label](advanced-usage.md#inheritable-labels), it is written alone; otherwise the classes are written as a union, `(:Actor|Group)`. Pass `capabilities` so nothing is described that the engine you will query cannot match: on Ladybug, which keeps no secondary labels, every class at an end is named. It defaults to every capability, so the function needs no connection. NetworkX cannot query a union of labels at all.

For a large ontology, describe part of it with `get_ontology_schema(base_type=...)` or `models=[...]`, as [above](#describing-part-of-your-models-or-one-class).

## A page for each node

`node_markdown()` renders a documentation page for a node: its description, labels, properties, and the relationships it can be the source of, with their properties.

```python
from neontology.schema import node_markdown


def link(node):
    return f"{node.class_name}.md"


person = next(node for node in schema.nodes if node.class_name == "Person")

print(node_markdown(person, schema, link=link))
```

```markdown
# Person

Someone who can work for an organisation.

- Label: `Person`
- Primary property: `slug`
- Inherits from: [Entity](Entity.md)

## Properties

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| slug | str | Yes | A unique, URL-safe identifier. |
| name | str | Yes |  |
| email | str \| None | No |  |

## Outgoing relationships

### WORKS_FOR

Someone's employment at an organisation.

To: [Organisation](Organisation.md)

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| role | str | Yes | Their job title. |
| since | date \| None | No |  |
```

The `link` function is how pages link to one another. It is given a `NodeSchema` and returns the URL of that node's page, relative to the page being rendered - or `None` to name the node without linking it. Without one, nodes are named but not linked. If two of your classes share a name, build URLs from `node.qualified_name` instead.

Pass `incoming=True` to include the relationships a node can be the target of too, and `heading_level` to render the page's title at a lower level inside a larger page. A property with allowed values lists them, and an abstract class' page says it is abstract and links to the classes inheriting from it.

### A documentation site

With [MkDocs](https://www.mkdocs.org/) and the [mkdocs-gen-files](https://oprypin.github.io/mkdocs-gen-files/) plugin (`pip install mkdocs-gen-files`), the pages can be generated each time the docs are built, so they never fall out of date. A script writes a page for each node, and an index of its own making:

```python
# docs/gen_ontology.py
import mkdocs_gen_files

import myapp.models  # noqa: F401 - defining the models is what makes them known
from neontology import get_ontology_schema
from neontology.schema import node_markdown

schema = get_ontology_schema()


def link(node):
    return f"{node.class_name}.md"


with mkdocs_gen_files.open("ontology/index.md", "w") as index:
    index.write("# Ontology\n\n")

    for node in schema.nodes:
        summary = (node.description or "").split("\n\n")[0]
        index.write(f"- [{node.class_name}]({link(node)}): {summary}\n")

        with mkdocs_gen_files.open(f"ontology/{link(node)}", "w") as page:
            page.write(node_markdown(node, schema, incoming=True, link=link))
```

Run it from `mkdocs.yml` - listing plugins replaces MkDocs' defaults, so keep `search` - and add `ontology/index.md` to your `nav` if you have one:

```yaml
plugins:
  - search
  - gen-files:
      scripts:
        - docs/gen_ontology.py
```

The script imports your models, so your project must be importable wherever the docs are built. To commit the pages instead, so they can be reviewed, write the same strings to files.

## Your own metadata and formats

Anything you add to a field with `json_schema_extra` is kept in the property's `json_schema`, so you can record your own metadata on your models and use it when describing them. For example, to flag personal data:

```python
class Person(Entity):
    __primarylabel__: ClassVar[Optional[str]] = "Person"

    email: str | None = Field(default=None, json_schema_extra={"personal_data": True})
```

```python
for node in get_ontology_schema().nodes:
    for prop in node.properties:
        if prop.json_schema.get("personal_data"):
            print(f"{node.class_name}.{prop.name}")
```

### Rendering it your own way

Everyone wants their ontology presented a little differently, so beyond the two renderers above, the schema is plain data to render however you like. This draws a [Mermaid](https://mermaid.js.org/) class diagram, with inheritance and an arrow for each relationship:

```python
lines = ["classDiagram"]

for node in schema.nodes:
    lines.append(f"    class {node.class_name}")

    for parent in node.parents:
        lines.append(f"    {parent.rsplit('.', 1)[-1]} <|-- {node.class_name}")

for rel in schema.relationships:
    source, target = rel.source.rsplit(".", 1)[-1], rel.target.rsplit(".", 1)[-1]
    lines.append(f"    {source} --> {target} : {rel.relationship_type}")

print("\n".join(lines))
```

```text
classDiagram
    class Entity
    class Person
    Entity <|-- Person
    class Organisation
    Entity <|-- Organisation
    Person --> Organisation : WORKS_FOR
```

Each `NodeSchema` names its `parents`, its `outgoing_relationships` and `incoming_relationships`, and whether it is `abstract`, and each `RelationshipSchema` its `source` and `target` classes and the `source_labels` and `target_labels` they expand to - enough to draw one node and its neighbours, or to write the index, tables or format your project needs.
