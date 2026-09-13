# Describing Your Ontology

Your node and relationship classes already say what your ontology is: which kinds of node there are, the properties they carry, and how they relate. Neontology can describe them as data, so you can generate documentation, diagrams or other tooling from the models themselves, and keep it in step with the code.

`get_ontology_schema()` returns that description. `neontology.schema` also has renderers which turn it into Markdown pages and a Mermaid diagram, and because the description is plain data, you can build anything else from it too.

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

## Rendering Markdown

`node_markdown()` renders a page for a node: its description, labels, properties, and the relationships it can be the source of, with their properties. `index_markdown()` renders an index of every node and relationship.

```python
from neontology.schema import index_markdown, node_markdown

schema = get_ontology_schema()


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

Pass `incoming=True` to include the relationships a node can be the target of, too. On the `Organisation` page that adds:

```markdown
## Incoming relationships

### WORKS_FOR

Someone's employment at an organisation.

From: [Person](Person.md)

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| role | str | Yes | Their job title. |
| since | date \| None | No |  |
```

A property with allowed values lists them in its description, such as "One of: `small`, `large`.", and an abstract class' page says it is abstract and links to the classes inheriting from it.

The index looks like this:

```markdown
# Ontology

## Nodes

| Node | Label | Description |
| --- | --- | --- |
| [Entity](Entity.md) | abstract | Anything in the ontology with a name. |
| [Person](Person.md) | `Person` | Someone who can work for an organisation. |
| [Organisation](Organisation.md) | `Organisation` | A company, charity or public body. |

## Relationships

| Relationship | From | To | Description |
| --- | --- | --- | --- |
| WORKS_FOR | [Person](Person.md) | [Organisation](Organisation.md) | Someone's employment at an organisation. |
```

Both take `heading_level`, to render the page's title at a lower level when you include it in a larger page, and `index_markdown()` takes a `title`.

## A documentation site with a page for each node

With [MkDocs](https://www.mkdocs.org/) and the [mkdocs-gen-files](https://oprypin.github.io/mkdocs-gen-files/) plugin, the pages can be generated each time the docs are built, so they never fall out of date. Install the plugin alongside MkDocs:

```bash
pip install mkdocs mkdocs-gen-files
```

Add a script which writes an index and a page for each node into an `ontology` folder:

```python
# docs/gen_ontology.py
import mkdocs_gen_files

import myapp.models  # noqa: F401 - defining the models is what makes them known
from neontology import get_ontology_schema
from neontology.schema import index_markdown, node_markdown

schema = get_ontology_schema()


def link(node):
    # every page is written to the same folder, so a link is just the file name
    return f"{node.class_name}.md"


with mkdocs_gen_files.open("ontology/index.md", "w") as page:
    page.write(index_markdown(schema, link=link))

for node in schema.nodes:
    with mkdocs_gen_files.open(f"ontology/{node.class_name}.md", "w") as page:
        page.write(node_markdown(node, schema, incoming=True, link=link))
```

And run it from `mkdocs.yml`. Listing plugins replaces MkDocs' defaults, so keep `search`:

```yaml
plugins:
  - search
  - gen-files:
      scripts:
        - docs/gen_ontology.py
```

The generated pages are built like any other. If `mkdocs.yml` has no `nav`, they appear in the navigation automatically; if it does, add the index, which links to every node page:

```yaml
nav:
  - Home: index.md
  - Ontology: ontology/index.md
```

To list every node page in the navigation as well, the [mkdocs-literate-nav](https://oprypin.github.io/mkdocs-literate-nav/) plugin can read a navigation file which the script writes alongside the pages.

The script imports your models, so your project must be importable wherever the docs are built. On Read the Docs, install it in `.readthedocs.yaml`, and add `mkdocs-gen-files` to your docs requirements.

If you would rather commit the pages, so they can be reviewed, write them to files instead:

```python
from pathlib import Path

folder = Path("docs/ontology")
folder.mkdir(parents=True, exist_ok=True)

(folder / "index.md").write_text(index_markdown(schema, link=link))

for node in schema.nodes:
    (folder / f"{node.class_name}.md").write_text(node_markdown(node, schema, incoming=True, link=link))
```

## Diagrams

`mermaid_diagram()` draws the ontology as a [Mermaid](https://mermaid.js.org/) class diagram: node classes, which classes inherit from which, and an arrow for each relationship, labelled with its type.

```python
from neontology.schema import mermaid_diagram

print(mermaid_diagram(schema))
```

```text
classDiagram
    class Entity
    <<abstract>> Entity
    class Person
    class Organisation
    Entity <|-- Person
    Entity <|-- Organisation
    Person --> Organisation : WORKS_FOR
```

Pass `focus` to draw one node with the classes it inherits from or is inherited by, and the nodes it has relationships with - a useful diagram for the top of each node's page:

```python
mermaid_diagram(schema, focus=person)
```

The diagram is Mermaid source, which needs Mermaid support where it is displayed. GitHub and GitLab render a `mermaid` code block in Markdown. In MkDocs, the [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/reference/diagrams/) theme renders one once its `superfences` extension is configured:

```yaml
theme:
  name: material

markdown_extensions:
  - pymdownx.superfences:
      custom_fences:
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
```

With that in place, the script above can add a diagram to each node's page:

```python
        page.write(f"\n```mermaid\n{mermaid_diagram(schema, focus=node)}```\n")
```

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

The renderers are ordinary functions over the schema, so if they don't produce what you need, write your own the same way. This prints each relationship as a Cypher pattern:

```python
for rel in get_ontology_schema().relationships:
    sources = "|".join(rel.source_labels)
    targets = "|".join(rel.target_labels)

    print(f"(:{sources})-[:{rel.relationship_type}]->(:{targets})")
```

```text
(:Person)-[:WORKS_FOR]->(:Organisation)
```
