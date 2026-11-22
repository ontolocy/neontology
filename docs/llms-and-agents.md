# Knowledge Graphs for LLMs and AI Agents

Neontology gives large language models (LLMs) and AI agents typed, validated access to a knowledge graph from Python. Your ontology is a set of Pydantic models, and the same models do four jobs:

- they describe the graph to a model, in Cypher pattern notation or as a JSON Schema;
- they constrain what a model writes, through structured output;
- they validate it before anything is written;
- they return what is read as typed objects.

It works the same way against Neo4j, Memgraph, the embedded LadybugDB and NetworkX, so an agent's tools don't change with the database behind them.

It is not a vector database, and it works alongside one. Similarity search finds the passage. The graph holds the entities in it and how they relate, under an ontology every write is checked against, so the structure doesn't drift however many extractions add to it.

This page brings together the parts of Neontology that matter for this. Each section links to the page that covers its part in full.

## An example ontology

The examples on this page use these models:

```python
from enum import Enum
from typing import ClassVar, Optional

from pydantic import Field

from neontology import BaseNode, BaseRelationship


class Sector(str, Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    CHARITY = "charity"


class Entity(BaseNode):
    """Something the graph records facts about."""

    __primaryproperty__: ClassVar[str] = "slug"
    __primarylabel__: ClassVar[Optional[str]] = None  # abstract: shares its properties

    slug: str = Field(description="A unique, lower case identifier, such as 'ada-lovelace'.")
    name: str
    source_url: Optional[str] = Field(default=None, description="Where this was most recently learnt from.")


class Person(Entity):
    """A person."""

    __primarylabel__: ClassVar[Optional[str]] = "Person"


class Organisation(Entity):
    """A company, charity or public body."""

    __primarylabel__: ClassVar[Optional[str]] = "Organisation"

    sector: Optional[Sector] = None


class WorksFor(BaseRelationship):
    """Someone's employment at an organisation."""

    __relationshiptype__: ClassVar[str] = "WORKS_FOR"

    source: Person
    target: Organisation

    role: Optional[str] = Field(default=None, description="Their job title.")
```

## Describing the graph to a model

A model working with a graph needs its shape first. `cypher_patterns()` describes the ontology in the notation Cypher queries are written in:

```python
from neontology import GraphConnection, get_ontology_schema
from neontology.schema import cypher_patterns

schema = get_ontology_schema(models=[Person, Organisation, WorksFor])

patterns = cypher_patterns(schema, capabilities=GraphConnection().engine.supported_capabilities)

prompt = (
    "The graph holds nodes and relationships matching these Cypher patterns."
    " PP marks the property each node is identified by.\n\n" + patterns
)
```

```text
(:Person {slug: str PP, name: str, source_url: str})
  // A person.
  // slug: A unique, lower case identifier, such as 'ada-lovelace'.
  // source_url: Where this was most recently learnt from.
(:Organisation {slug: str PP, name: str, source_url: str, sector: "public"|"private"|"charity"})
  // A company, charity or public body.
  // slug: A unique, lower case identifier, such as 'ada-lovelace'.
  // source_url: Where this was most recently learnt from.

(:Person)-[:WORKS_FOR {role: str}]->(:Organisation)
  // Someone's employment at an organisation.
  // role: Their job title.
```

The docstrings and `Field(description=...)` text are what a model reads about your ontology. They appear in these patterns, in the JSON Schema below and in the full schema, so write them for that reader: what a property holds, what form its values take, and what it is not.

Describe only what the task needs, to keep the prompt short: pass the classes to `get_ontology_schema(models=[...])`, or an abstract base class to describe one branch. Passing `capabilities` leaves out anything the connected engine can't query. For the full description, as data, see [Describing Your Ontology](describing-your-ontology.md).

## Structured output: content the graph accepts

`record_model()` builds a Pydantic type for a list of records your models accept. Its JSON Schema is what a structured-output API holds the model to, and what the model returns parses straight back into it:

```python
from neontology.tools import import_records, record_model

Records = record_model(models=[Person, Organisation, WorksFor])

json_schema = Records.model_json_schema()   # give this to your structured-output API

records = Records.model_validate_json(response_text)

import_records([record.model_dump(exclude_none=True) for record in records.root])
```

Each label and relationship type is a fixed value, a relationship allows only the end labels its class does, an `Enum` becomes a list of allowed values, and no key the import would reject is allowed. Dumping with `exclude_none=True` leaves out what the model didn't know, since a strict structured-output mode writes `null` for it and an imported `null` clears the property. For more on the record format and its limits, see [Content a program produces](importing-content.md#content-a-program-produces).

## Validating before writing, and letting the model correct itself

The JSON Schema holds a model to the format as it writes, but not to everything the import checks: your models' validators, and whether a relationship's ends exist. `validate_only=True` runs every check and writes nothing. It raises an `ImportValidationError` naming each problem once, against the record it is in, which makes a good correction to hand back to the model. That's clearer than the type's own errors, which report a mistake against every kind of record the type could have been:

```python
import json

from neontology.tools import import_records, record_model

Records = record_model(models=[Person, Organisation, WorksFor])


def extract(text, ask_model, attempts=3):
    """Extract what a text says into the graph, letting the model correct its mistakes.

    ask_model is your call to a model: it takes a prompt and a JSON Schema, and returns
    the JSON the model wrote.
    """
    prompt = f"{patterns}\n\nRecord the people and organisations in this text:\n\n{text}"

    for _ in range(attempts):
        response_text = ask_model(prompt, Records.model_json_schema())

        try:
            # nulls left out: a strict structured-output mode writes null for what the model didn't know
            written = json.loads(response_text)
            content = [{key: value for key, value in record.items() if value is not None} for record in written]

            import_records(content, validate_only=True, error_on_unmatched=True)

        # invalid JSON, or content that didn't validate
        except ValueError as exc:
            prompt += f"\n\nYou wrote:\n{response_text}\n\nwhich has these problems:\n{exc}\n\nPlease correct them."
            continue

        return import_records(content)

    raise RuntimeError(f"No valid records after {attempts} attempts.")
```

`error_on_unmatched=True` reports a relationship whose ends are neither in the graph nor in the content, often a misremembered identifier. The import returns an `ImportReport` saying how many of each label and relationship type it merged. Validation needs no database at all, so it can also run in CI over content a model produced. See [Validating](importing-content.md#validating).

## Reading: typed queries as an agent's tools

Neontology's queries make good tools for an agent: each takes a few typed arguments, works the same on every engine, and returns models rather than driver objects.

```python
Organisation.match_nodes(filters={"name__icontains": "acme"}, limit=20)

Person.get_count(filters={"source_url__isnull": True})

ada = Person.match("ada-lovelace")

result = ada.get_related(relationship_types=["WORKS_FOR"], limit=20)
```

A filter naming a property the class doesn't declare raises a `ValueError` listing the ones it does, rather than matching nothing. A model that misremembers a property name gets something to correct, instead of an empty result it would take as a fact. Filters, `get_related()` and paging are covered in [Running Queries](queries.md) and [Advanced Usage](advanced-usage.md#get_related).

### Queries a model writes

For the questions those don't cover, a model can write Cypher from the patterns above. Run what it writes read-only, so a query that writes is refused:

```python
result = GraphConnection().evaluate_query(generated_cypher, read_only=True)
```

Or connect read-only, so nothing on the connection can write. That suits an agent that only reads, and is the way on LadybugDB, which can't make one query read-only on a connection that writes:

```python
init_neontology(Neo4jConfig(..., read_only=True))
```

See [Read-only access](graph-engines.md#read-only-access) for how each engine enforces it.

## Handing results back to a model

A result dumps to JSON in the same record format the import reads. With `nested=True` it is one record per node, carrying the relationships that leave it:

```python
result.neontology_dump_json(nested=True)
```

```json
[
  {"slug": "ada-lovelace", "name": "Ada Lovelace", "LABEL": "Person",
   "RELATIONSHIPS_OUT": [{"role": "Analyst", "TARGET_LABEL": "Organisation", "RELATIONSHIP_TYPE": "WORKS_FOR", "TARGETS": ["analytical-engines"]}]},
  {"slug": "analytical-engines", "name": "Analytical Engines", "sector": "private", "LABEL": "Organisation"}
]
```

Because it's the same format, what a model reads it can amend and give back to `import_records()` as it is. That's the loop for an agent keeping a graph up to date: read, amend, validate, write. See [Exporting](importing-content.md#exporting).

## Modelling for agents

A few modelling choices make a model's writes much more reliable. None of them needs anything beyond ordinary Neontology models.

**Make primary properties canonical, and look before creating.** Nodes are merged on their primary property, so two spellings of one thing become two nodes. This is the commonest way an agent-built graph goes wrong. Describe the form identifiers take, as `slug`'s description does above, and give the model the ones already in use, or let it search first:

```python
known = ", ".join(org.slug for org in Organisation.match_nodes(limit=500))

prompt += f"\n\nOrganisations already recorded, by slug: {known}. Use these where they apply."
```

**Record provenance.** Put the properties every node shares, such as where a fact came from, on an abstract base class, as `Entity` does. Set them in your program rather than asking the model, since the program knows the source:

```python
for record in content:
    if "LABEL" in record:
        record["source_url"] = url
```

For created and modified timestamps, see [Created and Modified Timestamps](recipes.md#created-and-modified-timestamps).

**Use enums for closed vocabularies.** An `Enum` property is listed as its allowed values in the patterns and the JSON Schema, and rejected otherwise, so a model can't invent a new sector.

## Local and disposable graphs

An agent doesn't need a database server. LadybugDB and NetworkX run in the process, in memory by default, so each session, test or evaluation run can have a graph of its own:

```python
from neontology import init_neontology
from neontology.graphengines import LadybugConfig

init_neontology(LadybugConfig(auto_create=True), reuse_connection=True)
```

`auto_create=True` creates Ladybug's tables as each model is first written, and `reuse_connection=True` keeps the graph when the call runs again, rather than starting a new, empty one. Install them with `pip install neontology[ladybug]` or `neontology[grand]`. See [Graph Engines](graph-engines.md) for what each engine supports.
