# Importing Content

Neontology can build a graph from files you write by hand, or from records another program produced. The point is that the content stays readable — a directory of YAML, JSON or Markdown files you can review in a pull request — while the graph it produces is exact and repeatable.

This page covers the format those records use, the three ways of laying content out, and how to find out what is wrong when a file does not import.

## An example ontology

The examples below describe these models:

```python
# myapp/models.py
from typing import ClassVar, Optional

from neontology import BaseNode, BaseRelationship


class Host(BaseNode):
    __primarylabel__: ClassVar[str] = "Host"
    __primaryproperty__: ClassVar[str] = "hostname"

    hostname: str
    owner: Optional[str] = None
    asset_tag: Optional[str] = None


class IPAddress(BaseNode):
    __primarylabel__: ClassVar[str] = "IPAddress"
    __primaryproperty__: ClassVar[str] = "ip"

    ip: str


class ResolvesTo(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "RESOLVES_TO"

    source: Host
    target: IPAddress

    first_seen: Optional[str] = None


class ManagedBy(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "MANAGED_BY"

    source: IPAddress
    target: Host
```

A model class has to be **defined** before content using its label can be imported — defining it is what registers it — so import your models before you import content.

## Records

Content is made of records. Each one is a node or a relationship, and says which by carrying a `LABEL` or a `RELATIONSHIP_TYPE`.

Keys which say **how to build the graph** are written in uppercase. Every other key is a **property** of the model the record describes. That is the whole rule, and it is why `LABEL` shouts while `hostname` does not.

### A node record

```yaml
LABEL: Host
hostname: web1
owner: platform-team
```

### A relationship record

```yaml
RELATIONSHIP_TYPE: RESOLVES_TO
SOURCE_LABEL: Host
TARGET_LABEL: IPAddress
SOURCE: web1
TARGET: 10.0.0.1
first_seen: "2026-01-01"
```

`SOURCE` and `TARGET` are the primary property values of the nodes at each end. The relationship's own properties sit alongside them.

### The keys a record can use

| Key | On | Means |
| --- | --- | --- |
| `LABEL` | node | the node's primary label |
| `RELATIONSHIPS_OUT` | node | relationships leaving this node |
| `RELATIONSHIPS_IN` | node | relationships arriving at this node |
| `RELATIONSHIP_TYPE` | relationship | the relationship's type |
| `SOURCE_LABEL` | relationship | primary label of the node it leaves |
| `TARGET_LABEL` | relationship | primary label of the node it arrives at |
| `SOURCE` / `TARGET` | relationship | which node it leaves / arrives at |
| `SOURCES` / `TARGETS` | relationship | several nodes at that end |
| `SOURCE_NODES` / `TARGET_NODES` | relationship | nodes at that end, defined inline |
| `SOURCE_PROPERTY` / `TARGET_PROPERTY` | relationship | match that end on this property instead of its primary property |
| `BODY_PROPERTY` | markdown frontmatter | which property the markdown body belongs in |

Each end of a relationship is named the same way: a single value, a list, or records defining the nodes inline. One end names a single node and the other may name several, so a record expands to one relationship per node at that end. Naming several at *both* ends is an error, since which relates to which would not be defined.

A key written like a control key which is neither one of these nor a property of your model is reported as a mistake, with the nearest control key suggested — so `TARGET_PROPERTIES` tells you it meant `TARGET_PROPERTY` rather than complaining about an unexpected field.

!!! note
    `source` and `target` in lowercase are still accepted as aliases of `SOURCE` and `TARGET`, and warn once per import. They are removed in v4. Note that `merge_records()` and `merge_df()` on a relationship class are a different API with a different convention: those take lowercase `source` and `target` keys, and are unaffected.

## Laying content out

### Combined: relationships under the node they leave

A node record can carry the relationships that leave it, which keeps everything about a host in one place:

```yaml
LABEL: Host
hostname: web1
owner: platform-team
RELATIONSHIPS_OUT:
  - RELATIONSHIP_TYPE: RESOLVES_TO
    TARGET_LABEL: IPAddress
    TARGETS:
      - 10.0.0.1
      - 10.0.0.2
```

The node declaring them is the source of every one, so these records need no `SOURCE` or `SOURCE_LABEL`.

### Relationships arriving at a node

`RELATIONSHIPS_IN` is the mirror of `RELATIONSHIPS_OUT`: the node declaring them is the *target* of every one, so the far end is named with `SOURCES`, `SOURCE_NODES` or `SOURCE`.

```yaml
LABEL: IPAddress
ip: 10.0.0.1
RELATIONSHIPS_IN:
  - RELATIONSHIP_TYPE: RESOLVES_TO
    SOURCE_LABEL: Host
    SOURCES:
      - dns1
      - dns2
```

Declaring relationships only where they leave keeps it obvious from the content which node owns what, and is worth sticking to as a convention. `RELATIONSHIPS_IN` is there for the cases where the arriving end is the one with something to say — an IP address that knows which hosts resolve to it, say, where the hosts are managed elsewhere.

A node can declare both blocks. A record inside either may not name its own end, since the node declaring it is that end.

### Split: nodes in one file, relationships in another

```yaml
# hosts.yaml
- LABEL: Host
  hostname: web1
- LABEL: Host
  hostname: web2
```

```yaml
# resolves.yaml
- RELATIONSHIP_TYPE: RESOLVES_TO
  SOURCE_LABEL: Host
  TARGET_LABEL: IPAddress
  SOURCE: web1
  TARGET: 10.0.0.1
```

Order does not matter. Every node in the import is written before any relationship is, so a relationship may refer to a node defined in any file — including one read after it.

### Mixed

Both in one import is fine, and usual: most nodes described in their own files, with relationships declared wherever they read most naturally.

## Defining nodes inline

A relationship can bring the nodes at its far end into the graph rather than matching nodes already there, with `TARGET_NODES`. This is for the case where a node has nothing to say for itself and does not deserve a file:

```yaml
LABEL: Host
hostname: dns1
RELATIONSHIPS_OUT:
  - RELATIONSHIP_TYPE: RESOLVES_TO
    TARGET_LABEL: IPAddress
    TARGET_NODES:
      - LABEL: IPAddress
        ip: 10.0.0.1
      - LABEL: IPAddress
        ip: 10.0.0.2
```

Inline records **merge** their nodes; they do not match them. The same IP address may be defined inline under any number of hosts, and a node defined inline may also have a record of its own elsewhere.

A node defined inline cannot declare `RELATIONSHIPS_OUT`. Give it a record of its own to relate it to anything further.

## One node, several records

Content describing the same node in more than one place is combined into a single node, and the records have to agree. This is what makes the graph independent of the order files happen to be read in.

- A record at the **top level** of the content *defines* a node: the root of a YAML document, an element of a top level list, an entry under `nodes`, or a Markdown file's frontmatter. **A node has one definition** — a second raises `DuplicateNodeDefinitionError`, whether or not the two agree.
- A record inside `TARGET_NODES` is not a definition. Any number of them may name the same node.
- Where two records give the same property **different values**, that raises `ConflictingNodeRecordError`, naming the property, both values and both records. Preferring either one would make the graph depend on reading order.

So this is fine — the inline record names the node, the definition describes it:

```yaml
# dns.yaml
LABEL: Host
hostname: dns1
RELATIONSHIPS_OUT:
  - RELATIONSHIP_TYPE: RESOLVES_TO
    TARGET_LABEL: IPAddress
    TARGET_NODES:
      - LABEL: IPAddress
        ip: 10.0.0.1
```

```yaml
# ips.yaml
LABEL: IPAddress
ip: 10.0.0.1
```

Because records are combined before anything is written, the inline record cannot overwrite the definition's properties with defaults.

!!! note
    Give every node record an explicit primary property value, or one your model computes deterministically from the record. That value is how other content points at the node, and how records describing the same node are recognised as being about one node.

## Matching on another property

Where nodes carry a second unique identifier, a relationship can match on it instead of the primary property:

```yaml
RELATIONSHIP_TYPE: MANAGED_BY
SOURCE_LABEL: IPAddress
TARGET_LABEL: Host
SOURCE: 10.0.0.1
TARGETS:
  - asset-0091
TARGET_PROPERTY: asset_tag
```

Here the hosts are matched by their `asset_tag` rather than by `hostname`. The property must identify exactly one node; if it matches several, that is reported.

`SOURCE_PROPERTY` does the same for the other end, and both work inside `RELATIONSHIPS_OUT` and `RELATIONSHIPS_IN` — including for the declaring node's own end, whose record then has to carry that property:

```yaml
LABEL: Host
hostname: web1
asset_tag: asset-0091
RELATIONSHIPS_IN:
  - RELATIONSHIP_TYPE: MANAGED_BY
    SOURCE_LABEL: IPAddress
    SOURCES: [10.0.0.1]
    TARGET_PROPERTY: asset_tag
```

## Importing

```python
from neontology import init_neontology
from neontology.tools import import_yaml, import_json, import_md, import_csv, import_records

import myapp.models  # defining your models is what registers them

init_neontology(config)

report = import_yaml("content/")
```

Each importer takes a file or a directory to search, and returns an `ImportReport`:

```python
report.nodes             # {'Host': 12, 'IPAddress': 40}
report.relationships     # {'RESOLVES_TO': 40}
report.node_count        # 52
report.files             # the files it read
report.unresolved        # endpoints which did not identify exactly one node
```

`import_records()` takes records you already have as dictionaries, rather than files.

Markdown files hold one record as YAML frontmatter, and name the property their body belongs in:

```markdown
---
LABEL: Host
hostname: web1
BODY_PROPERTY: description
---
This host serves the public website.
```

### CSV

A CSV row is one flat record. It can describe a node or a relationship, but nothing nested: `RELATIONSHIPS_OUT`, `RELATIONSHIPS_IN`, `SOURCE_NODES` and `TARGET_NODES` all need a format that nests, or a CSV of their own naming the nodes at each end.

```csv
LABEL,hostname,owner
Host,web1,platform-team
Host,web2,data-team
```

Keys which are the same down a whole file can be given once in `defaults` rather than as a column, and a row with a column of its own overrides it:

```python
import_csv("hosts.csv", defaults={"LABEL": "Host"})

import_csv(
    "resolves.csv",
    defaults={
        "RELATIONSHIP_TYPE": "RESOLVES_TO",
        "SOURCE_LABEL": "Host",
        "TARGET_LABEL": "IPAddress",
    },
)
```

`defaults` describe the rows one call reads, so files of different kinds are imported by a call each — point the call at a file, or narrow it with `path_pattern`.

Two rules are worth knowing:

- **An empty cell means the property was not given**, so the model's default applies rather than the property being set to an empty string. A CSV cannot express an empty string as a value.
- **Every value is read as text and converted by the model**, so columns of numbers, booleans and dates arrive as the type the model declares. A property holding a list needs a format that nests.

A row which does not validate is reported with the line it is on, counting the header as line 1.

### Options

| Argument | Default | Means |
| --- | --- | --- |
| `path_pattern` | `**/*.yaml` etc. | which files to read under a directory |
| `check_unmatched` | `True` | check both ends of every relationship resolve |
| `error_on_unmatched` | `False` | raise rather than warn where one does not |
| `validate_only` | `False` | check the content, write nothing |
| `batch_size` | `None` | how many records to write per query |

`batch_size` limits the size of each write. It does not limit what a relationship can refer to.

## Finding out what is wrong

Every record knows which file and which entry it came from, so anything reported about it says where to go and look:

```
ImportContentError: content/hosts.yaml, record 31:
1 validation error for Host
owner
  Input should be a valid string [type=string_type, input_value=42, input_type=int]
```

A record nested inside another is reported with its path, such as `content/dns.yaml, record 2 (RELATIONSHIPS_OUT[0].TARGET_NODES[1])`.

### Validating

An import stops at the first problem. `validate_only=True` checks everything instead and reports all of it at once, which is what you want in CI or when fixing a repository:

```python
from neontology.tools import ImportValidationError, import_yaml

try:
    import_yaml("content/", validate_only=True)

except ImportValidationError as exc:
    print(exc)            # every problem, grouped by file
    print(len(exc.issues))  # and available as data

    for issue in exc.issues:
        print(issue.origin, issue.error)
```

```
ImportValidationError: 3 problems in the content to import:

content/hosts.yaml
  record 31
      1 validation error for Host
      owner
        Input should be a valid string [type=string_type, input_value=42, input_type=int]

content/dns.yaml
  record 2 (RELATIONSHIPS_OUT[0])
      'TARGET_PROPERTIES' is not one of the keys a record uses to say how to build the
      graph, and ResolvesTo has no property by that name. Did you mean 'TARGET_PROPERTY'?
```

Validating writes nothing, so relationship endpoints resolve against the graph **and** against the content being imported — a relationship pointing at a node the content itself defines is not reported as unresolved.

### Errors

All of these subclass `ValueError`.

| Error | Raised when |
| --- | --- |
| `ImportContentError` | content is malformed, or does not describe your models |
| `DuplicateNodeDefinitionError` | a node is defined more than once |
| `ConflictingNodeRecordError` | two records give a property different values |
| `ImportValidationError` | validating found problems; `.issues` lists them |

A batch of records can also fail for a reason no single record explains — the database rejecting the write, most often. That is reported against the batch, naming the files it covered, and suggests running with `validate_only=True`.

## Exporting

`neontology_dump()` on a node, a relationship or a query result produces records in this same format, so a graph can be written back out and read in again:

```python
result = GraphConnection().evaluate_query("MATCH (n:Host)-[r]->(o) RETURN n, r, o")

data = result.neontology_dump()       # {'nodes': [...], 'edges': [...]}

Path("content/graph.json").write_text(result.neontology_dump_json())

import_records([data])
```

That container — `nodes` and `edges` — is accepted by the importer as a third layout alongside combined and split.

### The node oriented shape

`nested=True` dumps a record per node instead, each carrying the relationships that leave it — the shape content is usually written in by hand, rather than nodes and edges side by side:

```python
records = result.neontology_dump(nested=True)

Path("content/graph.yaml").write_text(
    yaml.safe_dump(json.loads(result.neontology_dump_json(nested=True)), sort_keys=False)
)
```

```yaml
- LABEL: Host
  hostname: web1
  owner: platform-team
  RELATIONSHIPS_OUT:
    - RELATIONSHIP_TYPE: RESOLVES_TO
      TARGET_LABEL: IPAddress
      TARGETS:
        - 10.0.0.1
- LABEL: IPAddress
  ip: 10.0.0.1
```

A relationship is only built when the result holds the nodes at both of its ends, so return them alongside it — a query returning relationships alone dumps nothing to declare.

Use `neontology_dump_json()` rather than `neontology_dump()` when writing a file: it takes values through pydantic's own serialisation, so a datetime is written as ISO rather than however `str()` happens to render it.

!!! note
    `node_link_data` on a result is a different format, for NetworkX, D3 and Cytoscape. Those expect an edge to name its ends `source` and `target` in lowercase, so it keeps that convention and is not importable content.

    `neontology_dump(exclude_node_props=False)` embeds each end's whole node rather than its primary property. That form is for inspection; it is not importable.
