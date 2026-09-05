# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Neontology is an object-graph mapper: Pydantic models define nodes/relationships that are written to and read from openCypher/GQL property graph databases (Neo4j, Memgraph, and experimentally NetworkX via grand-cypher). Published to PyPI as `neontology`; docs are on Read the Docs (built from [docs/](docs/) with mkdocs).

## Commands

The project uses `uv`.

```bash
uv sync --all-extras          # install deps incl. the optional [grand] extra
uv build

uv run pytest -x
uv run pytest -m "not uses_graph"       # skip tests needing a live database
uv run pytest -k 'neo4j-engine'         # run against one engine only
uv run pytest --benchmark-skip          # skip the pytest-benchmark tests
uv run pytest tests/test_basenode.py::test_name -k 'neo4j-engine'   # single test
uv run pytest --cov=src/neontology

uv run ruff check src
uv run ruff format --check --diff src

uv run mkdocs serve
```

Work happens on branches which get merged when complete to a work-in-progress `develop`; CI runs on pushes to `develop` and PRs to `develop`/`main`. `main` should be releasable.

### Test environment

Graph-backed tests read connection details from env vars (a `.env` file works): `TEST_NEO4J_URI`, `TEST_NEO4J_USERNAME`, `TEST_NEO4J_PASSWORD`, `TEST_MEMGRAPH_URI`, `TEST_MEMGRAPH_USER`, `TEST_MEMGRAPH_PASSWORD`. The `get_graph_config` fixture asserts they are set, so *all* engine params fail without them — use `-k` to select an engine, or `-m "not uses_graph"`. CI runs neo4j on 7687 and memgraph on 9687 as service containers; the networkx engine is exercised by a separate workflow ([ci_grand.yml](.github/workflows/ci_grand.yml)) because it needs the optional extra.

## Architecture

### Model layer

`CommonModel` ([commonmodel.py](src/neontology/commonmodel.py)) is the shared Pydantic base for `BaseNode` and `BaseRelationship`. Its key job is classifying each field into `_always_set` / `_set_on_match` / `_set_on_create` buckets, read from `json_schema_extra` on the field (e.g. `Field(json_schema_extra={"set_on_match": True})`). Those buckets map directly onto Cypher `SET` / `ON MATCH SET` / `ON CREATE SET`. `_engine_dict` dumps a model through the active engine's `export_dict_converter`, coercing values to graph-supported types (dicts are rejected; lists must be homogeneous).

`BaseNode` ([basenode.py](src/neontology/basenode.py)) is identified by `__primaryproperty__` + `__primarylabel__` ClassVars, with optional `__secondarylabels__`. A subclass with `__primarylabel__ = None` is "abstract": it cannot be instantiated, and is skipped by type discovery — this is the mechanism for sharing common properties.

`BaseRelationship` ([baserelationship.py](src/neontology/baserelationship.py)) is identified by `__relationshiptype__` and requires `source`/`target` fields annotated with node classes. Extra fields marked `merge_on` become part of the MERGE pattern rather than a SET. Same abstract-class convention applies via `__relationshiptype__`.

### Type discovery

[utils.py](src/neontology/utils.py) walks `__subclasses__()` recursively to build `{primary_label: NodeClass}` and `{rel_type: RelationshipTypeData}` maps. This is how query results get rehydrated into the right model classes — **a class must be imported/defined before a query runs for its results to come back typed**. `GraphConnection.evaluate_query` refreshes these maps by default (`refresh_classes=True`). Source/target annotations that are still unresolved `ForwardRef`s are filtered out by `_validate_relationship_nodes` rather than raising.

### Connection layer

`GraphConnection` ([graphconnection.py](src/neontology/graphconnection.py)) is a **singleton** (`__new__` returns `_instance`) wrapping one `GraphEngineBase`. `init_neontology(config)` constructs it; `GraphConnection.change_engine(config)` swaps the engine on the live singleton — that is how the test suite parametrises across engines. Model methods call `GraphConnection()` with no args to reach the existing connection.

### Engine layer

`GraphEngineBase` ([graphengines/graphengine.py](src/neontology/graphengines/graphengine.py)) holds the *shared Cypher generation* — `create_nodes`, `merge_nodes`, `merge_relationships`, `match_nodes`, `get_count`, and the Django-style `_filters_to_where_clause` (`name__icontains`, `qty__gt`, …). Subclasses ([neo4jengine.py](src/neontology/graphengines/neo4jengine.py), [memgraphengine.py](src/neontology/graphengines/memgraphengine.py), [networkxengine.py](src/neontology/graphengines/networkxengine.py)) implement driver mechanics: `verify_connection`, `evaluate_query`/`evaluate_query_single` (including converting driver records into `NeontologyResult`), and constraint handling. New behaviour that is expressible in portable Cypher belongs in the base class.

Each engine pairs with a `GraphEngineConfig` subclass declaring `engine` and `env_fields`; the base validator auto-populates missing fields from environment variables (via `load_dotenv`) and raises if neither is provided. `NetworkxEngine`/`NetworkxConfig` import lazily — [graphengines/\_\_init\_\_.py](src/neontology/graphengines/__init__.py) and `init_neontology` both swallow the `ImportError` when the `grand` extra isn't installed, so guard any new reference to it the same way.

### Query safety

Cypher is built with parameters for values. Anything interpolated into the query string (labels, property keys, relationship types) must pass through `gql_identifier_adapter.validate_strings(...)` from [gql.py](src/neontology/gql.py), which enforces `^[a-zA-Z][a-zA-Z0-9_]+$`. Follow this in any new query construction.

### Results and tooling

`NeontologyResult` ([result.py](src/neontology/result.py)) carries typed `nodes`/`relationships` plus per-record maps, and can emit node-link data. [schema_utils.py](src/neontology/schema_utils.py) turns model annotations into `NodeSchema`/`RelationshipSchema` and renders markdown tables (`neontology_schema()` on nodes/relationships). [tools/](src/neontology/tools/) provides bulk ingest from JSON/YAML/Markdown-frontmatter files and from raw record dicts.

## Conventions

- Ruff with `line-length = 128` and pydocstyle (google convention) — docstrings are required on public functions/methods (`D100`, `D101`, `D104`, `D107` are ignored). Lint/format target is `src` only.
- `requires-python = ">=3.9"`: no `X | Y` unions at runtime, use `from __future__ import annotations` or `Optional`/`Union`; import `Self`/`ParamSpec` from `typing_extensions`.
- New user-facing behaviour normally needs a `docs/` update and a `CHANGELOG.md` entry under the next version.
