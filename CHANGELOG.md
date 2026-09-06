# Changelog

## v3.0.0

### Changed

- The `pandas` extra allows pandas 3 (`>=2.0,<4`). pandas 3 requires Python 3.11+, so 2.x is still resolved on Python 3.10; both are supported.
- **pandas is now an optional extra rather than a required dependency.** Install it with `pip install neontology[pandas]` if you use `merge_df`. The core ingest path, `merge_records`, takes plain dictionaries and needs nothing extra. Calling `merge_df` without pandas installed raises an `ImportError` explaining how to install it. This roughly halves `import neontology` time for everyone who does not use dataframes.
- `BaseNode.merge_records` now returns one node per input record, in the order given, rather than one per distinct node merged. It also takes a `deduplicate` argument (default `True`) so identical records are merged only once. `merge_df` is now a thin wrapper around it, so the deduplication and ordering behaviour that used to be dataframe-only is available without pandas.
- The `grand` extra now requires `grand-cypher>=1.2.0`. The NetworkX engine depends on the flattened relationship result shape introduced in 1.x.
- The NetworkX engine now supports case insensitive filters (`__icontains`, `__iexact`, `__istartswith`), which previously raised `NotImplementedError`. grand-cypher gained `toLower` in 1.2.0, so every engine now supports these and they are no longer a point of divergence.

### Fixed

- Methods decorated with `@related_nodes` or `@related_property` are now called once per invocation rather than twice. The decorator worked out whether the method returned a query or a `(query, parameters)` pair by unpacking optimistically and catching the failure, so a method returning just a query ran twice - and a `ValueError` raised inside the method was mistaken for that signal and swallowed.
- `merge_df` now converts missing values to `None` for every column type. It replaced them with `pandas.NA`/`NaN` in place, which on a typed column coerces straight back to that column's own missing value - so a missing string arrived at the model as `NaN` and failed validation. This was already wrong for numeric columns under pandas 2, and pandas 3's typed string columns made it wrong for text as well.
- `merge_df` no longer silently drops distinct rows. Deduplication keyed on every column stringified and concatenated, so `{"name": "ab", "role": "c"}` and `{"name": "a", "role": "bc"}` both keyed to `"abc"` and only one of the two nodes was created.
- Schema generation now reports parametrised generics consistently on Python 3.10. `isinstance(list[str], type)` is True on 3.10 but False from 3.11, so `extract_type_mapping` took the plain-type branch on 3.10 and described a `list[str]` property as `list`.

### Changed

- `init_neontology` now raises a `ValueError` naming the available engines when `NEONTOLOGY_ENGINE` is set to something unavailable, instead of a bare `KeyError`. Asking for `NETWORKX` without the optional `grand` extra installed now points at the extra.

## v2.2.2

### Fixed

- Fixed bug where using ForwardRef type annotations for source/target types on a relationship cause errors. Note that ForwardRefs must be resolved before using the relationship (for creation or querying).

## v2.2.1

### Fixed

- Updated handling of relationship merging in NetworkxEngine to prevent duplicate relationships and to support 'merge_on' properties.

## v2.2.0

### Features

- Support for filtering on `match_nodes` and `get_count` (thanks to @BiBzz)
- Experimental support for `NetworkxEngine` on Python v3.10+

### Dependencies and Support

- Addition of optional `[grand]` dependency group for `NetworkxEngine`
- Addition of `[all]` dependency group

## v2.1.3

### Changed

- Updated handling of development dependencies

## v2.1.2

### Changed

- Removed deprecation validation error related to the use of 'created' and 'merged' fields

## v2.1.1

### Changed

- Improved type hinting
- Improved docstrings
- Bugfixes

### Development

- Consolidate build info into pyproject.toml
- Adopt UV

## v2.1.0

### Features

- Add support for Pydantic Field Aliases for Node/Relationship property names (thanks to @Forgen)

### Changed

- Tidies up type hinting with generics from standard library (thanks to @BiBzz)

## v2.0.5

### Dependencies and Support

- Removed explicit dependency on numpy

## v2.0.0

### Features

- Added support for swappable `GraphEngine` backends, starting with Memgraph alongside Neo4j. [link](/docs/graph-engines.md)
- Added support for exploring relationships from Nodes with `@related_nodes`, `@related_property` and `get_related_nodes()`.
- Added import and export functionality.
- Added neontology schema methods to nodes and relationships.

### Changed

- Removed default 'merged' and 'created' properties. Users will need to reimplement on custom base nodes / relationships if required. [link](/docs/recipes.md)
- Changed the function signature for `init_neontology`. [link](/docs/usage.md)
- Removed some automatic type conversion which may require users to add Pydantic serializers for types not supported natively by their database. [link](/docs/advanced-usage.md)
- Changed behaviour of `GraphConnection` to more consistently raise an explicit error if the connection isn't established.
- Renamed `auto_constrain` to `auto_constrain_neo4j`.
- Renamed `apply_constraints` to `apply_neo4j_constraints`.
- Support to merge multiple relationships with heterogenous source labels and target labels (removing need to separately specify labels)

### Dependencies and Support

- Dropped support for Python v3.7, v3.8 which are both now end of life
- Bumped Pandas dependency to v2

## v1.0.0

### Features

- Support for multiple labels on nodes as '__secondarylabels__'

### Changed

- Upgrade pydantic dependency to v2+, this has significant repercussions
- Upgrade neo4j dependency to v5+
