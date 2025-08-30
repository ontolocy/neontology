# Changelog

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
