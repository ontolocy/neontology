# Changelog

## v2.0.0

### Features

- Added support for swappable `GraphEngine` backends, starting with Memgraph and Kuzu alongside Neo4j.
- Added support for exploring relationships from Nodes with `@related_nodes`, `@related_property` and `get_related_nodes()`.
- Added import and export functionality.
- Added neontology schema methods to nodes and relationships

### Changed

- Dropped support for Python v3.7
- Changed the function signature for `init_neontology`.
- Changed behaviour of `GraphConnection` to more consistently raise an explicit error if the connection isn't established.
- Renamed `auto_constrain` to `auto_constrain_neo4j`.
- Renamed `apply_constraints` to `apply_neo4j_constraints`.
- Trying to create a node with a primary that already exists will raise an exception
- merge multiple relationships with heterogenous source labels and target labels

### Dependencies

- Bumped Pandas dependency to v2

## v1.0.0

### Features

- Support for multiple labels on nodes as '__secondarylabels__'

### Changed

- Upgrade pydantic dependency to v2+, this has significant repercussions
- Upgrade neo4j dependency to v5+
