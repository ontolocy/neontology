# Changelog

## v2.0.0

### Features

- Added support for swappable `GraphEngine` backends, starting with Memgraph and Kuzu.
- Added support for exploring relationships from Nodes with `@related_nodes`, `@related_property` and `get_related_nodes()`.

### Changed

- Changed the function signature for `init_neontology` to support different GraphEngines.
- Changed behaviour of `GraphConnection` to more consistently raise an explicit error if the connection isn't established.
- Renamed `auto_constrain` to `auto_constrain_neo4j`.
- Renamed `apply_constraints` to `apply_neo4j_constraints`.

## v1.0.0

### Features

- Support for multiple labels on nodes as '__secondarylabels__'

### Changed

- Upgrade pydantic dependency to v2+, this has significant repurcussions
- Upgrade neo4j dependency to v5+
