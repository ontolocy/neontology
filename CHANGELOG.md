# Changelog

## v3.0.0

### Removed

- Support for Python 3.9, which is end of life. Neontology now requires Python 3.10 or later.
- `BaseNode.get_primary_property_value()`, deprecated since v2.0. Use `get_pp()`.
- **The `config` argument to `GraphConnection()`.** `GraphConnection()` now takes no arguments and is purely an accessor: it returns the connection that `init_neontology(config)` established. The argument was honoured on the first call in a process and silently ignored on every later one, so the signature promised to define the connection and did not. It was never a documented way to connect - every documented path is `init_neontology(config)` followed by `GraphConnection()` - but it did work for a first call, so any code using it needs changing to `init_neontology(config)`. Passing anything now raises a `TypeError` saying exactly that, rather than failing silently or with a bare argument-count error.
- The `neo4j_uri`, `neo4j_username` and `neo4j_password` keyword arguments to `init_neontology`, deprecated since v2.0. Pass a `Neo4jConfig` instead. `init_neontology` no longer accepts arbitrary keyword arguments, so the old call raises a `TypeError` naming the argument rather than being silently ignored.

### Added

- `get_node_types()`, `get_rels_by_type()`, `get_rels_by_source()` and `get_rels_by_target()` are now exported from the package root, so `from neontology import get_node_types` works rather than needing a `neontology.utils` submodule import.
- **Model classes now register themselves as they are defined**, into a registry (`neontology.registry`) rather than being discovered by walking `__subclasses__()` each time they are needed. Nothing has to be declared or passed in - defining a class is still all it takes - but the result is dependable in ways the walk was not, and the three fixes below all follow from it.
- A primary label or relationship type claimed by two different model classes now raises a `DuplicateLabelWarning` naming both classes and their modules, at the point the second one is defined. Previously the clash was resolved silently by whichever class the hierarchy happened to be walked into first, so data written as one model came back as the other. Set `registry.strict = True` (or escalate `DuplicateLabelWarning` with `warnings.simplefilter`) to make it an error. Re-running a notebook cell or reloading a module is not treated as a clash.
- A subclass that inherits a concrete `__primarylabel__` instead of declaring its own now raises an `InheritedLabelWarning`. It takes over that label, so nodes written as the parent came back as the subclass with defaults invented for fields that were never stored - silently, until now.
- Models defined inside a function no longer disappear from type discovery when the garbage collector runs. `__subclasses__()` holds weak references, so whether such a model could be used to build query results depended on garbage collection timing.
- A `rust` install extra (`pip install neontology[rust]`), which installs the Neo4j driver's Rust extensions. The driver detects and uses them automatically. Deliberately not part of `all`, since it needs a compiled wheel for the platform.
- Engine capabilities. Where an engine cannot do something, it is named in `Capability` (`neontology.graphengines.capabilities`) and declared on the engine, which reports it through `GraphEngineBase.supports()`. `docs/graph-engines.md` carries a matrix generated from those declarations. Only the experimental NetworkX engine currently diverges.
- Index management. `apply_index()`, `get_indexes()` and `drop_index()` on the engine and on `GraphConnection` create an index on a label/property pair without requiring uniqueness. Covered by the new `INDEXES` capability. `get_indexes()` deliberately excludes indexes backing a constraint and indexes the database maintains for itself (Neo4j's token `LOOKUP` indexes), so a teardown loop over the list cannot destroy them.
- Constraint support for Memgraph, which previously raised `NotImplementedError` despite Memgraph supporting uniqueness constraints. Memgraph rejects constraint and index manipulation inside the transaction `driver.execute_query` opens, so these go through an implicit (autocommit) transaction instead.
- `GraphConnection.supports(capability)`, so portable code can check whether the current backend does constraints or indexes rather than catching an error.
- `CapabilityNotSupportedError`, raised when an engine is asked for something its backend cannot do. It subclasses `NotImplementedError`, so existing `except NotImplementedError` callers keep working, and its message names the engine and capability. On the NetworkX engine it also explains that uniqueness on the primary property already holds structurally, since nodes there are keyed by a hash of (primary property, primary label).
- `Constraint` and `Index` (`neontology.graphengines.dbschema`) describing what the database holds. They are read types returned by `get_constraints()` / `get_indexes()` and passed back to the matching drop method - Neo4j names constraints and indexes while Memgraph identifies them by pattern, so the description travels rather than a name.
- `pandas` and `all` install extras, alongside the existing `grand`.
- **`__inheritablelabels__`, for labels which pass down a class hierarchy.** Labels listed there are carried by the declaring class and every class inheriting from it, and a subclass cannot replace them - unlike `__secondarylabels__`, which a subclass replaces by declaring its own. A class listing its own primary label passes it down, so an `Employee(Person)` is written as `:Employee:Person` and `MATCH (p:Person)` finds employees too. Abstract classes can declare them as well.
- Queries on a class now return nodes of its subclasses which carry its label, built as the subclass: `Person.match_nodes()` and `Person.match()` return an employee as an `Employee`. The same holds for any query, because a node is now built as the most derived class its labels allow.
- A class carrying the primary label of a class it does not inherit from, through `__secondarylabels__` or `__inheritablelabels__`, now raises a `DuplicateLabelWarning` naming both classes when it is defined, or a `DuplicateLabelError` in strict mode. Its nodes would match queries for that class without being one. Either class may be defined first.

### Changed

- `GraphConnection.global_nodes` and `global_rels` are deprecated and will be removed in v4. Use `get_node_types()` and `get_rels_by_type()`, which are the same lookup without needing a connection - which models you have defined is not a property of the connection, and inspecting them (to generate schema documentation, or to check your models in CI) should not require a live database. They were never documented.
- **Instantiating an abstract node or relationship now always raises `NotImplementedError`, naming the class and explaining that it is abstract.** Previously the two ways of spelling an abstract node behaved differently: `__primarylabel__ = None` raised `NotImplementedError` but only after warning that the label was not alphanumeric, while never declaring `__primarylabel__` at all raised a bare `AttributeError` that said nothing about the class being abstract. A missing label or relationship type is deliberate rather than malformed, so it is no longer reported as a bad identifier. `BaseNode._is_abstract()` and `BaseRelationship._is_abstract()` are the single place that decides.
- The `refresh_classes` argument to `GraphConnection.evaluate_query` is deprecated, has no effect, and will be removed in v4. The node and relationship type maps are now always current, because classes register themselves as they are defined, so there is nothing to refresh. This also removes the type discovery walk from every query: with 400 node and 400 relationship classes defined, that walk cost around 600us per query and now costs around 25us.
- `GraphConnection.global_nodes` and `global_rels` are now read-only views onto the registry rather than cached copies refreshed per query. They were previously assigned as a class attribute on connect and an instance attribute on query, so reading `GraphConnection.global_nodes` from the class gave a snapshot taken at connection time.
- Secondary labels are now validated as GQL identifiers when a node is instantiated, warning in the same way a malformed primary label already did. They are interpolated into Cypher alongside the primary label, so they are held to the same standard.
- **`init_neontology(config)` now (re)establishes the connection every time it is called**, rather than doing nothing when one already exists. Previously only the first call in a process had any effect, so pointing Neontology at a different database mid-process silently did nothing and you kept talking to the original one. Calling it again now connects with the new config and closes the previous connection. Two things make this safe: the new connection is established and verified *before* the old one is closed, so a config that cannot connect leaves the existing connection working; and the engine is swapped on the existing `GraphConnection`, so anything already holding one keeps working. Note that `init_neontology` is now an application startup call rather than something to call defensively - repeated calls reconnect each time.
- `GraphConnection.change_engine()` is deprecated and will be removed in v4. `init_neontology(config)` now does exactly the same thing, so there is one way to say which database to talk to rather than two.
- **Constraints have moved from `neontology.utils` onto the graph engines**, where the implementation and the question of whether the backend has them at all belong. `auto_constrain_neo4j()` is now `GraphConnection().auto_constrain()` and `apply_neo4j_constraints(node_types)` is now `GraphConnection().apply_constraints(node_types)` - the old names said neo4j but the call went to whichever engine was connected. Both old names still work and emit a `DeprecationWarning`; they will be removed in v4.
- Constraints are applied through named methods rather than a constraint object: `engine.apply_uniqueness_constraint(label, properties)` and `engine.apply_existence_constraint(label, properties)`. `properties` takes a single name or a sequence for a composite constraint.
- `GraphEngineBase.get_constraints()` returns `Constraint` objects rather than a list of names, and `drop_constraint()` takes one of those rather than a name. A bare name does not round trip on Memgraph, which does not name constraints. `apply_constraint(label, property)` is replaced by `apply_uniqueness_constraint`.
- Requires the Neo4j driver v6 (`neo4j>=6.0,<7`), up from v5. The driver's own breaking changes are in [its changelog](https://github.com/neo4j/neo4j-python-driver/wiki/6.x-changelog); none of the removed or changed APIs are used by neontology, so no code change is needed when upgrading.
- **pandas is now an optional extra rather than a required dependency.** Install it with `pip install neontology[pandas]` if you use `merge_df`. The core ingest path, `merge_records`, takes plain dictionaries and needs nothing extra. Calling `merge_df` without pandas installed raises an `ImportError` explaining how to install it. This roughly halves `import neontology` time for everyone who does not use dataframes.
- The `pandas` extra allows pandas 3 (`>=2.0,<4`). pandas 3 requires Python 3.11+, so 2.x is still resolved on Python 3.10; both are supported.
- `BaseNode.merge_records` now returns one node per input record, in the order given, rather than one per distinct node merged. It also takes a `deduplicate` argument (default `True`) so identical records are merged only once. `merge_df` is now a thin wrapper around it, so the deduplication and ordering behaviour that used to be dataframe-only is available without pandas.
- The `grand` extra now requires `grand-cypher>=1.2.0`. The NetworkX engine depends on the flattened relationship result shape introduced in 1.x.
- `evaluate_query_single` on the NetworkX engine now returns a plain value, as the other engines do, rather than the raw grand-cypher column.
- The NetworkX engine now supports case insensitive filters (`__icontains`, `__iexact`, `__istartswith`), which previously raised `NotImplementedError`. Every engine now supports these, so the `CASE_INSENSITIVE_FILTERS` capability has been removed from the vocabulary.
- The `COLLECTED_VALUES` capability is now `COLLECT_DISTINCT`. Plain `COLLECT` works on every engine; only `DISTINCT` inside an aggregation is unsupported by the NetworkX engine.
- On the NetworkX engine, datetime accessors in a query (`RETURN n.created.year`) now raise a parse error rather than silently returning the datetime and ignoring the accessor. Covered by the `DATETIME_FUNCTIONS` capability.
- `init_neontology` now raises a `ValueError` naming the available engines when `NEONTOLOGY_ENGINE` is set to something unavailable, instead of a bare `KeyError`. Asking for `NETWORKX` without the optional `grand` extra installed now points at the extra.
- **`merge()` now finds an existing node by its primary label and primary property alone**, then adds the model's other labels. On Neo4j and Memgraph it previously merged on every label, so adding a secondary label to a model made `merge()` stop matching the nodes already in the graph and create duplicates of them instead. Merging still never removes a label from a node, and on the NetworkX engine it now keeps labels already on a node rather than replacing them, as the other engines do.
- On the NetworkX engine, `delete()` now matches nodes on their label as the other engines do, so deleting through a parent class also deletes subclass nodes carrying its label.
- `neontology_schema()` now reports every label a node carries in `secondary_labels`, including inheritable ones.
- **Query results are now built the same way on every engine**, so the same query gives the same `NeontologyResult` whatever the backend. Each engine reads its driver's rows into engine-neutral values, and one builder - `build_result()` in `neontology.graphengines.hydration` - turns them into Neontology objects. The per-engine conversion functions it replaces (`neo4j_records_to_neontology_records`, `neo4j_node_to_neontology_node`, `neo4j_relationship_to_neontology_rel`, `grand_cypher_to_neontology_records`, `grand_node_to_neontology_node` and `grand_relationship_to_neontology_relationship`) have been removed, and `convert_neo4j_types` has moved to `neontology.graphengines.bolt`. Where the engines disagreed, they now all behave as follows:
  - `records` has one entry per row. The NetworkX engine returned no records for a query returning only values, and put `None` in a record for anything it could not build, where Neo4j and Memgraph left it out.
  - A relationship whose source or target node cannot be built is left out, with a warning saying so. Neo4j and Memgraph raised a `ValidationError`, and NetworkX warned that the query "did not include nodes", which was not the problem.
  - A path including a relationship which cannot be built is left out entirely, with a warning. Neo4j and Memgraph kept the path with `None` in place of the relationship, and NetworkX silently dropped that step and returned a shorter path.
  - A variable length relationship (`-[r*1..3]->`), which is returned as a list of relationships, is not built into a path. The NetworkX engine treated lists of two or more hops as paths; Neo4j and Memgraph never did.
- **`relationships` and `paths` now list each distinct relationship and path once**, as `nodes` already did, rather than once for every row it appeared on. `records` still shows every row, and `records_raw` the driver's own result. Distinct means the database's identity rather than equal contents, so two parallel relationships with the same properties are both kept - and `node_link_data`, which removed duplicate relationships by hashing their contents, no longer merges them into one edge.
- `nodes` is now deduplicated by database identity rather than by label and primary property. The two only differ where nothing constrains a label and primary property to be unique and two nodes share them: both are now returned, where one silently replaced the other and its properties were lost. `node_link_data` still identifies nodes by label and primary property, as its format requires.
- Each node and relationship is built once per result, so a relationship's `source` and `target` are the same objects as the nodes in `nodes`, and anything appearing on several rows is the same object on each. On Neo4j and Memgraph they were separate copies.
- `repr()` of a `NeontologyResult` no longer includes `node_link_data`, which was built - dumping every node and relationship - whenever a result was printed or logged. It is still a property, and still part of `model_dump()`.
- Neo4j and Memgraph share their connection and query code through a `BoltEngine` base class in `neontology.graphengines.bolt`, rather than each carrying an identical copy.
- New `MULTI_PATTERN_PATHS` capability. On the NetworkX engine, a named path in a query matching more than one pattern - two `MATCH` clauses, say - includes nodes and relationships from the other patterns, because grand-cypher builds the path from every node the query matched.
- Every engine now decides which class to build a node as in the same place, `node_class_for_labels()` in `neontology.graphengines.hydration`. A node matching no single class now warns the same way on every engine - "Unexpected primary labels returned", followed by why: no defined class has any of its labels, or they belong to classes which do not inherit from one another. The NetworkX engine previously worded these differently, and the other engines gave no reason.

### Fixed

- **Temporal properties now read back on Neo4j and Memgraph.** A `timedelta` property, or a list of `datetime`, `date`, `time` or `timedelta` values, could be written but not read: the driver's own `Duration` was never converted back, and neither were temporal values inside lists. Because `create()` and `merge()` build the node they return from the database, they raised a `ValidationError` after the write had already been committed. A duration counting months, which Neontology never writes, is left as the driver's `Duration`, since a month has no fixed length.
- `evaluate_query` given `relationship_classes` as a plain dictionary no longer raises a `KeyError` on Neo4j and Memgraph when the query returns a relationship type the dictionary lacks. The relationship is left out with a warning, as it was on NetworkX.
- On the NetworkX engine, a path returned without its nodes (`MATCH p=... RETURN p`) is now built, rather than raising `IndexError`.
- **A secondary label which is also another class' primary label no longer breaks reading nodes back.** Giving a subclass its parent's label - `Employee(Person)` with `__secondarylabels__ = ["Person"]` - matched the node to both classes. It was silently dropped from general query results, along with any relationship touching it, while `Person.match()` and `Person.match_nodes()` raised a `ValidationError` trying to build the employee as a `Person` and `Person.get_count()` counted it. The node is now built as the most derived class.
- **Filter keys are now validated before being interpolated into Cypher.** `match_nodes(filters=...)` and `get_count(filters=...)` built the WHERE clause by interpolating the filter key directly, so a key containing Cypher was injected into the query and executed. Values were always parameterised and were never affected; only keys. Field names are now validated as GQL identifiers and a `ValueError` is raised otherwise. Any application passing user-controlled strings as filter *keys* was exposed and should upgrade.
- A filter key containing more than one `__` (`created__date__gt`, or a field whose own name contains `__`) raised `ValueError: too many values to unpack` instead of being parsed. The lookup is now taken from the right, so `a__b__gt` reads as field `a__b` with the `gt` lookup. A key containing `__` must still end in a recognised lookup, so a typo like `name__startswit` remains an error rather than silently matching nothing.
- A failed `GraphConnection.change_engine()` no longer destroys the working connection. It closed the live engine and installed the new one *before* verifying it, so a config that could not connect left the singleton holding an unusable engine with the previous connection already closed. The new engine is now built and verified first, and the old one is only closed once the swap has succeeded.
- `GraphConnection.close()` now clears the singleton, so `init_neontology()` can establish a fresh connection afterwards. Previously the closed instance stayed cached: `init_neontology()` handed the same dead object back and every query raised `DriverError: Driver closed`, with no way to reconnect short of resetting `GraphConnection._instance` by hand.
- A connection that fails while being established is no longer cached. The instance was assigned to `GraphConnection._instance` before being verified and rolled back only when `verify_connection()` returned `False` - an engine that *raised* instead left the broken instance cached and handed out to every later call. The connection is now only published once it is known good, and an engine discarded after a failed verification is closed rather than leaked.
- `merge_df` no longer silently drops distinct rows. Deduplication keyed on every column stringified and concatenated, so `{"name": "ab", "role": "c"}` and `{"name": "a", "role": "bc"}` both keyed to `"abc"` and only one of the two nodes was created.
- `merge_df` now converts missing values to `None` for every column type. It replaced them in place, which on a typed column coerces back to that column's own missing value - so a missing string arrived at the model as `NaN` and failed validation. This was already wrong for numeric columns under pandas 2, and pandas 3's typed string columns made it wrong for text as well.
- Methods decorated with `@related_nodes` or `@related_property` are now called once per invocation rather than twice. The decorator worked out whether the method returned a query or a `(query, parameters)` pair by unpacking optimistically and catching the failure, so a method returning just a query ran twice - and a `ValueError` raised inside the method was mistaken for that signal and swallowed.
- Schema generation now reports parametrised generics consistently on Python 3.10. `isinstance(list[str], type)` is True on 3.10 but False from 3.11, so `extract_type_mapping` took the plain-type branch and described a `list[str]` property as `list`.

### Performance

- `get_pp()` now dumps only the primary property rather than the whole model. It went through `_get_merge_parameters()`, which dumps and converts every property and builds three more dictionaries to reach one value - and it is called once per node when query results are hydrated. Around 3.4x faster on a model with a dozen properties. The returned value is unchanged, including the engine type conversion that makes a UUID primary property arrive as a string.
- `BaseNode.create_nodes()` builds each node's property dictionary once instead of twice. It called `_engine_dict()` a second time purely to read the primary property back out, doubling the cost of the dump and conversion for every node in a bulk create.
- Neo4j and Memgraph no longer build a relationship's source and target nodes a second time: each node is built once per result, however many rows and relationships it appears in. A `RETURN a, r, b` query constructed four node models per row, and now constructs two.

Bulk operations and queries are substantially faster; behaviour is unchanged.

- Model property usage (`set_on_match`, `set_on_create`, `merge_on`) is worked out once per class rather than on every object construction, where it generated the pydantic JSON schema each time. Bulk merges improve by roughly 2x on Neo4j and Memgraph, and far more on NetworkX where database I/O does not mask the cost.
- The database connection is verified where it is established - in `init_neontology` and `change_engine` - rather than on every `GraphConnection()` call. `GraphConnection()` is how the library reaches the singleton, including once per model dump, so verification cost a database round trip per call: merging 1000 records made over 2000 of them. `change_engine` previously did not verify at all.
- Type discovery no longer walks the class hierarchy twice, and no longer reads `model_fields` twice per relationship. `RelationshipTypeData.all_source_classes` and `all_target_classes` are worked out on demand rather than when the object is built - nothing on the query path reads them. They remain readable as attributes and still appear in `model_dump()`.
- `get_rels_by_node` reads the attributes it needs rather than serialising the whole model to reach one of them, which dominated schema generation.

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
