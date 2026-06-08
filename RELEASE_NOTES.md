# ZIO-LMDB RELEASE NOTES

## Unreleased

- Add `tuple3KeyCodec` and `tuple4KeyCodec` derived by nesting `tuple2KeyCodec` on the left, preserving the prefix-scan property for all-fixed-width components
- Add `LMDBMulti.contains(key, document)` (and the matching `LMDBReadOps.multiContains`) for fast (key, value) existence checks against DUPSORT multi-collections
- Add `LMDB.streamPrefix[P, K, T](name, prefix)` and `streamPrefixWithKeys[P, K, T]` for cursor-backed scans of all records whose key starts with the byte-encoding of a partial-key value. Exposed as `LMDBCollection.streamPrefix` / `streamPrefixWithKeys` on the collection facade
- **Migrate JSON codec from `zio-json` to `jsoniter-scala`** for the default `LMDBCodecJson` derivation. Measured on a 400 000-record benchmark: write throughput rises from 322 606 r/s to 491 433 r/s (+52 %), read throughput rises from 683 887 r/s to 1 082 086 r/s (+58 %). The JSON-vs-ProtoBuf codec gap shrinks from 2.04× to 1.31× on writes and 1.18× on reads. `derives LMDBCodecJson` syntax and per-collection `LMDBCodec[T]` typeclass surface are unchanged. A single `import zio.lmdb.json.*` now exposes the primitive codecs and the `.toJson` / `.fromJson[T]` extension methods (previously imported from `zio.json.*`)
- **Removed**: the `zio.json.ast.Json` / `Json.Str` / `Json.Num` codecs that were previously exposed via `import zio.lmdb.json.*`. Users storing a generic JSON tree via these codecs must migrate: either to a typed case class with `derives LMDBCodecJson`, to a raw-text `LMDBCodec[String]`, or to the new `zio.lmdb.json.JValue` sum type (see next entry)
- Add `zio.lmdb.json.JValue`, a tagged sum type that fills the role of a generic JSON tree (`StringV`, `LongV`, `DoubleV`, `DecimalV`, `BoolV`, `InstantV`, `IdentifierV`, `ListV(Seq[JValue])`, `MapV(Map[String, JValue])`, `NullV`). `MapV` answers the `Map[String, Value]` open question for L3 property bags. The sum-type codec is hand-rolled (the jsoniter-scala 2.38 + Scala 3.3.7 macro emits a forward-reference error for this specific variant combination) and uses a `{"type":"VariantName","value":...}` wire format with a 256-deep recursion guard. Per-variant `derives LMDBCodecJson` is intact for narrowly-typed collections like `[String, StringV]`
- Introduce a per-collection schema catalog (L2A): the new `zio.lmdb.schema.SchemaArtifact` ADT (`JsonSchema` / `ProtobufSchema` / `OpaqueSchema` / `KeySchema`) and `LMDBSchema[T]` typeclass with a low-priority opaque fallback. `MetaDataEntry` now carries `keySchema`, `valueSchema` as `Option[SchemaArtifact]` plus a new `layoutVersion: Int`. The typed creation paths (`collectionCreate[K, T]`, `multiCreate[K, T]`, `indexCreate[FROM, TO]`) persist real schemas; untyped paths (`collectionAllocate(name)`) leave them as `None`
- **Wire-format change (breaking for variable-width keys)**: `tuple2KeyCodec` now escapes `0x00 → 0x00 0x01` and uses a two-byte separator `0x00 0x00`, fixing a decoding bug that misread the separator whenever the second component's first byte was `0xFF`. Databases holding tuple keys with a variable-width first component (e.g. `(String, X)`) must be re-encoded. Tuples whose first component is fixed-width (`UUID`, `Int`, `Long`, `Short`) are unaffected. See `docs/internal/MIGRATION_TUPLE_KEY_CODEC.md`
- **Wire-format change (breaking for metadata)**: `MetaDataEntry` gained a `layoutVersion` field and its `keySchema` / `valueSchema` are now `Option[SchemaArtifact]` rather than `Option[Json]`. Metadata collections written by 2.8.x cannot be read by this release. Existing databases must be re-bootstrapped by re-creating each collection through the typed APIs. See `docs/internal/MIGRATION_METADATA_ENTRY.md`
- Fix coordinate denormalization in GEOTools to ensure stable encoding-decoding behavior
- Make every `KeyCodec[K]` self-identifying via an abstract `keyId: KeyTypeId` — a versioned URN naming the exact key type (`lmdb:int64`, `lmdb:uuid`, `lmdb-geo:location/v1`, …) — implemented by all built-in and extension key codecs and persisted as `SchemaArtifact.KeySchema(keyId)`. A key's type is therefore recorded rather than guessed from byte width (a `Long` and a geo key are both 8 bytes; `UUID`/`UUIDv7`/`ULID` are all 16), which lets readers like the SQL layer decode keys without ambiguity. Also promote the byte-array identity codec to a first-class `given KeyCodec[Array[Byte]]` (`lmdb:bytes`) in the core `keycodecs` module
- Add `derives LMDBSchema`: automatic structural schema derivation via the new `SchemaShape[T]` (Mirror-based, no reflection) emitting a JSON-Schema-like `JValue`; keys resolve automatically to a `SchemaArtifact.KeySchema(keyId)`. Makes derived collections self-describing for the catalog and SQL `DESCRIBE`
- Add a SQL layer (`zio-lmdb-sql`) that treats a collection as a table (key = `_key`, value fields = columns): `SELECT` (projections, `AS` aliases, `DISTINCT`), `WHERE` (`= != <> < <= > >=`, `AND`/`OR`/`NOT`, `LIKE`, `IS [NOT] NULL`, `LENGTH`), `GROUP BY`/`HAVING`, `ORDER BY`/`LIMIT`, aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), `INSERT`/`UPDATE`/`DELETE`, `DESCRIBE`, `SHOW`. Ships an interactive REPL (psql-style `\c`/`\l`/`\d`/`\format`, TAB completion, table/json/csv output) and a pure streaming `SqlEngine.run` pipeline
- Add `INNER`/`LEFT JOIN` to the SQL layer: table aliases, qualified columns (`alias.col`), `ON` equi-conditions, and value→key coercion — when one side of a join equality is a `_key`, the other side is converted to that key's datatype (value↔value joins require matching types). Joins compose with `WHERE`/`GROUP BY`/`HAVING`/`ORDER BY` and run as hash joins
- Add nested/dotted value-field access to the SQL layer: a column reference is a dotted path of any depth (`o.location.altitude`, `location.altitude`). The leading segment is a table alias when it names a source, otherwise the whole path indexes into the value; `_key`/`_value` may also lead a path. Resolution walks the JSON value tree (a missing field or a step through a non-object yields `NULL`) and works in `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, and `JOIN … ON`. Writes (`INSERT`/`UPDATE` columns) remain top-level
- Generalize SQL scalar functions to arbitrary `name(arg, …)` calls and allow function/expression results as `SELECT` projections and `ORDER BY` targets (not only columns). Add geo functions over value-side coordinates: `GEO_DISTANCE(lat1, lon1, lat2, lon2)` and the object form `GEO_DISTANCE(point, lat, lon)` (reading `latitude`/`longitude`) return the haversine great-circle distance in metres; `GEO_WITHIN(lat1, lon1, lat2, lon2, radius)` / `GEO_WITHIN(point, lat, lon, radius)` return a boolean — so `WHERE GEO_DISTANCE(...) <= r` filters within a radius and `ORDER BY GEO_DISTANCE(...)` returns nearest-first. Backed by a new `GEOTools.haversineMeters`; coordinate arguments are `NULL`-safe (a row without a location is excluded)
- Make SQL `AS` aliases referenceable in `WHERE` and `HAVING` (not only `ORDER BY`), a friendly extension to standard SQL: a projected alias is substituted by the expression it names, so `SELECT geo_distance(...) AS dist … WHERE dist <= 50000 ORDER BY dist` works without repeating the expression. An alias that resolves to an aggregate is still rejected in `WHERE` (it belongs in `HAVING`); where an alias shares a name with a real column, the alias wins within that query
- Add arithmetic expressions to the SQL layer (`+`, `-`, `*`, `/`, `%`) with standard precedence (`* / %` over `+ -`) and parentheses, usable anywhere an expression is allowed (`SELECT`, `WHERE`, `HAVING`, `ORDER BY`) and composable with functions and aliases — e.g. `geo_distance(m.location, 48.8566, 2.3522) / 1000 AS distKm … WHERE distKm <= 10`. Integer operands keep an integer result for `+`/`-`/`*`/`%`; division yields a decimal; a non-numeric operand or division/modulo by zero yields `NULL`
- Document the jsoniter codec migration, schema support, and SQL layer (including JOINs, nested value paths, and geo functions) under `docs/` (new `schema.md` and `sql.md` pages)

## 2.8 - 2026-05-18

- Introduce console with basic operations
- Add a meta-collection collection to store metadata about collections and indices
- Add multi collection type to support collections with several values for the same key
- Provide a protobuf usage example through a dedicated test spec
- Enhance performance under heavy concurrency
- Add `insert` operation to improve user experience 
- Add some basic/naive performance tests
- Fix concurrency-read crashes under a heavy load
- Add new key binary codecs (Long, Int, Short)
- Fix direct buffers memory leak

## 2.7 - 2026-04-12

- Add advanced index query support with filtering, limiting, and joins
- Add `indexName` to `IndexUpdater` trait to support indexing metadata
- Resolve concurrency freeze and optimize index rebuild
- Replace `TReentrantLock` with `Semaphore` for improved concurrency handling
  - fully relying on LMDB Multi-Version Concurrency Control (MVCC) mechanism
- Forbid nested write transaction
- Enhance dev environment

## 2.6 - 2026-04-09

- Add generic index support with `withIndexFull`
- Enhance `LMDBCollection` with high-performance `rebuildIndexes` and transactional streaming
- Enhance `QueryBuilder` with transactional support and indexed streaming methods
- Resolve concurrency deadlock and optimize codec errors
- Update dependencies

## 2.5 - 2026-03-07

- Add collection query DSL with basic join features
- Support automatic index content update in sync with collection changes

## 2.4 - 2026-02-24

- KeyCodec API consistency improvements
- Indexes API improvements
  - head, last, next, previous, hasKey operations added
  - indexed signature modified
    - no longer limited to only key associated values (with `limitToKey=false`)
    - now returns both the `from` key and the `to` key
- Enhanced error messages

## 2.3 - 2026-02-01

- generic `keycodecs` module added
  - basic UUID codec
  - basic String codec
  - compound tuple2 codec
- add specialized `keycodecs` for
  - latitude/longitude codec using Morton coding (8 bytes)
  - timestamps codec using nanoseconds precision (12 bytes instead of 30 bytes string)
  - UUIDv7 codec (16 bytes instead of 36 bytes string)
  - ULID codec (16 bytes instead of 26 bytes string)
- `LMDBKodec` renamed to `KeyCodec` and moved to the `keycodecs` module
- cross-collections transaction using `lift` through several collections 
- cross-indexes transaction using `lift` through several indexes and collections

## 2.2 - 2026-01-30

- transactions official support added
- index collections support added
- optimized storage for UUID keys provided
- improved performance
- switch to multi-module project structure

## 2.1 - 2025-07-18

- add support for customizable key types within the API
- introduce KeyCodec type class for key serialization/deserialization
- refactor API to use generic key type parameter instead of String only
- maintain backward compatibility with String keys
- enhance type safety and flexibility for key handling

## 2.0 - 2025-04-07

- support custom serialization layer using type class
- provide default json serialization layer using zio json
- support derivation for serialization auto-configuration
- enhance streaming internals
- drop scala 2.13 support

## 1.8 - 2024-01-21

- dependency updates
- add update operation
  - `def update(key: RecordKey, modifier: T => T): IO[UpdateErrors, Option[T]]`
  - will return None if no record was found
- change upsert method signature to return the updated/inserted record (instead of Unit previously)
  - `def upsert(key: RecordKey, modifier: Option[T] => T): IO[UpsertErrors, T]`
  - now the updated or inserted record is returned

## 1.7 - 2024-01-01

- upgrade to lmdb-java 1.9.0
- update dependencies
- add collectionDrop operation to delete a collection
- add the failIfExists parameter to collectionCreate
  - simplify API usage for various use cases
- enhance collect / stream / streamWithKey (#19)
  - in forward or backward key ordering
  - start after/before a given key
- do not display logs during unit test execution
- add more unit tests

## 1.5 - 2023-09-24

- add collection head, previous, next, last record operations (#18)
- update scala releases
- update dependencies

## 1.4 - 2023-08-25

- Add stream operations (#13)

## 1.3 - 2023-08-05

- `UpsertOverwrite` now doesn't care about the json definition of the previous stored value (#6)
- Change `upsert` & `upsertOverwrite` return type (#12)
    - `Unit` instead of `UpsertState`
    - `UpsertState` data type has been removed
- Add collection `contains` key operation

## 1.2 - 2023-06-17

- Add collection `clear` all content operation (#7)
