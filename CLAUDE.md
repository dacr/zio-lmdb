# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

zio-lmdb (`fr.janalyse`) — an embedded, ACID, key-value database library for ZIO, built on lmdbjava. Scala 3, sbt multi-module build. Development happens inside the Nix flake dev shell (`nix develop`), which provides the JDK 21, sbt, scala-cli, scalafmt, and the native `protoc` that `build.sbt` picks up for the core module's protobuf test sources.

## Commands

```bash
sbt test                                        # all unit tests
sbt core/test                                   # one module (core, keycodecs, queryDsl, sql, ...)
sbt "core/testOnly zio.lmdb.LMDBBasicUsageSpec"                    # single spec
sbt "core/testOnly zio.lmdb.LMDBBasicUsageSpec -- -t \"some test\""  # single test in a spec
make readme-test                                # runs the README quick example via scala-cli
make performance-test                           # just the performance specs (JSON + protobuf)
sbt sql/assembly && make sql-console            # build & launch the SQL REPL jar
sbt dependencyUpdates                           # dependency freshness check
scalafmt                                        # formatting (.scalafmt.conf: scala3 dialect, maxColumn 250)
```

The test JVM requires `--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED`; `build.sbt` already sets this via `fork := true` + `javaOptions`, and any standalone/scala-cli run needs the same flags.

## Module layout & dependencies

- `core` (artifact `zio-lmdb`) — the database itself; depends on `keycodecs`.
- `codecs/keycodecs` — the `KeyCodec[K]` abstraction plus codecs for standard types; sibling modules `keycodecs-ulid`, `keycodecs-uuidv7`, `keycodecs-geo`, `keycodecs-timestamp`, `keycodecs-uca` add one dependency-heavy codec each and all depend only on `keycodecs`.
- `query-dsl` — `QueryBuilder` fluent filtering/streaming API on top of `core`.
- `vector-search` (artifact `zio-lmdb-vector`) — `LMDBVectorIndex[K]` nearest-neighbor search over fixed-dimension float vectors (embeddings), backed by a plain `LMDBCollection`. Two search paths: `searchNearest` (exact full scan, parallelized across cores, optionally over a `warm()` in-memory snapshot) and `searchApproximate` (in-memory `HnswIndex` graph built by `buildApproximateIndex`, sub-linear but approximate). Depends only on `core`.
- `sql` (artifact `zio-lmdb-sql`) — SQL REPL over any zio-lmdb database; depends on core, query-dsl, and all keycodec modules. Assembles to a runnable jar (`assembly / mainClass` = `zio.lmdb.sql.repl.Main`).

## Core architecture

- `LMDB` (core/src/main/scala/zio/lmdb/LMDB.scala) is the service trait; its companion provides accessor methods and the layers `LMDB.live` / `LMDB.liveWithDatabaseName(name)` (both need `Scope`). Configuration comes from zio-config (`LMDBConfig`).
- `LMDBLive` is the single implementation (~2700 lines). Its concurrency model is load-bearing and documented inline: one write mutex (`TSemaphore`) with a dedicated single-thread write `Executor`, plus a `Semaphore`-bounded read pool pinned to a fixed read `Executor` so JNI/LMDB native calls always run on stable, bounded thread sets. Streams take scoped read permits. Be very careful moving effects across executors here — several specs (`LMDBConcurrencySpec`, `LMDBFreezeReproductionSpec`, `LMDBThreadCrashSpec`, `LMDBReadOpsDbiOpenRaceSpec`) exist to pin past deadlock/crash bugs.
- Three typed collection facades, all obtained from the `LMDB` service: `LMDBCollection[K,T]` (1 key → 1 value), `LMDBMulti[K,T]` (1 key → N values), `LMDBIndex[FROM,TO]` (1 key → N keys). `IndexUpdater` maintains auto-indexes.
- `LMDBOps` defines `LMDBReadOps`/`LMDBWriteOps`, the operation sets shared by facades and transactions.
- Transactions follow LMDB's MVCC: many concurrent readers, exactly one writer, no nested write transactions (`NestedWriteTransactionError`). Per-collection: `facade.readOnly { tx => ... }` / `facade.readWrite { tx => ... }`. Cross-collection: `lmdb.readWrite { ops => ... }` with each facade adapted via `lift`. Commit on success, rollback on any error — all automatic.
- Two codec families: `LMDBCodec[T]` for values (JSON via jsoniter-scala, derivable with `derives LMDBCodecJson`; protobuf codecs exercised in tests) and `KeyCodec[K]` for keys, whose byte encodings must preserve lexicographic ordering since LMDB sorts keys and range/prefix streaming depends on it.
- `schema/` (`LMDBSchema`, `SchemaShape`, `SchemaArtifact`) records key/value schemas in collection metadata to detect schema drift.
- Errors are typed ADTs in `LMDBIssues.scala` (`StorageSystemError`, union aliases like `CreateErrors`, `GetErrors`...).

## SQL module architecture

Pipeline: `parser/SqlParser` (fastparse) → `parser/Ast` → `engine/Engine` executing against `engine/Catalog` (collections discovered from the LMDB database) → `result/QueryResult` rendered by `result/Renderers` (multiple output formats). `repl/Main` is the jline-based REPL and also supports non-interactive execution via `--execute`/`-e` and `--format`/`-f`. `runtime/KeyRegistry` maps declared key types to keycodecs. Follow standard SQL clause order and semantics; don't add lenient parsing.

## Docs

`docs/` is the user-facing GitHub Pages site (getting-started, collections, transactions, codecs, query-dsl, sql, configuration, schema) — update the relevant page when changing public API behavior. `docs/internal/` holds design notes, audits, and evolution plans, not user documentation.
