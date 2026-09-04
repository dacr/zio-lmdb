---
title: Home
layout: default
nav_order: 1
---
# ZIO-LMDB

[![Maven Central](https://img.shields.io/maven-central/v/fr.janalyse/zio-lmdb_3.svg)](https://mvnrepository.com/artifact/fr.janalyse/zio-lmdb)
[![Scaladoc](https://javadoc.io/badge2/fr.janalyse/zio-lmdb_3/scaladoc.svg)](https://javadoc.io/doc/fr.janalyse/zio-lmdb_3/latest/zio/lmdb/LMDB$.html)

**ZIO-LMDB** is an embedded, ACID, key-value database for Scala applications built on [ZIO](https://zio.dev/).  
It wraps [lmdb-java](https://github.com/lmdbjava/lmdbjava) with a higher-level, type-safe API that integrates naturally with the ZIO effect system.

---

## Why ZIO-LMDB?

- **Zero infrastructure** — the database lives in-process, no server to run or maintain.
- **ACID guarantees** — full transactional semantics backed by LMDB's copy-on-write B+ tree.
- **Type-safe API** — keys and values are fully typed; codecs are resolved at compile time.
- **ZIO-native** — every operation returns a `ZIO` effect with precise error channels.
- **Honest signatures** — the return type tells you exactly what can go wrong.
- **JSON storage by default** — `derives LMDBCodecJson` is all you need; custom codecs are supported.
- **Lexicographic ordering** — keys are automatically sorted, enabling efficient range scans.

---

## Collection types

| Type | Cardinality | Facade |
|---|---|---|
| Regular | one key → one value | [`LMDBCollection[K, T]`](collection.html) |
| Multi | one key → many values | [`LMDBMulti[K, T]`](multi.html) |
| Index | one key → many keys | [`LMDBIndex[FROM, TO]`](lmdb-index.html) |
| Vector | one key → one embedding | [`LMDBVectorIndex[K]`](vector-search.html) (separate `zio-lmdb-vector` module) |

---

## Quick install

Add the dependency to your `build.sbt`:

```scala
libraryDependencies += "fr.janalyse" %% "zio-lmdb" % "2.8.6"
```

For `scala-cli` scripts, add at the top of your file:

```scala
//> using dep fr.janalyse::zio-lmdb:2.8.6
//> using javaOpt --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

{: .note }
The JVM options are required when running on recent JVMs (Java 17+).

---

## Minimal example

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*

case class User(name: String, age: Int) derives LMDBCodecJson

val program = for {
  users  <- LMDB.collectionCreate[String, User]("users", failIfExists = false)
  _      <- users.insert("alice", User("Alice", 30))
  result <- users.fetch("alice")
  _      <- Console.printLine(result)
} yield ()

object Main extends ZIOAppDefault:
  def run = program.provide(LMDB.liveWithDatabaseName("my-app"), Scope.default)
```

---

## JVM requirements

When LMDB is used with recent JVMs, add the following options at startup:

```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

---

## In this documentation

- **[Getting Started](getting-started.html)** — setup, layers, and your first CRUD operations.
- **[LMDBCollection](collection.html)** — one key → one value collections.
- **[LMDBMulti](multi.html)** — one key → many values collections.
- **[LMDBIndex](lmdb-index.html)** — key-to-key index collections.
- **[Transactions](transactions.html)** — atomic multi-operation transactions.
- **[Codecs](codecs.html)** — key codecs and value codecs reference.
- **[Schemas](schema.html)** — self-describing collections and drift detection.
- **[SQL](sql.html)** — query and mutate collections with SQL, plus the REPL.
- **[Query DSL](query-dsl.html)** — fluent query and join API.
- **[Configuration](configuration.html)** — all configuration parameters.
- **[Vector Search](vector-search.html)** — nearest-neighbor search over embeddings (`zio-lmdb-vector` module).
