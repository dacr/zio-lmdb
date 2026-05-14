---
title: Getting Started
nav_order: 2
---

# Getting Started
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Dependencies

### sbt

```scala
// build.sbt
libraryDependencies += "fr.janalyse" %% "zio-lmdb" % "2.8.1"
```

### scala-cli

```scala
//> using scala 3
//> using dep fr.janalyse::zio-lmdb:2.8.1
//> using javaOpt --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

### Optional modules

| Module | Artifact | Purpose |
|---|---|---|
| Key codecs | `fr.janalyse::keycodecs` | Base key codec infrastructure |
| Timestamp keys | `fr.janalyse::keycodecs-timestamp` | Nanosecond-precision timestamp keys |
| UUIDv7 keys | `fr.janalyse::keycodecs-uuidv7` | Time-ordered UUID keys |
| ULID keys | `fr.janalyse::keycodecs-ulid` | Universally Unique Lexicographically Sortable Identifiers |
| Geo keys | `fr.janalyse::keycodecs-geo` | Latitude/longitude keys via Morton coding |
| UCA keys | `fr.janalyse::keycodecs-uca` | Unicode Collation Algorithm string keys |
| Query DSL | `fr.janalyse::query-dsl` | Fluent query and join API |

---

## Providing the LMDB layer

ZIO-LMDB follows ZIO's service pattern. You provide an `LMDB` layer to your application:

```scala
// Reads configuration from environment variables or Java properties
LMDB.live

// Override just the database name, read the rest from environment
LMDB.liveWithDatabaseName("my-database")
```

The database is stored at `$HOME/.lmdb/<name>/` by default. See [Configuration](configuration.html) for all options.

---

## Defining your data model

Values are serialized with `LMDBCodec[T]`. The default implementation uses JSON via `zio-json`:

```scala
import zio.lmdb.json.*

case class Product(id: String, name: String, price: Double) derives LMDBCodecJson
```

The `derives LMDBCodecJson` macro generates both the `JsonEncoder` and `JsonDecoder` needed by the codec.

---

## Creating a collection

Collections are created (or retrieved if they already exist) with `LMDB.collectionCreate`:

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*
import java.util.UUID

case class Product(id: UUID, name: String, price: Double) derives LMDBCodecJson

val program = for {
  products <- LMDB.collectionCreate[UUID, Product]("products", failIfExists = false)
  // products: LMDBCollection[UUID, Product]
  ...
} yield ()
```

The type parameters `[UUID, Product]` fix the key and value types for all subsequent operations.  
Set `failIfExists = true` (the default) if you want the operation to fail when the collection already exists.

---

## CRUD operations

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*
import java.util.UUID, java.time.OffsetDateTime

case class Record(name: String, age: Int, createdAt: OffsetDateTime) derives LMDBCodecJson

object CrudExample extends ZIOAppDefault:
  def run =
    example.provide(LMDB.liveWithDatabaseName("my-app"), Scope.default)

  val example = for {
    col      <- LMDB.collectionCreate[UUID, Record]("records", failIfExists = false)
    id       <- Random.nextUUID
    now      <- Clock.currentDateTime
    record    = Record("Alice", 30, now)

    // Insert or overwrite
    _        <- col.upsertOverwrite(id, record)

    // Fetch by key
    fetched  <- col.fetch(id)

    // Atomic update (receives previous value as Option[T])
    _        <- col.upsert(id, prev => prev.map(_.copy(age = 31)).getOrElse(record))

    // Update only if key exists
    updated  <- col.update(id, r => r.copy(name = "Alice Smith"))

    // Delete
    deleted  <- col.delete(id)
    _        <- Console.printLine(s"Deleted: $deleted")

    // Key navigation
    first    <- col.head()
    last     <- col.last()
    next     <- first.flatMap((k, _) => col.next(k).option).getOrElse(None)
  } yield ()
```

---

## Streaming and collecting

```scala
// Load all records into memory
val all: IO[CollectErrors, List[Record]] = col.collect()

// Load filtered records
val young: IO[CollectErrors, List[Record]] =
  col.collect(valueFilter = _.age < 30)

// Stream records lazily
val stream: ZStream[Any, StreamErrors, Record] = col.stream()

// Stream (key, value) pairs in reverse order
val reversed: ZStream[Any, StreamErrors, (UUID, Record)] =
  col.streamWithKeys(backward = true)
```

---

## Next steps

- [LMDBCollection](collection.html) — full API reference for regular collections.
- [LMDBMulti](multi.html) — one key, many values.
- [LMDBIndex](lmdb-index.html) — secondary indexes.
- [Transactions](transactions.html) — group multiple writes atomically.
- [Codecs](codecs.html) — customize key and value serialization.
