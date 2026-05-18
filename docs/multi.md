---
title: LMDBMulti
nav_order: 4
---

# LMDBMulti
{: .no_toc }

`LMDBMulti[K, T]` is the API facade for **multi-value collections** where a single key can hold multiple values.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

A regular `LMDBCollection` enforces one-value-per-key semantics. `LMDBMulti` lifts that restriction: the same key can be associated with **any number of values**, and each (key, value) pair is stored as a distinct entry in the underlying B+ tree.

Typical use cases:
- Storing multiple tags or labels per entity
- Recording historical events per entity ID
- Adjacency lists for graph-like relationships

```scala
val tags: LMDBMulti[String, String] =
  LMDB.multiCreate[String, String]("tags", failIfExists = false)
```

---

## Creating a multi-collection

```scala
LMDB.multiCreate[K, T](name: String, failIfExists: Boolean = true): IO[..., LMDBMulti[K, T]]
```

| Parameter | Description |
|---|---|
| `name` | Unique collection name within the database |
| `failIfExists` | If `true` (default), fails when the collection already exists |

---

## Read operations

### fetch

```scala
def fetch(key: K): IO[FetchErrors, List[T]]
```

Returns **all values** stored under `key`. Returns an empty list if the key has no associated values.

```scala
tags.fetch("product-42").map { values =>
  println(s"Tags: ${values.mkString(", ")}")
}
```

### size

```scala
def size(): IO[SizeErrors, Long]
```

Returns the **total number of (key, value) pairs** across all keys in the collection (not the number of distinct keys).

---

## Write operations

### put

```scala
def put(key: K, document: T): IO[UpsertErrors, Unit]
```

Adds `document` as a new value under `key`. Existing values for the same key are preserved.

```scala
// Accumulate tags for a product
tags.put("product-42", "electronics") *>
tags.put("product-42", "sale")        *>
tags.put("product-42", "new-arrival")
```

### delete

```scala
def delete(key: K, document: T): IO[DeleteErrors, Boolean]
```

Removes a **specific value** from the set of values stored under `key`. Returns `true` if the value was found and removed.

```scala
tags.delete("product-42", "sale")   // removes only the "sale" tag
```

### deleteAll

```scala
def deleteAll(key: K): IO[DeleteErrors, Boolean]
```

Removes **all values** associated with `key`. Returns `true` if any values were present and removed.

```scala
tags.deleteAll("product-42")   // removes the key and all its values
```

### clear

```scala
def clear(): IO[ClearErrors, Unit]
```

Removes all entries from the collection.

---

## Transactions

### readOnly

```scala
def readOnly[R, E, A](f: LMDBMultiReadOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A]
```

Executes read operations atomically within a single read-only transaction.

```scala
multiCol.readOnly { tx =>
  for {
    size  <- tx.size()
    items <- tx.fetch("my-key")
  } yield (size, items)
}
```

The `LMDBMultiReadOps` facade provides `exists()`, `size()`, and `fetch()`.

### readWrite

```scala
def readWrite[R, E, A](f: LMDBMultiWriteOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError | NestedWriteTransactionError, A]
```

Executes read and write operations atomically.

```scala
multiCol.readWrite { tx =>
  tx.put("key-a", "value1") *>
  tx.put("key-a", "value2") *>
  tx.delete("key-b", "old-value")
}
```

The `LMDBMultiWriteOps` facade provides all read operations plus `clear()`, `put()`, `delete()`, and `deleteAll()`.

---

## Cross-collection transactions with lift

Use `lift` to participate in a **global transaction** that spans multiple collections or mix of regular and multi-value collections:

```scala
lmdb.readWrite { ops =>
  val productsTx = products.lift(ops)   // LMDBCollection
  val tagsTx     = tags.lift(ops)       // LMDBMulti

  productsTx.upsertOverwrite(productId, product) *>
  tagsTx.put(productId, "featured")
}
```

See [Transactions](transactions.html) for a full guide.

---

## Example: event log

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*
import java.time.Instant

case class Event(action: String, timestamp: Instant) derives LMDBCodecJson

val program = for {
  events <- LMDB.multiCreate[String, Event]("events", failIfExists = false)

  // Record events for entity "user-1"
  now    <- Clock.instant
  _      <- events.put("user-1", Event("login",  now))
  _      <- events.put("user-1", Event("action", now.plusSeconds(10)))
  _      <- events.put("user-1", Event("logout", now.plusSeconds(300)))

  // Retrieve all events for "user-1"
  log    <- events.fetch("user-1")
  _      <- ZIO.foreachDiscard(log)(e => Console.printLine(e))

  // Cleanup
  _      <- events.deleteAll("user-1")
} yield ()
```

---

## Difference from LMDBCollection

| Feature | `LMDBCollection[K, T]` | `LMDBMulti[K, T]` |
|---|---|---|
| Values per key | Exactly one | Zero or more |
| Insert semantics | `insert` (strict, fails on conflict) / `upsertOverwrite` (replaces) | `put` accumulates |
| Fetch result | `Option[T]` | `List[T]` |
| Streaming | `stream()`, `streamWithKeys()` | — |
| Cursor navigation | `head`, `last`, `next`, `previous` | — |
| Index support | Yes (via `withIndex`) | No |
