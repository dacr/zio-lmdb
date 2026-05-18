---
title: LMDBCollection
nav_order: 3
---

# LMDBCollection
{: .no_toc }

`LMDBCollection[K, T]` is the primary API facade for **regular collections** where each key maps to exactly one value.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

A `LMDBCollection[K, T]` is obtained by calling `LMDB.collectionCreate` and it carries the collection name, the LMDB service reference, and any attached index updaters. The type parameters fix the key type `K` and value type `T` for all operations.

```scala
val products: LMDBCollection[UUID, Product] =
  LMDB.collectionCreate[UUID, Product]("products", failIfExists = false)
```

All operations return `ZIO` or `ZStream` effects with typed error channels. No exceptions leak through.

---

## Creating a collection

```scala
LMDB.collectionCreate[K, T](name: String, failIfExists: Boolean = true): IO[..., LMDBCollection[K, T]]
```

| Parameter | Description |
|---|---|
| `name` | Unique collection name within the database |
| `failIfExists` | If `true` (default), fails if the collection already exists |

---

## Read operations

### fetch

```scala
def fetch(key: K): IO[FetchErrors, Option[T]]
```

Returns the value for `key`, or `None` if the key does not exist.

```scala
col.fetch(myId).map {
  case Some(record) => println(s"Found: $record")
  case None         => println("Not found")
}
```

### contains

```scala
def contains(key: K): IO[ContainsErrors, Boolean]
```

Returns `true` if `key` exists in the collection.

### size

```scala
def size(): IO[SizeErrors, Long]
```

Returns the number of key-value pairs in the collection.

### Cursor navigation

```scala
def head(): IO[FetchErrors, Option[(K, T)]]
def last(): IO[FetchErrors, Option[(K, T)]]
def next(afterThatKey: K): IO[FetchErrors, Option[(K, T)]]
def previous(beforeThatKey: K): IO[FetchErrors, Option[(K, T)]]
```

Keys are stored in **lexicographic order**. These operations navigate the sorted key space efficiently without loading the entire collection:

```scala
for {
  first  <- col.head()                    // (minKey, record)
  second <- first.flatMap((k,_) => col.next(k).option).getOrElse(None)
  last   <- col.last()                    // (maxKey, record)
} yield (first, second, last)
```

---

## Write operations

### insert

```scala
def insert(key: K, document: T): IO[InsertErrors | IndexErrors, Unit]
```

Strictly inserts `document` at `key`. Fails with `StorageUserError.KeyAlreadyExists` if the key is already present — the existing record is left untouched. The check and the write happen atomically in a single LMDB operation (`MDB_NOOVERWRITE`), so there is no race window between "does the key exist?" and "write".

```scala
col.insert(id, Product(id, "Widget", 9.99))
```

Attached indexes are only updated when the insert succeeds.

### upsertOverwrite

```scala
def upsertOverwrite(key: K, document: T): IO[UpsertErrors | IndexErrors, Unit]
```

Inserts `document` at `key`. If a record already exists for that key it is silently overwritten.

```scala
col.upsertOverwrite(id, Product(id, "Widget", 9.99))
```

### upsert

```scala
def upsert(key: K, modifier: Option[T] => T): IO[UpsertErrors | IndexErrors, T]
```

Atomically inserts or updates a record. The `modifier` function receives `Some(existing)` if the key already exists, or `None` for a new key. Returns the new value.

```scala
col.upsert(id, {
  case Some(existing) => existing.copy(price = existing.price * 1.1) // 10% price increase
  case None           => Product(id, "New Product", 19.99)           // default on insert
})
```

### update

```scala
def update(key: K, modifier: T => T): IO[UpdateErrors | IndexErrors, Option[T]]
```

Updates an existing record. If the key does not exist, returns `None` without creating a new entry.

```scala
col.update(id, _.copy(name = "Updated Name"))
// Returns Some(updatedRecord) or None if key was missing
```

### delete

```scala
def delete(key: K): IO[DeleteErrors | IndexErrors, Option[T]]
```

Removes the record at `key` and returns it. Returns `None` if the key did not exist.

```scala
col.delete(id).map {
  case Some(r) => println(s"Deleted $r")
  case None    => println("Key was not found")
}
```

### clear

```scala
def clear(): IO[ClearErrors | IndexErrors, Unit]
```

Removes all records from the collection. Also clears any attached indexes.

---

## Streaming and collecting

### collect

```scala
def collect(
  keyFilter: K => Boolean = _ => true,
  valueFilter: T => Boolean = _ => true,
  startAfter: Option[K] = None,
  backward: Boolean = false,
  limit: Option[Long] = None
): IO[CollectErrors, List[T]]
```

Loads matching records into memory. The `keyFilter` is applied **before** deserialization for efficiency.

```scala
// All records
col.collect()

// Records with keys starting with "A"
col.collect(keyFilter = _.startsWith("A"))

// First 100 active products, cheapest first (ascending by key)
col.collect(valueFilter = _.active, limit = Some(100))

// Page 2: records after the last key of page 1
col.collect(startAfter = Some(lastKeyFromPage1), limit = Some(25))
```

### stream

```scala
def stream(
  keyFilter: K => Boolean = _ => true,
  startAfter: Option[K] = None,
  backward: Boolean = false
): ZStream[Any, StreamErrors, T]
```

Streams values lazily without loading everything into memory:

```scala
col.stream(backward = true)
   .take(10)
   .runCollect
```

### streamWithKeys

```scala
def streamWithKeys(
  keyFilter: K => Boolean = _ => true,
  startAfter: Option[K] = None,
  backward: Boolean = false
): ZStream[Any, StreamErrors, (K, T)]
```

Like `stream` but yields `(key, value)` tuples.

---

## Indexes

Attach an `LMDBIndex` to a collection so that it is **automatically updated** whenever the collection changes.

### withIndex

```scala
def withIndex[IK](index: LMDBIndex[IK, K])(extractor: T => Iterable[IK]): LMDBCollection[K, T]
```

Links `index` to the collection. The `extractor` function derives the index keys from each record value. The collection primary key becomes the index target key automatically.

```scala
// Index products by category
val categoryIndex: LMDBIndex[String, UUID] = ...

val indexedProducts = products.withIndex(categoryIndex)(p => List(p.category))

// Now, every upsert/update/delete on indexedProducts also updates categoryIndex
indexedProducts.upsertOverwrite(id, Product(id, "Widget", "electronics", 9.99))
```

### withIndexFull

```scala
def withIndexFull[IK, IV](index: LMDBIndex[IK, IV])(extractor: (K, T) => Iterable[(IK, IV)]): LMDBCollection[K, T]
```

Generic variant where the extractor receives both the collection key and the value, and produces `(indexKey, indexValue)` pairs. Use this when the index value should differ from the collection key.

### rebuildIndexes

```scala
def rebuildIndexes(): IO[IndexErrors | StreamErrors | ClearErrors, Unit]
```

Clears and rebuilds all attached indexes from the current collection content. Useful when adding an index to an already-populated collection.

```scala
val indexedProducts = products.withIndex(categoryIndex)(_.category)
indexedProducts.rebuildIndexes()
```

---

## Transactions

### readOnly

```scala
def readOnly[R, E, A](f: LMDBCollectionReadOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A]
```

Executes `f` within a single **read-only transaction**. Use this to read multiple keys consistently.

```scala
col.readOnly { tx =>
  for {
    first <- tx.head()
    last  <- tx.last()
    size  <- tx.size()
  } yield (first, last, size)
}
```

### readWrite

```scala
def readWrite[R, E, A](f: LMDBCollectionWriteOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError | NestedWriteTransactionError, A]
```

Executes `f` within a single **read-write transaction**. All writes inside `f` are applied atomically.

```scala
col.readWrite { tx =>
  tx.upsertOverwrite(id1, record1) *>
  tx.upsertOverwrite(id2, record2) *>
  tx.delete(id3)
}
```

{: .warning }
LMDB allows only **one active write transaction at a time**. Attempting to open a nested write transaction will fail with `NestedWriteTransactionError`.

---

## Cross-collection transactions with lift

The `lift` method adapts a collection facade to an already-open global transaction, enabling **atomic writes across multiple collections**:

```scala
lmdb.readWrite { ops =>
  val usersTx  = users.lift(ops)
  val ordersTx = orders.lift(ops)

  usersTx.upsertOverwrite(userId, user) *>
  ordersTx.upsertOverwrite(orderId, order)
}
```

See [Transactions](transactions.html) for a full guide.

---

## Error types

| Error type | When it occurs |
|---|---|
| `FetchErrors` | Key or collection not found, codec decode failure |
| `ContainsErrors` | Storage error while checking key existence |
| `SizeErrors` | Storage error while reading collection size |
| `UpsertErrors` | Serialization failure or storage error on write |
| `UpdateErrors` | Key not found for update, serialization/storage error |
| `DeleteErrors` | Storage error on delete |
| `ClearErrors` | Storage error on clear |
| `IndexErrors` | Index update failure |
| `CollectErrors` | Error during in-memory collection |
| `StreamErrors` | Error during streaming |
| `StorageSystemError` | Low-level LMDB error |
| `NestedWriteTransactionError` | Attempt to open a write transaction inside a write transaction |
