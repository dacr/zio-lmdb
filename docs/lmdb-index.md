---
title: LMDBIndex
nav_order: 5
---

# LMDBIndex
{: .no_toc }

`LMDBIndex[FROM_KEY, TO_KEY]` is the API facade for **index collections** — a special multi-value collection where each key maps to a set of target keys from another collection.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

An index maintains a **secondary key → primary key** mapping that lets you look up records in one collection by an attribute stored in their values. For example, you can look up `Product` records by their `category` field without scanning the entire collection.

```
categoryIndex: String → UUID
  "electronics" → [uuid-1, uuid-4, uuid-7]
  "kitchen"     → [uuid-2, uuid-5]
  "toys"        → [uuid-3]
```

Indexes are stored as separate collections. They are kept consistent with the source collection by attaching them via [`LMDBCollection.withIndex`](collection.html#withindex).

```scala
val categoryIndex: LMDBIndex[String, UUID] =
  LMDB.indexCreate[String, UUID]("category-idx", failIfExists = false)
```

---

## Creating an index

```scala
LMDB.indexCreate[FROM_KEY, TO_KEY](
  name: String,
  description: Option[String] = None,
  failIfExists: Boolean = true
): IO[..., LMDBIndex[FROM_KEY, TO_KEY]]
```

| Parameter | Description |
|---|---|
| `name` | Unique index name |
| `description` | Optional human-readable description stored in metadata |
| `failIfExists` | If `true` (default), fails when the index already exists |

---

## Manual index operations

These are low-level operations you can use when managing index entries directly. In most cases you should prefer the **automatic index update** via [`LMDBCollection.withIndex`](collection.html#withindex) instead.

### index

```scala
def index(key: FROM_KEY, toKey: TO_KEY): IO[IndexErrors, Unit]
```

Adds the mapping `key → toKey` to the index.

```scala
categoryIndex.index("electronics", productId)
```

### unindex

```scala
def unindex(key: FROM_KEY, toKey: TO_KEY): IO[IndexErrors, Boolean]
```

Removes the mapping `key → toKey`. Returns `true` if the entry was present and removed.

```scala
categoryIndex.unindex("electronics", productId)
```

### indexContains

```scala
def indexContains(key: FROM_KEY, toKey: TO_KEY): IO[IndexErrors, Boolean]
```

Returns `true` if the exact mapping `key → toKey` exists.

### hasKey

```scala
def hasKey(key: FROM_KEY): IO[IndexErrors, Boolean]
```

Returns `true` if `key` has at least one target mapping.

```scala
categoryIndex.hasKey("discontinued")  // false if category is unused
```

---

## Read operations

### fetch

```scala
def fetch(key: FROM_KEY): IO[FetchErrors, Option[TO_KEY]]
```

Returns **one** target key for the given source key, or `None` if the key has no mappings. When multiple mappings exist, one is returned (use `indexed` to get all).

### indexed

```scala
def indexed(key: FROM_KEY, limitToKey: Boolean = true): ZStream[Any, IndexErrors, (FROM_KEY, TO_KEY)]
```

Streams **all** `(fromKey, toKey)` pairs for the given `key`.

```scala
// Get all product IDs in the "electronics" category
categoryIndex.indexed("electronics")
  .map(_._2)           // extract just the TO_KEY
  .runCollect
```

When `limitToKey = false`, the stream continues past the given key and yields all subsequent entries (useful for full index scans).

### Cursor navigation

```scala
def head(): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]
def last(): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]
def next(afterThatKey: FROM_KEY): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]
def previous(beforeThatKey: FROM_KEY): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]
```

Navigates the index in lexicographic order of `FROM_KEY`. Note that when multiple `TO_KEY` values exist for the same `FROM_KEY`, these operations return one entry at a time (the first `TO_KEY` for the given `FROM_KEY` when using `next`/`previous`).

---

## Automatic index maintenance

The most common and recommended approach is to **attach the index to an `LMDBCollection`** so that inserts, updates, and deletes automatically keep the index in sync.

### withIndex

```scala
// On LMDBCollection:
def withIndex[IK](index: LMDBIndex[IK, K])(extractor: T => Iterable[IK]): LMDBCollection[K, T]
```

The `extractor` function derives index keys from each record value. The collection primary key is used as the target key automatically.

```scala
case class Product(id: UUID, name: String, category: String, tags: List[String]) derives LMDBCodecJson

val categoryIndex: LMDBIndex[String, UUID] = ...
val tagsIndex: LMDBIndex[String, UUID]     = ...

// Attach both indexes
val products = rawProducts
  .withIndex(categoryIndex)(p => List(p.category))
  .withIndex(tagsIndex)(p => p.tags)

// Every mutation now updates the indexes atomically
products.upsertOverwrite(id, Product(id, "Widget", "electronics", List("sale", "new")))

// Later: look up all electronics
categoryIndex.indexed("electronics")
  .mapZIO { case (_, productId) => products.fetch(productId) }
  .collectSome
  .runCollect
```

### withIndexFull

```scala
def withIndexFull[IK, IV](index: LMDBIndex[IK, IV])(extractor: (K, T) => Iterable[(IK, IV)]): LMDBCollection[K, T]
```

Generic variant when the index target value should differ from the collection's primary key. The extractor receives both the collection key and the value, and returns `(indexKey, indexValue)` pairs.

---

## Transactions

### readOnly

```scala
def readOnly[R, E, A](f: LMDBIndexReadOps[FROM_KEY, TO_KEY] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A]
```

Read multiple index entries within a single consistent snapshot.

```scala
categoryIndex.readOnly { tx =>
  for {
    hasElectronics <- tx.hasKey("electronics")
    firstEntry     <- tx.head()
  } yield (hasElectronics, firstEntry)
}
```

### readWrite

```scala
def readWrite[R, E, A](f: LMDBIndexWriteOps[FROM_KEY, TO_KEY] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError | NestedWriteTransactionError, A]
```

Execute index mutations atomically.

```scala
categoryIndex.readWrite { tx =>
  tx.index("electronics", newProductId) *>
  tx.unindex("discontinued", oldProductId)
}
```

---

## Cross-collection transactions with lift

Use `lift` to include index operations in a broader transaction:

```scala
lmdb.readWrite { ops =>
  val productsTx = products.lift(ops)
  val idxTx      = categoryIndex.lift(ops)

  productsTx.upsertOverwrite(id, updatedProduct) *>
  idxTx.index("electronics", id)
}
```

See [Transactions](transactions.html) for a full guide.

---

## Full example: category lookup

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*
import java.util.UUID

case class Product(name: String, category: String, price: Double) derives LMDBCodecJson

val program = for {
  lmdb <- ZIO.service[LMDB]

  // Create collections and index
  rawProducts   <- lmdb.collectionCreate[UUID, Product]("products", failIfExists = false)
  categoryIndex <- lmdb.indexCreate[String, UUID]("category-index", failIfExists = false)
  products       = rawProducts.withIndex(categoryIndex)(p => List(p.category))

  // Insert data — category index is updated automatically
  id1 <- Random.nextUUID
  id2 <- Random.nextUUID
  id3 <- Random.nextUUID
  _   <- products.upsertOverwrite(id1, Product("Laptop",  "electronics", 999.0))
  _   <- products.upsertOverwrite(id2, Product("Phone",   "electronics", 599.0))
  _   <- products.upsertOverwrite(id3, Product("Spatula", "kitchen",     12.0))

  // Look up all electronics via the index
  electronics <- categoryIndex.indexed("electronics")
                   .mapZIO { case (_, pId) => products.fetch(pId) }
                   .collectSome
                   .runCollect

  _ <- Console.printLine(s"Electronics: $electronics")
} yield ()
```
