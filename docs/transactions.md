---
title: Transactions
nav_order: 6
---

# Transactions
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

ZIO-LMDB exposes LMDB's **MVCC** (Multi-Version Concurrency Control) transaction model:

- **Multiple concurrent read transactions** are supported.
- **Only one write transaction can be active at a time.** Concurrent write attempts queue until the current one commits or rolls back.
- **Nested write transactions are forbidden.** Attempting one fails with `NestedWriteTransactionError`.

Transactions are managed automatically — they commit on success and roll back on any error.

---

## Per-collection transactions

Every collection facade (`LMDBCollection`, `LMDBMulti`, `LMDBIndex`) has `readOnly` and `readWrite` methods that open a transaction scoped to that single collection.

```scala
// Read-only: consistent snapshot across multiple reads
col.readOnly { tx =>
  for {
    size  <- tx.size()
    first <- tx.head()
    last  <- tx.last()
  } yield (size, first, last)
}

// Read-write: all writes applied atomically
col.readWrite { tx =>
  tx.upsertOverwrite(id1, recordA) *>
  tx.upsertOverwrite(id2, recordB) *>
  tx.delete(id3)
}
```

The lambda receives a `LMDBCollectionWriteOps[K, T]` (or `ReadOps` for `readOnly`) that delegates to the current transaction.

---

## Cross-collection transactions

To atomically write to **multiple collections or indexes**, use `LMDB.readWrite` (or `LMDB.readOnly`) and adapt each facade to the shared transaction with `lift`.

```scala
lmdb.readWrite { ops =>
  // Lift each facade into the shared transaction
  val usersTx    = users.lift(ops)      // LMDBCollectionWriteOps
  val ordersTx   = orders.lift(ops)     // LMDBCollectionWriteOps
  val categoryTx = categoryIdx.lift(ops) // LMDBIndexWriteOps

  // All three writes happen in the same transaction
  usersTx.upsertOverwrite(userId, user)     *>
  ordersTx.upsertOverwrite(orderId, order)  *>
  categoryTx.index(order.category, orderId)
}
```

{: .note }
`lift` returns the appropriate `WriteOps` or `ReadOps` facade depending on the method called and the transaction type passed.

---

## Indexed collections and transactions

When a collection has attached indexes (via `withIndex`), every mutation method (`upsert`, `update`, `delete`, `clear`) internally opens a **write transaction** to keep the index consistent.

This means you cannot call `upsert` on an indexed collection **inside** an outer write transaction — doing so would create a nested write transaction. Instead, call the write operation through the `LMDBCollectionWriteOps` facade obtained via `lift`:

```scala
// WRONG — will fail with NestedWriteTransactionError:
lmdb.readWrite { ops =>
  products.upsertOverwrite(id, record)  // opens another write tx internally
}

// CORRECT — lift gives you the write ops facade for the shared transaction:
lmdb.readWrite { ops =>
  val productsTx = products.lift(ops)   // products has indexes attached
  productsTx.upsertOverwrite(id, record) *>
  productsTx.upsertOverwrite(id2, record2)
}
```

---

## Transaction semantics summary

| Scenario | How to write |
|---|---|
| Single collection, multiple operations | `col.readWrite { tx => ... }` |
| Multiple collections, atomic | `lmdb.readWrite { ops => col1.lift(ops) ... col2.lift(ops) ... }` |
| Indexed collection inside outer tx | Use `col.lift(ops)` inside `lmdb.readWrite` |
| Read-only consistent snapshot | `col.readOnly { tx => ... }` or `lmdb.readOnly { ops => ... }` |

---

## Error handling

- A transaction is **automatically rolled back** when the effect fails.
- `NestedWriteTransactionError` is raised when a write transaction is opened inside another write transaction.
- `StorageSystemError` covers low-level LMDB errors (disk full, environment corruption, etc.).

```scala
col.readWrite { tx =>
  tx.upsertOverwrite(id, record)
}.catchSome {
  case StorageUserError.NestedWriteTransactionError => ZIO.logError("Nested tx!")
}
```

---

## Example: atomic transfer

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*

case class Account(id: String, balance: Double) derives LMDBCodecJson

def transfer(accounts: LMDBCollection[String, Account], from: String, to: String, amount: Double) =
  accounts.readWrite { tx =>
    for {
      src  <- tx.fetch(from).someOrFail(new Exception(s"Account $from not found"))
      dst  <- tx.fetch(to).someOrFail(new Exception(s"Account $to not found"))
      _    <- ZIO.fail(new Exception("Insufficient funds")).when(src.balance < amount)
      _    <- tx.upsertOverwrite(from, src.copy(balance = src.balance - amount))
      _    <- tx.upsertOverwrite(to,   dst.copy(balance = dst.balance + amount))
    } yield ()
  }
```

Both writes succeed or both are rolled back — no partial state is ever visible.
