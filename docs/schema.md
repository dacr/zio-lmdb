---
title: Schemas
nav_order: 8
---

# Schemas
{: .no_toc }

Alongside the [codec](codecs.html) (which round-trips bytes), each collection records a **schema**
describing the *shape* of what it stores. Schemas are what let tools — the catalog, the
[SQL](sql.html) `DESCRIBE`, and drift detection — understand a collection without reading values.

{: .note }
Schema support is new in 3.x. It is opt-in for value types and automatic for keys, so existing code
keeps compiling; deriving `LMDBSchema` simply makes a collection self-describing.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## LMDBSchema[T]

`LMDBSchema[T]` is a small typeclass that yields a persisted `SchemaArtifact` for a type. It is kept
separate from `LMDBCodec[T]` on purpose: the codec cares about bytes, the schema cares about
describing structure. A custom codec can therefore declare a matching schema without changing the
codec API.

```scala
trait LMDBSchema[T]:
  def artifact: SchemaArtifact
```

`collectionCreate[K, T]` requires both an `LMDBSchema[K]` and an `LMDBSchema[T]`. Both are resolved
implicitly, so in practice you never pass them by hand:

```scala
def collectionCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(using
  KeyCodec[K], LMDBCodec[T], LMDBSchema[K], LMDBSchema[T]
): IO[CreateErrors, LMDBCollection[K, T]]
```

---

## Keys are always described

A key always has a `KeyCodec[K]`, and every codec carries a stable `keyId` (e.g. `lmdb:int64`,
`lmdb-geo:location/v1`). So a key's schema is automatically a `KeySchema(keyId)` — no derivation
needed. This is what lets the catalog distinguish byte-compatible but semantically different keys:
`Int` (`lmdb:int32`) vs `Long` (`lmdb:int64`), `UUID` vs `ULID` vs `UUIDv7`, and so on.

---

## Values: derive for a rich schema

A **value** type opts in with `derives LMDBSchema`. The derivation produces a `JsonSchema` whose
payload is the structural [shape](#schemashape) of the type:

```scala
import zio.lmdb.json.*
import zio.lmdb.schema.*

case class User(name: String, age: Int, nickname: Option[String])
  derives LMDBCodecJson, LMDBSchema
```

Without `derives LMDBSchema`, a value type falls back to a low-priority **opaque** schema
(`OpaqueSchema`). It still works, but downstream layers surface it as a "shape unknown" warning
rather than a precise description, and `DESCRIBE` cannot list its columns.

---

## SchemaShape

`SchemaShape[T]` is the compile-time, `Mirror`-based derivation behind `derives LMDBSchema`. It walks
the model — no runtime reflection, no extra dependency — and emits a lightweight JSON-Schema-like
`JValue` tree:

| Scala | Shape |
|---|---|
| `String` / `Boolean` | `{"type":"string"}` / `{"type":"boolean"}` |
| `Int` / `Long` / `BigInt` | `{"type":"integer"}` |
| `Float` / `Double` / `BigDecimal` | `{"type":"number"}` |
| `Instant` / `OffsetDateTime` | `{"type":"string","format":"date-time"}` |
| `UUID` | `{"type":"string","format":"uuid"}` |
| `case class` | `{"type":"object","properties":{…},"required":[…]}` |
| `sealed trait` / `enum` | `{"oneOf":[…]}` |
| `Option[T]` | the inner shape, field dropped from `required` |
| `Seq`/`List`/`Vector`/`Set`/`Array[T]` | `{"type":"array","items":…}` |
| `Map[K, V]` | `{"type":"object","additionalProperties":…}` |
| `(A, B[, C[, D]])` | `{"type":"array","prefixItems":[…]}` (positional, for composite keys) |

For example, `User` above derives roughly:

```json
{
  "type": "object",
  "properties": {
    "name":     { "type": "string" },
    "age":      { "type": "integer" },
    "nickname": { "type": "string" }
  },
  "required": ["name", "age"]
}
```

{: .warning }
**Recursive models** (e.g. `case class Tree(children: Seq[Tree])`) would make the inductive
derivation diverge at compile time. Declare an explicit `given LMDBSchema[T]` for such types.

---

## SchemaArtifact

The persisted artifact is a small sum type, stored next to the collection metadata:

| Variant | Carries |
|---|---|
| `JsonSchema(schema: JValue)` | the structural shape derived from a value type |
| `KeySchema(keyId: String)` | the key codec's stable identity |
| `ProtobufSchema(proto: String)` | a `.proto` source for Protobuf-backed codecs |
| `OpaqueSchema(hint: String)` | fallback for a type that exposes no shape |

Every artifact has a stable **`fingerprint`** — a SHA-256 of its canonical JSON form — used for
structural drift detection (the fixed field order makes it stable across JVMs and platforms).

```scala
val art = LMDBSchema[User].artifact     // SchemaArtifact.JsonSchema(...)
val fp  = art.fingerprint               // "…64-hex-chars…"
```

---

## Custom schema for a custom codec

If you wire your own `LMDBCodec[T]`, declare a matching schema given so the collection stays
self-describing:

```scala
given LMDBSchema[MyRecord] =
  LMDBSchema.from(SchemaArtifact.ProtobufSchema(MyRecord.protoSource))

// or, to keep it explicit but opaque:
given LMDBSchema[MyRecord] =
  LMDBSchema.from(SchemaArtifact.OpaqueSchema("MyRecord"))
```

---

## How the SQL layer uses it

The [SQL](sql.html) `DESCRIBE` / `\d` reads these artifacts directly: the key row comes from the
`KeySchema` id, and the value columns and their types come from the `JsonSchema` `properties`. That
is why `DESCRIBE` reports real columns for a derived value type and only `_value` for an opaque one.
