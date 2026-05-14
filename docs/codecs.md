---
title: Codecs
nav_order: 7
---

# Codecs
{: .no_toc }

ZIO-LMDB separates value serialization (`LMDBCodec[T]`) from key encoding (`KeyCodec[K]`). Both are resolved implicitly at compile time, so the API is fully type-safe with no runtime reflection.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Value codecs — LMDBCodec[T]

`LMDBCodec[T]` encodes and decodes collection values to and from raw bytes.

```scala
trait LMDBCodec[T]:
  def encode(value: T): Array[Byte]
  def decode(valueBytes: ByteBuffer): Either[String, T]
```

### JSON codec (default)

The `zio.lmdb.json` package provides `LMDBCodecJson`, backed by [zio-json](https://github.com/zio/zio-json).

```scala
import zio.lmdb.json.*

case class Product(id: String, name: String, price: Double) derives LMDBCodecJson
```

`derives LMDBCodecJson` generates the `JsonEncoder` and `JsonDecoder` and wires them to `LMDBCodec[Product]`. No additional code required.

### Custom codec

Implement `LMDBCodec[T]` directly to use any serialization format (Protobuf, Avro, MessagePack, etc.):

```scala
import zio.lmdb.*
import java.nio.ByteBuffer

given LMDBCodec[MyRecord] = new LMDBCodec[MyRecord]:
  def encode(value: MyRecord): Array[Byte] =
    MySerializer.toBytes(value)

  def decode(buf: ByteBuffer): Either[String, MyRecord] =
    MySerializer.fromBytes(buf.array()).left.map(_.getMessage)
```

---

## Key codecs — KeyCodec[K]

`KeyCodec[K]` encodes and decodes **collection keys**. Key bytes must produce a meaningful lexicographic order because LMDB stores entries sorted by key.

```scala
trait KeyCodec[K]:
  def encode(key: K): Array[Byte]
  def decode(keyBytes: ByteBuffer): Either[KeyCodecError, K]
  def width: Option[Int] = None   // fixed byte width, if applicable
```

### Built-in key codecs

The `keycodecs` module (included transitively with `zio-lmdb`) provides:

| Key type | Encoding | Width | Module |
|---|---|---|---|
| `String` | UTF-8 | Variable | `keycodecs` |
| `UUID` | 16-byte binary | 16 bytes fixed | `keycodecs` |
| `(A, B)` | Composite (A escaped + B) | Variable | `keycodecs` |

### Specialized key codec modules

Add these as separate dependencies for advanced key types:

#### keycodecs-timestamp

Encodes `java.time.Instant` (or a custom timestamp wrapper) as **12 fixed bytes** with nanosecond precision. 12 bytes instead of the 30+ bytes of an ISO-8601 string, with correct lexicographic ordering.

```scala
libraryDependencies += "fr.janalyse" %% "keycodecs-timestamp" % "2.8.1"
```

```scala
import zio.lmdb.keycodecs.timestamp.given

val events: LMDBCollection[Instant, Event] = ...
```

#### keycodecs-uuidv7

Encodes UUIDv7 (time-ordered UUID) as **16 fixed bytes**. UUIDv7 keys sort chronologically, making this ideal for time-series or event-sourcing collections.

```scala
libraryDependencies += "fr.janalyse" %% "keycodecs-uuidv7" % "2.8.1"
```

```scala
import zio.lmdb.keycodecs.uuidv7.given
import java.util.UUID

val timeline: LMDBCollection[UUID, Event] = ...
// Keys are chronologically ordered
```

#### keycodecs-ulid

Encodes ULID as **16 fixed bytes**. Like UUIDv7 but following the ULID spec.

```scala
libraryDependencies += "fr.janalyse" %% "keycodecs-ulid" % "2.8.1"
```

```scala
import zio.lmdb.keycodecs.ulid.given
```

#### keycodecs-geo

Encodes `(latitude, longitude)` pairs as **8 bytes** using Morton (Z-curve) interleaving. Enables efficient spatial proximity queries via lexicographic range scans.

```scala
libraryDependencies += "fr.janalyse" %% "keycodecs-geo" % "2.8.1"
```

```scala
import zio.lmdb.keycodecs.geo.given

// (lat, lon) keys sorted by spatial proximity
val locations: LMDBCollection[(Double, Double), Place] = ...
```

#### keycodecs-uca

Encodes strings using the **Unicode Collation Algorithm (UCA)** so that lexicographic byte order matches natural language sort order for any locale.

```scala
libraryDependencies += "fr.janalyse" %% "keycodecs-uca" % "2.8.1"
```

### Composite keys

Combine two key codecs with a tuple:

```scala
import zio.lmdb.keycodecs.given  // includes (A, B) codec

// (userId, timestamp) compound key — scan all events for a user
val userEvents: LMDBCollection[(String, Instant), Event] = ...

// Range scan: all events for "user-42"
userEvents.stream(keyFilter = _._1 == "user-42")
```

The composite codec correctly handles variable-width first elements by escaping the separator byte.

### Custom key codec

```scala
given KeyCodec[MyKey] = new KeyCodec[MyKey]:
  def encode(key: MyKey): Array[Byte] = ???
  def decode(buf: ByteBuffer): Either[KeyCodecError, MyKey] = ???
  override def width: Option[Int] = Some(16)  // if fixed-width
```

Fixed-width codecs (`width = Some(n)`) enable LMDB's `MDB_DUPSORT` optimizations for multi-value collections.

---

## Choosing a key type

| Use case | Recommended key | Module |
|---|---|---|
| Random / opaque IDs | `UUID` | `keycodecs` |
| Time-ordered IDs | UUIDv7 or ULID | `keycodecs-uuidv7` / `keycodecs-ulid` |
| Natural string keys | `String` | `keycodecs` |
| Locale-aware string sorting | UCA `String` | `keycodecs-uca` |
| Time-series data | `Instant` | `keycodecs-timestamp` |
| Spatial data | `(Double, Double)` | `keycodecs-geo` |
| Compound keys | `(A, B)` | `keycodecs` |
