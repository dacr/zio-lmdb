---
title: Vector Search
nav_order: 12
---

# Vector Search
{: .no_toc }

`LMDBVectorIndex[K]`, from the separate `zio-lmdb-vector` module, is a nearest-neighbor search facade over fixed-dimension float vectors (embeddings) — face features, text/image embeddings, and similar.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

A vector index stores one `Array[Float]` per key and answers "which `k` keys have the vector closest to this query vector?". It's a separate module (`"fr.janalyse" %% "zio-lmdb-vector" % version`) so consumers who don't need it don't pull it in.

```
faceIndex: FaceId → Array[Float] (512 components)
  search(queryVector, k = 5) → [(faceId7, 0.04), (faceId3, 0.11), ...]  // sorted by ascending distance
```

Vectors are stored as an ordinary collection — one record per key — so they get the usual LMDB properties for free: ACID writes, mmap'd/page-cached reads, backup/restore. `searchNearest` is an **exact**, full-scan search: every stored vector is compared against the query, with the scan spread across CPU cores instead of running on a single thread (see below).

For workloads that run many searches against a mostly-static corpus, `searchApproximate` trades a little recall for search cost that grows logarithmically rather than linearly — see [Approximate search](#approximate-search-hnsw) below.

---

## Creating an index

```scala
LMDBVectorIndex.create[K](
  name: String,
  dimension: Int,
  metric: VectorMetric = VectorMetric.Cosine,
  failIfExists: Boolean = true
): ZIO[LMDB, CreateErrors, LMDBVectorIndex[K]]
```

| Parameter | Description |
|---|---|
| `name` | Unique name for the backing collection |
| `dimension` | Fixed number of components every vector in this index must have |
| `metric` | Distance function used by `searchNearest`; defaults to cosine distance |
| `failIfExists` | If `true` (default), fails when the backing collection already exists |

```scala
import zio.lmdb.vector.*

val faceIndex: ZIO[LMDB, CreateErrors, LMDBVectorIndex[FaceId]] =
  LMDBVectorIndex.create[FaceId]("face-index", dimension = 512, metric = VectorMetric.Cosine)
```

### Metrics

```scala
sealed trait VectorMetric {
  def distance(a: Array[Float], b: Array[Float]): Double
}
```

| Metric | Formula | Use when |
|---|---|---|
| `VectorMetric.Cosine` | `1 - cosine similarity` | Vector direction matters, not magnitude — the common case for embeddings (face features, text/image embeddings, ...) |
| `VectorMetric.Euclidean` | straight-line (L2) distance | Vectors live in a genuine metric space |
| `VectorMetric.DotProduct` | `-(a · b)` | Magnitudes are meaningful/normalized on purpose |

Smaller is always "closer", regardless of the metric.

---

## Operations

### insert

```scala
def insert(key: K, vector: Array[Float]): IO[UpsertErrors | IndexErrors | VectorDimensionMismatch, Unit]
```

Adds or replaces the vector stored for `key`. Fails with `VectorDimensionMismatch` if `vector.length != dimension`.

### get / delete / size

```scala
def get(key: K): IO[FetchErrors, Option[Array[Float]]]
def delete(key: K): IO[DeleteErrors | IndexErrors, Option[Array[Float]]]
def size(): IO[SizeErrors, Long]
```

Plain lookups/removal against the backing collection.

### searchNearest

```scala
def searchNearest(
  query: Array[Float],
  k: Int,
  batchSize: Int = 2048,
  parallelism: Int = <number of available cores>
): IO[StreamErrors | VectorDimensionMismatch, Chunk[(K, Double)]]
```

Returns the `k` closest keys to `query`, sorted by ascending distance.

The vector source — the backing collection streamed fresh (the usual bounded, I/O-oriented LMDB read path), or an in-memory snapshot if `warm()` was called (see below) — is split into batches of `batchSize` records, and each batch is scored **in parallel** across up to `parallelism` fibers, since distance computation is plain CPU-bound Scala code once a batch of vectors has been read. Each batch keeps only its own top `k` before being merged into the running top `k`, so memory stays bounded by `k`, never by the collection size.

```scala
faceIndex.searchNearest(queryFeatures, k = 5)
// Chunk((faceId7, 0.04), (faceId3, 0.11), (faceId19, 0.13), ...)
```

### warm / cooldown

```scala
def warm(): IO[StreamErrors, Unit]
def cooldown(): UIO[Unit]
```

`searchNearest` re-streaming the collection on every call is fine for occasional queries against a large corpus, but a **batch job that runs many searches back-to-back against a corpus that isn't changing** — matching a large set of query vectors against the same known set, say — pays that re-read cost every single time. `warm()` snapshots the whole collection into memory once; every `searchNearest` afterwards scores that snapshot instead. Any `insert`/`delete` invalidates the snapshot automatically, so results never silently go stale — the next `searchNearest` just falls back to reading the collection directly until `warm()` is called again. `cooldown()` drops the snapshot explicitly (e.g. to free the memory once a batch of searches is done).

```scala
_    <- ZIO.foreachDiscard(knownVectors) { case (key, v) => faceIndex.insert(key, v) }
_    <- faceIndex.warm()
top  <- ZIO.foreach(queries)(q => faceIndex.searchNearest(q, k = 5)) // all scored in-memory
_    <- faceIndex.cooldown()
```

---

## Full example

```scala
import zio.*, zio.lmdb.*, zio.lmdb.vector.*

val program = for {
  index <- LMDBVectorIndex.create[String]("embeddings", dimension = 4, failIfExists = false)
  _     <- index.insert("a", Array(1f, 0f, 0f, 0f))
  _     <- index.insert("b", Array(0f, 1f, 0f, 0f))
  _     <- index.insert("c", Array(0.9f, 0.1f, 0f, 0f))
  top   <- index.searchNearest(Array(1f, 0f, 0f, 0f), k = 2)
  _     <- Console.printLine(s"Nearest to a: $top") // Chunk((a, 0.0), (c, ...))
} yield ()
```

---

## Approximate search (HNSW)

When the number of searches to run is large compared to the number of vectors, an exact full scan per query stops being the right shape: it costs O(n) *per search*, however well parallelized. `buildApproximateIndex` builds an in-memory [HNSW](https://arxiv.org/abs/1603.09320) graph whose search cost grows logarithmically with the corpus instead.

```scala
def buildApproximateIndex(params: HnswParams = HnswParams()): IO[StreamErrors, Unit]
def searchApproximate(query: Array[Float], k: Int, ef: Option[Int] = None): IO[..., Chunk[(K, Double)]]
```

```scala
_    <- ZIO.foreachDiscard(knownVectors) { case (key, v) => index.insert(key, v) }
_    <- index.buildApproximateIndex(HnswParams(m = 16, efConstruction = 100, efSearch = 64))
hits <- ZIO.foreach(manyQueries)(q => index.searchApproximate(q, k = 8))
```

| Parameter | Meaning |
|---|---|
| `m` | neighbors kept per node per layer (layer 0 keeps `2 * m`) — better connectivity, more memory, slower build |
| `efConstruction` | candidate-list width during build — better graph, proportionally slower build |
| `efSearch` | default candidate-list width per query — better recall, proportionally slower search |
| `seed` | makes the layer assignment, and therefore the build, reproducible |

**The tradeoff is recall, and it is not free.** A search explores a bounded neighborhood rather than every vector, so it can miss a true nearest neighbor. Note that "miss a neighbor" is not always a smaller answer: if your logic *rejects* a candidate because a conflicting neighbor is present, dropping that conflicting neighbor can flip the decision the other way. Measure recall on your own data — `searchNearest` is the exact reference to compare against — and treat any downstream decision rule as part of what you are measuring.

Both the build and the resulting graph are in-memory and snapshot-like: any `insert`/`delete` invalidates them, after which `searchApproximate` transparently falls back to the exact scan until you build again. The graph is immutable once built, so searches run concurrently from any number of fibers without locking.

**When it pays**: the build reads and links every vector, so it costs meaningfully more than a single exact scan. It wins when many searches amortize it — a batch job matching a large query set against a mostly-static corpus — and loses for a handful of one-off queries, where `searchNearest` finishes sooner than a build would.

An on-disk, incrementally-maintained variant (persisting the graph in LMDB rather than rebuilding it per process) is sketched in `docs/internal/vector-search-hnsw-design.md`.
