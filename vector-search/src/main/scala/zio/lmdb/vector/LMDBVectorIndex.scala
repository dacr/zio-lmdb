/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zio.lmdb.vector

import zio.*
import zio.lmdb.*
import zio.lmdb.keycodecs.KeyCodec
import zio.stream.*

/** The vector doesn't have the dimension declared for the index it's being written to, or a search query vector doesn't match the dimension of the index being searched.
  */
case class VectorDimensionMismatch(indexName: CollectionName, expected: Int, actual: Int)

/** A flat (exact) nearest-neighbor vector index: every `searchNearest` call scores every stored vector against `metric`, but spreads that computation across CPU cores instead of doing it on a single thread — see `searchNearest`.
  *
  * Vectors are persisted as an ordinary [[LMDBCollection]] (one record per key), so they get all the usual LMDB properties for free: ACID writes, mmap'd/page-cached reads, backup/restore. There is no separate graph structure to maintain, so writes
  * are trivial and there's nothing to get out of sync — the tradeoff is O(n) search instead of the sub-linear cost of an approximate index (HNSW, IVF, ...). That tradeoff is fine up to a few hundred thousand/some millions of vectors; beyond that, an
  * ANN-based index is the better fit.
  *
  * By default `searchNearest` streams the backing collection fresh on every call — fine for occasional queries against a large corpus. Running **many** searches back-to-back against a corpus that isn't changing (a batch/offline job, typically) is a
  * different access pattern: call `warm()` once first, and every `searchNearest` afterwards scores an in-memory snapshot instead of re-reading the collection from scratch each time. Any `insert`/`delete` invalidates the snapshot, so results never
  * silently go stale — the next `searchNearest` just falls back to the collection until `warm()` is called again.
  *
  * @param collection
  *   the backing collection: one `key -> vector` record per indexed item
  * @param dimension
  *   the fixed number of components every vector in this index must have
  * @param metric
  *   the distance function used to rank candidates; smaller is always "closer"
  * @param cache
  *   in-memory snapshot of the collection, populated by `warm()` and invalidated by writes; `None` means "not warmed, read the collection directly"
  */
case class LMDBVectorIndex[K](
  collection: LMDBCollection[K, Array[Float]],
  dimension: Int,
  metric: VectorMetric,
  private val cache: Ref[Option[Chunk[(K, Array[Float])]]]
) {

  /** Adds or replaces the vector stored for `key`. Invalidates the warm cache, if any. */
  def insert(key: K, vector: Array[Float]): IO[UpsertErrors | IndexErrors | VectorDimensionMismatch, Unit] =
    checkDimension(vector.length) *> collection.upsertOverwrite(key, vector) <* cache.set(None)

  /** Removes the vector stored for `key`, if any, returning it. Invalidates the warm cache, if any. */
  def delete(key: K): IO[DeleteErrors | IndexErrors, Option[Array[Float]]] = collection.delete(key) <* cache.set(None)

  /** Fetches the vector stored for `key`, if any. */
  def get(key: K): IO[FetchErrors, Option[Array[Float]]] = collection.fetch(key)

  /** Number of vectors currently indexed. */
  def size(): IO[SizeErrors, Long] = collection.size()

  /** Snapshots the whole collection into memory so that subsequent `searchNearest` calls score that snapshot instead of re-streaming the collection every time. Worth it when many searches will run back-to-back against a corpus that isn't
    * concurrently changing — a batch job matching many query vectors against the same known set, for instance. A single `insert`/`delete` drops the snapshot; call `warm()` again after a batch of writes, before the next round of searches.
    */
  def warm(): IO[StreamErrors, Unit] =
    collection.streamWithKeys().runCollect.flatMap(vectors => cache.set(Some(vectors)))

  /** Drops the warm snapshot, if any, so the next `searchNearest` reads the collection directly again. */
  def cooldown(): UIO[Unit] = cache.set(None)

  /** Finds the `k` vectors closest to `query` according to `metric`, sorted by ascending distance.
    *
    * The vector source — the warm in-memory snapshot if `warm()` was called and nothing has been written since, otherwise the backing collection streamed fresh — is split into batches that are scored **in parallel** across up to `parallelism`
    * fibers, decoupled from LMDB's own small read-executor pool since distance computation is plain CPU-bound Scala code once a batch of vectors is in hand. Each batch keeps only its own top `k` before being merged into the running top `k`, so
    * memory stays bounded by `k`, never by the collection size.
    *
    * @param query
    *   the vector to search neighbors for; must have `dimension` components
    * @param k
    *   how many nearest neighbors to return
    * @param batchSize
    *   how many records each parallel worker scores at a time
    * @param parallelism
    *   how many batches are scored concurrently; defaults to the number of available cores
    */
  def searchNearest(
    query: Array[Float],
    k: Int,
    batchSize: Int = 2048,
    parallelism: Int = java.lang.Runtime.getRuntime.availableProcessors()
  ): IO[StreamErrors | VectorDimensionMismatch, Chunk[(K, Double)]] =
    if (k <= 0) ZIO.succeed(Chunk.empty)
    else
      checkDimension(query.length) *>
        cache.get.flatMap { snapshot =>
          val source: ZStream[Any, StreamErrors, (K, Array[Float])] =
            snapshot.fold(collection.streamWithKeys())(ZStream.fromChunk)
          source
            .grouped(batchSize)
            .mapZIOParUnordered(parallelism) { batch => ZIO.succeed(batchTopK(batch, query, k)) }
            .runFold(Chunk.empty[(K, Double)])((acc, next) => mergeTopK(acc, next, k))
        }

  private def checkDimension(actual: Int): IO[VectorDimensionMismatch, Unit] =
    ZIO.unless(actual == dimension)(ZIO.fail(VectorDimensionMismatch(collection.name, dimension, actual))).unit

  /** Scores one batch against `query` and keeps only its `k` closest, in one pass: distance is computed and immediately folded into a small bounded-size running top-k instead of first materializing a `(key, distance)` pair for every one of the
    * batch's (possibly thousands of) candidates and sorting all of them — sorting the whole batch costs O(n log n) and re-pays that for every single one of the many searches a batch job like a nearest-neighbor sweep runs; bounded insertion here
    * costs O(n * k), and `k` is normally tiny compared to the batch.
    */
  private def batchTopK(batch: Chunk[(K, Array[Float])], query: Array[Float], k: Int): Chunk[(K, Double)] = {
    val n = batch.length
    if (n <= k) batch.map { case (key, vector) => key -> metric.distance(query, vector) }.sortBy(_._2)
    else {
      // `best` holds the current top-k, unsorted, with `worstPos` tracking the index of its
      // largest (i.e. weakest) distance so a new candidate only needs one comparison to be
      // rejected, and a replacement only needs a fresh O(k) scan for the new worst.
      val bestKeys  = new Array[Any](k)
      val bestDists = new Array[Double](k)
      var filled    = 0
      var worstPos  = 0
      var worstDist = Double.NegativeInfinity

      def recomputeWorst(): Unit = {
        var pos = 0
        var i   = 1
        while (i < k) {
          if (bestDists(i) > bestDists(pos)) pos = i
          i += 1
        }
        worstPos = pos
        worstDist = bestDists(pos)
      }

      var i = 0
      while (i < n) {
        val (key, vector) = batch(i)
        val dist          = metric.distance(query, vector)
        if (filled < k) {
          bestKeys(filled) = key
          bestDists(filled) = dist
          filled += 1
          if (filled == k) recomputeWorst()
        } else if (dist < worstDist) {
          bestKeys(worstPos) = key
          bestDists(worstPos) = dist
          recomputeWorst()
        }
        i += 1
      }

      Chunk.fromArray(Array.tabulate(filled)(j => (bestKeys(j).asInstanceOf[K], bestDists(j)))).sortBy(_._2)
    }
  }

  /** Merges two already-bounded (`size <= k`) top-k chunks into one. Cheap: both sides are at most `k` elements, regardless of how large the corpus or the batches scored to produce them were.
    */
  private def mergeTopK(a: Chunk[(K, Double)], b: Chunk[(K, Double)], k: Int): Chunk[(K, Double)] =
    (a ++ b).sortBy(_._2).take(k)
}

object LMDBVectorIndex {

  /** Creates a new vector index, backed by a fresh collection named `name`.
    *
    * @param name
    *   the backing collection's name
    * @param dimension
    *   the fixed number of components every vector in this index must have
    * @param metric
    *   the distance function used by `searchNearest`; defaults to cosine distance
    * @param failIfExists
    *   if `true` (default), fails when a collection of that name already exists
    */
  def create[K](
    name: CollectionName,
    dimension: Int,
    metric: VectorMetric = VectorMetric.Cosine,
    failIfExists: Boolean = true
  )(implicit kodec: KeyCodec[K]): ZIO[LMDB, CreateErrors, LMDBVectorIndex[K]] = {
    import VectorCodec.given
    for {
      collection <- LMDB.collectionCreate[K, Array[Float]](name, failIfExists)
      cache      <- Ref.make(Option.empty[Chunk[(K, Array[Float])]])
    } yield LMDBVectorIndex(collection, dimension, metric, cache)
  }
}
