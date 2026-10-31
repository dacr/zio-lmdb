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
import zio.lmdb.StorageUserError.CodecFailure
import zio.lmdb.keycodecs.KeyCodec
import zio.stream.*

import java.nio.ByteBuffer

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
  private val cache: Ref[Option[Chunk[(K, Array[Float])]]],
  private val approximate: Ref[Option[HnswIndex[K]]],
  private val quantized: Ref[Option[QuantizedIndex[K]]]
) {

  /** Adds or replaces the vector stored for `key`. Invalidates the warm cache and the approximate index, if any; updates the quantized index, if any. */
  def insert(key: K, vector: Array[Float]): IO[UpsertErrors | IndexErrors | VectorDimensionMismatch, Unit] =
    checkDimension(vector.length) *> collection.upsertOverwrite(key, vector) <* invalidate <*
      quantized.get.map(_.foreach(_.put(key, QuantizedIndex.quantize(vector))))

  /** Removes the vector stored for `key`, if any, returning it. Invalidates the warm cache and the approximate index, if any; updates the quantized index, if any. */
  def delete(key: K): IO[DeleteErrors | IndexErrors, Option[Array[Float]]] =
    collection.delete(key) <* invalidate <* quantized.get.map(_.foreach(_.remove(key)))

  /** Fetches the vector stored for `key`, if any. */
  def get(key: K): IO[FetchErrors, Option[Array[Float]]] = collection.fetch(key)

  /** Number of vectors currently indexed. */
  def size(): IO[SizeErrors, Long] = collection.size()

  /** Snapshots the whole collection into memory so that subsequent `searchNearest` calls score that snapshot instead of re-streaming the collection every time. Worth it when many searches will run back-to-back against a corpus that isn't
    * concurrently changing — a batch job matching many query vectors against the same known set, for instance. A single `insert`/`delete` drops the snapshot; call `warm()` again after a batch of writes, before the next round of searches.
    */
  def warm(): IO[StreamErrors, Unit] =
    collection.streamWithKeys().runCollect.flatMap(vectors => cache.set(Some(vectors)))

  /** Drops the warm snapshot and the approximate index, if any, so the next search reads the collection directly again. */
  def cooldown(): UIO[Unit] = invalidate

  /** Builds an in-memory [[HnswIndex]] over the collection, enabling `searchApproximate`. This is the expensive step of approximate search — it reads every vector and wires up a navigable graph — and it buys searches that no longer scale with the
    * size of the corpus. Worth it when the number of searches to run is large compared to the number of vectors; for a handful of queries, the exact `searchNearest` will finish sooner than this build does.
    *
    * Like `warm()`, the result is a point-in-time snapshot that any `insert`/`delete` invalidates, after which `searchApproximate` falls back to an exact scan until this is called again.
    */
  def buildApproximateIndex(params: HnswParams = HnswParams()): IO[StreamErrors, Unit] =
    collection.streamWithKeys().runCollect.flatMap { vectors =>
      ZIO.succeedBlocking(HnswIndex.build(vectors, metric, params)).flatMap(graph => approximate.set(Some(graph)))
    }

  private def invalidate: UIO[Unit] = cache.set(None) *> approximate.set(None)

  /** Builds an in-memory copy of the vectors quantized to one byte per component, enabling `searchQuantized`: a fourth of the memory the float vectors would take, scanned at memory speed instead of being read from LMDB at each search.
    *
    * Unlike `warm()` and `buildApproximateIndex`, `insert` and `delete` keep it up to date instead of dropping it, including the ones made while it is being built: build it once, at the start of the process. `searchQuantized` falls back to
    * `searchNearest` until the build is over.
    *
    * @param batchSize
    *   how many records each parallel worker quantizes at a time
    * @param parallelism
    *   how many batches are quantized concurrently; defaults to the number of available cores
    */
  def buildQuantizedIndex(
    batchSize: Int = 2048,
    parallelism: Int = java.lang.Runtime.getRuntime.availableProcessors()
  ): IO[StreamErrors, Unit] = {
    val expectedBytes = dimension * java.lang.Float.BYTES
    for {
      index <- ZIO.succeed(QuantizedIndex[K](dimension, metric))
      // set before the collection is read : the writes made while it is read go to this index too, and take precedence over what the read finds
      _     <- quantized.set(Some(index))
      _     <- collection.lmdb
                 .streamRawRange(collection.name)
                 .grouped(batchSize)
                 .mapZIOParUnordered(parallelism) { batch =>
                   ZIO.foreachDiscard(batch) { (keyBytes, vectorBytes) =>
                     if (vectorBytes.length != expectedBytes)
                       ZIO.fail(CodecFailure(s"invalid vector byte length in ${collection.name}: ${vectorBytes.length}, expected $expectedBytes for dimension $dimension"): CodecFailure)
                     else
                       ZIO.fromEither(collection.kodec.decode(ByteBuffer.wrap(keyBytes))).mapError(CodecFailure(_): CodecFailure).map { key =>
                         val vector = new Array[Float](dimension)
                         VectorCodec.readFloats(ByteBuffer.wrap(vectorBytes), vector)
                         index.load(key, QuantizedIndex.quantize(vector))
                       }
                   }
                 }
                 .runDrain
                 .tapError(_ => quantized.update(_.filterNot(_ eq index)))
      _     <- ZIO.succeed(index.loadDone())
    } yield ()
  }

  /** Drops the quantized index, if any, freeing its memory: `searchQuantized` falls back to `searchNearest` until it is built again. */
  def dropQuantizedIndex(): UIO[Unit] = quantized.set(None)

  /** Finds the `k` vectors closest to `query`, using the index built by `buildQuantizedIndex`: the `k * oversampling` closest by the quantized vectors, then the `k` closest of those by the exact vectors, read from the collection. The distances
    * returned are exact; a true neighbor is missed only when quantization pushes it beyond the `k * oversampling` first, which a larger `oversampling` makes rarer. Falls back to `searchNearest` while no quantized index is built.
    *
    * @param query
    *   the vector to search neighbors for; must have `dimension` components
    * @param k
    *   how many nearest neighbors to return
    * @param oversampling
    *   how many candidates per neighbor are selected by the quantized vectors and checked with the exact ones
    * @param parallelism
    *   how many segments of the quantized index are scanned concurrently; defaults to the number of available cores
    */
  def searchQuantized(
    query: Array[Float],
    k: Int,
    oversampling: Int = 4,
    parallelism: Int = java.lang.Runtime.getRuntime.availableProcessors()
  ): IO[StreamErrors | VectorDimensionMismatch, Chunk[(K, Double)]] =
    if (k <= 0) ZIO.succeed(Chunk.empty)
    else
      checkDimension(query.length) *>
        quantized.get.flatMap {
          case Some(index) if index.isReady =>
            val candidates     = k * math.max(1, oversampling)
            val quantizedQuery = QuantizedIndex.quantize(query)
            for {
              hits  <- ZIO
                         .foreachPar(0 until index.segmentCount)(segment => ZIO.succeed(index.searchSegment(segment, quantizedQuery, candidates)))
                         .withParallelism(parallelism)
              exact <- collection.readOnly { tx =>
                         ZIO.foreach(index.best(hits, candidates)) { (key, _) => tx.fetch(key).map(_.map(vector => key -> metric.distance(query, vector))) }
                       }
            } yield exact.flatten.sortBy(_._2).take(k)
          case _                            => searchNearest(query, k)
        }

  /** Finds the `k` vectors closest to `query` according to `metric`, sorted by ascending distance.
    *
    * The vector source — the warm in-memory snapshot if `warm()` was called and nothing has been written since, otherwise the raw records of the backing collection streamed fresh, decoded by the workers — is split into batches that are scored **in
    * parallel** across up to `parallelism` fibers, decoupled from LMDB's own small read-executor pool since distance computation is plain CPU-bound Scala code once a batch of vectors is in hand. Each batch keeps only its own top `k` before being
    * merged into the running top `k`, so memory stays bounded by `k`, never by the collection size.
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
        cache.get.flatMap {
          case Some(snapshot) =>
            ZStream
              .fromChunk(snapshot)
              .grouped(batchSize)
              .mapZIOParUnordered(parallelism) { batch => ZIO.succeed(batchTopK(batch, k)(_._1, entry => metric.distance(query, entry._2))) }
              .runFold(Chunk.empty[(K, Double)])((acc, next) => mergeTopK(acc, next, k))
          case None           =>
            // raw bytes from the cursor, decoded by the parallel workers: decoding on the single cursor thread would cap the scan at the speed of one core
            collection.lmdb
              .streamRawRange(collection.name)
              .grouped(batchSize)
              .mapZIOParUnordered(parallelism) { batch => ZIO.fromEither(rawBatchTopK(batch, query, k)) }
              .runFold(Chunk.empty[(Array[Byte], Double)])((acc, next) => mergeTopK(acc, next, k))
              .flatMap { best =>
                ZIO.foreach(best) { (keyBytes, distance) =>
                  ZIO.fromEither(collection.kodec.decode(ByteBuffer.wrap(keyBytes))).mapBoth(CodecFailure(_), _ -> distance)
                }
              }
        }

  /** Finds approximately the `k` closest vectors to `query`, using the graph built by `buildApproximateIndex`.
    *
    * Unlike `searchNearest`, this does **not** look at every stored vector: it walks a navigable graph, so its cost grows logarithmically rather than linearly with the size of the corpus. The price is recall — a search can miss a true neighbor —
    * which `ef` trades back against time. Measure recall for your own data against `searchNearest`, which remains the exact reference.
    *
    * With no approximate index built (or after a write invalidated it), this falls back to the exact `searchNearest`, so the results stay correct and only the speed advantage is lost.
    *
    * @param query
    *   the vector to search neighbors for; must have `dimension` components
    * @param k
    *   how many nearest neighbors to return
    * @param ef
    *   candidate-list width for this query; defaults to the built index's `efSearch`, and is always raised to at least `k`
    */
  def searchApproximate(query: Array[Float], k: Int, ef: Option[Int] = None): IO[StreamErrors | VectorDimensionMismatch, Chunk[(K, Double)]] =
    if (k <= 0) ZIO.succeed(Chunk.empty)
    else
      checkDimension(query.length) *>
        approximate.get.flatMap {
          case Some(graph) => ZIO.succeed(graph.search(query, k, ef.getOrElse(graph.params.efSearch)))
          case None        => searchNearest(query, k)
        }

  private def checkDimension(actual: Int): IO[VectorDimensionMismatch, Unit] =
    ZIO.unless(actual == dimension)(ZIO.fail(VectorDimensionMismatch(collection.name, dimension, actual))).unit

  /** Scores one batch against `query` and keeps only its `k` closest, in one pass: distance is computed and immediately folded into a small bounded-size running top-k instead of first materializing a `(key, distance)` pair for every one of the
    * batch's (possibly thousands of) candidates and sorting all of them — sorting the whole batch costs O(n log n) and re-pays that for every single one of the many searches a batch job like a nearest-neighbor sweep runs; bounded insertion here
    * costs O(n * k), and `k` is normally tiny compared to the batch.
    */
  private def batchTopK[E, A](batch: Chunk[E], k: Int)(keyOf: E => A, distanceOf: E => Double): Chunk[(A, Double)] = {
    val n = batch.length
    if (n <= k) batch.map(entry => keyOf(entry) -> distanceOf(entry)).sortBy(_._2)
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
        val entry = batch(i)
        val dist  = distanceOf(entry)
        if (filled < k) {
          bestKeys(filled) = keyOf(entry)
          bestDists(filled) = dist
          filled += 1
          if (filled == k) recomputeWorst()
        } else if (dist < worstDist) {
          bestKeys(worstPos) = keyOf(entry)
          bestDists(worstPos) = dist
          recomputeWorst()
        }
        i += 1
      }

      Chunk.fromArray(Array.tabulate(filled)(j => (bestKeys(j).asInstanceOf[A], bestDists(j)))).sortBy(_._2)
    }
  }

  /** `batchTopK` over raw (key bytes, vector bytes) records, as the cursor gives them: each vector is decoded in bulk into a single buffer reused for the whole batch, and the keys are kept as bytes, only the final winners' being decoded.
    */
  private def rawBatchTopK(batch: Chunk[(Array[Byte], Array[Byte])], query: Array[Float], k: Int): Either[CodecFailure, Chunk[(Array[Byte], Double)]] = {
    val expectedBytes = dimension * java.lang.Float.BYTES
    batch.find(_._2.length != expectedBytes) match {
      case Some((_, bytes)) => Left(CodecFailure(s"invalid vector byte length in ${collection.name}: ${bytes.length}, expected $expectedBytes for dimension $dimension"))
      case None             =>
        val vector = new Array[Float](dimension)
        Right(
          batchTopK(batch, k)(
            _._1,
            entry => {
              VectorCodec.readFloats(ByteBuffer.wrap(entry._2), vector)
              metric.distance(query, vector)
            }
          )
        )
    }
  }

  /** Merges two already-bounded (`size <= k`) top-k chunks into one. Cheap: both sides are at most `k` elements, regardless of how large the corpus or the batches scored to produce them were.
    */
  private def mergeTopK[A](a: Chunk[(A, Double)], b: Chunk[(A, Double)], k: Int): Chunk[(A, Double)] =
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
      collection  <- LMDB.collectionCreate[K, Array[Float]](name, failIfExists)
      cache       <- Ref.make(Option.empty[Chunk[(K, Array[Float])]])
      approximate <- Ref.make(Option.empty[HnswIndex[K]])
      quantized   <- Ref.make(Option.empty[QuantizedIndex[K]])
    } yield LMDBVectorIndex(collection, dimension, metric, cache, approximate, quantized)
  }
}
