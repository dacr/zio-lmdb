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

import zio.Chunk

import java.util.concurrent.atomic.AtomicReferenceArray
import scala.util.Random

/** Tuning knobs for [[HnswIndex]], with defaults that are the usual starting point for embeddings of a few hundred dimensions.
  *
  * @param m
  *   how many neighbors each node keeps per layer (layer 0 keeps `2 * m`). Higher means a better-connected graph: better recall, more memory, slower build.
  * @param efConstruction
  *   how wide the candidate list is kept while inserting. Higher means a better graph, and a proportionally slower build.
  * @param efSearch
  *   default candidate-list width at query time. Higher means better recall and a proportionally slower search; it must be `>= k` to return `k` results.
  * @param seed
  *   seed for the layer-assignment randomness. It fixes which layers each vector lands on, but a build with `buildParallelism > 1` is still not bit-for-bit reproducible: threads insert concurrently, so the links they end up choosing depend on how
  *   far along the graph was when each insert ran.
  * @param buildParallelism
  *   how many threads insert concurrently during `HnswIndex.build`. Building is the expensive part of an approximate index and parallelizes well, so this defaults to every available core; set it to 1 for a deterministic (and much slower) build.
  */
final case class HnswParams(
  m: Int = 16,
  efConstruction: Int = 100,
  efSearch: Int = 64,
  seed: Long = 0x5eedL,
  buildParallelism: Int = java.lang.Runtime.getRuntime.availableProcessors()
)

/** A binary heap over `(id, distance)` pairs kept in parallel primitive arrays, so the inner loops of graph traversal don't allocate or box per candidate. `minFirst` picks the ordering: nearest-first for the candidate queue, furthest-first for the
  * bounded result set.
  */
private final class NodeHeap(initialCapacity: Int, minFirst: Boolean) {
  private var ids   = new Array[Int](math.max(4, initialCapacity))
  private var dists = new Array[Double](math.max(4, initialCapacity))
  private var count = 0

  def size: Int              = count
  def isEmpty: Boolean       = count == 0
  def nonEmpty: Boolean      = count > 0
  def topId: Int             = ids(0)
  def topDist: Double        = dists(0)
  def idAt(i: Int): Int      = ids(i)
  def distAt(i: Int): Double = dists(i)
  def clear(): Unit          = count = 0

  /** True when `a` should sit closer to the root than `b`. */
  private inline def precedes(a: Double, b: Double): Boolean = if (minFirst) a < b else a > b

  private def grow(): Unit = {
    val nextIds   = new Array[Int](ids.length * 2)
    val nextDists = new Array[Double](dists.length * 2)
    System.arraycopy(ids, 0, nextIds, 0, count)
    System.arraycopy(dists, 0, nextDists, 0, count)
    ids = nextIds
    dists = nextDists
  }

  def push(id: Int, dist: Double): Unit = {
    if (count == ids.length) grow()
    ids(count) = id
    dists(count) = dist
    count += 1
    var child = count - 1
    while (child > 0) {
      val parent = (child - 1) >>> 1
      if (precedes(dists(child), dists(parent))) {
        swap(child, parent)
        child = parent
      } else child = 0
    }
  }

  def pop(): Unit = {
    count -= 1
    ids(0) = ids(count)
    dists(0) = dists(count)
    var parent = 0
    var done   = false
    while (!done) {
      val left  = parent * 2 + 1
      val right = left + 1
      var best  = parent
      if (left < count && precedes(dists(left), dists(best))) best = left
      if (right < count && precedes(dists(right), dists(best))) best = right
      if (best == parent) done = true
      else {
        swap(parent, best)
        parent = best
      }
    }
  }

  private def swap(a: Int, b: Int): Unit = {
    val id = ids(a); ids(a) = ids(b); ids(b) = id
    val d  = dists(a); dists(a) = dists(b); dists(b) = d
  }
}

/** An in-memory [[https://arxiv.org/abs/1603.09320 HNSW]] (Hierarchical Navigable Small World) graph: an **approximate** nearest-neighbor index whose search cost grows logarithmically with the number of indexed vectors, rather than linearly the way
  * a full scan does.
  *
  * The tradeoff for that speed is recall: a search explores a bounded neighborhood of the graph rather than every vector, so it can occasionally miss a true nearest neighbor. `efSearch` buys recall back at a proportional cost in time — measure it
  * for your own data (`LMDBVectorIndex.searchNearest` is the exact reference to measure against).
  *
  * The graph is built once from a fixed set of vectors and is immutable afterwards, which makes searches safe to run concurrently from as many fibers as you like with no locking at all.
  */
final class HnswIndex[K] private (
  private val keys: Array[AnyRef],
  private val vectors: Array[Array[Float]],
  private val layers: Array[Array[Array[Int]]], // layer -> node -> its neighbors on that layer (null when the node isn't on it)
  private val entryPoint: Int,
  private val maxLayer: Int,
  private val metric: VectorMetric,
  private val normalized: Boolean,
  val params: HnswParams
) {

  /** How many vectors the graph holds. */
  def size: Int = vectors.length

  /** Finds approximately the `k` closest vectors to `query`, sorted by ascending distance. Distances are the real ones under this index's metric — only the *set* of returned neighbors is approximate.
    *
    * @param ef
    *   candidate-list width for this query, defaulting to the index's `efSearch`. Raising it improves recall and costs proportionally more time; it is always raised to at least `k`.
    */
  def search(query: Array[Float], k: Int, ef: Int = params.efSearch): Chunk[(K, Double)] = {
    if (k <= 0 || size == 0) Chunk.empty
    else {
      val probe   = if (normalized) HnswIndex.unitVector(query) else query
      val breadth = math.max(ef, k)

      // Descend the sparse upper layers greedily: each one narrows down the entry point for the layer below.
      var current     = entryPoint
      var currentDist = distance(probe, current)
      var layer       = maxLayer
      while (layer > 0) {
        var improved = true
        while (improved) {
          improved = false
          val neighbors = layers(layer)(current)
          if (neighbors != null) {
            var i = 0
            while (i < neighbors.length) {
              val candidate = neighbors(i)
              val d         = distance(probe, candidate)
              if (d < currentDist) {
                current = candidate
                currentDist = d
                improved = true
              }
              i += 1
            }
          }
        }
        layer -= 1
      }

      val found   = searchLayer(probe, current, currentDist, breadth, 0, new java.util.BitSet(vectors.length))
      // `found` is a furthest-first heap; drain it to get ascending order, keeping only the k best.
      val keep    = math.min(k, found.size)
      val results = new Array[(K, Double)](keep)
      var slot    = found.size - 1
      while (found.nonEmpty) {
        if (slot < keep) results(slot) = (keys(found.topId).asInstanceOf[K], found.topDist)
        found.pop()
        slot -= 1
      }
      Chunk.fromArray(results)
    }
  }

  /** Best-first traversal of one layer, keeping the `ef` closest nodes seen. Returns them as a furthest-first heap. The caller supplies `visited` so that a build, which runs this repeatedly on one thread, can reuse a single bit set instead of
    * allocating one the size of the graph per traversal; concurrent searches each pass their own.
    */
  private def searchLayer(probe: Array[Float], entry: Int, entryDist: Double, ef: Int, layer: Int, visited: java.util.BitSet): NodeHeap = {
    visited.clear()
    val candidates = new NodeHeap(ef, minFirst = true)  // nearest first: where to expand next
    val results    = new NodeHeap(ef, minFirst = false) // furthest first: the bounded best-so-far set
    val layerLinks = layers(layer)

    visited.set(entry)
    candidates.push(entry, entryDist)
    results.push(entry, entryDist)

    while (candidates.nonEmpty) {
      val nearestDist = candidates.topDist
      if (nearestDist > results.topDist && results.size >= ef) {
        // Everything left to explore is further away than the worst result we already hold.
        candidates.clear()
      } else {
        val node      = candidates.topId
        candidates.pop()
        val neighbors = layerLinks(node)
        if (neighbors != null) {
          var i = 0
          while (i < neighbors.length) {
            val candidate = neighbors(i)
            if (!visited.get(candidate)) {
              visited.set(candidate)
              val d = distance(probe, candidate)
              if (results.size < ef || d < results.topDist) {
                candidates.push(candidate, d)
                results.push(candidate, d)
                if (results.size > ef) results.pop()
              }
            }
            i += 1
          }
        }
      }
    }
    results
  }

  private inline def distance(probe: Array[Float], node: Int): Double =
    if (normalized) HnswIndex.cosineOfUnitVectors(probe, vectors(node)) else metric.distance(probe, vectors(node))
}

object HnswIndex {

  /** Builds a graph over `entries`. Cost grows with `params.efConstruction` and `params.m`; this is the expensive half of using an approximate index, and it is paid once per build rather than once per query.
    */
  def build[K](entries: Chunk[(K, Array[Float])], metric: VectorMetric, params: HnswParams = HnswParams()): HnswIndex[K] = {
    val n = entries.length

    // Cosine distance over unit-length vectors is just `1 - dot`, so normalizing once at build time removes two
    // norm accumulations from every one of the many distance computations a graph traversal performs. The values
    // returned by `search` are unchanged: `1 - dot` of the normalized pair *is* the cosine distance of the originals.
    val normalized = metric == VectorMetric.Cosine

    val keys    = new Array[AnyRef](n)
    val vectors = new Array[Array[Float]](n)
    var i       = 0
    while (i < n) {
      val (key, vector) = entries(i)
      keys(i) = key.asInstanceOf[AnyRef]
      vectors(i) = if (normalized) unitVector(vector) else vector
      i += 1
    }

    val mMax0    = params.m * 2
    val mMax     = params.m
    val levelLen = 1.0 / math.log(params.m.toDouble)
    val random   = new Random(params.seed)

    // Node levels first, so the layer arrays can be sized once.
    val levels   = new Array[Int](n)
    var topLevel = 0
    i = 0
    while (i < n) {
      val level = (-math.log(math.max(random.nextDouble(), 1e-12)) * levelLen).toInt
      levels(i) = level
      if (level > topLevel) topLevel = level
      i += 1
    }

    // While the graph is being built its links live in atomic arrays, so the worker threads publishing
    // into them are guaranteed to see each other's writes. Once every worker has joined they are frozen
    // into plain arrays, leaving the query path — the hot one — free of that indirection.
    val building = Array.tabulate(topLevel + 1)(_ => new AtomicReferenceArray[Array[Int]](n))
    i = 0
    while (i < n) {
      var l = 0
      while (l <= levels(i)) {
        building(l).set(i, HnswIndex.emptyLinks)
        l += 1
      }
      i += 1
    }

    // Node 0 seeds the graph as its entry point; every other node is inserted into it, across as many
    // threads as `buildParallelism` allows. Inserts interleave (`start + t`, stepping by the thread
    // count) rather than taking contiguous blocks, so the threads stay in step as inserts get more
    // expensive with a growing graph.
    val builder = new HnswBuilder(vectors, levels, building, metric, normalized, params)
    if (n > 1) {
      val threads = math.max(1, math.min(params.buildParallelism, n - 1))
      if (threads == 1) builder.insertStriped(1, 1)
      else {
        val workers = Array.tabulate(threads)(t => new Thread(() => builder.insertStriped(1 + t, threads), s"hnsw-build-$t"))
        workers.foreach(_.start())
        workers.foreach(_.join()) // also the happens-before edge that publishes every link written above
      }
    }

    val layers = Array.tabulate(topLevel + 1) { layer =>
      val source = building(layer)
      val frozen = new Array[Array[Int]](n)
      var node   = 0
      while (node < n) {
        frozen(node) = source.get(node)
        node += 1
      }
      frozen
    }

    new HnswIndex[K](keys, vectors, layers, builder.entryPointId, builder.topLayerReached, metric, normalized, params)
  }

  private[vector] val emptyLinks = new Array[Int](0)

  /** The paper's neighbor-selection heuristic: walk candidates nearest-first and keep one only when it is closer to the query than to every neighbor already kept. That keeps links pointing in diverse directions instead of piling them all into one
    * dense cluster — which matters a lot for embeddings like face features, where thousands of vectors of the same subject sit almost on top of each other and would otherwise absorb every link.
    */
  private[vector] def selectNeighbors(ids: Array[Int], dists: Array[Double], m: Int, distanceBetween: (Int, Int) => Double): Array[Int] = {
    if (ids.length <= m) ids
    else {
      val kept  = new Array[Int](m)
      var count = 0
      var i     = 0
      while (i < ids.length && count < m) {
        val candidate = ids(i)
        var diverse   = true
        var j         = 0
        while (j < count && diverse) {
          if (distanceBetween(candidate, kept(j)) <= dists(i)) diverse = false
          j += 1
        }
        if (diverse) {
          kept(count) = candidate
          count += 1
        }
        i += 1
      }
      // If the diversity rule was strict enough to leave the list short, top it back up with the nearest
      // candidates it rejected, so the node keeps its full degree.
      if (count < m) {
        i = 0
        while (i < ids.length && count < m) {
          val candidate = ids(i)
          var already   = false
          var j         = 0
          while (j < count && !already) {
            if (kept(j) == candidate) already = true
            j += 1
          }
          if (!already) {
            kept(count) = candidate
            count += 1
          }
          i += 1
        }
      }
      if (count == m) kept else java.util.Arrays.copyOf(kept, count)
    }
  }

  /** Drains a furthest-first heap into ids/distances sorted nearest-first. */
  private[vector] def drainAscending(heap: NodeHeap): (Array[Int], Array[Double]) = {
    val size  = heap.size
    val ids   = new Array[Int](size)
    val dists = new Array[Double](size)
    var slot  = size - 1
    while (heap.nonEmpty) {
      ids(slot) = heap.topId
      dists(slot) = heap.topDist
      heap.pop()
      slot -= 1
    }
    (ids, dists)
  }

  /** In-place insertion sort of `ids` by `dists`, both reordered together. Only ever called on neighbor lists, which are at most a few dozen entries long. */
  private[vector] def sortByDistance(ids: Array[Int], dists: Array[Double]): Unit = {
    var i = 1
    while (i < ids.length) {
      val id = ids(i)
      val d  = dists(i)
      var j  = i - 1
      while (j >= 0 && dists(j) > d) {
        ids(j + 1) = ids(j)
        dists(j + 1) = dists(j)
        j -= 1
      }
      ids(j + 1) = id
      dists(j + 1) = d
      i += 1
    }
  }

  private[vector] def unitVector(vector: Array[Float]): Array[Float] = {
    var norm   = 0.0
    var i      = 0
    while (i < vector.length) {
      val v = vector(i).toDouble
      norm += v * v
      i += 1
    }
    val length = math.sqrt(norm)
    if (length == 0.0) vector.clone()
    else {
      val scaled = new Array[Float](vector.length)
      i = 0
      while (i < vector.length) {
        scaled(i) = (vector(i) / length).toFloat
        i += 1
      }
      scaled
    }
  }

  /** Cosine distance for vectors already scaled to unit length: `1 - dot`. */
  private[vector] def cosineOfUnitVectors(a: Array[Float], b: Array[Float]): Double = {
    var dot = 0.0
    var i   = 0
    while (i < a.length) {
      dot += a(i) * b(i)
      i += 1
    }
    1.0 - dot
  }
}

/** Fills in an [[HnswIndex]]'s links, potentially from several threads at once.
  *
  * Concurrency here is deliberately minimal, which an approximate structure can afford:
  *   - Traversal reads links without any locking. A reader may catch a node mid-insertion and see a shorter neighbor list than it will eventually have; the cost of that is a marginally worse graph, never a wrong or corrupt one.
  *   - A neighbor list is only ever *modified* while holding its owner's stripe lock, so concurrent inserts can't lose each other's back-links. No thread ever holds two of those locks at once, so striping them cannot deadlock no matter how many
  *     nodes share a stripe.
  *   - The entry point and the top layer move together under one small lock, and each insert takes a consistent snapshot of the pair before it starts. A snapshot can go stale mid-insert; that just means the node enters via a slightly older entry
  *     point, which is again a graph-quality question rather than a correctness one.
  */
private final class HnswBuilder(
  vectors: Array[Array[Float]],
  levels: Array[Int],
  layers: Array[AtomicReferenceArray[Array[Int]]],
  metric: VectorMetric,
  normalized: Boolean,
  params: HnswParams
) {
  private val nodeCount = vectors.length
  private val mMax      = params.m
  private val mMax0     = params.m * 2
  private val locks     = Array.fill(HnswBuilder.LockStripes)(new Object)
  private val topology  = new Object

  private var entryPoint = 0
  private var topLayer   = levels(0)

  def entryPointId: Int    = topology.synchronized(entryPoint)
  def topLayerReached: Int = topology.synchronized(topLayer)

  /** Inserts `start`, `start + step`, `start + 2 * step`, ... Each caller thread runs this once, on its own stripe of the node range. */
  def insertStriped(start: Int, step: Int): Unit = {
    val visited = new java.util.BitSet(nodeCount) // reused across this thread's inserts rather than reallocated per traversal
    var node    = start
    while (node < nodeCount) {
      insert(node, visited)
      node += step
    }
  }

  private def insert(node: Int, visited: java.util.BitSet): Unit = {
    val level                    = levels(node)
    val (startPoint, startLayer) = topology.synchronized((entryPoint, topLayer))

    // Greedy descent through the layers above this node's own top level.
    var current     = startPoint
    var currentDist = distanceBetween(node, current)
    var layer       = startLayer
    while (layer > level) {
      var improved = true
      while (improved) {
        improved = false
        val neighbors = layers(layer).get(current)
        if (neighbors != null) {
          var i = 0
          while (i < neighbors.length) {
            val candidate = neighbors(i)
            val d         = distanceBetween(node, candidate)
            if (d < currentDist) {
              current = candidate
              currentDist = d
              improved = true
            }
            i += 1
          }
        }
      }
      layer -= 1
    }

    // Then connect, layer by layer, from this node's top level down to 0.
    layer = math.min(startLayer, level)
    while (layer >= 0) {
      val found                          = traverse(vectors(node), current, currentDist, params.efConstruction, layer, visited)
      val (candidateIds, candidateDists) = HnswIndex.drainAscending(found)
      val cap                            = if (layer == 0) mMax0 else mMax
      val selected                       = HnswIndex.selectNeighbors(candidateIds, candidateDists, cap, distanceBetween)

      link(layer, node, selected, cap)
      // Links are bidirectional: add this node back into each chosen neighbor's list too.
      var s = 0
      while (s < selected.length) {
        link(layer, selected(s), node, cap)
        s += 1
      }

      if (candidateIds.length > 0) {
        current = candidateIds(0)
        currentDist = candidateDists(0)
      }
      layer -= 1
    }

    if (level > startLayer) topology.synchronized {
      if (level > topLayer) {
        topLayer = level
        entryPoint = node
      }
    }
  }

  private def link(layer: Int, target: Int, addition: Int, cap: Int): Unit =
    link(layer, target, HnswBuilder.singleton(addition), cap)

  /** Merges `additions` into `target`'s neighbor list on `layer`, pruning back to `cap` when the result overflows. Only `target`'s own stripe is locked, so inserts touching unrelated nodes never wait on each other. */
  private def link(layer: Int, target: Int, additions: Array[Int], cap: Int): Unit =
    locks(target & (HnswBuilder.LockStripes - 1)).synchronized {
      val stored   = layers(layer).get(target)
      val existing = if (stored == null) HnswIndex.emptyLinks else stored
      var merged   = existing

      var a = 0
      while (a < additions.length) {
        val candidate = additions(a)
        if (candidate != target && !contains(merged, candidate)) {
          merged = java.util.Arrays.copyOf(merged, merged.length + 1)
          merged(merged.length - 1) = candidate
        }
        a += 1
      }

      if (merged ne existing) {
        if (merged.length <= cap) layers(layer).set(target, merged)
        else {
          val dists = new Array[Double](merged.length)
          var t     = 0
          while (t < merged.length) {
            dists(t) = distanceBetween(target, merged(t))
            t += 1
          }
          HnswIndex.sortByDistance(merged, dists)
          layers(layer).set(target, HnswIndex.selectNeighbors(merged, dists, cap, distanceBetween))
        }
      }
    }

  /** Best-first traversal of one layer over the graph as it currently stands. Mirrors `HnswIndex.searchLayer`, reading through the atomic arrays that the build writes into instead of the frozen ones a finished index queries. */
  private def traverse(probe: Array[Float], entry: Int, entryDist: Double, ef: Int, layer: Int, visited: java.util.BitSet): NodeHeap = {
    visited.clear()
    val candidates = new NodeHeap(ef, minFirst = true)
    val results    = new NodeHeap(ef, minFirst = false)
    val layerLinks = layers(layer)

    visited.set(entry)
    candidates.push(entry, entryDist)
    results.push(entry, entryDist)

    while (candidates.nonEmpty) {
      if (candidates.topDist > results.topDist && results.size >= ef) candidates.clear()
      else {
        val node      = candidates.topId
        candidates.pop()
        val neighbors = layerLinks.get(node)
        if (neighbors != null) {
          var i = 0
          while (i < neighbors.length) {
            val candidate = neighbors(i)
            if (!visited.get(candidate)) {
              visited.set(candidate)
              val d = distanceTo(probe, candidate)
              if (results.size < ef || d < results.topDist) {
                candidates.push(candidate, d)
                results.push(candidate, d)
                if (results.size > ef) results.pop()
              }
            }
            i += 1
          }
        }
      }
    }
    results
  }

  private def contains(links: Array[Int], value: Int): Boolean = {
    var i     = 0
    var found = false
    while (i < links.length && !found) {
      if (links(i) == value) found = true
      i += 1
    }
    found
  }

  private def distanceBetween(a: Int, b: Int): Double =
    if (normalized) HnswIndex.cosineOfUnitVectors(vectors(a), vectors(b)) else metric.distance(vectors(a), vectors(b))

  private def distanceTo(probe: Array[Float], node: Int): Double =
    if (normalized) HnswIndex.cosineOfUnitVectors(probe, vectors(node)) else metric.distance(probe, vectors(node))
}

private object HnswBuilder {

  /** Power of two, so the stripe for a node is a cheap mask of its id. Enough stripes that unrelated inserts almost never collide, while staying far smaller than one lock per node. */
  val LockStripes = 1024

  def singleton(value: Int): Array[Int] = {
    val single = new Array[Int](1)
    single(0) = value
    single
  }
}
