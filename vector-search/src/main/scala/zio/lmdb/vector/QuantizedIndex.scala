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

/** A vector quantized to one signed byte per component: `component ≈ code * scale`, with the exact norm of the original vector kept aside. */
private[vector] final case class QuantizedVector(codes: Array[Byte], scale: Float, norm: Float)

/** The best candidates of a part of a [[QuantizedIndex]], by slot, with their approximate distances. */
private[vector] final case class SlotHits(slots: Array[Int], distances: Array[Double])

/** An in-memory copy of the vectors of an index, each component quantized to a signed byte: 4 times smaller than the float vectors, so that a corpus too large to be kept in memory as floats, or to be read from LMDB at each search, fits in the heap
  * and is scanned at memory speed.
  *
  * Unlike the warm snapshot and the HNSW graph, it is kept up to date by the writes (`put`, `remove`) rather than dropped, so that it is built once per process. The scan computes approximate distances — the query stays in floats, only the stored
  * vectors are quantized, with one scale per vector — good enough to select candidates whose exact distances are then computed from the stored vectors.
  *
  * Writes are serialized; scans run concurrently with them without locking. A scan reads the vectors added before it started; one replaced or removed during a scan may be scored from a mix of its old and new codes, at worst ranking a candidate
  * wrongly for that one scan.
  *
  * @param segmentSize
  *   how many vectors a segment holds: the memory is allocated, and the scans are parallelized, by segment
  */
private[vector] final class QuantizedIndex[K](val dimension: Int, metric: VectorMetric, segmentSize: Int = 16384) {

  private final class Segment {
    val codes: Array[Byte]   = new Array[Byte](segmentSize * dimension)
    val scales: Array[Float] = new Array[Float](segmentSize)
    val norms: Array[Float]  = new Array[Float](segmentSize)
    val keys: Array[AnyRef]  = new Array[AnyRef](segmentSize) // null for a free slot
  }

  // written under the lock only ; `used` is written after the slot it hands out, so that a scan reading it sees the slots below it filled
  @volatile private var segments: Array[Segment] = Array.empty
  @volatile private var used: Int                = 0
  @volatile private var loaded: Boolean          = false

  // guarded by `this`
  private val slots                     = new java.util.HashMap[K, Integer]()
  private val free                      = new java.util.ArrayDeque[Integer]()
  private var touched: java.util.Set[K] = new java.util.HashSet[K]() // keys written by the application while the initial load runs

  /** Whether the initial load is over : before that, the index misses vectors and must not be searched. */
  def isReady: Boolean = loaded

  def size: Int = synchronized(slots.size)

  def segmentCount: Int = (used + segmentSize - 1) / segmentSize

  /** Adds or replaces the vector of `key`, written by the application. */
  def put(key: K, vector: QuantizedVector): Unit = synchronized {
    if (touched != null) touched.add(key)
    write(key, vector)
  }

  /** Removes the vector of `key`, removed by the application. */
  def remove(key: K): Unit = synchronized {
    if (touched != null) touched.add(key)
    val slot = slots.remove(key)
    if (slot != null) {
      segments(slot / segmentSize).keys(slot % segmentSize) = null
      free.push(slot)
    }
  }

  /** Adds the vector of `key` read by the initial load, unless the application wrote or removed `key` since the load started : the load reads a snapshot of the collection, the application's writes are more recent. */
  def load(key: K, vector: QuantizedVector): Unit = synchronized {
    if (touched == null || !touched.contains(key)) write(key, vector)
  }

  /** Ends the initial load : the index is complete, it can be searched. */
  def loadDone(): Unit = synchronized {
    touched = null
    loaded = true
  }

  private def write(key: K, vector: QuantizedVector): Unit = {
    val known   = slots.get(key)
    val slot    =
      if (known != null) known.intValue
      else if (!free.isEmpty) free.pop().intValue
      else {
        if (used == segments.length * segmentSize) segments = segments :+ new Segment
        used
      }
    val segment = segments(slot / segmentSize)
    val offset  = slot % segmentSize
    System.arraycopy(vector.codes, 0, segment.codes, offset * dimension, dimension)
    segment.scales(offset) = vector.scale
    segment.norms(offset) = vector.norm
    segment.keys(offset) = key.asInstanceOf[AnyRef]
    if (known == null) slots.put(key, slot)
    if (slot == used) used = slot + 1
  }

  /** The `m` vectors of the segment closest to `query`, quantized too, by approximate distance. */
  def searchSegment(index: Int, query: QuantizedVector, m: Int): SlotHits = {
    val count   = math.min(segmentSize, used - index * segmentSize)
    val segment = segments(index)
    val heap    = new NodeHeap(m + 1, minFirst = false)
    var offset  = 0
    while (offset < count) {
      if (segment.keys(offset) != null) {
        val dist = distance(query, segment, offset)
        if (heap.size < m) heap.push(offset, dist)
        else if (dist < heap.topDist) {
          heap.pop()
          heap.push(offset, dist)
        }
      }
      offset += 1
    }
    val base    = index * segmentSize
    SlotHits(Array.tabulate(heap.size)(i => base + heap.idAt(i)), Array.tabulate(heap.size)(heap.distAt))
  }

  /** The `m` best of the hits of all the segments, with their keys, closest first ; the slots freed since are left out. */
  def best(hits: Iterable[SlotHits], m: Int): Chunk[(K, Double)] = {
    val heap  = new NodeHeap(m + 1, minFirst = false)
    hits.foreach { part =>
      var i = 0
      while (i < part.slots.length) {
        val dist = part.distances(i)
        if (heap.size < m) heap.push(part.slots(i), dist)
        else if (dist < heap.topDist) {
          heap.pop()
          heap.push(part.slots(i), dist)
        }
        i += 1
      }
    }
    val found = Array.tabulate(heap.size)(i => (heap.idAt(i), heap.distAt(i))).sortBy(_._2)
    Chunk.fromArray(found).flatMap { (slot, dist) =>
      val key = segments(slot / segmentSize).keys(slot % segmentSize)
      if (key == null) Chunk.empty else Chunk.single(key.asInstanceOf[K] -> dist)
    }
  }

  private def distance(query: QuantizedVector, segment: Segment, offset: Int): Double = {
    val codes     = segment.codes
    val q         = query.codes
    val base      = offset * dimension
    // integer products of bytes : a loop the JIT turns into SIMD instructions, which it can't do for a float sum, whose order it must keep
    var sum       = 0
    var i         = 0
    while (i < dimension) {
      sum += q(i) * codes(base + i)
      i += 1
    }
    val dot       = sum.toDouble * query.scale * segment.scales(offset)
    val norm      = segment.norms(offset).toDouble
    val queryNorm = query.norm.toDouble
    metric match {
      case VectorMetric.Cosine     =>
        val denom = queryNorm * norm
        if (denom == 0.0) 1.0 else 1.0 - dot / denom
      case VectorMetric.Euclidean  => math.sqrt(math.max(0.0, queryNorm * queryNorm + norm * norm - 2 * dot))
      case VectorMetric.DotProduct => -dot
    }
  }
}

private[vector] object QuantizedIndex {

  /** The vector quantized : each component divided by the scale bringing the largest one to 127, rounded. */
  def quantize(vector: Array[Float]): QuantizedVector = {
    var maxAbs = 0f
    var normSq = 0.0
    var i      = 0
    while (i < vector.length) {
      val x = vector(i)
      maxAbs = math.max(maxAbs, math.abs(x))
      normSq += x * x
      i += 1
    }
    val codes  = new Array[Byte](vector.length)
    val scale  = maxAbs / 127f
    if (scale > 0f) {
      i = 0
      while (i < vector.length) {
        codes(i) = math.max(-127, math.min(127, math.round(vector(i) / scale))).toByte
        i += 1
      }
    }
    QuantizedVector(codes, scale, math.sqrt(normSq).toFloat)
  }
}
