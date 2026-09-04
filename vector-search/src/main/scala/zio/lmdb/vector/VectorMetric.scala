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

/** A similarity/distance function over fixed-dimension float vectors. Lower is always closer, regardless of the underlying metric, so callers can uniformly sort/take-smallest.
  */
sealed trait VectorMetric {

  /** Computes the distance between two vectors of the same length. Smaller means more similar. Behavior is undefined if `a.length != b.length` — callers (`LMDBVectorIndex`) are expected to have already validated both vectors against the index's
    * declared dimension.
    */
  def distance(a: Array[Float], b: Array[Float]): Double
}

object VectorMetric {

  /** `1 - cosine similarity`, in `[0, 2]`. The most common choice for embeddings (face features, text/image embeddings, ...) whose magnitude is not meaningful, only their direction.
    */
  case object Cosine extends VectorMetric {
    def distance(a: Array[Float], b: Array[Float]): Double = {
      var dot   = 0.0
      var na    = 0.0
      var nb    = 0.0
      var i     = 0
      while (i < a.length) {
        val x = a(i)
        val y = b(i)
        dot += x * y
        na += x * x
        nb += y * y
        i += 1
      }
      val denom = math.sqrt(na) * math.sqrt(nb)
      if (denom == 0.0) 1.0 else 1.0 - (dot / denom)
    }
  }

  /** Straight-line (L2) distance. */
  case object Euclidean extends VectorMetric {
    def distance(a: Array[Float], b: Array[Float]): Double = {
      var sum = 0.0
      var i   = 0
      while (i < a.length) {
        val d = (a(i) - b(i)).toDouble
        sum += d * d
        i += 1
      }
      math.sqrt(sum)
    }
  }

  /** Negated dot product, so that "more similar" (higher dot product) still sorts as "smaller distance". Only meaningful when vector magnitudes are themselves meaningful/normalized.
    */
  case object DotProduct extends VectorMetric {
    def distance(a: Array[Float], b: Array[Float]): Double = {
      var dot = 0.0
      var i   = 0
      while (i < a.length) {
        dot += a(i) * b(i)
        i += 1
      }
      -dot
    }
  }
}
