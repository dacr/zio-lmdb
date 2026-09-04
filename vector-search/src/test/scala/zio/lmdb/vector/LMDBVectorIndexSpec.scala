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
import zio.test.*

object LMDBVectorIndexSpec extends ZIOSpecDefault {

  private def randomVector(dimension: Int): UIO[Array[Float]] =
    Random.nextIntBounded(1000).replicateZIO(dimension).map(_.map(_.toFloat - 500f).toArray)

  private def naiveNearest(vectors: List[(String, Array[Float])], query: Array[Float], k: Int, metric: VectorMetric): List[String] =
    vectors.map { case (key, v) => key -> metric.distance(query, v) }.sortBy(_._2).take(k).map(_._1)

  override def spec = suite("LMDBVectorIndex")(
    test("insert/get/delete round-trip") {
      for {
        index <- LMDBVectorIndex.create[String]("vec-roundtrip", dimension = 4, failIfExists = false)
        v      = Array(1f, 2f, 3f, 4f)
        _     <- index.insert("a", v)
        got   <- index.get("a")
        sz1   <- index.size()
        del   <- index.delete("a")
        got2  <- index.get("a")
        sz2   <- index.size()
      } yield assertTrue(
        got.exists(_.sameElements(v)),
        sz1 == 1L,
        del.exists(_.sameElements(v)),
        got2.isEmpty,
        sz2 == 0L
      )
    },
    test("rejects a vector whose dimension doesn't match the index") {
      for {
        index <- LMDBVectorIndex.create[String]("vec-dimension-mismatch", dimension = 4, failIfExists = false)
        exit  <- index.insert("a", Array(1f, 2f, 3f)).exit
      } yield assertTrue(exit.isFailure)
    },
    test("searchNearest finds the closest axis vector (euclidean)") {
      for {
        index <- LMDBVectorIndex.create[String]("vec-euclidean", dimension = 3, metric = VectorMetric.Euclidean, failIfExists = false)
        _     <- index.insert("x", Array(1f, 0f, 0f))
        _     <- index.insert("y", Array(0f, 1f, 0f))
        _     <- index.insert("z", Array(0f, 0f, 1f))
        top   <- index.searchNearest(Array(0.9f, 0.1f, 0f), k = 2)
      } yield assertTrue(top.map(_._1) == Chunk("x", "y"))
    },
    test("searchNearest matches a naive brute-force scan (cosine, parallel batches)") {
      val dimension = 32
      val count     = 500
      val batchSize = 37 // deliberately not a divisor of `count`, to exercise the merge across many small batches
      for {
        index   <- LMDBVectorIndex.create[String]("vec-cosine-batches", dimension, metric = VectorMetric.Cosine, failIfExists = false)
        keys     = (0 until count).map(i => s"k$i").toList
        vectors <- ZIO.foreach(keys)(key => randomVector(dimension).map(key -> _))
        _       <- ZIO.foreach(vectors) { case (key, v) => index.insert(key, v) }
        query   <- randomVector(dimension)
        got     <- index.searchNearest(query, k = 5, batchSize = batchSize, parallelism = 4)
        expected = naiveNearest(vectors, query, k = 5, VectorMetric.Cosine)
      } yield assertTrue(got.map(_._1).toList == expected)
    },
    test("searchApproximate uses the graph once built, and falls back to the exact scan before that") {
      val dimension = 16
      val count     = 400
      for {
        index    <- LMDBVectorIndex.create[String]("vec-approximate", dimension, metric = VectorMetric.Cosine, failIfExists = false)
        keys      = (0 until count).map(i => s"k$i").toList
        vectors  <- ZIO.foreach(keys)(key => randomVector(dimension).map(key -> _))
        _        <- ZIO.foreach(vectors) { case (key, v) => index.insert(key, v) }
        query    <- randomVector(dimension)
        // No graph yet: this must still answer, by falling back to the exact scan.
        fallback <- index.searchApproximate(query, k = 5)
        exact    <- index.searchNearest(query, k = 5)
        _        <- index.buildApproximateIndex(HnswParams(m = 16, efConstruction = 100, efSearch = 64))
        viaGraph <- index.searchApproximate(query, k = 5)
      } yield assertTrue(
        fallback == exact,
        viaGraph.map(_._1) == exact.map(_._1)
      )
    },
    test("searchNearest gives the same result warm or cold, and a write invalidates the warm snapshot") {
      for {
        index <- LMDBVectorIndex.create[String]("vec-warm", dimension = 3, metric = VectorMetric.Euclidean, failIfExists = false)
        _     <- index.insert("x", Array(1f, 0f, 0f))
        _     <- index.insert("y", Array(0f, 1f, 0f))
        query  = Array(0.9f, 0.1f, 0f)
        cold  <- index.searchNearest(query, k = 2)
        _     <- index.warm()
        warm  <- index.searchNearest(query, k = 2)
        _     <- index.insert("z", Array(0.95f, 0.05f, 0f)) // closer than "x"; invalidates the snapshot
        after <- index.searchNearest(query, k = 1)
      } yield assertTrue(
        cold == warm,
        after.map(_._1) == Chunk("z")
      )
    }
  ).provideShared(
    Scope.default,
    LMDB.liveWithDatabaseName("vector-index-test-db")
  ) @@ TestAspect.sequential
}
