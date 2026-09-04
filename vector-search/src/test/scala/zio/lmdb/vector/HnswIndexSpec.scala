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
import zio.test.*

import scala.util.Random

object HnswIndexSpec extends ZIOSpecDefault {

  /** Vectors shaped like real embeddings: a number of tight clusters (think "photos of the same person") rather than uniform noise, which is the case an approximate index finds hardest and where the neighbor-selection heuristic earns its keep.
    */
  private def clusteredVectors(count: Int, dimension: Int, clusters: Int, spread: Float, random: Random): Vector[Array[Float]] = {
    val centers = Vector.fill(clusters)(Array.fill(dimension)(random.nextGaussian().toFloat))
    Vector.tabulate(count) { i =>
      val center = centers(i % clusters)
      Array.tabulate(dimension)(d => center(d) + (random.nextGaussian().toFloat * spread))
    }
  }

  private def exactNearest(vectors: Vector[Array[Float]], query: Array[Float], k: Int, metric: VectorMetric): Vector[Int] =
    vectors.zipWithIndex.map((v, i) => i -> metric.distance(query, v)).sortBy(_._2).take(k).map(_._1)

  override def spec = suite("HnswIndex")(
    test("finds the exact neighbors on a small, well-separated set") {
      val entries = Chunk(
        "x" -> Array(1f, 0f, 0f),
        "y" -> Array(0f, 1f, 0f),
        "z" -> Array(0f, 0f, 1f),
        "w" -> Array(-1f, 0f, 0f)
      )
      val graph   = HnswIndex.build(entries, VectorMetric.Cosine)
      val nearest = graph.search(Array(0.9f, 0.1f, 0f), k = 2)
      assertTrue(
        graph.size == 4,
        nearest.map(_._1) == Chunk("x", "y")
      )
    },
    test("returns real cosine distances, not the normalized proxy used internally") {
      val entries   = Chunk("a" -> Array(2f, 0f, 0f), "b" -> Array(0f, 3f, 0f))
      val graph     = HnswIndex.build(entries, VectorMetric.Cosine)
      val query     = Array(1f, 0f, 0f)
      val Some(hit) = graph.search(query, k = 1).headOption: @unchecked
      val expected  = VectorMetric.Cosine.distance(query, Array(2f, 0f, 0f))
      assertTrue(hit._1 == "a", math.abs(hit._2 - expected) < 1e-6)
    },
    test("a build across threads is as good as a single-threaded one") {
      // A lost back-link or a torn neighbor list under concurrency would show up as a graph that navigates
      // badly, so comparing recall between a parallel and a sequential build over the same vectors is the
      // practical check that the locking holds up.
      val dimension = 128
      val count     = 8000
      val queries   = 100
      val k         = 8
      val random    = new Random(99L)
      val vectors   = clusteredVectors(count, dimension, clusters = 120, spread = 0.3f, random)
      val entries   = Chunk.fromIterable(vectors.zipWithIndex.map((v, i) => i -> v))
      val probes    = Vector.fill(queries)(vectors(random.nextInt(count)).map(_ + (random.nextGaussian().toFloat * 0.1f)))
      val expected  = probes.map(query => exactNearest(vectors, query, k, VectorMetric.Cosine).toSet)

      def recallOf(parallelism: Int): Double = {
        val graph = HnswIndex.build(entries, VectorMetric.Cosine, HnswParams(m = 16, efConstruction = 100, efSearch = 64, buildParallelism = parallelism))
        probes.zip(expected).map { (query, wanted) => wanted.intersect(graph.search(query, k).map(_._1).toSet).size.toDouble / k }.sum / queries
      }

      val sequential = recallOf(1)
      val parallel   = recallOf(8)
      println(s"HNSW recall@$k - sequential build: ${(sequential * 100).round / 100.0}, 8-thread build: ${(parallel * 100).round / 100.0}")
      assertTrue(sequential > 0.95, parallel > 0.95, parallel > sequential - 0.05)
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(10.minutes),
    test("recall against an exact scan stays high on clustered 512-dim embeddings") {
      val dimension = 512
      val count     = 20000
      val queries   = 200
      val k         = 8
      val random    = new Random(1234L)
      val vectors   = clusteredVectors(count, dimension, clusters = 300, spread = 0.35f, random)
      val entries   = Chunk.fromIterable(vectors.zipWithIndex.map((v, i) => i -> v))
      val graph     = HnswIndex.build(entries, VectorMetric.Cosine, HnswParams(m = 16, efConstruction = 100, efSearch = 64))

      val recalls = (0 until queries).map { _ =>
        val query    = vectors(random.nextInt(count)).map(_ + (random.nextGaussian().toFloat * 0.1f))
        val expected = exactNearest(vectors, query, k, VectorMetric.Cosine).toSet
        val found    = graph.search(query, k).map(_._1).toSet
        expected.intersect(found).size.toDouble / k
      }
      val recall  = recalls.sum / queries

      // Reported so a regression shows the actual number rather than just a failed assertion.
      println(s"HNSW recall@$k over $count clustered ${dimension}d vectors: ${(recall * 100).round / 100.0}")
      assertTrue(recall > 0.95)
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(10.minutes)
  )
}
