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
package zio.lmdb

import zio.*
import zio.test.*
import zio.test.TestAspect.*
import zio.json.*
import zio.lmdb.json.*
import zio.lmdb.keycodecs.uuidv7.*
import zio.lmdb.keycodecs.uuidv7.UUIDv7Codec.given

object LMDBPerformanceJsonSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  private val recordCount = 400000

  override def spec = suite("LMDB Performance Suite")(
    test("write and read throughput benchmark") {
      for {
        collection <- LMDB.collectionCreate[UUIDv7, UserProfile]("perf_test")

        // Prepare data
        records = (1 to recordCount).map { i =>
                    val id      = UUIDv7.generate()
                    val profile = UserProfile(
                      id = id.asUUID.toString,
                      name = s"User $i",
                      email = s"user$i@example.com",
                      age = 20 + (i % 50)
                    )
                    (id, profile)
                  }

        // Write Benchmark
        writeStart     <- Clock.nanoTime
        _              <- collection.readWrite { ops =>
                            ZIO.foreachDiscard(records) { case (id, profile) =>
                              ops.upsertOverwrite(id, profile)
                            }
                          }
        writeEnd       <- Clock.nanoTime
        writeDuration   = Duration.fromNanos(writeEnd - writeStart)
        writeThroughput = recordCount.toDouble / (writeDuration.toNanos.toDouble / 1e9)

        avgRecordSize = records.map(_._2.toJson.getBytes("UTF-8").length).sum.toDouble / recordCount

        // Read Benchmark
        readStart     <- Clock.nanoTime
        _             <- collection.readOnly { ops =>
                           ZIO.foreachDiscard(records) { case (id, _) =>
                             ops.fetch(id)
                           }
                         }
        readEnd       <- Clock.nanoTime
        readDuration   = Duration.fromNanos(readEnd - readStart)
        readThroughput = recordCount.toDouble / (readDuration.toNanos.toDouble / 1e9)

        benchmarkReport =
          f"""|Performance Results for $recordCount%,d JSON records:
              |Average record size: $avgRecordSize%,.2f bytes
              |Write Throughput:    $writeThroughput%,.2f records/s (${writeDuration.toMillis} ms)
              |Read Throughput:     $readThroughput%,.2f records/s (${readDuration.toMillis} ms)""".stripMargin

        _ <- ZIO.debug(benchmarkReport)

      } yield assertTrue(true)
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ timed
}
