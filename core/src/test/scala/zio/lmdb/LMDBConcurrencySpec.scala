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
import zio.json.*
import zio.test.TestAspect.*
import zio.test.*
import zio.lmdb.json.*

import java.util.UUID

case class Dummy(
  uuid: UUID,
  x: Int,
  y: Double
)

object Dummy {
  implicit val codec: LMDBCodec[Dummy] = LMDBCodecJson.derived

  def random = for {
    uuid <- Random.nextUUID
    x    <- Random.nextInt
    y    <- Random.nextDouble
  } yield Dummy(uuid, x, y)
}

object LMDBConcurrencySpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  val collectionLimit = 50
  val recordsLimit    = 1_000

  override def spec = suite("concurrency behavior checks")(
    List(1, 5, 10, 20).map { parallelism =>
      // -----------------------------------------------------------------------------
      test(s"many collections writes in parallel ${recordsLimit * collectionLimit} records through $collectionLimit collections - parallelism=$parallelism") {
        val strategy = ExecutionStrategy.ParallelN(parallelism)
        for {
          collections <- ZIO.foreachExec(1.to(collectionLimit))(strategy) { n =>
                           LMDB.collectionCreate[String, Dummy](s"concurrent-collection-$n")
                         }
          _           <- ZIO.foreachExec(collections)(strategy) { collection =>
                           ZIO.foreachExec(1.to(recordsLimit))(strategy) { n =>
                             Dummy.random.flatMap { dummy =>
                               collection.upsertOverwrite(dummy.uuid.toString, dummy)
                             }
                           }
                         }
          sizes       <- ZIO.foreachExec(collections)(strategy) { collection =>
                           collection.size()
                         }
        } yield assertTrue(
          collections.size == collectionLimit,
          sizes.size == collectionLimit,
          sizes.forall(_ == recordsLimit)
        )
      }
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
