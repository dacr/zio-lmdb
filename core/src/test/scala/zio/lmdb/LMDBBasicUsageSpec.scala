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
import zio.nio.file.Files
import zio.lmdb.json.*

case class Record(name: String, age: Long) derives LMDBCodecJson

object LMDBBasicUsageSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB for ZIO as a service")(
    test("basic usage")(
      for {
        collection    <- LMDB.collectionCreate[String, Record]("example")
        record         = Record("John Doe", 42)
        recordId      <- Random.nextUUID.map(_.toString)
        _             <- collection.insert(recordId, record)
        updated       <- collection.update(recordId, prev => prev.copy(age = prev.age + 1)).some
        _             <- collection.update(recordId, prev => prev.copy(age = prev.age - 1))
        exists        <- collection.contains(recordId)
        gotten        <- collection.fetch(recordId).some
        deletedRecord <- collection.delete(recordId)
        gotNothing    <- collection.fetch(recordId)
      } yield assertTrue(
        gotten == record,
        updated.age == record.age + 1,
        deletedRecord.contains(record),
        gotNothing.isEmpty,
        exists
      )
    )
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
