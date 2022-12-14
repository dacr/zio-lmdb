/*
 * Copyright 2022 David Crosson
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
import zio.json.*
import zio.nio.file.Files
import org.junit.runner.RunWith

case class Record(name: String, age: Long)
object Record {
  given JsonCodec[Record] = DeriveJsonCodec.gen
}

@RunWith(classOf[zio.test.junit.ZTestJUnitRunner])
class LMDBSpec extends ZIOSpecDefault {
  val lmdbTestConfigLayer = ZLayer.scoped(
    for {
      scope <- Files.createTempDirectoryScoped(prefix = Some("lmdb"), fileAttributes = Nil)
    } yield LMDBConfig(databasesPath = scope.toFile)
  )

  override def spec = suite("LMDB for ZIO as a service")(
    test("basic usage")(
      for {
        _             <- LMDB.databaseCreate("example")
        record         = Record("John Doe", 42)
        recordId      <- Random.nextUUID.map(_.toString)
        updateState   <- LMDB.upsertOverwrite[Record]("example", recordId, record)
        gotten        <- LMDB.fetch[Record]("example", recordId).some
        deletedRecord <- LMDB.delete[Record]("example", recordId)
        gotNothing    <- LMDB.fetch[Record]("example", recordId)
      } yield assertTrue(
        updateState.previous.isEmpty,
        updateState.current == record,
        gotten == record,
        deletedRecord.contains(record),
        gotNothing.isEmpty
      )
    )
  ).provide(LMDB.live, lmdbTestConfigLayer.orDie, Scope.default)
}
