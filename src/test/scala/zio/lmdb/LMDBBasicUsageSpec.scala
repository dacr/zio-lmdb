/*
 * Copyright 2023 David Crosson
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

import zio._
import zio.test._
import zio.json._
import zio.nio.file.Files

case class Record(name: String, age: Long) derives JsonCodec

object LMDBBasicUsageSpec extends ZIOSpecDefault {
  val lmdbLayer = ZLayer.scoped(
    for {
      path  <- Files.createTempDirectoryScoped(prefix = Some("lmdb"), fileAttributes = Nil)
      config = LMDBConfig(
                 databaseName = "test",
                 databasesHome = Some(path.toString),
                 fileSystemSynchronized = false,
                 maxReaders = 100,
                 mapSize = BigInt(100_000_000_000L),
                 maxCollections = 10_000
               )
      lmdb  <- LMDBLive.setup(config)
    } yield lmdb
  )

  override def spec = suite("LMDB for ZIO as a service")(
    test("basic usage")(
      for {
        collection    <- LMDB.collectionCreate[Record]("example")
        record         = Record("John Doe", 42)
        recordId      <- Random.nextUUID.map(_.toString)
        updatedState  <- collection.upsert(recordId, previousRecord => record)
        gotten        <- collection.fetch(recordId).some
        deletedRecord <- collection.delete(recordId)
        gotNothing    <- collection.fetch(recordId)
      } yield assertTrue(
        updatedState.previous.isEmpty,
        updatedState.current == record,
        gotten == record,
        deletedRecord.contains(record),
        gotNothing.isEmpty
      )
    )
  ).provide(lmdbLayer)
}
