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
import zio.json.*
import zio.json.ast.Json
import zio.json.ast.Json.*
import zio.nio.file.*
import zio.stream.{ZSink, ZStream}
import zio.test.*
import zio.test.Gen.*
import zio.test.TestAspect.*

import java.util.UUID
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith

@RunWith(classOf[zio.test.junit.ZTestJUnitRunner])
class LMDBOperationsSpec extends ZIOSpecDefault {

  val lmdbLayer = ZLayer.scoped(
    for {
      scope <- Files.createTempDirectoryScoped(prefix = Some("lmdb"), fileAttributes = Nil)
      lmdb  <- LMDBOperations.setup(LMDBConfig(databasesPath = scope.toFile))
    } yield lmdb
  )

  val keygen   = stringBounded(1, 510)(asciiChar)
  val valuegen = stringBounded(0, 1024)(asciiChar)

  val limit = 10_000

  val randomUUID = Random.nextUUID.map(_.toString)

  val randomCollectionName = for {
    uuid <- randomUUID
    name  = s"collection-$uuid"
  } yield name

  override def spec = suite("Lightening Memory Mapped Database abstraction layer spec")(
    // -----------------------------------------------------------------------------
    test("platform check")(
      for {
        lmdb         <- ZIO.service[LMDBOperations]
        hasSucceeded <- lmdb.platformCheck().isSuccess
      } yield assertTrue(
        hasSucceeded
      )
    ) @@ ignore, // HAS IT HAS A GLOBAL IMPACT ON THE DATABASE IF IT HAS BEEN SHARED BETWEEN ALL TESTS !!
    // -----------------------------------------------------------------------------
    test("create and list collections")(
      for {
        colName1  <- randomCollectionName
        colName2  <- randomCollectionName
        colName3  <- randomCollectionName
        _         <- LMDB.collectionCreate[String](colName1)
        _         <- LMDB.collectionCreate[Double](colName2)
        _         <- LMDB.collectionCreate[Json](colName3)
        databases <- LMDB.collectionsAvailable()
      } yield assertTrue(
        databases.contains(colName1),
        databases.contains(colName2)
      ).label(s"colName1=$colName1 colName2=$colName2")
    ),
    // -----------------------------------------------------------------------------
    test("try to set/get a key")(
      check(keygen, string) { (id, data) =>
        val value = Str(data)
        for {
          colName <- randomCollectionName
          col     <- LMDB.collectionCreate[Str](colName)
          _       <- col.upsertOverwrite(id, value)
          gotten  <- col.fetch(id)
        } yield assertTrue(
          gotten == Some(value)
        ).label(s"for key $id")
      }
    ) @@ samples(100),
    // -----------------------------------------------------------------------------
    test("try to get an non existent key")(
      for {
        colName  <- randomCollectionName
        id       <- randomUUID
        col      <- LMDB.collectionCreate[Str](colName)
        isFailed <- col.fetch(id).some.isFailure
      } yield assertTrue(isFailed).label(s"for key $id")
    ),
    // -----------------------------------------------------------------------------
    test("basic CRUDL operations") {
      check(keygen, valuegen, valuegen) { (id, data1, data2) =>
        val value        = Str(data1)
        val updatedValue = Str(data2)
        for {
          lmdb          <- ZIO.service[LMDBOperations]
          colName       <- randomCollectionName
          col           <- lmdb.collectionCreate[Str](colName)
          _             <- col.upsertOverwrite(id, value)
          gotten        <- col.fetch(id)
          _             <- col.upsertOverwrite(id, updatedValue)
          gottenUpdated <- col.fetch(id)
          listed1       <- col.collect()
          // listed2       <- ZIO.scoped(col.stream(colName).runCollect)
          _             <- col.delete(id)
          isFailed      <- col.fetch(id).some.isFailure
        } yield assertTrue(
          gotten == Some(value),
          gottenUpdated == Some(updatedValue),
          listed1.contains(updatedValue),
          // listed2.contains(updatedValue),
          isFailed
        ).label(s"for key $id")
      }
    } @@ tag("slow") @@ samples(100),
    // -----------------------------------------------------------------------------
    test("many overwrite updates") {
      for {
        lmdb    <- ZIO.service[LMDBOperations]
        id      <- randomUUID
        maxValue = limit
        colName <- randomCollectionName
        col     <- lmdb.collectionCreate[Num](colName)
        _       <- ZIO.foreach(1.to(maxValue))(i => col.upsertOverwrite(id, Num(i)))
        num     <- col.fetch(id)
      } yield assertTrue(
        num.map(_.value.intValue()) == Some(maxValue)
      )
    } @@ tag("slow"),
    // -----------------------------------------------------------------------------
    test("safe update in place") {
      def modifier(from: Option[Num]): Num = from match {
        case None      => Num(1)
        case Some(num) => Num(num.value.intValue() + 1)
      }

      for {
        id      <- randomUUID
        count    = limit
        colName <- randomCollectionName
        col     <- LMDB.collectionCreate[Num](colName)
        _       <- ZIO.foreach(1.to(count))(i => col.upsert(id, modifier))
        num     <- col.fetch(id)
      } yield assertTrue(
        num.map(_.value.intValue()) == Some(count)
      )
    },
    // -----------------------------------------------------------------------------
    test("many updates within multiple collection") {
      def modifier(from: Option[Num]): Num = from match {
        case None      => Num(1)
        case Some(num) => Num(num.value.intValue() + 1)
      }

      val colCount = if (limit < 1000) 5 else 100
      val max      = limit

      for {
        id               <- randomUUID
        colName          <- randomCollectionName
        cols             <- ZIO.foreach(1.to(colCount))(i => LMDB.collectionCreate[Num](s"$colName#${i % colCount}")).map(_.toVector)
        _                <- ZIO.foreach(1.to(max))(i => cols(i % colCount).upsert(id, modifier))
        num1             <- cols(0).fetch(id)
        num2             <- cols(1).fetch(id)
        createdDatabases <- LMDB.collectionsAvailable()
      } yield assertTrue(
        num1.map(_.value.intValue()) == Some(max / colCount),
        num2.map(_.value.intValue()) == Some(max / colCount),
        createdDatabases.size >= colCount
      )
    },
    // -----------------------------------------------------------------------------
    test("list collection content") {
      for {
        lmdb          <- ZIO.service[LMDBOperations]
        count          = limit
        value          = Num(42)
        dbName        <- randomCollectionName
        _             <- lmdb.collectionCreate(dbName)
        _             <- ZIO.foreach(1.to(count))(num => lmdb.upsertOverwrite[Num](dbName, s"id#$num", value))
        collected     <- lmdb.collect[Num](dbName)
        collectedCount = collected.size
      } yield assertTrue(
        collectedCount == count
      )
    }
    // -----------------------------------------------------------------------------
//    test("stream collection content") {
//      for {
//        lmdb          <- ZIO.service[LMDBOperations]
//        count          = limit
//        dbName        <- randomDatabaseName
//        _             <- lmdb.databaseCreate(dbName)
//        _             <- ZIO.foreach(1.to(count))(num => lmdb.upsertOverwrite[Num](dbName, s"id#$num", Num(num)))
//        returnedCount <- ZIO.scoped(lmdb.stream[Num](dbName).filter(_.value.intValue() % 2 == 0).runCount)
//      } yield assertTrue(
//        returnedCount.toInt == count / 2
//      )
//    }
    // -----------------------------------------------------------------------------
  ).provide(lmdbLayer.orDie) @@ withLiveClock @@ withLiveRandom
}
