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
import zio.nio.file.Files
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

  val limit = 5_000

  def randomUUID = for {
    seed <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _    <- Random.setSeed(seed)
    uuid <- Random.nextUUID.map(_.toString)
  } yield uuid

  def randomDatabaseName = for {
    uuid <- randomUUID
    name  = s"collection-$uuid"
    // _    <- Console.printLine(s"Using random database name $name")
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
    test("create and list databases")(
      for {
        dbName1   <- randomDatabaseName
        dbName2   <- randomDatabaseName
        lmdb      <- ZIO.service[LMDBOperations]
        _         <- lmdb.databaseCreate(dbName1)
        _         <- lmdb.databaseCreate(dbName2)
        databases <- lmdb.databases()
      } yield assertTrue(
        databases.contains(dbName1),
        databases.contains(dbName2)
      ).label(s"dbName1=$dbName1 dbName2=$dbName2")
    ),
    // -----------------------------------------------------------------------------
    test("try to set/get a key")(
      check(keygen, string) { (id, data) =>
        val value = Str(data)
        for {
          dbName <- randomDatabaseName
          lmdb   <- ZIO.service[LMDBOperations]
          _      <- lmdb.databaseCreate(dbName)
          _      <- lmdb.upsertOverwrite[Str](dbName, id, value)
          gotten <- lmdb.fetch[Str](dbName, id)
        } yield assertTrue(
          gotten == Some(value)
        ).label(s"for key $id")
      }
    ),
    // -----------------------------------------------------------------------------
    test("try to get an non existent key")(
      for {
        dbName   <- randomDatabaseName
        id       <- randomUUID
        lmdb     <- ZIO.service[LMDBOperations]
        _        <- lmdb.databaseCreate(dbName)
        isFailed <- lmdb.fetch[Str](dbName, id).some.isFailure
      } yield assertTrue(isFailed).label(s"for key $id")
    ),
    // -----------------------------------------------------------------------------
    test("basic CRUDL operations") {
      check(keygen, valuegen, valuegen) { (id, data1, data2) =>
        val value        = Str(data1)
        val updatedValue = Str(data2)
        for {
          lmdb          <- ZIO.service[LMDBOperations]
          dbName         = "basic-crudl-operations"
          //          dbName        <- randomDatabaseName
          _             <- lmdb.databaseCreate(dbName)
          _             <- lmdb.upsertOverwrite[Str](dbName, id, value)
          gotten        <- lmdb.fetch[Str](dbName, id)
          _             <- lmdb.upsertOverwrite(dbName, id, updatedValue)
          gottenUpdated <- lmdb.fetch[Str](dbName, id)
          listed1       <- lmdb.collect(dbName)
          listed2       <- ZIO.scoped(lmdb.stream[Str](dbName).runCollect)
          _             <- lmdb.delete(dbName, id)
          isFailed      <- lmdb.fetch[Str](dbName, id).some.isFailure
        } yield assertTrue(
          gotten == Some(value),
          gottenUpdated == Some(updatedValue),
          listed1.contains(updatedValue),
          listed2.contains(updatedValue),
          isFailed
        ).label(s"for key $id")
      }
    } @@ tag("slow"),
    // -----------------------------------------------------------------------------
    test("many overwrite updates") {
      for {
        lmdb    <- ZIO.service[LMDBOperations]
        id      <- randomUUID
        maxValue = limit
        dbName  <- randomDatabaseName
        _       <- lmdb.databaseCreate(dbName)
        _       <- ZIO.foreach(1.to(maxValue))(i => lmdb.upsertOverwrite[Num](dbName, id, Num(i)))
        num     <- lmdb.fetch[Num](dbName, id)
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
        lmdb   <- ZIO.service[LMDBOperations]
        id     <- randomUUID
        count   = limit
        dbName <- randomDatabaseName
        _      <- lmdb.databaseCreate(dbName)
        _      <- ZIO.foreach(1.to(count))(i => lmdb.upsert[Num](dbName, id, modifier))
        num    <- lmdb.fetch[Num](dbName, id)
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

      val dbcount = if (limit < 1000) 5 else 100
      val max     = limit

      for {
        lmdb             <- ZIO.service[LMDBOperations]
        id               <- randomUUID
        dbName           <- randomDatabaseName
        _                <- ZIO.foreach(1.to(max))(i => lmdb.databaseCreate(s"$dbName#${i % dbcount}"))
        _                <- ZIO.foreach(1.to(max))(i => lmdb.upsert[Num](s"$dbName#${i % dbcount}", id, modifier))
        num1             <- lmdb.fetch[Num](s"$dbName#1", id)
        num2             <- lmdb.fetch[Num](s"$dbName#2", id)
        createdDatabases <- lmdb.databases()
      } yield assertTrue(
        num1.map(_.value.intValue()) == Some(max / dbcount),
        num2.map(_.value.intValue()) == Some(max / dbcount),
        createdDatabases.size >= dbcount
      )
    },
    // -----------------------------------------------------------------------------
    test("list collection content") {
      for {
        lmdb          <- ZIO.service[LMDBOperations]
        count          = limit
        value          = Num(42)
        dbName        <- randomDatabaseName
        _             <- lmdb.databaseCreate(dbName)
        _             <- ZIO.foreach(1.to(count))(num => lmdb.upsertOverwrite[Num](dbName, s"id#$num", value))
        collected     <- lmdb.collect[Num](dbName)
        collectedCount = collected.size
      } yield assertTrue(
        collectedCount == count
      )
    },
    // -----------------------------------------------------------------------------
    test("stream collection content") {
      for {
        lmdb          <- ZIO.service[LMDBOperations]
        count          = limit
        dbName        <- randomDatabaseName
        _             <- lmdb.databaseCreate(dbName)
        _             <- ZIO.foreach(1.to(count))(num => lmdb.upsertOverwrite[Num](dbName, s"id#$num", Num(num)))
        returnedCount <- ZIO.scoped(lmdb.stream[Num](dbName).filter(_.value.intValue() % 2 == 0).runCount)
      } yield assertTrue(
        returnedCount.toInt == count / 2
      )
    }
    // -----------------------------------------------------------------------------
  ).provideCustomShared(
    lmdbLayer.orDie
  ) @@ withLiveClock @@ withLiveRandom // @@ sequential
  // TODO Using provideCustomShared generates issues with some tests - Slower when shared is used
  // TODO Using provideCustomShared is a good test case to check LMDB concurrent access behavior
}
