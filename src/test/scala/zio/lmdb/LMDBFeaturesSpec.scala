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
import zio.json.ast.Json
import zio.json.ast.Json._
import zio.nio.file._
import zio.stream.{ZSink, ZStream}
import zio.test._
import zio.test.Gen._
import zio.test.TestAspect._

import LMDBCodecsJson._

object LMDBFeaturesSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  val keygen   = stringBounded(1, 510)(asciiChar)
  val valuegen = stringBounded(0, 1024)(asciiChar)
  val limit    = 30_000

  override def spec = suite("Lightening Memory Mapped Database abstraction layer spec")(
    // -----------------------------------------------------------------------------
    test("platform check")(
      for {
        hasSucceeded <- LMDB.platformCheck().isSuccess
      } yield assertTrue(
        hasSucceeded
      )
    ), // @@ ignore, // AS IT HAS A GLOBAL IMPACT ON THE DATABASE IF IT HAS BEEN SHARED BETWEEN ALL TESTS !!
    // -----------------------------------------------------------------------------
    test("create collection")(
      for {
        colName               <- randomCollectionName
        createSuccess         <- LMDB.collectionCreate[String](colName).isSuccess
        recreateFailure       <- LMDB.collectionCreate[String](colName, failIfExists = true).isFailure
        createIfNeededSuccess <- LMDB.collectionCreate[String](colName, failIfExists = false).isSuccess
      } yield assertTrue(
        createSuccess,
        recreateFailure,
        createIfNeededSuccess
      )
    ),
    // -----------------------------------------------------------------------------
    test("delete collection")(
      for {
        colName                <- randomCollectionName
        _                      <- LMDB.collectionCreate[String](colName)
        collectionCreated      <- LMDB.collectionExists(colName)
        _                      <- LMDB.collectionDrop(colName)
        collectionStillExists  <- LMDB.collectionExists(colName)
        collectionCreatedAgain <- LMDB.collectionCreate[String](colName, failIfExists = true).isSuccess
      } yield assertTrue(
        collectionCreated,
        !collectionStillExists,
        collectionCreatedAgain
      )
    ),
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
    test("check key existence")(
      for {
        colName <- randomCollectionName
        id      <- randomUUID
        col     <- LMDB.collectionCreate[Str](colName)
        _       <- col.upsertOverwrite(id, Str("some data"))
        result  <- col.contains(id)
      } yield assertTrue(
        result == true
      ).label(s"for key $id")
    ),
    // -----------------------------------------------------------------------------
    test("check key non existence")(
      for {
        colName <- randomCollectionName
        id      <- randomUUID
        col     <- LMDB.collectionCreate[Str](colName)
        result  <- col.contains(id)
      } yield assertTrue(
        result == false
      ).label(s"for key $id")
    ),
    // -----------------------------------------------------------------------------
    test("basic CRUDL operations") {
      check(keygen, valuegen, valuegen) { (id, data1, data2) =>
        val value        = Str(data1)
        val updatedValue = Str(data2)
        for {
          lmdb          <- ZIO.service[LMDBLive]
          colName       <- randomCollectionName
          col           <- lmdb.collectionCreate[Str](colName)
          _             <- col.upsertOverwrite(id, value)
          gotten        <- col.fetch(id)
          _             <- col.upsertOverwrite(id, updatedValue)
          gottenUpdated <- col.fetch(id)
          listed        <- col.collect()
          _             <- col.delete(id)
          isFailed      <- col.fetch(id).some.isFailure
        } yield assertTrue(
          gotten.contains(value),
          gottenUpdated.contains(updatedValue),
          listed.contains(updatedValue),
          listed.size == 1,
          isFailed
        ).label(s"for key $id")
      }
    } @@ tag("slow") @@ samples(50),
    // -----------------------------------------------------------------------------
    test("clear collection content") {
      for {
        lmdb       <- ZIO.service[LMDBLive]
        colName    <- randomCollectionName
        col        <- lmdb.collectionCreate[Str](colName)
        id1        <- randomUUID
        id2        <- randomUUID
        _          <- col.upsertOverwrite(id1, Str("value1"))
        _          <- col.upsertOverwrite(id2, Str("value2"))
        sizeBefore <- col.size()
        _          <- col.clear()
        sizeAfter  <- col.size()
      } yield assertTrue(
        sizeBefore == 2,
        sizeAfter == 0
      )
    },
    // -----------------------------------------------------------------------------
    test("many overwrite updates") {
      for {
        lmdb    <- ZIO.service[LMDBLive]
        id      <- randomUUID
        maxValue = limit
        colName <- randomCollectionName
        col     <- lmdb.collectionCreate[Num](colName)
        _       <- ZIO.foreachDiscard(1.to(maxValue))(i => col.upsertOverwrite(id, Num(i)))
        num     <- col.fetch(id)
      } yield assertTrue(
        num.map(_.value.intValue()).contains(maxValue)
      )
    } @@ tag("slow"),
    // -----------------------------------------------------------------------------
    test("safe update in place") {
      def modifier(from: Num): Num = Num(from.value.intValue() + 1)

      for {
        id            <- randomUUID
        count          = limit
        colName       <- randomCollectionName
        col           <- LMDB.collectionCreate[Num](colName)
        shouldBeEmpty <- col.update(id, modifier)
        _             <- col.upsertOverwrite(id, Num(0))
        _             <- ZIO.foreachDiscard(1.to(count))(i => col.update(id, modifier))
        num           <- col.fetch(id)
      } yield assertTrue(
        shouldBeEmpty.isEmpty,
        num.map(_.value.intValue()).contains(count)
      )
    }, // -----------------------------------------------------------------------------
    test("safe upsert in place") {
      def modifier(from: Option[Num]): Num = from match {
        case None      => Num(1)
        case Some(num) => Num(num.value.intValue() + 1)
      }

      for {
        id      <- randomUUID
        count    = limit
        colName <- randomCollectionName
        col     <- LMDB.collectionCreate[Num](colName)
        _       <- ZIO.foreachDiscard(1.to(count))(i => col.upsert(id, modifier))
        num     <- col.fetch(id)
      } yield assertTrue(
        num.map(_.value.intValue()).contains(count)
      )
    },
    // -----------------------------------------------------------------------------
    test("many updates within multiple collection") {
      def modifier(from: Option[Num]): Num = from match {
        case None      => Num(1)
        case Some(num) => Num(num.value.intValue() + 1)
      }

      val localLimit = 10_000
      val colCount   = if (localLimit < 1000) 5 else 100
      val max        = localLimit

      for {
        id               <- randomUUID
        colName          <- randomCollectionName
        cols             <- ZIO.foreach(1.to(colCount))(i => LMDB.collectionCreate[Num](s"$colName#${i % colCount}")).map(_.toVector)
        _                <- ZIO.foreachParDiscard(1.to(max))(i => cols(i % colCount).upsert(id, modifier))
        num1             <- cols(0).fetch(id)
        num2             <- cols(1).fetch(id)
        createdDatabases <- LMDB.collectionsAvailable()
      } yield assertTrue(
        num1.map(_.value.intValue()).contains(max / colCount),
        num2.map(_.value.intValue()).contains(max / colCount),
        createdDatabases.size >= colCount
      )
    },
    // -----------------------------------------------------------------------------
    test("list collection content") {
      val count = limit
      val value = Num(42)
      for {
        colName    <- randomCollectionName
        col        <- LMDB.collectionCreate[Num](colName)
        _          <- ZIO.foreachDiscard(1.to(count))(num => col.upsertOverwrite(s"id#$num", value))
        gottenSize <- col.size()
        collected  <- col.collect()
      } yield assertTrue(
        collected.size == count,
        gottenSize == count
      )
    },
    // -----------------------------------------------------------------------------
    test("stream collection content") {
      val count = limit
      for {
        colName        <- randomCollectionName
        col            <- LMDB.collectionCreate[Num](colName)
        _              <- ZIO.foreachDiscard(1.to(count))(num => col.upsertOverwrite(s"id#$num", Num(num)))
        returnedCount1 <- col.stream().filter(_.value.intValue() % 2 == 0).runCount
        returnedCount2 <- col.streamWithKeys().filter { case (key, record) => record.value.intValue() % 2 == 0 }.runCount
      } yield assertTrue(
        returnedCount1.toInt == count / 2,
        returnedCount2.toInt == count / 2
      )
    },
    // -----------------------------------------------------------------------------
    test("moves in empty collection") {
      for {
        colName    <- randomCollectionName
        col        <- LMDB.collectionCreate[Num](colName)
        headOption <- col.head()
        lastOption <- col.last()
      } yield assertTrue(
        headOption.isEmpty,
        lastOption.isEmpty
      )
    },
    // -----------------------------------------------------------------------------
    test("going forward / backward in a collection using collect or stream") {
      for {
        colName                      <- randomCollectionName
        col                          <- LMDB.collectionCreate[Num](colName)
        keys                          = 'A'.to('Z').toList.map(_.toString)
        values                        = keys.zipWithIndex.map { case (k, v) => Num(v) }
        keyvalues                     = keys.zip(values)
        _                            <- ZIO.foreachDiscard(keyvalues) { case (k, v) => col.upsertOverwrite(k, v) }
        // ----------------------
        backwards                    <- col.collect(startAfter = None, backward = true)
        backwardsAfter               <- col.collect(startAfter = Some("D"), backward = true)
        forwards                     <- col.collect(startAfter = None, backward = false)
        forwardsAfter                <- col.collect(startAfter = Some("T"), backward = false)
        // ----------------------
        streamBackwards              <- col.stream(startAfter = None, backward = true).runCollect
        streamBackwardsAfter         <- col.stream(startAfter = Some("D"), backward = true).runCollect
        streamForwards               <- col.stream(startAfter = None, backward = false).runCollect
        streamForwardsAfter          <- col.stream(startAfter = Some("T"), backward = false).runCollect
        // ----------------------
        streamWithKeysBackwards      <- col.streamWithKeys(startAfter = None, backward = true).runCollect
        streamWithKeysBackwardsAfter <- col.streamWithKeys(startAfter = Some("D"), backward = true).runCollect
        streamWithKeysForwards       <- col.streamWithKeys(startAfter = None, backward = false).runCollect
        streamWithKeysForwardsAfter  <- col.streamWithKeys(startAfter = Some("T"), backward = false).runCollect
      } yield assertTrue(
        // ----------------------
        backwards == values.reverse,
        backwardsAfter == values.take(3).reverse,
        forwards == values,
        forwardsAfter == values.takeRight(6),
        // ----------------------
        streamBackwards.toList == values.reverse,
        streamBackwardsAfter.toList == values.take(3).reverse,
        streamForwards.toList == values,
        streamForwardsAfter.toList == values.takeRight(6),
        // ----------------------
        streamWithKeysBackwards.toList == keyvalues.reverse,
        streamWithKeysBackwardsAfter.toList == keyvalues.take(3).reverse,
        streamWithKeysForwards.toList == keyvalues,
        streamWithKeysForwardsAfter.toList == keyvalues.takeRight(6)
      )
    },
    // -----------------------------------------------------------------------------
    test("moves in collection") {
      for {
        colName <- randomCollectionName
        col     <- LMDB.collectionCreate[Num](colName)
        data     = List("bbb" -> 2, "aaa" -> 1, "ddd" -> 4, "ccc" -> 3)
        _       <- ZIO.foreachDiscard(data) { case (key, value) => col.upsertOverwrite(key, Num(value)) }
        head    <- col.head()
        last    <- col.last()
        next    <- col.next("aaa")
        prev    <- col.previous("ddd")
        noNext  <- col.next("ddd")
        noPrev  <- col.previous("aaa")
      } yield assertTrue(
        head.contains("aaa" -> Num(1)),
        last.contains("ddd" -> Num(4)),
        next.contains("bbb" -> Num(2)),
        prev.contains("ccc" -> Num(3)),
        noNext.isEmpty,
        noPrev.isEmpty
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
