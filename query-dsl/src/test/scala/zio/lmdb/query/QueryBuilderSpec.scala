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

package zio.lmdb.query

import zio._
import zio.test._
import zio.test.Assertion._
import zio.lmdb._
import zio.lmdb.json.LMDBCodecJson.given
import zio.json._
import zio.lmdb.query.QueryBuilder._

object QueryBuilderSpec extends ZIOSpecDefault {

  case class User(id: String, name: String, age: Int, active: Boolean)
  object User {
    implicit val codec: JsonCodec[User] = DeriveJsonCodec.gen[User]
  }

  val users = List(
    User("1", "Alice", 25, true),
    User("2", "Bob", 30, false),
    User("3", "Charlie", 35, true),
    User("4", "Diana", 28, true),
    User("5", "Eve", 40, false)
  )

  def spec = suite("QueryBuilderSpec")(
    test("can query all records") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionCreate[String, User]("users")
        _    <- ZIO.foreachDiscard(users)(u => col.upsertOverwrite(u.id, u))

        results <- col.query.toList
      } yield assertTrue(results.size == 5)
    },
    test("can filter by value") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.whereValue(_.age > 30).toList
      } yield assertTrue(
        results.size == 2,
        results.map(_.name).toSet == Set("Charlie", "Eve")
      )
    },
    test("can filter by key") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.whereKey(k => k == "1" || k == "3").toList
      } yield assertTrue(
        results.size == 2,
        results.map(_.name).toSet == Set("Alice", "Charlie")
      )
    },
    test("can combine filters") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query
                     .whereValue(_.active)
                     .whereValue(_.age < 30)
                     .toList
      } yield assertTrue(
        results.size == 2,
        results.map(_.name).toSet == Set("Alice", "Diana")
      )
    },
    test("can limit results") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.limit(2).toList
      } yield assertTrue(results.size == 2)
    },
    test("can reverse results") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.reverse().toList
      } yield assertTrue(
        results.head.id == "5",
        results.last.id == "1"
      )
    },
    test("can stream results") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.whereValue(_.active).toStream.runCollect
      } yield assertTrue(
        results.size == 3,
        results.map(_.name).toSet == Set("Alice", "Charlie", "Diana")
      )
    },
    test("can stream with keys") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.limit(2).toStreamWithKeys.runCollect
      } yield assertTrue(
        results.size == 2,
        results.head._1 == "1",
        results.head._2.name == "Alice"
      )
    }
  ).provideShared(
    Scope.default,
    LMDB.liveWithDatabaseName("query-builder-test-db")
  ) @@ TestAspect.sequential
}
