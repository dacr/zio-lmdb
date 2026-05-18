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
import zio.lmdb.StorageUserError.*

object LMDBInsertSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB insert")(
    test("inserts a new record into an empty key") {
      for {
        col   <- LMDB.collectionCreate[String, Record]("insert_basic")
        rec    = Record("Ada", 36)
        _     <- col.insert("k1", rec)
        found <- col.fetch("k1")
      } yield assertTrue(found.contains(rec))
    },
    test("fails with KeyAlreadyExists when the key already exists and preserves the original value") {
      for {
        col     <- LMDB.collectionCreate[String, Record]("insert_conflict")
        original = Record("Ada", 36)
        intruder = Record("Eve", 99)
        _       <- col.insert("k1", original)
        failure <- col.insert("k1", intruder).either
        kept    <- col.fetch("k1")
      } yield assertTrue(
        failure match {
          case Left(KeyAlreadyExists("insert_conflict", "k1")) => true
          case _                                               => false
        },
        kept.contains(original)
      )
    },
    test("works inside a readWrite transaction (success and conflict)") {
      for {
        col     <- LMDB.collectionCreate[String, Record]("insert_txn")
        first    = Record("Ada", 36)
        second   = Record("Eve", 99)
        outcome <- LMDB.readWrite { ops =>
                     for {
                       _   <- ops.insert("insert_txn", "k1", first)
                       err <- ops.insert("insert_txn", "k1", second).either
                     } yield err
                   }
        kept    <- col.fetch("k1")
      } yield assertTrue(
        outcome match {
          case Left(KeyAlreadyExists("insert_txn", "k1")) => true
          case _                                          => false
        },
        kept.contains(first)
      )
    },
    test("triggers index updaters on success and not on conflict") {
      for {
        idx        <- LMDB.indexCreate[String, String]("insert_idx_by_name")
        rawCol     <- LMDB.collectionCreate[String, Record]("insert_idx_users")
        users       = rawCol.withIndex(idx)(u => Some(u.name))
        alice       = Record("Alice", 42)
        impostor    = Record("Mallory", 69)
        _          <- users.insert("u1", alice)
        afterFirst <- idx.indexed("Alice").runCollect
        failure    <- users.insert("u1", impostor).either
        afterFail  <- idx.indexed("Alice").runCollect
        malloryHit <- idx.indexed("Mallory").runCollect
      } yield assertTrue(
        afterFirst == Chunk(("Alice", "u1")),
        failure match {
          case Left(KeyAlreadyExists("insert_idx_users", "u1")) => true
          case _                                                => false
        },
        afterFail == Chunk(("Alice", "u1")),
        malloryHit.isEmpty
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
