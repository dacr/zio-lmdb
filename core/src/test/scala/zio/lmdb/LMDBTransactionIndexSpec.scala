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

object LMDBTransactionIndexSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB Index Transactions")(
    test("atomically update index and collection") {
      for {
        // Setup collection and index
        users    <- LMDB.collectionCreate[String, TxnUser]("users_idx_test")
        userIndex <- LMDB.indexCreate[String, String]("users_by_name")

        userId = "user1"
        userName = "Alice"
        
        // Initial data
        _ <- LMDB.readWrite { ops =>
          val usersTxn = users.lift(ops)
          val indexTxn = userIndex.lift(ops)
          
          for {
            _ <- usersTxn.upsertOverwrite(userId, TxnUser(userName))
            _ <- indexTxn.index(userName, userId)
          } yield ()
        }

        // Verify initial state
        alice <- users.fetch(userId)
        indexedId <- userIndex.indexed(userName).runHead

        _ <- assertTrue(
          alice.exists(_.name == userName),
          indexedId.contains((userName, userId))
        )

        // Transactional update: rename user and update index
        newUserName = "Bob"
        _ <- LMDB.readWrite { ops =>
          val usersTxn = users.lift(ops)
          val indexTxn = userIndex.lift(ops)

          for {
             _ <- usersTxn.update(userId, u => u.copy(name = newUserName))
             _ <- indexTxn.unindex(userName, userId)
             _ <- indexTxn.index(newUserName, userId)
          } yield ()
        }

        // Verify updated state
        bob <- users.fetch(userId)
        oldIndex <- userIndex.indexed(userName).runHead
        newIndex <- userIndex.indexed(newUserName).runHead

      } yield assertTrue(
        bob.exists(_.name == newUserName),
        oldIndex.isEmpty,
        newIndex.contains((newUserName, userId))
      )
    },
    test("readOnly transaction on index") {
      for {
         idx <- LMDB.indexCreate[String, String]("read_only_idx")
         _   <- idx.index("key1", "val1")
         
         res <- idx.readOnly { ops =>
           ops.contains("key1", "val1")
         }
      } yield assertTrue(res)
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
