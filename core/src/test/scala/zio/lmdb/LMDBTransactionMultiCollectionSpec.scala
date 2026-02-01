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
import zio.json.*
import zio.lmdb.json.*

case class TxnUser(name: String) derives LMDBCodecJson
case class TxnAccount(balance: Long) derives LMDBCodecJson

object LMDBTransactionMultiCollectionSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB Multi-Collection Transactions")(
    test("atomically update two collections") {
      for {
        // Setup collections
        users    <- LMDB.collectionCreate[String, TxnUser]("users")
        accounts <- LMDB.collectionCreate[String, TxnAccount]("accounts")

        userId = "user1"
        accountId = "account1"
        
        // Initial data
        _ <- users.upsertOverwrite(userId, TxnUser("Alice"))
        _ <- accounts.upsertOverwrite(accountId, TxnAccount(100))

        // Transactional update across both collections
        _ <- LMDB.readWrite { ops =>
          val usersTxn = users.lift(ops)
          val accountsTxn = accounts.lift(ops)

          for {
             _ <- usersTxn.update(userId, u => u.copy(name = "Bob"))
             _ <- accountsTxn.update(accountId, a => a.copy(balance = 200))
          } yield ()
        }

        alice <- users.fetch(userId)
        bal   <- accounts.fetch(accountId)

      } yield assertTrue(
        alice.exists(_.name == "Bob"),
        bal.exists(_.balance == 200)
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
